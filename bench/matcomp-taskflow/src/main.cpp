/**
 * tf_matcomp — Taskflow matrix-compute benchmark.
 *
 * Replicates the 5-stage matrix-compute DAG from examples/matrix-compute-C/
 * using the same C kernels (FFTW3 single-precision + OpenBLAS cblas_cgemm).
 *
 * DAG per stream (N items, buf_size complex elements each):
 *
 *   gen_vec[0..N]       factor=N   malloc complex_f32[buf_size] via generate_vector()
 *       |
 *   compute_fft[0..N]   factor=N   in-place FFT via fftwf_execute_dft (FFTW ESTIMATE)
 *       |
 *   vec_to_mat[0..N]    factor=N   reshape flat vector → Matrix* (malloc + memcpy)
 *       |
 *   mat_mul[0..N]       factor=N   Matrix self-product via cblas_cgemm → new Matrix*
 *       |
 *   write_res           factor=1   serialise all N matrices to file
 *
 * Per-task timing — two measurements per task, per stage:
 *
 *   kernel_us   — time from lambda start to lambda end (pure kernel body).
 *                 Comparable to Tomii's task execution time minus marshaling.
 *
 *   dispatch_us — time from predecessor's last line (predecessor end timestamp
 *                 stored in slot state) to this lambda's first line.
 *                 This is Taskflow's scheduling latency: the gap between a task
 *                 becoming eligible and a worker picking it up.
 *                 For gen_vec (no predecessor), measured from flow submit time.
 *
 *   total_us    = kernel_us + dispatch_us — full task time from eligibility to
 *                 completion. Comparable to Tomii's per-task time (which starts
 *                 at execution start and includes marshaling, not dispatch wait).
 *
 * The two measurements together answer:
 *   "What does Tomii's marshaling add compared to TF's kernel?"  (kernel column)
 *   "What does TF's scheduling dispatch add?"                     (dispatch column)
 *   "What is the net polyglot cost per task?"  (Tomii total − TF kernel, or
 *                                               equivalently marshaling − dispatch)
 *
 * Execution model: S independent tf::Taskflow clones submitted concurrently
 * to a shared tf::Executor (W workers). Batches of S streams are submitted and
 * awaited together before the next batch begins — mirrors Tomii's slot model.
 */

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <numeric>
#include <string>
#include <vector>

#include "helpers.hpp"
#include <taskflow/taskflow.hpp>

extern "C" {
#include "matcomp.h"
}

using Clock = std::chrono::high_resolution_clock;
using TP    = Clock::time_point;
using Dur   = std::chrono::duration<double, std::micro>;

// ---------------------------------------------------------------------------
// Per-slot state
// ---------------------------------------------------------------------------

struct SlotState
{
    int N;
    int buf_size;

    // Kernel output storage
    std::vector<complex_f32*> vecs;
    std::vector<void*>        mats;
    std::vector<void*>        results;

    // Predecessor end timestamps (written by each stage, read by next)
    TP                  submit_time;   // set before executor.run(); used by gen_vec dispatch
    std::vector<TP>     gv_end;        // gen_vec[i] completion time
    std::vector<TP>     fft_end;       // compute_fft[i] completion time
    std::vector<TP>     vtm_end;       // vec_to_mat[i] completion time
    std::vector<TP>     mm_end;        // mat_mul[i] completion time

    // Kernel body time (lambda start → lambda end), per stage
    std::vector<double> gen_vec_us;
    std::vector<double> fft_us;
    std::vector<double> vec_to_mat_us;
    std::vector<double> mat_mul_us;

    // Dispatch latency (predecessor end → lambda start), per stage
    std::vector<double> gv_disp_us;    // submit_time → gen_vec lambda start
    std::vector<double> fft_disp_us;   // gv_end → fft lambda start
    std::vector<double> vtm_disp_us;   // fft_end → vtm lambda start
    std::vector<double> mm_disp_us;    // vtm_end → mm lambda start

    // write_res (single value per stream)
    double write_res_us = 0.0;

    explicit SlotState(int n, int buf)
        : N(n)
        , buf_size(buf)
        , vecs(static_cast<size_t>(n), nullptr)
        , mats(static_cast<size_t>(n), nullptr)
        , results(static_cast<size_t>(n), nullptr)
        , gv_end(static_cast<size_t>(n))
        , fft_end(static_cast<size_t>(n))
        , vtm_end(static_cast<size_t>(n))
        , mm_end(static_cast<size_t>(n))
        , gen_vec_us(static_cast<size_t>(n), 0.0)
        , fft_us(static_cast<size_t>(n), 0.0)
        , vec_to_mat_us(static_cast<size_t>(n), 0.0)
        , mat_mul_us(static_cast<size_t>(n), 0.0)
        , gv_disp_us(static_cast<size_t>(n), 0.0)
        , fft_disp_us(static_cast<size_t>(n), 0.0)
        , vtm_disp_us(static_cast<size_t>(n), 0.0)
        , mm_disp_us(static_cast<size_t>(n), 0.0)
    {}
};

// ---------------------------------------------------------------------------
// Build one stream's DAG into `flow`.
// ---------------------------------------------------------------------------

static void build_stream_dag(
    tf::Taskflow      &flow,
    SlotState         &s,
    void*              fft_plan,
    const std::string &result_file,
    std::mutex        &write_mutex)
{
    const int N      = s.N;
    const int buf_sz = s.buf_size;

    std::vector<tf::Task> gv(static_cast<size_t>(N));
    std::vector<tf::Task> fft(static_cast<size_t>(N));
    std::vector<tf::Task> vtm(static_cast<size_t>(N));
    std::vector<tf::Task> mm(static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        auto ii = static_cast<size_t>(i);

        // gen_vec[i]: dispatch latency from flow submit time
        gv[ii] = flow.emplace([&s, ii, buf_sz]() noexcept {
            auto t_start = Clock::now();
            s.gv_disp_us[ii]  = Dur(t_start - s.submit_time).count();
            s.vecs[ii]        = generate_vector(static_cast<size_t>(buf_sz));
            s.gv_end[ii]      = Clock::now();
            s.gen_vec_us[ii]  = Dur(s.gv_end[ii] - t_start).count();
        });

        // compute_fft[i]: dispatch latency from gv_end[i]
        fft[ii] = flow.emplace([&s, ii, fft_plan, buf_sz]() noexcept {
            auto t_start      = Clock::now();
            s.fft_disp_us[ii] = Dur(t_start - s.gv_end[ii]).count();
            compute_fft(fft_plan, s.vecs[ii], static_cast<size_t>(buf_sz));
            s.fft_end[ii]     = Clock::now();
            s.fft_us[ii]      = Dur(s.fft_end[ii] - t_start).count();
        });

        // vec_to_mat[i]: dispatch latency from fft_end[i]
        vtm[ii] = flow.emplace([&s, ii, buf_sz]() noexcept {
            auto t_start         = Clock::now();
            s.vtm_disp_us[ii]   = Dur(t_start - s.fft_end[ii]).count();
            s.mats[ii]           = vec_to_mat(s.vecs[ii], static_cast<size_t>(buf_sz));
            s.vtm_end[ii]        = Clock::now();
            s.vec_to_mat_us[ii]  = Dur(s.vtm_end[ii] - t_start).count();
            free_vector(static_cast<void*>(s.vecs[ii]));
            s.vecs[ii] = nullptr;
        });

        // mat_mul[i]: dispatch latency from vtm_end[i]
        mm[ii] = flow.emplace([&s, ii]() noexcept {
            auto t_start        = Clock::now();
            s.mm_disp_us[ii]   = Dur(t_start - s.vtm_end[ii]).count();
            s.results[ii]       = mat_mul(s.mats[ii], s.mats[ii]);
            s.mm_end[ii]        = Clock::now();
            s.mat_mul_us[ii]    = Dur(s.mm_end[ii] - t_start).count();
            free_matrix(s.mats[ii]);
            s.mats[ii] = nullptr;
        });

        gv[ii].precede(fft[ii]);
        fft[ii].precede(vtm[ii]);
        vtm[ii].precede(mm[ii]);
    }

    // write_res — fans in all N mat_mul outputs
    tf::Task wr = flow.emplace([&s, &result_file, &write_mutex, N]() noexcept {
        auto t0 = Clock::now();
        {
            std::lock_guard<std::mutex> lk(write_mutex);
            write_to_file(result_file.c_str(),
                          s.results.data(),
                          static_cast<size_t>(N));
        }
        s.write_res_us = Dur(Clock::now() - t0).count();
        for (auto &r : s.results) {
            free_matrix(r);
            r = nullptr;
        }
    });

    for (int i = 0; i < N; ++i)
        mm[static_cast<size_t>(i)].precede(wr);
}

// ---------------------------------------------------------------------------
// Aggregate results
// ---------------------------------------------------------------------------

struct Results
{
    double ms_per_stream;

    // Kernel body times (µs/invocation)
    double gen_vec_kernel_us;
    double fft_kernel_us;
    double vtm_kernel_us;
    double mm_kernel_us;

    // Dispatch latencies (µs/invocation): predecessor end → lambda start
    double gen_vec_disp_us;
    double fft_disp_us;
    double vtm_disp_us;
    double mm_disp_us;

    double write_res_us;
};

// ---------------------------------------------------------------------------
// run_clone
// ---------------------------------------------------------------------------

static Results run_clone(
    tf::Executor      &executor,
    const Cli         &cli,
    void*              fft_plan,
    const std::string &result_file)
{
    const int N    = cli.n;
    const int S    = cli.slots;
    const int T    = cli.streams;
    const int WARM = cli.warmup;

    std::mutex write_mutex;

    std::vector<SlotState>   slots;
    slots.reserve(static_cast<size_t>(S));
    for (int s = 0; s < S; ++s)
        slots.emplace_back(N, cli.buf_size);

    std::vector<tf::Taskflow> flows(static_cast<size_t>(S));
    for (int s = 0; s < S; ++s)
        build_stream_dag(flows[static_cast<size_t>(s)],
                         slots[static_cast<size_t>(s)],
                         fft_plan, result_file, write_mutex);

    // Accumulators
    double total_ms        = 0.0;
    double sum_gv_kern     = 0.0, sum_gv_disp     = 0.0;
    double sum_fft_kern    = 0.0, sum_fft_disp    = 0.0;
    double sum_vtm_kern    = 0.0, sum_vtm_disp    = 0.0;
    double sum_mm_kern     = 0.0, sum_mm_disp     = 0.0;
    double sum_wr_us       = 0.0;
    int    measured_count  = 0;

    auto run_batch = [&](int batch_sz, bool record) {
        std::vector<tf::Future<void>> futs;
        futs.reserve(static_cast<size_t>(batch_sz));

        // Stamp submit time into each slot just before handing to executor.
        auto submit_tp = Clock::now();
        for (int s = 0; s < batch_sz; ++s)
            slots[static_cast<size_t>(s)].submit_time = submit_tp;

        auto wall_t0 = Clock::now();
        for (int s = 0; s < batch_sz; ++s)
            futs.push_back(executor.run(flows[static_cast<size_t>(s)]));
        for (auto &f : futs)
            f.get();
        double batch_ms =
            std::chrono::duration<double>(Clock::now() - wall_t0).count() * 1000.0;

        if (record) {
            total_ms += batch_ms;
            for (int s = 0; s < batch_sz; ++s) {
                auto &sl = slots[static_cast<size_t>(s)];
                sum_gv_kern  += std::accumulate(sl.gen_vec_us.begin(),   sl.gen_vec_us.end(),   0.0);
                sum_fft_kern += std::accumulate(sl.fft_us.begin(),       sl.fft_us.end(),       0.0);
                sum_vtm_kern += std::accumulate(sl.vec_to_mat_us.begin(),sl.vec_to_mat_us.end(),0.0);
                sum_mm_kern  += std::accumulate(sl.mat_mul_us.begin(),   sl.mat_mul_us.end(),   0.0);
                sum_gv_disp  += std::accumulate(sl.gv_disp_us.begin(),   sl.gv_disp_us.end(),   0.0);
                sum_fft_disp += std::accumulate(sl.fft_disp_us.begin(),  sl.fft_disp_us.end(),  0.0);
                sum_vtm_disp += std::accumulate(sl.vtm_disp_us.begin(),  sl.vtm_disp_us.end(),  0.0);
                sum_mm_disp  += std::accumulate(sl.mm_disp_us.begin(),   sl.mm_disp_us.end(),   0.0);
                sum_wr_us    += sl.write_res_us;
            }
            measured_count += batch_sz;
        }
    };

    // Warmup
    for (int remaining = WARM; remaining > 0; ) {
        int batch = std::min(remaining, S);
        run_batch(batch, false);
        remaining -= batch;
    }

    // Measured
    for (int remaining = T; remaining > 0; ) {
        int batch = std::min(remaining, S);
        run_batch(batch, true);
        remaining -= batch;
    }

    const double inv_tasks   = 1.0 / (static_cast<double>(measured_count) * N);
    const double inv_streams = 1.0 / static_cast<double>(measured_count);

    Results r;
    r.ms_per_stream      = total_ms * inv_streams;
    r.gen_vec_kernel_us  = sum_gv_kern  * inv_tasks;
    r.fft_kernel_us      = sum_fft_kern * inv_tasks;
    r.vtm_kernel_us      = sum_vtm_kern * inv_tasks;
    r.mm_kernel_us       = sum_mm_kern  * inv_tasks;
    r.gen_vec_disp_us    = sum_gv_disp  * inv_tasks;
    r.fft_disp_us        = sum_fft_disp * inv_tasks;
    r.vtm_disp_us        = sum_vtm_disp * inv_tasks;
    r.mm_disp_us         = sum_mm_disp  * inv_tasks;
    r.write_res_us       = sum_wr_us    * inv_streams;
    return r;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char **argv)
{
    Cli cli;
    try {
        cli = parse_args(argc, argv);
    } catch (const std::exception &e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }

    std::printf(
        "tf_matcomp | N=%d | buf=%d | slots=%d | workers=%d | streams=%d | warmup=%d\n",
        cli.n, cli.buf_size, cli.slots, cli.workers, cli.streams, cli.warmup);

    void *fft_plan = fft_planner(static_cast<size_t>(cli.buf_size));
    if (!fft_plan) { std::fprintf(stderr, "fft_planner returned NULL\n"); return 1; }

    const char *out_dir = std::getenv("MATCOMP_OUT_DIR");
    if (!out_dir) out_dir = "/tmp";
    std::string result_file = std::string(out_dir) + "/tf_matcomp_result.txt";
    { FILE *f = std::fopen(result_file.c_str(), "w"); if (f) std::fclose(f); }

    tf::Executor executor(static_cast<size_t>(cli.workers));
    if (cli.pin) pin_workers(executor, cli.pin_core);

    Results r = run_clone(executor, cli, fft_plan, result_file);

    // Derived: total_us = kernel + dispatch per stage × N
    double kern_only_us =
        (r.gen_vec_kernel_us + r.fft_kernel_us + r.vtm_kernel_us + r.mm_kernel_us)
        * cli.n;
    double disp_only_us =
        (r.gen_vec_disp_us + r.fft_disp_us + r.vtm_disp_us + r.mm_disp_us)
        * cli.n;
    double total_task_us = kern_only_us + disp_only_us;

    std::printf(
        "\n"
        "=== Per-task breakdown (µs/invocation, W=%d S=%d N=%d, %d streams) ===\n"
        "              kernel    dispatch    total\n"
        "  gen_vec:   %7.2f    %7.2f    %7.2f\n"
        "  fft:       %7.2f    %7.2f    %7.2f\n"
        "  vec_to_mat:%7.2f    %7.2f    %7.2f\n"
        "  mat_mul:   %7.2f    %7.2f    %7.2f\n"
        "  write_res: %7.0f    (I/O; mutex-serialised across %d clones)\n"
        "\n"
        "=== Per-stream compute totals (kernel+dispatch) × N=%d ===\n"
        "  Kernel only:       %6.0f µs\n"
        "  TF dispatch only:  %6.0f µs\n"
        "  Kernel + dispatch: %6.0f µs\n"
        "\n"
        "  Wall-clock per stream: %.3f ms  (incl. write_res)\n"
        "  Throughput:            %.1f streams/s\n",
        cli.workers, cli.slots, cli.n, cli.streams,
        r.gen_vec_kernel_us, r.gen_vec_disp_us, r.gen_vec_kernel_us + r.gen_vec_disp_us,
        r.fft_kernel_us,     r.fft_disp_us,     r.fft_kernel_us     + r.fft_disp_us,
        r.vtm_kernel_us,     r.vtm_disp_us,     r.vtm_kernel_us     + r.vtm_disp_us,
        r.mm_kernel_us,      r.mm_disp_us,       r.mm_kernel_us      + r.mm_disp_us,
        r.write_res_us,      cli.slots,
        cli.n,
        kern_only_us, disp_only_us, total_task_us,
        r.ms_per_stream, 1000.0 / r.ms_per_stream);

    std::printf("Peak RSS: %ld kB\n", peak_rss_kb());

    if (!cli.output.empty()) {
        append_matcomp_csv(
            cli.output, cli.n, cli.buf_size, cli.slots, cli.workers,
            cli.streams, r.ms_per_stream,
            r.gen_vec_kernel_us, r.fft_kernel_us, r.vtm_kernel_us, r.mm_kernel_us,
            r.gen_vec_disp_us,   r.fft_disp_us,   r.vtm_disp_us,  r.mm_disp_us);
        std::printf("CSV: %s\n", cli.output.c_str());
    }

    return 0;
}
