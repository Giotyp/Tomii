/**
 * tf_pipeline — Taskflow 4-stage linear pipeline benchmark.
 *
 * Models the same fan-out/fan-in workload as the Tomii pl-bench:
 *
 *   ingest[0..N]      — N parallel tasks, each produces one f64
 *       |
 *   transform[0..N]   — N parallel tasks, 1:1 from ingest
 *       |
 *   aggregate         — 1 task, fans-in all N transform outputs
 *       |
 *   emit              — 1 task, records the mean
 *
 * Two execution modes
 * -------------------
 *   clone      (default) — S independent tf::Taskflow instances submitted
 *                          concurrently to a shared executor; batches of S
 *                          streams are drained before the next batch starts.
 *                          This is the fairest head-to-head with Tomii: both
 *                          systems run exactly S streams concurrently with
 *                          proper N-way intra-stream parallelism.
 *
 *   sequential          — Single tf::Taskflow, streams processed one at a
 *                         time.  Baseline for overhead measurement.
 *
 * CLI
 * ---
 *   --n N          items per stream        (default 256)
 *   --slots S      concurrent streams      (default 1)
 *   --streams T    total streams           (default 2000)
 *   --warmup W     warmup streams          (default 200)
 *   --workers W    executor threads        (default 4)
 *   --mode M       clone | sequential      (default clone)
 *   --output PATH  CSV output path
 *
 * CSV columns (same as Tomii):
 *   system,n,items_per_stream,slots,workers,streams,ms_per_stream
 */

#include <chrono>
#include <cmath>
#include <cstdio>
#include <numeric>
#include <string>
#include <vector>

#include "helpers.hpp"
#include <taskflow/taskflow.hpp>

// Must match TRANSFORM_ITERS in pipeline-bench/tomii/src/lib.rs.
static constexpr int TRANSFORM_ITERS = 8192;

// 2048 sin accumulations ≈ 30 µs per call on a modern x86 core.
static double heavy_transform(double x) noexcept {
    double acc = 0.0;
    for (int i = 1; i <= TRANSFORM_ITERS; ++i)
        acc += std::sin(x * static_cast<double>(i));
    return acc / static_cast<double>(TRANSFORM_ITERS);
}

// ---------------------------------------------------------------------------
// Per-stream work buffer
// ---------------------------------------------------------------------------

struct StreamBuffer
{
    std::vector<double> data;   // [0, N) — per-item working storage

    explicit StreamBuffer(int n) : data(static_cast<std::size_t>(n), 0.0) {}

    // mean is stored in data[0] after aggregate runs
    double mean() const { return data[0]; }
};

// ---------------------------------------------------------------------------
// Build one stream's 4-stage DAG into `flow`.
//
// Stage 1 (ingest):    N tasks, each writes data[i] = (i+1)/N
// Stage 2 (transform): N tasks, each applies sqrt(x) + x*0.5 in-place
// Stage 3 (aggregate): 1 task,  computes mean of data[], stores in data[0]
// Stage 4 (emit):      1 task,  no-op placeholder (mean already in data[0])
//
// The graph is fully data-parallel for stages 1-2 and serialised at the
// aggregate/emit barrier — exactly mirroring the Tomii graph topology.
// ---------------------------------------------------------------------------

static void build_stream_dag(tf::Taskflow &flow, StreamBuffer &buf, int N)
{
    double *ptr = buf.data.data();

    // Stage 1 — ingest
    std::vector<tf::Task> ingest(static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i)
    {
        ingest[static_cast<std::size_t>(i)] =
            flow.emplace([ptr, i, N]() noexcept {
                ptr[i] = static_cast<double>(i + 1) / N;
            });
    }

    // Stage 2 — transform (1:1 after ingest[i])
    std::vector<tf::Task> transform(static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i)
    {
        transform[static_cast<std::size_t>(i)] =
            flow.emplace([ptr, i]() noexcept {
                ptr[i] = heavy_transform(ptr[i]);
            });
        ingest[static_cast<std::size_t>(i)].precede(
            transform[static_cast<std::size_t>(i)]);
    }

    // Stage 3 — aggregate (fans in all N transform outputs)
    tf::Task agg = flow.emplace([ptr, N]() noexcept {
        double sum = 0.0;
        for (int i = 0; i < N; ++i)
            sum += ptr[i];
        ptr[0] = sum / N;   // store mean at index 0
    });
    for (int i = 0; i < N; ++i)
        transform[static_cast<std::size_t>(i)].precede(agg);

    // Stage 4 — emit (depends on aggregate; currently a no-op)
    tf::Task emit = flow.emplace([ptr]() noexcept {
        (void)ptr[0]; // mean is already in ptr[0]; no further work needed
    });
    agg.precede(emit);
}

// ---------------------------------------------------------------------------
// clone mode: S independent Taskflows submitted concurrently per batch
// ---------------------------------------------------------------------------

static double run_clone(
    tf::Executor &executor,
    int N,
    int S,
    int total_streams,
    int warmup)
{
    // Pre-allocate S reusable taskflows and work buffers.
    std::vector<tf::Taskflow> flows(static_cast<std::size_t>(S));
    std::vector<StreamBuffer> bufs;
    bufs.reserve(static_cast<std::size_t>(S));
    for (int s = 0; s < S; ++s)
        bufs.emplace_back(N);

    // Build the S DAGs once — they are reset and reused between batches.
    for (int s = 0; s < S; ++s)
        build_stream_dag(flows[static_cast<std::size_t>(s)],
                         bufs[static_cast<std::size_t>(s)], N);

    const int total_with_warmup = total_streams + warmup;

    // Helper: run one full batch of S streams concurrently.
    auto run_batch = [&]()
    {
        std::vector<tf::Future<void>> futs;
        futs.reserve(static_cast<std::size_t>(S));
        for (int s = 0; s < S; ++s)
            futs.push_back(executor.run(flows[static_cast<std::size_t>(s)]));
        for (auto &f : futs)
            f.get();
    };

    // Warmup: process `warmup` streams in batches of S.
    {
        int remaining = warmup;
        while (remaining > 0)
        {
            // Use a smaller batch for the last partial batch.
            int batch = std::min(remaining, S);
            if (batch < S)
            {
                // Run only `batch` flows for the partial batch.
                std::vector<tf::Future<void>> futs;
                futs.reserve(static_cast<std::size_t>(batch));
                for (int s = 0; s < batch; ++s)
                    futs.push_back(
                        executor.run(flows[static_cast<std::size_t>(s)]));
                for (auto &f : futs)
                    f.get();
            }
            else
            {
                run_batch();
            }
            remaining -= batch;
        }
    }

    // Timed run: process `total_streams` streams in batches of S.
    double total_ms = 0.0;
    {
        int remaining = total_streams;
        while (remaining > 0)
        {
            int batch = std::min(remaining, S);

            auto t0 = std::chrono::high_resolution_clock::now();
            if (batch < S)
            {
                std::vector<tf::Future<void>> futs;
                futs.reserve(static_cast<std::size_t>(batch));
                for (int s = 0; s < batch; ++s)
                    futs.push_back(
                        executor.run(flows[static_cast<std::size_t>(s)]));
                for (auto &f : futs)
                    f.get();
            }
            else
            {
                run_batch();
            }
            auto t1 = std::chrono::high_resolution_clock::now();

            double batch_ms =
                std::chrono::duration<double>(t1 - t0).count() * 1000.0;
            total_ms += batch_ms;
            remaining -= batch;
        }
    }

    return total_ms / total_streams; // ms per stream
}

// ---------------------------------------------------------------------------
// sequential mode: single Taskflow, one stream at a time
// ---------------------------------------------------------------------------

static double run_sequential(
    tf::Executor &executor,
    int N,
    int total_streams,
    int warmup)
{
    StreamBuffer buf(N);
    tf::Taskflow flow;
    build_stream_dag(flow, buf, N);

    // Warmup
    for (int i = 0; i < warmup; ++i)
        executor.run(flow).get();

    // Timed run
    double total_ms = 0.0;
    for (int i = 0; i < total_streams; ++i)
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        executor.run(flow).get();
        auto t1 = std::chrono::high_resolution_clock::now();
        total_ms +=
            std::chrono::duration<double>(t1 - t0).count() * 1000.0;
    }

    return total_ms / total_streams;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char **argv)
{
    Cli cli = parse_args(argc, argv);

    const int N       = cli.n;
    const int S       = cli.slots;
    const int T       = cli.streams;
    const int WARM    = cli.warmup;
    const int W       = cli.workers;

    std::printf(
        "Taskflow Pipeline | mode=%s | N=%d | slots=%d | workers=%d | "
        "streams=%d | warmup=%d\n",
        cli.mode.c_str(), N, S, W, T, WARM);

    tf::Executor executor(static_cast<size_t>(W));

    double ms_per_stream = 0.0;

    if (cli.mode == "clone")
        ms_per_stream = run_clone(executor, N, S, T, WARM);
    else
        ms_per_stream = run_sequential(executor, N, T, WARM);

    double throughput = 1000.0 / ms_per_stream; // streams/s

    std::printf(
        "Result | system=taskflow_%s | n=%d | slots=%d | workers=%d | "
        "streams=%d | %.6f ms/stream | %.1f streams/s\n",
        cli.mode.c_str(), N, S, W, T, ms_per_stream, throughput);

    long rss_kb = peak_rss_kb();
    std::printf("Peak RSS: %ld kB\n", rss_kb);

    std::string system_label = "taskflow_" + cli.mode;
    append_pipeline_csv(cli.output, system_label,
                        N, N, S, W, T, ms_per_stream, TRANSFORM_ITERS);

    // Append RSS to a sidecar file alongside the CSV.
    if (!cli.output.empty()) {
        std::string rss_path = cli.output + ".rss";
        std::ofstream rf(rss_path, std::ios::app);
        rf << system_label << "," << N << "," << S << "," << W << ","
           << T << "," << rss_kb << "\n";
    }

    return 0;
}
