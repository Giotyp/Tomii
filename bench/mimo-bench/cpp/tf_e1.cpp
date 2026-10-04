// tf_e1 — Taskflow formulations of the MIMO uplink for experiment E1.
//
//   --mode tf-orig   The previously published baseline (taskflow/src/main.cpp):
//                    collect the whole frame, rebuild a fresh tf::Taskflow per
//                    frame (named tasks), CSI -> serial chain of beam tasks ->
//                    demul, FFT -> demul. Kept to reproduce the old numbers.
//   --mode tf-dag    Best static-graph formulation: collect the whole frame, then
//                    run a per-slot tf::Taskflow that is built ONCE and re-run
//                    for every frame; beam tasks run in parallel.
//                    --dag tasks   one task per fft/csi/beam/demul item + gates
//                                  (per-UL-symbol FFT gates, like Tomii's graph)
//                    --dag foreach one for_each_index per stage (guided partitioner)
//   --mode tf-async  Streaming formulation (Taskflow's async API): the receive
//                    thread submits one silent_async FFT/CSI task per packet as
//                    it arrives; the last CSI task fans out the beam tasks, and
//                    as soon as a UL symbol's FFTs and all beams are done that
//                    symbol's demul tasks are fanned out (per-frame / per-symbol
//                    atomic counters implement the barriers — the same
//                    fft group_by-antenna barrier the Tomii graph uses).
//                    --chunk 1 fans out W "puller" tasks that share an atomic
//                    cursor instead of one task per item.
//
// Up to --slots frames are in flight concurrently in every mode.
#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>

#include "e1_runtime.hpp"

using namespace e1;

struct TfIface : tf::WorkerInterface {
    Runtime* rt;
    explicit TfIface(Runtime* r) : rt(r) {}
    void scheduler_prologue(tf::Worker&) override { rt->worker_entry(); }
    void scheduler_epilogue(tf::Worker&, std::exception_ptr) override {}
};

struct TfEngine : Engine {
    std::unique_ptr<tf::Executor> ex;
    void start() override {
        ex = std::make_unique<tf::Executor>(rt->args.workers, std::make_shared<TfIface>(rt));
    }
    void stop() override { ex->wait_for_all(); }
    void on_all_workers(const std::function<void(int)>& f) override {
        const int W = int(rt->args.workers);
        std::atomic<int> next{0}, arrived{0};
        for (int i = 0; i < W; ++i)
            ex->silent_async([&] {
                f(next.fetch_add(1));
                arrived.fetch_add(1);
                while (arrived.load() < W) std::this_thread::yield();
            });
        ex->wait_for_all();
    }
};

// ---------------------------------------------------------------------------
struct TfOrig : TfEngine {
    struct PerSlot {
        tf::Taskflow flow;
        std::future<void> fut;
    };
    std::vector<PerSlot> ps;
    std::string name() const override { return "tf-orig"; }
    void start() override {
        TfEngine::start();
        ps = std::vector<PerSlot>(rt->slots.size());
    }
    void on_rx_complete(Slot& s) override {
        PerSlot& p = ps[s.idx];
        tf::Taskflow& flow = p.flow;
        Runtime* r = rt;
        Slot* sp = &s;
        flow.clear();
        tf::Task csi_sync = flow.emplace([]() {}).name("csi_sync");
        tf::Task fft_sync = flow.emplace([]() {}).name("fft_sync");
        tf::Task beam_sync = flow.emplace([]() {}).name("beam_sync");
        for (uint32_t k = 0; k < r->n_fft; ++k)
            flow.emplace([r, sp, k]() { r->exec_pkt(*sp, r->ul_idx[k], false); })
                .name("fft_" + std::to_string(k))
                .precede(fft_sync);
        for (uint32_t k = 0; k < r->n_csi; ++k)
            flow.emplace([r, sp, k]() { r->exec_pkt(*sp, r->pilot_idx[k], true); })
                .name("csi_" + std::to_string(k))
                .precede(csi_sync);
        tf::Task prev = csi_sync;
        for (uint32_t b = 0; b < r->n_beam; ++b) {
            tf::Task t = flow.emplace([r, sp, b]() { r->exec_beam(*sp, b); })
                             .name("beam_" + std::to_string(b));
            prev.precede(t);
            prev = t;
        }
        prev.precede(beam_sync);
        for (uint32_t d = 0; d < r->n_demul; ++d) {
            tf::Task t = flow.emplace([r, sp, d]() { r->exec_demul(*sp, d); })
                             .name("demul_" + std::to_string(d));
            fft_sync.precede(t);
            beam_sync.precede(t);
        }
        p.fut = ex->run(flow);
    }
    bool reusable(Slot& s) override {
        auto& f = ps[s.idx].fut;
        if (!f.valid()) return true;
        if (f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) return false;
        f.get();
        return true;
    }
};

// ---------------------------------------------------------------------------
struct TfDag : TfEngine {
    struct PerSlot {
        tf::Taskflow flow;
        std::future<void> fut;
    };
    std::vector<PerSlot> ps;
    std::string name() const override { return "tf-dag"; }
    void start() override {
        TfEngine::start();
        ps = std::vector<PerSlot>(rt->slots.size());
        Runtime* r = rt;
        for (auto& sp_u : rt->slots) {
            Slot* sp = sp_u.get();
            tf::Taskflow& flow = ps[sp->idx].flow;
            if (rt->args.dag == "foreach") {
                tf::Task csi = flow.for_each_index(uint32_t(0), r->n_csi, uint32_t(1), [r, sp](uint32_t k) {
                    r->exec_pkt(*sp, r->pilot_idx[k], true);
                });
                tf::Task fft = flow.for_each_index(uint32_t(0), r->n_fft, uint32_t(1), [r, sp](uint32_t k) {
                    r->exec_pkt(*sp, r->ul_idx[k], false);
                });
                tf::Task beam = flow.for_each_index(uint32_t(0), r->n_beam, uint32_t(1),
                                                    [r, sp](uint32_t b) { r->exec_beam(*sp, b); });
                tf::Task demul = flow.for_each_index(uint32_t(0), r->n_demul, uint32_t(1),
                                                     [r, sp](uint32_t d) { r->exec_demul(*sp, d); });
                csi.precede(beam);
                beam.precede(demul);
                fft.precede(demul);
            } else {
                // Per-symbol FFT gates (== Tomii's fft.wait(group_by antennas)).
                tf::Task csi_sync = flow.emplace([]() {});
                tf::Task beam_sync = flow.emplace([]() {});
                std::vector<tf::Task> fft_sync(r->n_sym);
                for (auto& t : fft_sync) t = flow.emplace([]() {});
                for (uint32_t k = 0; k < r->n_fft; ++k)
                    flow.emplace([r, sp, k]() { r->exec_pkt(*sp, r->ul_idx[k], false); })
                        .precede(fft_sync[k / r->cfg.bs_ant_num]);
                for (uint32_t k = 0; k < r->n_csi; ++k)
                    flow.emplace([r, sp, k]() { r->exec_pkt(*sp, r->pilot_idx[k], true); }).precede(csi_sync);
                for (uint32_t b = 0; b < r->n_beam; ++b) {
                    tf::Task t = flow.emplace([r, sp, b]() { r->exec_beam(*sp, b); });
                    csi_sync.precede(t);
                    t.precede(beam_sync);
                }
                for (uint32_t d = 0; d < r->n_demul; ++d) {
                    tf::Task t = flow.emplace([r, sp, d]() { r->exec_demul(*sp, d); });
                    fft_sync[d / r->demul_events].precede(t);
                    beam_sync.precede(t);
                }
            }
        }
    }
    void on_rx_complete(Slot& s) override { ps[s.idx].fut = ex->run(ps[s.idx].flow); }
    bool reusable(Slot& s) override {
        auto& f = ps[s.idx].fut;
        if (!f.valid()) return true;
        if (f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) return false;
        f.get();
        return true;
    }
};

// ---------------------------------------------------------------------------
struct TfAsync : TfEngine {
    std::string name() const override { return "tf-async"; }
    void on_packet(Slot& s, uint32_t idx, bool pilot) override {
        Runtime* r = rt;
        Slot* sp = &s;
        ex->silent_async([r, sp, idx, pilot]() { r->task_pkt(*sp, idx, pilot); });
    }
    void beam_ready(Slot& s) override {
        Runtime* r = rt;
        Slot* sp = &s;
        if (rt->args.chunk) {
            uint32_t n = std::min<uint32_t>(r->n_beam, uint32_t(r->args.workers));
            for (uint32_t i = 0; i < n; ++i) ex->silent_async([r, sp]() { r->pull_beam(*sp); });
        } else {
            for (uint32_t b = 0; b < r->n_beam; ++b)
                ex->silent_async([r, sp, b]() { r->task_beam(*sp, b); });
        }
    }
    void demul_ready_sym(Slot& s, uint32_t sym) override {
        Runtime* r = rt;
        Slot* sp = &s;
        if (rt->args.chunk) {
            uint32_t n = std::min<uint32_t>(r->demul_events, uint32_t(r->args.workers));
            for (uint32_t i = 0; i < n; ++i) ex->silent_async([r, sp, sym]() { r->pull_demul_sym(*sp, sym); });
        } else {
            uint32_t base = sym * r->demul_events;
            for (uint32_t j = 0; j < r->demul_events; ++j)
                ex->silent_async([r, sp, base, j]() { r->task_demul(*sp, base + j); });
        }
    }
};

int main(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    Runtime rt(a);
    std::unique_ptr<Engine> eng;
    if (a.mode == "tf-orig") eng = std::make_unique<TfOrig>();
    else if (a.mode == "tf-dag") eng = std::make_unique<TfDag>();
    else if (a.mode == "tf-async") eng = std::make_unique<TfAsync>();
    else {
        std::cerr << "unknown --mode " << a.mode << "\n";
        return 2;
    }
    return rt.run(*eng);
}
