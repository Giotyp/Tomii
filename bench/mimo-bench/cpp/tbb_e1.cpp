// tbb_e1 — oneTBB flow_graph formulations of the MIMO uplink for experiment E1.
//
//   --mode tbb-flow  Streaming flow graph with per-packet message injection.
//                    The receive thread try_put()s every packet into `pkt`
//                    (multifunction_node, one body = FFT or CSI). Edges:
//                      pkt --(last CSI of frame)--> beam_fan --> beam
//                      pkt --(last FFT of UL symbol k)--> gate.in0 (tag frame,k)
//                      beam --(last beam of frame)--> gate.in1 x n_ul_symbols
//                      gate = join_node<tuple<Tok,Tok>, tag_matching>
//                      gate --> demul_fan (symbol k's demul tasks) --> demul
//                    N-to-1 completion inside a stage uses per-frame atomic
//                    counters in the node bodies (flow_graph has no dynamic
//                    counted barrier); frames overlap freely up to --slots.
//                    --conc N  concurrency limit of the work nodes (0 = unlimited)
//                    --prio 1  beam path at higher node priority than demul
//                    --chunk 1 fan nodes emit W "puller" messages instead of one per item
//   --mode tbb-dag   Collect-then-submit: per slot a static flow graph of
//                    continue_nodes (built once) is triggered when the whole
//                    frame has arrived.
//                    --dag tasks   one continue_node per item + sync nodes
//                                  (per-UL-symbol FFT sync, like Tomii's graph)
//                    --dag foreach one continue_node per stage running parallel_for
//
// Threads: a task_arena of W worker slots (global_control caps the process at
// W+1 so exactly W TBB workers exist); the receive thread never joins the arena.
#include <oneapi/tbb/flow_graph.h>
#include <oneapi/tbb/global_control.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/task_arena.h>
#include <oneapi/tbb/task_scheduler_observer.h>
#include <oneapi/tbb/version.h>

#include "e1_runtime.hpp"

using namespace e1;
namespace flow = oneapi::tbb::flow;

struct PinObserver : oneapi::tbb::task_scheduler_observer {
    Runtime* rt;
    PinObserver(oneapi::tbb::task_arena& a, Runtime* r) : task_scheduler_observer(a), rt(r) {
        observe(true);
    }
    void on_scheduler_entry(bool is_worker) override {
        if (is_worker) rt->worker_entry();
    }
};

struct TbbEngine : Engine {
    std::unique_ptr<oneapi::tbb::global_control> gc;
    std::unique_ptr<oneapi::tbb::task_arena> arena;
    std::unique_ptr<PinObserver> obs;
    std::unique_ptr<flow::graph> g;
    void start_arena() {
        gc = std::make_unique<oneapi::tbb::global_control>(
            oneapi::tbb::global_control::max_allowed_parallelism, rt->args.workers + 1);
        arena = std::make_unique<oneapi::tbb::task_arena>(int(rt->args.workers), 0);
        arena->initialize();
        obs = std::make_unique<PinObserver>(*arena, rt);
    }
    size_t conc() const {
        return rt->args.conc == 0 ? size_t(flow::unlimited) : rt->args.conc;
    }
    void stop() override {
        arena->execute([&] { g->wait_for_all(); });
    }
    void on_all_workers(const std::function<void(int)>& f) override {
        const int W = int(rt->args.workers);
        std::atomic<int> next{0}, arrived{0}, left{0};
        for (int i = 0; i < W; ++i)
            arena->enqueue([&] {
                f(next.fetch_add(1));
                arrived.fetch_add(1);
                while (arrived.load() < W) std::this_thread::yield();
                left.fetch_add(1);
            });
        while (left.load() < W) std::this_thread::yield();
    }
};

// ---------------------------------------------------------------------------
struct PMsg { Slot* s; uint32_t i; uint8_t pilot; };
struct Tok { Slot* s; uint64_t frame; uint32_t sym; };
inline flow::tag_value tok_tag(const Tok& t) { return flow::tag_value(t.frame * MAXSYM + t.sym); }
struct BMsg { Slot* s; uint32_t i; };

struct TbbFlow : TbbEngine {
    using PktNode = flow::multifunction_node<PMsg, std::tuple<Tok, Tok>>;
    using FanNode = flow::multifunction_node<Tok, std::tuple<BMsg>>;
    using BeamNode = flow::multifunction_node<BMsg, std::tuple<Tok>>;
    using Gate = flow::join_node<std::tuple<Tok, Tok>, flow::tag_matching>;
    using DFanNode = flow::multifunction_node<std::tuple<Tok, Tok>, std::tuple<BMsg>>;
    using DemulNode = flow::function_node<BMsg, flow::continue_msg>;
    std::unique_ptr<PktNode> pkt;
    std::unique_ptr<FanNode> beam_fan;
    std::unique_ptr<BeamNode> beam;
    std::unique_ptr<Gate> gate;
    std::unique_ptr<DFanNode> demul_fan;
    std::unique_ptr<DemulNode> demul;

    std::string name() const override { return "tbb-flow"; }
    void start() override {
        start_arena();
        Runtime* r = rt;
        const bool chunk = rt->args.chunk != 0;
        const uint32_t W = uint32_t(rt->args.workers);
        flow::node_priority_t hi = rt->args.prio ? 1 : flow::no_priority;
        arena->execute([&] {
            g = std::make_unique<flow::graph>();
            pkt = std::make_unique<PktNode>(
                *g, conc(),
                [r](const PMsg& m, PktNode::output_ports_type& op) {
                    Runtime::PktRes e = r->exec_pkt(*m.s, m.i, m.pilot);
                    uint64_t f = uint64_t(m.s->frame);
                    if (e.csi_last) std::get<0>(op).try_put(Tok{m.s, f, 0});
                    else if (e.fft_sym_done) std::get<1>(op).try_put(Tok{m.s, f, e.sym});
                },
                flow::queueing(), hi);
            beam_fan = std::make_unique<FanNode>(
                *g, flow::unlimited,
                [r, chunk, W](const Tok& t, FanNode::output_ports_type& op) {
                    uint32_t n = chunk ? std::min(r->n_beam, W) : r->n_beam;
                    for (uint32_t b = 0; b < n; ++b) std::get<0>(op).try_put(BMsg{t.s, b});
                },
                flow::queueing(), hi);
            beam = std::make_unique<BeamNode>(
                *g, conc(),
                [r, chunk](const BMsg& m, BeamNode::output_ports_type& op) {
                    bool last = chunk ? r->pull_beam_last(*m.s) : r->exec_beam(*m.s, m.i);
                    if (last)  // beams gate every UL symbol's demul
                        for (uint32_t k = 0; k < r->n_sym; ++k)
                            std::get<0>(op).try_put(Tok{m.s, uint64_t(m.s->frame), k});
                },
                flow::queueing(), hi);
            // Per-(frame, UL symbol) gate: symbol's FFTs done AND all beams done.
            gate = std::make_unique<Gate>(*g, tok_tag, tok_tag);
            demul_fan = std::make_unique<DFanNode>(
                *g, flow::unlimited,
                [r, chunk, W](const std::tuple<Tok, Tok>& t, DFanNode::output_ports_type& op) {
                    const Tok& k = std::get<0>(t);
                    if (chunk) {
                        uint32_t n = std::min(r->demul_events, W);
                        for (uint32_t i = 0; i < n; ++i) std::get<0>(op).try_put(BMsg{k.s, k.sym});
                    } else {
                        for (uint32_t j = 0; j < r->demul_events; ++j)
                            std::get<0>(op).try_put(BMsg{k.s, k.sym * r->demul_events + j});
                    }
                });
            demul = std::make_unique<DemulNode>(*g, conc(), [r, chunk](const BMsg& m) {
                if (chunk) r->pull_demul_sym(*m.s, m.i);  // m.i = symbol
                else r->exec_demul(*m.s, m.i);
                return flow::continue_msg();
            });
            flow::make_edge(flow::output_port<0>(*pkt), *beam_fan);
            flow::make_edge(flow::output_port<1>(*pkt), flow::input_port<0>(*gate));
            flow::make_edge(flow::output_port<0>(*beam_fan), *beam);
            flow::make_edge(flow::output_port<0>(*beam), flow::input_port<1>(*gate));
            flow::make_edge(*gate, *demul_fan);
            flow::make_edge(flow::output_port<0>(*demul_fan), *demul);
        });
    }
    void on_packet(Slot& s, uint32_t idx, bool pilot) override {
        pkt->try_put(PMsg{&s, idx, uint8_t(pilot)});
    }
};

// ---------------------------------------------------------------------------
struct TbbDag : TbbEngine {
    using CN = flow::continue_node<flow::continue_msg>;
    struct PerSlot {
        std::unique_ptr<flow::broadcast_node<flow::continue_msg>> start;
        std::vector<std::unique_ptr<CN>> nodes;
    };
    std::vector<PerSlot> ps;
    std::string name() const override { return "tbb-dag"; }
    void start() override {
        start_arena();
        Runtime* r = rt;
        ps.resize(rt->slots.size());
        arena->execute([&] {
            g = std::make_unique<flow::graph>();
            for (auto& sp_u : rt->slots) {
                Slot* sp = sp_u.get();
                PerSlot& p = ps[sp->idx];
                p.start = std::make_unique<flow::broadcast_node<flow::continue_msg>>(*g);
                auto mk = [&](auto body) {
                    p.nodes.push_back(std::make_unique<CN>(*g, [body](const flow::continue_msg&) {
                        body();
                        return flow::continue_msg();
                    }));
                    return p.nodes.back().get();
                };
                if (rt->args.dag == "foreach") {
                    CN* csi = mk([r, sp] {
                        oneapi::tbb::parallel_for(uint32_t(0), r->n_csi, [r, sp](uint32_t k) {
                            r->exec_pkt(*sp, r->pilot_idx[k], true);
                        });
                    });
                    CN* fft = mk([r, sp] {
                        oneapi::tbb::parallel_for(uint32_t(0), r->n_fft, [r, sp](uint32_t k) {
                            r->exec_pkt(*sp, r->ul_idx[k], false);
                        });
                    });
                    CN* beam = mk([r, sp] {
                        oneapi::tbb::parallel_for(uint32_t(0), r->n_beam,
                                                  [r, sp](uint32_t b) { r->exec_beam(*sp, b); });
                    });
                    CN* demul = mk([r, sp] {
                        oneapi::tbb::parallel_for(uint32_t(0), r->n_demul,
                                                  [r, sp](uint32_t d) { r->exec_demul(*sp, d); });
                    });
                    flow::make_edge(*p.start, *csi);
                    flow::make_edge(*p.start, *fft);
                    flow::make_edge(*csi, *beam);
                    flow::make_edge(*beam, *demul);
                    flow::make_edge(*fft, *demul);
                } else {
                    CN* csi_sync = mk([] {});
                    CN* beam_sync = mk([] {});
                    std::vector<CN*> fft_sync(r->n_sym);
                    for (auto& n : fft_sync) n = mk([] {});
                    for (uint32_t k = 0; k < r->n_csi; ++k) {
                        CN* n = mk([r, sp, k] { r->exec_pkt(*sp, r->pilot_idx[k], true); });
                        flow::make_edge(*p.start, *n);
                        flow::make_edge(*n, *csi_sync);
                    }
                    for (uint32_t k = 0; k < r->n_fft; ++k) {
                        CN* n = mk([r, sp, k] { r->exec_pkt(*sp, r->ul_idx[k], false); });
                        flow::make_edge(*p.start, *n);
                        flow::make_edge(*n, *fft_sync[k / r->cfg.bs_ant_num]);
                    }
                    for (uint32_t b = 0; b < r->n_beam; ++b) {
                        CN* n = mk([r, sp, b] { r->exec_beam(*sp, b); });
                        flow::make_edge(*csi_sync, *n);
                        flow::make_edge(*n, *beam_sync);
                    }
                    for (uint32_t d = 0; d < r->n_demul; ++d) {
                        CN* n = mk([r, sp, d] { r->exec_demul(*sp, d); });
                        flow::make_edge(*fft_sync[d / r->demul_events], *n);
                        flow::make_edge(*beam_sync, *n);
                    }
                }
            }
        });
    }
    void on_rx_complete(Slot& s) override { ps[s.idx].start->try_put(flow::continue_msg()); }
};

int main(int argc, char** argv) {
    Args a = Args::parse(argc, argv);
    Runtime rt(a);
    std::cout << "oneTBB " << TBB_VERSION_STRING << " runtime " << TBB_runtime_version() << "\n";
    std::unique_ptr<Engine> eng;
    if (a.mode == "tbb-flow") eng = std::make_unique<TbbFlow>();
    else if (a.mode == "tbb-dag") eng = std::make_unique<TbbDag>();
    else {
        std::cerr << "unknown --mode " << a.mode << "\n";
        return 2;
    }
    return rt.run(*eng);
}
