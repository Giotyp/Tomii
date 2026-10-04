// e1_runtime.hpp — shared receive / slot / measurement / verification layer for
// the C++ MIMO baselines (Taskflow, oneTBB). Only the scheduling of the four
// kernels differs between baselines; everything a measurement depends on is
// here and identical for all of them:
//
//   * UDP ingest: one socket per antenna (port = bs_server_port + ant), one
//     dedicated receive thread, epoll busy-poll + recvmmsg straight into pooled
//     packet buffers (no copy). Same wire format/ports as Tomii.
//   * Slots: S concurrent frames; frame f lives in slot f % S. Packets of a
//     frame whose slot is still busy are PARKED and replayed when the slot
//     frees. Parking is bounded exactly like Tomii's (tomii-core
//     runtime/packet_processing.rs park_pending_packet): at most ppf x S parked
//     packets; on overflow the frame furthest from admission (highest frame id,
//     the incoming one included) is dropped whole and its later packets are
//     discarded. So every system applies the same admission/drop policy.
//   * Probe: per-frame first_rx / last_rx / done stamps on CLOCK_MONOTONIC
//     (same clock as the Tomii plugin probe and the Agora sender's
//     tx_result.txt), FFT/CSI tasks overlapped with arrival, and a byte-exact
//     comparison of every frame's demod output against a golden file.
//   * Output: identical record format to tomii/src/e1probe.rs.
#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <functional>
#include <vector>

#include "kernels.hpp"

namespace e1 {

inline uint64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return uint64_t(ts.tv_sec) * 1000000000ull + uint64_t(ts.tv_nsec);
}

inline void pin_self(int core) {
    if (core < 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

// ---------------------------------------------------------------------------
// Arguments (common + engine knobs)
// ---------------------------------------------------------------------------
struct Args {
    std::string mode;
    size_t slots = 1, workers = 4, frames = 500;
    std::string config, out, golden, dump_golden;
    size_t golden_frame = 10;
    bool pin = true;
    int core_base = 1;           // rx thread core; workers on core_base+1 ..
    uint64_t stall_ms = 3000;
    // engine knobs
    int chunk = 0;               // streaming fan-out: 0 = one task per item, 1 = W pullers
    int prio = 0;                // tbb: node priorities (beam high)
    size_t conc = 0;             // tbb: function_node concurrency (0 = unlimited)
    std::string dag = "tasks";   // tf-dag/tbb-dag: "tasks" (one node per item) or "foreach"
    std::map<std::string, std::string> raw;

    static Args parse(int argc, char** argv) {
        Args a;
        for (int i = 1; i < argc; ++i) {
            std::string k = argv[i];
            if (k.rfind("--", 0) != 0 || i + 1 >= argc) {
                std::cerr << "bad arg " << k << "\n";
                std::exit(2);
            }
            std::string v = argv[++i];
            a.raw[k.substr(2)] = v;
            if (k == "--mode") a.mode = v;
            else if (k == "--slots") a.slots = std::stoul(v);
            else if (k == "--workers") a.workers = std::stoul(v);
            else if (k == "--frames") a.frames = std::stoul(v);
            else if (k == "--config") a.config = v;
            else if (k == "--out") a.out = v;
            else if (k == "--golden") a.golden = v;
            else if (k == "--dump-golden") a.dump_golden = v;
            else if (k == "--golden-frame") a.golden_frame = std::stoul(v);
            else if (k == "--pin") a.pin = std::stoi(v) != 0;
            else if (k == "--core-base") a.core_base = std::stoi(v);
            else if (k == "--stall-ms") a.stall_ms = std::stoull(v);
            else if (k == "--chunk") a.chunk = std::stoi(v);
            else if (k == "--prio") a.prio = std::stoi(v);
            else if (k == "--conc") a.conc = std::stoul(v);
            else if (k == "--dag") a.dag = v;
            else {
                std::cerr << "unknown arg " << k << "\n";
                std::exit(2);
            }
        }
        if (a.config.empty() || a.out.empty()) {
            std::cerr << "--config and --out are required\n";
            std::exit(2);
        }
        return a;
    }
    std::string knobs_json() const {
        std::ostringstream o;
        o << "{";
        bool first = true;
        for (auto& [k, v] : raw) {
            if (k == "out" || k == "golden" || k == "dump-golden" || k == "config") continue;
            o << (first ? "" : ",") << "\"" << k << "\":\"" << v << "\"";
            first = false;
        }
        o << "}";
        return o.str();
    }
};

// ---------------------------------------------------------------------------
// Per-frame probe record (same fields as tomii/src/e1probe.rs)
// ---------------------------------------------------------------------------
static constexpr size_t MAXF = 1 << 16;
struct Rec {
    std::atomic<uint64_t> first_rx{0}, last_rx{0}, done{0};
    std::atomic<uint32_t> rx{0}, overlap{0}, verify{0}, depviol{0};
};
static constexpr size_t MAXSYM = 32;

enum SlotStateE : int { FREE = 0, BUSY = 1, DONE = 2 };

struct alignas(64) Slot {
    size_t idx = 0;
    SlotBuffers buf;
    std::vector<uint8_t*> pkt;           // [ppf] packet buffers of the current frame
    int64_t frame = -1;                   // frame id owned (rx thread writes before BUSY)
    uint32_t rx = 0;                      // rx thread only
    alignas(64) std::atomic<uint32_t> csi_done{0};
    alignas(64) std::atomic<uint32_t> fft_done{0};
    alignas(64) std::atomic<uint32_t> beam_done{0};
    alignas(64) std::atomic<uint32_t> demul_done{0};
    alignas(64) std::atomic<uint32_t> gate{0};
    alignas(64) std::atomic<uint32_t> beam_next{0};
    alignas(64) std::atomic<uint32_t> demul_next{0};
    alignas(64) std::atomic<int> state{FREE};
    alignas(64) std::atomic<uint32_t> fft_sym[MAXSYM];   // FFTs done per UL symbol
    alignas(64) std::atomic<uint32_t> sym_gate[MAXSYM];  // per-symbol demul gate (fft sym + beam)
    alignas(64) std::atomic<uint32_t> dsym_next[MAXSYM]; // per-symbol demul puller cursor
    void* eng = nullptr;                  // engine-private per-slot state
};

struct Runtime;

// Scheduling policy plug-in. Called on the rx thread unless noted.
struct Engine {
    Runtime* rt = nullptr;
    virtual ~Engine() = default;
    virtual std::string name() const = 0;
    virtual void start() {}
    virtual void on_admit(Slot&) {}
    virtual void on_packet(Slot&, uint32_t /*idx*/, bool /*pilot*/) {}
    virtual void on_rx_complete(Slot&) {}
    virtual void beam_ready(Slot&) {}   // worker thread: last CSI of the frame finished
    virtual void demul_ready_sym(Slot&, uint32_t /*sym*/) {}  // worker: symbol's FFTs + all beams done
    virtual bool reusable(Slot&) { return true; }
    virtual void stop() {}
    // Run f(k), k = 0..W-1, once on each of the W worker threads (spin barrier
    // inside, so no worker can take two). Used only for pre-arrival warm-up.
    virtual void on_all_workers(const std::function<void(int)>& f) = 0;
};

struct Runtime {
    Args args;
    Config cfg;
    Engine* eng = nullptr;
    size_t ppf = 0, pkt_len = 0, pkt_alloc = 0;
    uint32_t n_csi = 0, n_fft = 0, n_beam = 0, n_demul = 0, demul_events = 0;
    std::vector<uint8_t> is_pilot;        // [schedule_len]
    std::vector<uint32_t> pilot_idx, ul_idx;  // packet indices of csi / fft tasks
    std::vector<std::unique_ptr<Slot>> slots;
    std::unique_ptr<Rec[]> recs;
    std::vector<uint8_t> golden;
    size_t n_sym = 0, n_ss = 0, cell_len = 0;
    std::atomic<size_t> done_count{0}, verify_fail{0};
    std::atomic<uint64_t> last_activity{0};
    std::atomic<int> worker_seq{0};
    // rx-thread-only state
    std::vector<uint8_t*> pool;
    std::map<uint64_t, std::vector<uint8_t*>> pending;
    uint64_t pkts_rx = 0, pkts_stale = 0, pkts_bad = 0, frames_parked = 0;
    size_t pending_count = 0;             // parked packets (Tomii: pending_count)
    std::vector<uint8_t> dropped;         // [MAXF] frame dropped by the parking policy
    uint64_t frames_dropped = 0;
    std::vector<int> fds;

    explicit Runtime(const Args& a) : args(a) {
        cfg = load_config(a.config);
        if (const char* dp = std::getenv("E1_DUMP_PILOTS")) {
            std::ofstream o(dp, std::ios::binary);
            o.write(reinterpret_cast<const char*>(cfg.pilots_sgn.data()),
                    cfg.pilots_sgn.size() * sizeof(std::complex<float>));
        }
        ppf = cfg.packets_per_frame();
        pkt_len = cfg.packet_length();
        pkt_alloc = ((pkt_len + 128 + 63) / 64) * 64;
        is_pilot.assign(cfg.schedule_length(), 0);
        for (size_t s : cfg.pilot_symbols) is_pilot[s] = 1;
        for (size_t s : cfg.pilot_symbols)
            for (size_t a2 = 0; a2 < cfg.bs_ant_num; ++a2) pilot_idx.push_back(s * cfg.bs_ant_num + a2);
        for (size_t s : cfg.ul_symbols)
            for (size_t a2 = 0; a2 < cfg.bs_ant_num; ++a2) ul_idx.push_back(s * cfg.bs_ant_num + a2);
        n_csi = pilot_idx.size();
        n_fft = ul_idx.size();
        n_beam = cfg.beam_events_per_symbol;
        demul_events = cfg.demul_events_per_symbol;
        n_demul = demul_events * cfg.NumUlSyms();
        n_sym = cfg.NumUlSyms();
        if (n_sym > MAXSYM) throw std::runtime_error("too many UL symbols");
        n_ss = cfg.num_spatial_streams;
        cell_len = cfg.ul_mod_order_bits * cfg.ofdm_data_num;
        recs.reset(new Rec[MAXF]);
        dropped.assign(MAXF, 0);
        slots.resize(a.slots);
        for (size_t s = 0; s < a.slots; ++s) {
            slots[s] = std::make_unique<Slot>();
            slots[s]->idx = s;
            slots[s]->buf.init(cfg);
            slots[s]->pkt.assign(ppf, nullptr);
        }
        if (!a.golden.empty()) {
            std::ifstream g(a.golden, std::ios::binary);
            if (!g) { std::cerr << "cannot read golden " << a.golden << "\n"; std::exit(2); }
            golden.assign(std::istreambuf_iterator<char>(g), {});
        }
        // Pre-fault a pool sized for S frames + a margin of parked frames.
        size_t prealloc = (a.slots + 2) * ppf;
        pool.reserve(prealloc * 2);
        for (size_t i = 0; i < prealloc; ++i) pool.push_back(alloc_pkt());
        std::cout << "E1 " << a.mode << ": bs_ant=" << cfg.bs_ant_num << " ue=" << cfg.ue_ant_num
                  << " ppf=" << ppf << " n_csi=" << n_csi << " n_fft=" << n_fft
                  << " n_beam=" << n_beam << " n_demul=" << n_demul << " mod_bits="
                  << cfg.ul_mod_order_bits << " S=" << a.slots << " W=" << a.workers << "\n"
                  << std::flush;
    }

    uint8_t* alloc_pkt() {
        void* p = nullptr;
        if (posix_memalign(&p, 64, pkt_alloc) != 0) std::abort();
        std::memset(p, 0, pkt_alloc);
        return static_cast<uint8_t*>(p);
    }
    uint8_t* get_buf() {
        if (pool.empty()) return alloc_pkt();
        uint8_t* b = pool.back();
        pool.pop_back();
        return b;
    }

    // Worker threads call this once on entry (engines hook it into their
    // worker-start callback). Workers go on core_base+1, core_base+2, ...
    void worker_entry() {
        thread_local bool entered = false;  // TBB threads may re-enter an arena
        if (entered) return;
        entered = true;
        int k = worker_seq.fetch_add(1);
        if (args.pin) pin_self(args.core_base + 1 + (k % int(args.workers)));
    }

    // ------------------------------------------------------------------
    // Task bodies (worker threads). Counters implement the per-frame
    // barriers for streaming engines; DAG engines get the same counters for
    // free and only rely on the demul one (to stamp `done`).
    // ------------------------------------------------------------------
    inline void probe_fftcsi(Slot& s) {
        Rec& r = recs[s.frame % MAXF];
        if (r.rx.load(std::memory_order_acquire) < ppf)
            r.overlap.fetch_add(1, std::memory_order_relaxed);
    }
    // --- kernel + counter primitives ---
    // The per-frame barriers of the graph are:
    //   beam            after ALL csi of the frame
    //   demul[sym][*]   after the bs_ant FFTs of UL symbol `sym` AND all beam
    // (Tomii: csi.wait(all) / fft.wait(group_by antennas) + beam.wait(all)).
    // DAG engines encode them as graph edges; streaming engines use the counters.
    // Every kernel entry re-checks its dependencies and counts violations
    // (Rec::depviol) — an early-firing barrier would otherwise go unnoticed
    // after the first frames, because the sender replays identical IQ.
    struct PktRes {
        bool csi_last = false, fft_sym_done = false;
        uint32_t sym = 0;
    };
    PktRes exec_pkt(Slot& s, uint32_t pidx, bool pilot) {
        PktRes r;
        if (pilot) {
            do_csi(s.pkt[pidx], cfg, s.buf);
            probe_fftcsi(s);
            r.csi_last = s.csi_done.fetch_add(1, std::memory_order_acq_rel) + 1 == n_csi;
            return r;
        }
        do_fft(s.pkt[pidx], cfg, s.buf);
        probe_fftcsi(s);
        r.sym = uint32_t(cfg.GetUlSymbolIdx(pidx / cfg.bs_ant_num));
        r.fft_sym_done =
            s.fft_sym[r.sym].fetch_add(1, std::memory_order_acq_rel) + 1 == cfg.bs_ant_num;
        s.fft_done.fetch_add(1, std::memory_order_acq_rel);
        return r;
    }
    bool exec_beam(Slot& s, uint32_t b) {
        if (s.csi_done.load(std::memory_order_acquire) < n_csi)
            recs[s.frame % MAXF].depviol.fetch_add(1, std::memory_order_relaxed);
        do_beam(cfg, s.buf, s.frame, b);
        return s.beam_done.fetch_add(1, std::memory_order_acq_rel) + 1 == n_beam;
    }
    void exec_demul(Slot& s, uint32_t d) {
        size_t si = d / demul_events;
        if (s.fft_sym[si].load(std::memory_order_acquire) < cfg.bs_ant_num ||
            s.beam_done.load(std::memory_order_acquire) < n_beam)
            recs[s.frame % MAXF].depviol.fetch_add(1, std::memory_order_relaxed);
        do_demul(cfg, s.buf, s.frame, cfg.ul_symbols[si], d, demul_events);
        if (s.demul_done.fetch_add(1, std::memory_order_acq_rel) + 1 == n_demul) finish(s);
    }
    // --- callback-style wrappers (streaming engines) ---
    void task_pkt(Slot& s, uint32_t pidx, bool pilot) {
        PktRes r = exec_pkt(s, pidx, pilot);
        if (r.csi_last) eng->beam_ready(s);
        else if (r.fft_sym_done) arrive_sym(s, r.sym);
    }
    void beams_done(Slot& s) {
        for (uint32_t k = 0; k < n_sym; ++k) arrive_sym(s, k);
    }
    void task_beam(Slot& s, uint32_t b) {
        if (exec_beam(s, b)) beams_done(s);
    }
    void arrive_sym(Slot& s, uint32_t sym) {
        if (s.sym_gate[sym].fetch_add(1, std::memory_order_acq_rel) + 1 == 2) eng->demul_ready_sym(s, sym);
    }
    void task_demul(Slot& s, uint32_t d) { exec_demul(s, d); }
    // Chunked fan-out: each puller grabs indices from a shared cursor.
    void pull_beam(Slot& s) {
        uint32_t b;
        while ((b = s.beam_next.fetch_add(1, std::memory_order_relaxed)) < n_beam) task_beam(s, b);
    }
    bool pull_beam_last(Slot& s) {  // true for the puller that ran the last beam
        uint32_t b;
        bool last = false;
        while ((b = s.beam_next.fetch_add(1, std::memory_order_relaxed)) < n_beam)
            last |= exec_beam(s, b);
        return last;
    }
    void pull_demul_sym(Slot& s, uint32_t sym) {
        uint32_t j;
        while ((j = s.dsym_next[sym].fetch_add(1, std::memory_order_relaxed)) < demul_events)
            exec_demul(s, sym * demul_events + j);
    }

    // Pre-arrival warm-up, the C++ counterpart of Tomii's init phase (which
    // builds every per-task Fft/DFTI descriptor and scratch struct before the
    // network starts). Without it the baselines create worker threads, DFTI
    // descriptors and thread-local scratch lazily inside the first frames:
    // ~13 ms first-frame latency at 4x4 under oneTBB, enough to overflow the
    // bounded parking and drop frames 8-13. Each worker commits its DFTI
    // descriptor and runs each kernel once on slot 0 (disjoint indices per
    // worker; slot 0's buffers are fully rewritten by the first real frame).
    void warmup(Engine& engine) {
        Slot& s = *slots[0];
        std::vector<uint8_t> pkt(pkt_alloc, 0);
        engine.on_all_workers([&](int k) {
            tl_fft_handle(cfg.ofdm_ca_num);
            std::vector<uint8_t> my(pkt);
            Packet* h = reinterpret_cast<Packet*>(my.data());
            h->frame_id = 0;
            h->symbol_id = uint32_t(cfg.ul_symbols[0]);
            h->ant_id = uint32_t(size_t(k) % cfg.bs_ant_num);
            if (size_t(k) < cfg.bs_ant_num) do_fft(my.data(), cfg, s.buf);
            else fft_front(my.data(), cfg);
            if (uint32_t(k) < n_beam) do_beam(cfg, s.buf, 0, k);
            if (uint32_t(k) < demul_events) do_demul(cfg, s.buf, 0, cfg.ul_symbols[0], k, demul_events);
        });
        std::cout << "E1 warm-up done on " << args.workers << " workers\n" << std::flush;
    }

    void finish(Slot& s) {
        uint64_t t = now_ns();
        Rec& r = recs[s.frame % MAXF];
        r.done.store(t, std::memory_order_release);
        last_activity.store(t, std::memory_order_relaxed);
        // Verification happens after the `done` stamp (outside the measured
        // interval) but before the slot is released, exactly as in the Tomii probe.
        if (!golden.empty()) {
            bool ok = golden.size() == n_sym * n_ss * cell_len;
            size_t fs = s.frame % Config::FRAME_WND;
            for (size_t a = 0; ok && a < n_sym; ++a)
                for (size_t b = 0; ok && b < n_ss; ++b)
                    ok = std::memcmp(s.buf.demod_cell(fs, a, b, cfg.ul_symbols.size()),
                                     golden.data() + (a * n_ss + b) * cell_len, cell_len) == 0;
            r.verify.store(ok ? 1 : 2, std::memory_order_release);
            if (!ok) verify_fail.fetch_add(1);
        }
        if (!args.dump_golden.empty() && size_t(s.frame) == args.golden_frame) {
            std::ofstream g(args.dump_golden, std::ios::binary);
            size_t fs = s.frame % Config::FRAME_WND;
            for (size_t a = 0; a < n_sym; ++a)
                for (size_t b = 0; b < n_ss; ++b)
                    g.write(reinterpret_cast<const char*>(
                                s.buf.demod_cell(fs, a, b, cfg.ul_symbols.size())),
                            cell_len);
        }
        done_count.fetch_add(1, std::memory_order_acq_rel);
        s.state.store(DONE, std::memory_order_release);
    }

    // ------------------------------------------------------------------
    // Receive side (rx thread)
    // ------------------------------------------------------------------
    void bind_sockets() {
        for (size_t i = 0; i < cfg.bs_ant_num; ++i) {
            int fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
            if (fd < 0) throw std::runtime_error("socket");
            int rcvbuf = 1 << 24;
            ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(uint16_t(cfg.bs_server_port + int(i)));
            if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
                throw std::runtime_error("bind port " + std::to_string(cfg.bs_server_port + i));
            fds.push_back(fd);
        }
    }

    void admit(Slot& s, uint64_t f) {
        s.frame = int64_t(f);
        s.rx = 0;
        s.csi_done.store(0, std::memory_order_relaxed);
        s.fft_done.store(0, std::memory_order_relaxed);
        s.beam_done.store(0, std::memory_order_relaxed);
        s.demul_done.store(0, std::memory_order_relaxed);
        s.gate.store(0, std::memory_order_relaxed);
        s.beam_next.store(0, std::memory_order_relaxed);
        s.demul_next.store(0, std::memory_order_relaxed);
        for (size_t k = 0; k < MAXSYM; ++k) {
            s.fft_sym[k].store(0, std::memory_order_relaxed);
            s.sym_gate[k].store(0, std::memory_order_relaxed);
            s.dsym_next[k].store(0, std::memory_order_relaxed);
        }
        s.state.store(BUSY, std::memory_order_release);
        eng->on_admit(s);
    }

    void deliver(Slot& s, uint8_t* buf) {
        const Packet* h = packet_hdr(buf);
        uint32_t idx = h->symbol_id * cfg.bs_ant_num + h->ant_id;
        if (s.pkt[idx] != nullptr) {  // duplicate — cannot happen with the Agora sender
            pool.push_back(buf);
            ++pkts_bad;
            return;
        }
        s.pkt[idx] = buf;
        ++s.rx;
        eng->on_packet(s, idx, is_pilot[h->symbol_id] != 0);
        if (s.rx == ppf) eng->on_rx_complete(s);
    }

    void handle(uint8_t* buf) {
        const Packet* h = packet_hdr(buf);
        uint64_t f = h->frame_id;
        if (h->symbol_id >= cfg.schedule_length() || h->ant_id >= cfg.bs_ant_num || f >= MAXF ||
            (!is_pilot[h->symbol_id] && cfg.GetUlSymbolIdx(h->symbol_id) == SIZE_MAX)) {
            pool.push_back(buf);
            ++pkts_bad;
            return;
        }
        ++pkts_rx;
        if (dropped[f]) {  // frame dropped by the parking policy: discard
            pool.push_back(buf);
            return;
        }
        uint64_t t = now_ns();
        Rec& r = recs[f];
        if (r.first_rx.load(std::memory_order_relaxed) == 0) r.first_rx.store(t, std::memory_order_relaxed);
        uint32_t n = r.rx.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (n == ppf) r.last_rx.store(t, std::memory_order_release);
        last_activity.store(t, std::memory_order_relaxed);

        Slot& s = *slots[f % slots.size()];
        int st = s.state.load(std::memory_order_acquire);
        if (s.frame == int64_t(f) && st == BUSY) {
            deliver(s, buf);
        } else if (int64_t(f) <= s.frame) {
            pool.push_back(buf);  // stale: frame already processed
            ++pkts_stale;
        } else if (st == FREE && pending_for_slot(s.idx) == pending.end()) {
            admit(s, f);
            deliver(s, buf);
        } else {
            park(f, buf);
        }
    }

    // Mirror of tomii-core park_pending_packet (cap = ppf x S, evict the
    // highest parked frame id or the incoming frame, whichever is larger).
    void park(uint64_t f, uint8_t* buf) {
        size_t cap = ppf * slots.size();
        if (pending_count >= cap) {
            uint64_t victim = f;
            if (!pending.empty()) victim = std::max(victim, pending.rbegin()->first);
            auto it = pending.find(victim);
            if (it != pending.end()) {
                for (uint8_t* b : it->second) pool.push_back(b);
                pending_count -= it->second.size();
                pending.erase(it);
            }
            if (!dropped[victim]) { dropped[victim] = 1; ++frames_dropped; }
            if (victim == f) { pool.push_back(buf); return; }
        }
        auto& v = pending[f];
        if (v.empty()) ++frames_parked;
        v.push_back(buf);
        ++pending_count;
    }

    std::map<uint64_t, std::vector<uint8_t*>>::iterator pending_for_slot(size_t sidx) {
        for (auto it = pending.begin(); it != pending.end(); ++it)
            if (it->first % slots.size() == sidx) return it;
        return pending.end();
    }

    void reap() {
        for (auto& sp : slots) {
            Slot& s = *sp;
            if (s.state.load(std::memory_order_acquire) != DONE) continue;
            if (!eng->reusable(s)) continue;
            for (auto& p : s.pkt)
                if (p) { pool.push_back(p); p = nullptr; }
            s.state.store(FREE, std::memory_order_release);
            // Replay the oldest parked frame that maps to this slot.
            if (pending.empty()) continue;
            auto it = pending_for_slot(s.idx);
            if (it == pending.end() || int64_t(it->first) <= s.frame) continue;
            std::vector<uint8_t*> bufs = std::move(it->second);
            pending_count -= bufs.size();
            uint64_t f = it->first;
            pending.erase(it);
            admit(s, f);
            for (uint8_t* b : bufs) deliver(s, b);
        }
    }

    int run(Engine& engine) {
        eng = &engine;
        engine.rt = this;
        bind_sockets();
        if (args.pin) pin_self(args.core_base);
        engine.start();
        warmup(engine);

        int ep = epoll_create1(0);
        for (size_t i = 0; i < fds.size(); ++i) {
            epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.u32 = uint32_t(i);
            epoll_ctl(ep, EPOLL_CTL_ADD, fds[i], &ev);
        }
        constexpr int B = 32;
        mmsghdr msgs[B];
        iovec iov[B];
        uint8_t* bufs[B];
        for (int k = 0; k < B; ++k) bufs[k] = get_buf();
        std::vector<epoll_event> evs(fds.size());
        std::cout << "Waiting for packets on ports " << cfg.bs_server_port << ".."
                  << cfg.bs_server_port + int(cfg.bs_ant_num) - 1 << "\n" << std::flush;

        bool stalled = false;
        while (done_count.load(std::memory_order_acquire) + frames_dropped < args.frames) {
            int ne = epoll_wait(ep, evs.data(), int(evs.size()), 0);
            for (int e = 0; e < ne; ++e) {
                int fd = fds[evs[e].data.u32];
                for (;;) {
                    for (int k = 0; k < B; ++k) {
                        iov[k].iov_base = bufs[k];
                        iov[k].iov_len = pkt_alloc;
                        std::memset(&msgs[k].msg_hdr, 0, sizeof(msghdr));
                        msgs[k].msg_hdr.msg_iov = &iov[k];
                        msgs[k].msg_hdr.msg_iovlen = 1;
                    }
                    int n = recvmmsg(fd, msgs, B, MSG_DONTWAIT, nullptr);
                    if (n <= 0) break;
                    for (int k = 0; k < n; ++k) {
                        uint8_t* b = bufs[k];
                        bufs[k] = get_buf();
                        handle(b);
                    }
                    if (n < B) break;
                }
            }
            reap();
            if (ne <= 0) {
                uint64_t la = last_activity.load(std::memory_order_relaxed);
                if (la != 0 && now_ns() - la > args.stall_ms * 1000000ull) {
                    stalled = true;
                    break;
                }
            }
        }
        engine.stop();
        write_output(stalled);
        close(ep);
        return 0;
    }

    void write_output(bool stalled) {
        size_t pending_pkts = 0;
        for (auto& [f, v] : pending) pending_pkts += v.size();
        std::ofstream o(args.out + ".tmp");
        o << "# {\"system\":\"" << eng->name() << "\",\"ppf\":" << ppf
          << ",\"done\":" << done_count.load() << ",\"verify_fail\":" << verify_fail.load()
          << ",\"golden\":" << (golden.empty() ? "false" : "true") << ",\"stalled\":"
          << (stalled ? "true" : "false") << ",\"pkts_rx\":" << pkts_rx
          << ",\"pkts_stale\":" << pkts_stale << ",\"pkts_bad\":" << pkts_bad
          << ",\"frames_parked\":" << frames_parked << ",\"frames_dropped_parking\":" << frames_dropped << ",\"pending_pkts_left\":" << pending_pkts
          << ",\"slots\":" << args.slots << ",\"workers\":" << args.workers
          << ",\"knobs\":" << args.knobs_json() << "}\n";
        o << "frame,first_rx_ns,last_rx_ns,done_ns,rx_pkts,overlap,verify,depviol\n";
        for (size_t f = 0; f < MAXF; ++f) {
            Rec& r = recs[f];
            uint64_t fr = r.first_rx.load();
            if (fr == 0) continue;
            o << f << "," << fr << "," << r.last_rx.load() << "," << r.done.load() << ","
              << r.rx.load() << "," << r.overlap.load() << "," << r.verify.load() << ","
              << r.depviol.load() << "\n";
        }
        o.close();
        std::rename((args.out + ".tmp").c_str(), args.out.c_str());
        std::cout << "E1 done: " << done_count.load() << "/" << args.frames << " frames, verify_fail="
                  << verify_fail.load() << (stalled ? " (STALLED)" : "") << "\n" << std::flush;
    }
};

}  // namespace e1
