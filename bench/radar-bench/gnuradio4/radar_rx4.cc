// GNU Radio 4.0 baseline for the radar-pipeline workload (C++23).
//
// Two flowgraph topologies over the same UDP chirp stream and verifier:
//
//   serial (default, the original baseline):
//     ChirpSource (complex<float> stream) -> RangeFft (FFTW + Hann per chirp)
//       -> DetectSink (corner turn + Doppler/CFAR/cluster, all tiles serially,
//          via the shared libradar_kernels.so)
//
//   tiled (--topology tiled, "GR4 at its best": the same intra-frame data
//   parallelism the Tomii graph expresses, as native GR4 blocks):
//     TokSource (one uint32 token per chirp; payload in a packet ring)
//       -> RangeBlk   (rk_range_fft per chirp, corner-turned into the rd ring;
//                      emits one frame token when the frame's chirps are done)
//       -> T x DopplerBlk(tile t)   (rk_doppler_fft, fan-out of the frame token)
//       -> Join(T)                  (sync ports = barrier on all tiles)
//       -> T x CfarBlk(tile t)      (rk_cfar)
//       -> ClusterSink(T inputs)    (barrier + rk_cluster + detection dump)
//     Every DSP stage then calls exactly the kernels Tomii calls (range FFT
//     included), so the two systems differ only in runtime/scheduling.
//
// Latency is logged per frame as frame_id,latency_us,done_ns where
// latency_us = chirp-0 recv -> detections written (runtime-internal view) and
// done_ns = CLOCK_MONOTONIC after the detection line is written (joined with
// sender.py --timestamps by the E5 driver for a system-independent metric).
//
// Usage: radar_rx4 <port> <n_samples> <n_chirps> <tiles> <guard> <train>
//                  <pfa_scale> <frames> <det_path> <lat_path> [options]
// Options (defaults reproduce the original baseline):
//   --topology serial|tiled     flowgraph shape                  [serial]
//   --sched simple|bfs|dfs      GR4 scheduler                    [simple]
//   --policy mt|st              multiThreaded / singleThreaded   [mt]
//   --threads N                 default CPU pool thread bound    [GR default]
//   --buf N                     min buffer size (items) per edge [GR default]
//   --src fill|nb|poll          source recv policy:
//        fill: block in recv() until the output span is full (original)
//        nb:   block only for the first packet, drain the rest non-blocking
//        poll: never block (MSG_DONTWAIT)                        [fill]
//   --pin-pool LO-HI            pin CPU-pool workers across cores LO..HI via GR4's
//                               own setAffinityMask (aborts at this GR4 revision)
//   --pin-threads LO-HI         pin every runtime thread 1:1 to cores LO..HI (round
//                               robin) from outside GR4, by scanning /proc/self/task
//                               (repeated during start-up, so lazily spawned pool
//                               workers are caught); Tomii-style per-thread pinning
//   --max-work N                scheduler max_work_items          [GR default]
//   --slots K                   rd/power ring depth (tiled)       [8]
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdio>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <mutex>
#include <sched.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <set>
#include <string>
#include <vector>

#include <fftw3.h>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

extern "C" {
struct rk_detection { uint32_t range_bin, doppler_bin; float power; };
void* rk_init(uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, float, uint32_t);
void* rk_make_range_ws(void*);
void* rk_make_doppler_ws(void*);
void* rk_alloc_rd(void*);
void* rk_alloc_power(void*);
void* rk_alloc_dets(void*);
void rk_range_fft(void*, void*, const int16_t*, uint32_t, uint32_t, void*, uint32_t);
void rk_doppler_fft(void*, void*, uint32_t, const void*, void*, uint32_t);
uint32_t rk_cfar(void*, uint32_t, const void*, void*, uint32_t);
uint32_t rk_cluster(void*, const void*, uint32_t, rk_detection*, uint32_t);
}

// Bench-app configuration/state shared between blocks (set in main before start).
struct Cfg {
    int port{}, n{}, m{}, tiles{}, guard{}, train{}, frames{};
    float scale{};
    const char *det_path{}, *lat_path{};
    std::string topology = "serial", sched = "simple", policy = "mt", src = "fill";
    int threads = 0, buf = 0, pin_lo = -1, pin_hi = -1, slots = 8;
    int pint_lo = -1, pint_hi = -1;
    long max_work = 0;
} g_cfg;

enum class SrcMode { Fill, Nb, Poll };
static SrcMode src_mode() {
    return g_cfg.src == "poll" ? SrcMode::Poll : g_cfg.src == "nb" ? SrcMode::Nb : SrcMode::Fill;
}

// frame_id + arrival time of each frame's first packet, in arrival order.
static std::mutex g_ts_mu;
static std::deque<std::pair<uint32_t, uint64_t>> g_ts;
static std::atomic<int> g_frames_done{0};

static uint64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

// Optional per-stage trace (tiled topology; env E5_STAGE_TRACE=<csv path>):
// CLOCK_MONOTONIC ns per frame for range-done, per-tile Doppler start/end,
// join emit, per-tile CFAR start/end and cluster start, written at exit.
// Used only for the E5 mechanism analysis; off by default (no cost).
struct StageTrace {
    bool on = false;
    int T = 0, frames = 0;
    std::vector<uint64_t> v;  // frames x (3 + 4T)
    enum { RANGE = 0, JOIN = 1, CLUSTER = 2 };
    void init(int frames_, int T_) {
        on = true; frames = frames_; T = T_;
        v.assign((size_t)frames * (3 + 4 * T), 0);
    }
    uint64_t* row(uint32_t fid) { return fid < (uint32_t)frames ? &v[(size_t)fid * (3 + 4 * T)] : nullptr; }
    void mark(uint32_t fid, int col, uint64_t t);
    void dump(const char* path) {
        FILE* f = fopen(path, "w");
        if (!f) return;
        fprintf(f, "frame_id,range_done,join,cluster_start");
        for (int t = 0; t < T; t++) fprintf(f, ",dop%d_s,dop%d_e", t, t);
        for (int t = 0; t < T; t++) fprintf(f, ",cfar%d_s,cfar%d_e", t, t);
        fprintf(f, "\n");
        for (int i = 0; i < frames; i++) {
            fprintf(f, "%d", i);
            for (int c = 0; c < 3 + 4 * T; c++) fprintf(f, ",%llu", (unsigned long long)v[(size_t)i * (3 + 4 * T) + c]);
            fprintf(f, "\n");
        }
        fclose(f);
    }
} g_trace;
inline void StageTrace::mark(uint32_t fid, int col, uint64_t t) {
    if (auto* r = row(fid)) r[col] = t;
}

// ---------------------------------------------------------------------------
// UDP receive helper shared by both sources.
// ---------------------------------------------------------------------------
struct Udp {
    int fd = -1;
    std::set<uint32_t> seen;
    uint64_t last_pkt_ns = 0;
    bool done = false;

    void open(size_t pkt_len) {
        (void)pkt_len;
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        int buf = 32 << 20;
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
        timeval tv{0, 200000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(static_cast<uint16_t>(g_cfg.port));
        bind(fd, (sockaddr*)&addr, sizeof(addr));
    }
    // Returns bytes received, or <0 if nothing (timeout / would block).
    ssize_t recv_one(uint8_t* p, size_t len, bool block) {
        ssize_t r = recv(fd, p, len, block ? 0 : MSG_DONTWAIT);
        uint64_t t = now_ns();
        if (r < 0) {
            // End of stream: all frames seen and the link has gone quiet.
            if (!seen.empty() && (int)seen.size() >= g_cfg.frames &&
                (block || (last_pkt_ns && t - last_pkt_ns > 200000000ull)))
                done = true;
            return r;
        }
        last_pkt_ns = t;
        uint32_t fid, chirp;
        std::memcpy(&fid, p, 4);
        std::memcpy(&chirp, p + 4, 4);
        if (chirp == 0) {
            std::lock_guard lk(g_ts_mu);
            g_ts.emplace_back(fid, t);
        }
        seen.insert(fid);
        return r;
    }
};

static void write_frame(FILE* det_f, FILE* lat_f, uint32_t fid_hint, rk_detection* out, uint32_t nd) {
    std::sort(out, out + nd, [](auto& a, auto& b) {
        return a.range_bin != b.range_bin ? a.range_bin < b.range_bin : a.doppler_bin < b.doppler_bin;
    });
    uint64_t fid = fid_hint, t_arr = 0;
    {
        std::lock_guard lk(g_ts_mu);
        while (!g_ts.empty() && g_ts.front().first < fid_hint) g_ts.pop_front();
        if (!g_ts.empty()) {
            fid = g_ts.front().first;
            t_arr = g_ts.front().second;
            g_ts.pop_front();
        }
    }
    fprintf(det_f, "frame %lu:", (unsigned long)fid);
    for (uint32_t k = 0; k < nd; k++)
        fprintf(det_f, " %u,%u,%.3e", out[k].range_bin, out[k].doppler_bin, out[k].power);
    fprintf(det_f, "\n");
    fflush(det_f);
    uint64_t t_done = now_ns();
    if (t_arr) fprintf(lat_f, "%lu,%.2f,%lu\n", (unsigned long)fid, (t_done - t_arr) / 1e3, (unsigned long)t_done);
    fflush(lat_f);
    g_frames_done.fetch_add(1);
}

// ===========================================================================
// serial topology (original baseline)
// ===========================================================================
struct ChirpSource : gr::Block<ChirpSource> {
    gr::PortOut<std::complex<float>> out;
    GR_MAKE_REFLECTABLE(ChirpSource, out);

    Udp udp;
    int off = 0;
    std::deque<std::vector<std::complex<float>>> chunks;
    std::vector<uint8_t> pkt;

    void start() {
        pkt.resize(64 + 4 * static_cast<size_t>(g_cfg.n));
        udp.open(pkt.size());
    }

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        const SrcMode mode = src_mode();
        size_t produced = 0;
        bool got_any = false;
        while (produced < outSpan.size()) {
            if (!chunks.empty()) {
                auto& c = chunks.front();
                size_t nn = std::min(outSpan.size() - produced, c.size() - off);
                std::memcpy(outSpan.data() + produced, c.data() + off, nn * sizeof(std::complex<float>));
                produced += nn;
                off += nn;
                if (off == (int)c.size()) { chunks.pop_front(); off = 0; }
                continue;
            }
            if (udp.done) break;
            bool block = mode == SrcMode::Fill || (mode == SrcMode::Nb && !got_any);
            ssize_t r = udp.recv_one(pkt.data(), pkt.size(), block);
            if (r < 0) break;
            got_any = true;
            const int16_t* iq = (const int16_t*)(pkt.data() + 64);
            std::vector<std::complex<float>> s(g_cfg.n);
            for (int i = 0; i < g_cfg.n; i++) s[i] = {(float)iq[2 * i], (float)iq[2 * i + 1]};
            chunks.push_back(std::move(s));
        }
        outSpan.publish(produced);
        if (udp.done && chunks.empty()) return gr::work::Status::DONE;
        return gr::work::Status::OK;
    }
};

struct RangeFft : gr::Block<RangeFft> {
    gr::PortIn<std::complex<float>> in;
    gr::PortOut<std::complex<float>> out;
    GR_MAKE_REFLECTABLE(RangeFft, in, out);

    std::vector<float> win;
    std::vector<std::complex<float>> pending, partial;
    fftwf_complex *fin{}, *fout{};
    fftwf_plan plan{};

    void start() {
        int n = g_cfg.n;
        win.resize(n);
        for (int i = 0; i < n; i++) win[i] = 0.5f * (1.0f - cosf(2.0f * (float)M_PI * i / (n - 1)));
        fin = fftwf_alloc_complex(n);
        fout = fftwf_alloc_complex(n);
        plan = fftwf_plan_dft_1d(n, fin, fout, FFTW_FORWARD, FFTW_ESTIMATE);
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan, gr::OutputSpanLike auto& outSpan) {
        const int n = g_cfg.n;
        partial.insert(partial.end(), inSpan.begin(), inSpan.end());
        std::ignore = inSpan.consume(inSpan.size());
        size_t chirp_off = 0;
        while (partial.size() - chirp_off >= (size_t)n) {
            for (int i = 0; i < n; i++) {
                fin[i][0] = partial[chirp_off + i].real() * win[i];
                fin[i][1] = partial[chirp_off + i].imag() * win[i];
            }
            fftwf_execute(plan);
            pending.insert(pending.end(), (std::complex<float>*)fout, (std::complex<float>*)fout + n);
            chirp_off += n;
        }
        partial.erase(partial.begin(), partial.begin() + chirp_off);
        size_t nn = std::min(outSpan.size(), pending.size());
        std::memcpy(outSpan.data(), pending.data(), nn * sizeof(std::complex<float>));
        pending.erase(pending.begin(), pending.begin() + nn);
        outSpan.publish(nn);
        return gr::work::Status::OK;
    }
};

struct DetectSink : gr::Block<DetectSink> {
    gr::PortIn<std::complex<float>> in;
    GR_MAKE_REFLECTABLE(DetectSink, in);

    void *ctx{}, *dws{}, *rd{}, *power{}, *dets{};
    std::vector<std::complex<float>> frame;
    FILE *det_f{}, *lat_f{};
    uint32_t next_fid = 0;

    void start() {
        ctx = rk_init(g_cfg.n, g_cfg.m, 1, g_cfg.tiles, g_cfg.guard, g_cfg.train, g_cfg.scale, 64);
        dws = rk_make_doppler_ws(ctx);
        rd = rk_alloc_rd(ctx);
        power = rk_alloc_power(ctx);
        dets = rk_alloc_dets(ctx);
        det_f = fopen(g_cfg.det_path, "w");
        lat_f = fopen(g_cfg.lat_path, "w");
        fprintf(lat_f, "frame_id,latency_us,done_ns\n");
        frame.reserve((size_t)g_cfg.n * g_cfg.m);
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan) {
        const size_t flen = (size_t)g_cfg.n * g_cfg.m;
        for (auto& v : inSpan) {
            frame.push_back(v);
            if (frame.size() == flen) {
                processFrame();
                frame.clear();
            }
        }
        std::ignore = inSpan.consume(inSpan.size());
        if (g_frames_done.load() >= g_cfg.frames) return gr::work::Status::DONE;
        return gr::work::Status::OK;
    }

    void processFrame() {
        const int n = g_cfg.n, m = g_cfg.m;
        auto* rdp = (std::complex<float>*)rd;
        for (int r = 0; r < n; r++)  // corner turn [chirp][range] -> [range][chirp]
            for (int c = 0; c < m; c++) rdp[(size_t)r * m + c] = frame[(size_t)c * n + r];
        for (int t = 0; t < g_cfg.tiles; t++) rk_doppler_fft(ctx, dws, t, rd, power, 0);
        for (int t = 0; t < g_cfg.tiles; t++) rk_cfar(ctx, t, power, dets, 0);
        rk_detection out[1024];
        uint32_t nd = rk_cluster(ctx, dets, 0, out, 1024);
        write_frame(det_f, lat_f, 0, out, nd);
    }
};

// ===========================================================================
// tiled topology: frame tokens over GR4 ports, shared kernel buffers.
// ===========================================================================
struct Shared {
    void *ctx{}, *rd{}, *power{}, *dets{};
    std::vector<uint8_t> ring;  // packet ring (header + i16 IQ)
    size_t pkt_len = 0, ring_pkts = 0;
    std::atomic<uint64_t> frames_retired{0};  // frames fully clustered (slot reuse guard)
} g_sh;

static uint8_t* ring_at(uint32_t idx) { return g_sh.ring.data() + (size_t)(idx % g_sh.ring_pkts) * g_sh.pkt_len; }

struct TokSource : gr::Block<TokSource> {
    gr::PortOut<uint32_t> out;
    GR_MAKE_REFLECTABLE(TokSource, out);

    Udp udp;
    uint32_t widx = 0;

    void start() { udp.open(g_sh.pkt_len); }

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        const SrcMode mode = src_mode();
        size_t produced = 0;
        // Keep the ring from lapping unconsumed packets: at most one span in flight.
        const size_t cap = std::min(outSpan.size(), g_sh.ring_pkts / 2);
        while (produced < cap && !udp.done) {
            bool block = mode == SrcMode::Fill || (mode == SrcMode::Nb && produced == 0);
            ssize_t r = udp.recv_one(ring_at(widx), g_sh.pkt_len, block);
            if (r < 0) break;
            outSpan[produced++] = widx++;
        }
        outSpan.publish(produced);
        if (udp.done) return gr::work::Status::DONE;
        return gr::work::Status::OK;
    }
};

struct RangeBlk : gr::Block<RangeBlk> {
    gr::PortIn<uint32_t> in;
    gr::PortOut<uint32_t> out;
    GR_MAKE_REFLECTABLE(RangeBlk, in, out);

    void* ws{};
    std::vector<int> count;  // chirps done per ring slot

    void start() {
        ws = rk_make_range_ws(g_sh.ctx);
        count.assign(g_cfg.slots, 0);
    }

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan, gr::OutputSpanLike auto& outSpan) {
        size_t used = 0, emitted = 0;
        for (; used < inSpan.size(); used++) {
            const uint8_t* p = ring_at(inSpan[used]);
            uint32_t fid, chirp;
            std::memcpy(&fid, p, 4);
            std::memcpy(&chirp, p + 4, 4);
            // Back-pressure: never overwrite a ring slot still being processed.
            if (fid >= g_sh.frames_retired.load(std::memory_order_acquire) + (uint64_t)g_cfg.slots) break;
            if (emitted >= outSpan.size()) break;
            uint32_t slot = fid % g_cfg.slots;
            rk_range_fft(g_sh.ctx, ws, (const int16_t*)(p + 64), g_cfg.n, chirp, g_sh.rd, slot);
            if (++count[slot] == g_cfg.m) {
                count[slot] = 0;
                if (g_trace.on) g_trace.mark(fid, StageTrace::RANGE, now_ns());
                outSpan[emitted++] = fid;
            }
        }
        std::ignore = inSpan.consume(used);
        outSpan.publish(emitted);
        return gr::work::Status::OK;
    }
};

struct DopplerBlk : gr::Block<DopplerBlk> {
    gr::PortIn<uint32_t> in;
    gr::PortOut<uint32_t> out;
    gr::Size_t tile = 0U;
    GR_MAKE_REFLECTABLE(DopplerBlk, in, out, tile);

    void* ws{};
    void start() { ws = rk_make_doppler_ws(g_sh.ctx); }

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan, gr::OutputSpanLike auto& outSpan) {
        const size_t k = std::min(inSpan.size(), outSpan.size());
        for (size_t i = 0; i < k; i++) {
            const uint64_t t0 = g_trace.on ? now_ns() : 0;
            rk_doppler_fft(g_sh.ctx, ws, tile, g_sh.rd, g_sh.power, inSpan[i] % g_cfg.slots);
            if (g_trace.on) {
                g_trace.mark(inSpan[i], 3 + 2 * (int)tile, t0);
                g_trace.mark(inSpan[i], 4 + 2 * (int)tile, now_ns());
            }
            outSpan[i] = inSpan[i];
        }
        std::ignore = inSpan.consume(k);
        outSpan.publish(k);
        return gr::work::Status::OK;
    }
};

struct CfarBlk : gr::Block<CfarBlk> {
    gr::PortIn<uint32_t> in;
    gr::PortOut<uint32_t> out;
    gr::Size_t tile = 0U;
    GR_MAKE_REFLECTABLE(CfarBlk, in, out, tile);

    gr::work::Status processBulk(gr::InputSpanLike auto& inSpan, gr::OutputSpanLike auto& outSpan) {
        const size_t k = std::min(inSpan.size(), outSpan.size());
        for (size_t i = 0; i < k; i++) {
            const uint64_t t0 = g_trace.on ? now_ns() : 0;
            rk_cfar(g_sh.ctx, tile, g_sh.power, g_sh.dets, inSpan[i] % g_cfg.slots);
            if (g_trace.on) {
                g_trace.mark(inSpan[i], 3 + 2 * g_trace.T + 2 * (int)tile, t0);
                g_trace.mark(inSpan[i], 4 + 2 * g_trace.T + 2 * (int)tile, now_ns());
            }
            outSpan[i] = inSpan[i];
        }
        std::ignore = inSpan.consume(k);
        outSpan.publish(k);
        return gr::work::Status::OK;
    }
};

// Barrier: forwards a frame token once every input carries it.
struct Join : gr::Block<Join> {
    std::vector<gr::PortIn<uint32_t>> in;
    gr::PortOut<uint32_t> out;
    gr::Size_t n_inputs = 0U;
    GR_MAKE_REFLECTABLE(Join, in, out, n_inputs);

    void settingsChanged(const gr::property_map& old_settings, const gr::property_map& new_settings) {
        if (new_settings.contains("n_inputs") && old_settings.find_value("n_inputs") != new_settings.find_value("n_inputs")) {
            in.resize(n_inputs);
        }
    }

    template<gr::InputSpanLike TInSpan>
    gr::work::Status processBulk(const std::span<TInSpan>& ins, gr::OutputSpanLike auto& sout) {
        size_t k = sout.size();
        for (auto& s : ins) k = std::min(k, s.size());
        for (size_t i = 0; i < k; i++) {
            sout[i] = ins[0][i];
            if (g_trace.on) g_trace.mark(ins[0][i], StageTrace::JOIN, now_ns());
        }
        for (auto& s : ins) std::ignore = s.consume(k);
        sout.publish(k);
        return gr::work::Status::OK;
    }
};

struct ClusterSink : gr::Block<ClusterSink> {
    std::vector<gr::PortIn<uint32_t>> in;
    gr::Size_t n_inputs = 0U;
    GR_MAKE_REFLECTABLE(ClusterSink, in, n_inputs);

    FILE *det_f{}, *lat_f{};

    void settingsChanged(const gr::property_map& old_settings, const gr::property_map& new_settings) {
        if (new_settings.contains("n_inputs") && old_settings.find_value("n_inputs") != new_settings.find_value("n_inputs")) {
            in.resize(n_inputs);
        }
    }

    void start() {
        det_f = fopen(g_cfg.det_path, "w");
        lat_f = fopen(g_cfg.lat_path, "w");
        fprintf(lat_f, "frame_id,latency_us,done_ns\n");
    }

    template<gr::InputSpanLike TInSpan>
    gr::work::Status processBulk(const std::span<TInSpan>& ins) {
        size_t k = SIZE_MAX;
        for (auto& s : ins) k = std::min(k, s.size());
        for (size_t i = 0; i < k; i++) {
            uint32_t fid = ins[0][i];
            if (g_trace.on) g_trace.mark(fid, StageTrace::CLUSTER, now_ns());
            rk_detection out[1024];
            uint32_t nd = rk_cluster(g_sh.ctx, g_sh.dets, fid % g_cfg.slots, out, 1024);
            write_frame(det_f, lat_f, fid, out, nd);
            g_sh.frames_retired.store(fid + 1, std::memory_order_release);
        }
        for (auto& s : ins) std::ignore = s.consume(k);
        if (g_frames_done.load() >= g_cfg.frames) return gr::work::Status::DONE;
        return gr::work::Status::OK;
    }
};

// ===========================================================================

static gr::EdgeParameters edge() {
    gr::EdgeParameters p{};
    if (g_cfg.buf > 0) p.minBufferSize = (size_t)g_cfg.buf;
    return p;
}

static bool build_serial(gr::Graph& flow) {
    auto& src = flow.emplaceBlock<ChirpSource>();
    auto& rfft = flow.emplaceBlock<RangeFft>();
    auto& sink = flow.emplaceBlock<DetectSink>();
    return flow.connect<"out", "in">(src, rfft, edge()).has_value() &&
           flow.connect<"out", "in">(rfft, sink, edge()).has_value();
}

static bool build_tiled(gr::Graph& flow) {
    const int T = g_cfg.tiles;
    g_sh.ctx = rk_init(g_cfg.n, g_cfg.m, g_cfg.slots, T, g_cfg.guard, g_cfg.train, g_cfg.scale, 64);
    g_sh.rd = rk_alloc_rd(g_sh.ctx);
    g_sh.power = rk_alloc_power(g_sh.ctx);
    g_sh.dets = rk_alloc_dets(g_sh.ctx);
    g_sh.pkt_len = 64 + 4 * (size_t)g_cfg.n;
    g_sh.ring_pkts = 1u << 15;  // >> any token buffer, so the ring never laps a reader
    g_sh.ring.assign(g_sh.ring_pkts * g_sh.pkt_len, 0);

    auto& src = flow.emplaceBlock<TokSource>();
    auto& range = flow.emplaceBlock<RangeBlk>();
    auto& join = flow.emplaceBlock<Join>({{"n_inputs", (gr::Size_t)T}});
    auto& sink = flow.emplaceBlock<ClusterSink>({{"n_inputs", (gr::Size_t)T}});
    bool ok = flow.connect<"out", "in">(src, range, edge()).has_value();
    std::vector<DopplerBlk*> dop;
    std::vector<CfarBlk*> cfar;
    for (int t = 0; t < T; t++) {
        dop.push_back(&flow.emplaceBlock<DopplerBlk>({{"tile", (gr::Size_t)t}}));
        cfar.push_back(&flow.emplaceBlock<CfarBlk>({{"tile", (gr::Size_t)t}}));
    }
    using namespace std::string_literals;
    for (int t = 0; t < T; t++) {
        ok = ok && flow.connect<"out", "in">(range, *dop[t], edge()).has_value();
        ok = ok && flow.connect(*dop[t], "out"s, join, "in#"s + std::to_string(t), edge()).has_value();
        ok = ok && flow.connect<"out", "in">(join, *cfar[t], edge()).has_value();
        ok = ok && flow.connect(*cfar[t], "out"s, sink, "in#"s + std::to_string(t), edge()).has_value();
    }
    return ok;
}

// Per-thread pinning from outside the runtime: every thread of this process
// except the main thread gets its own core from [lo, hi] (round robin), in tid
// order. Rescanned for the first 3 s so lazily created pool workers are pinned
// before the sender starts (the E5 driver waits 5 s).
static void start_thread_pinner(int lo, int hi) {
    const pid_t main_tid = (pid_t)getpid();
    std::thread([=] {
        const pid_t self = (pid_t)syscall(SYS_gettid);
        std::set<pid_t> done{main_tid, self};
        int next = lo;
        for (int it = 0; it < 30; it++) {
            std::vector<pid_t> tids;
            if (DIR* d = opendir("/proc/self/task")) {
                while (dirent* e = readdir(d))
                    if (e->d_name[0] != '.') tids.push_back((pid_t)atoi(e->d_name));
                closedir(d);
            }
            std::sort(tids.begin(), tids.end());
            for (pid_t t : tids) {
                if (done.count(t)) continue;
                cpu_set_t cs;
                CPU_ZERO(&cs);
                CPU_SET(next, &cs);
                if (sched_setaffinity(t, sizeof(cs), &cs) == 0) {
                    fprintf(stderr, "pin-threads: tid %d -> core %d\n", (int)t, next);
                    next = next >= hi ? lo : next + 1;
                }
                done.insert(t);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }).detach();
}

template<typename Sched>
static int run(gr::Graph&& flow) {
    Sched sched;
    if (g_cfg.max_work > 0) sched.max_work_items = (std::size_t)g_cfg.max_work;
    if (auto r = sched.exchange(std::move(flow)); !r) { fprintf(stderr, "exchange failed\n"); return 1; }
    auto ret = sched.runAndWait();
    printf("gnuradio4: processed %d frames\n", g_frames_done.load());
    return g_frames_done.load() >= g_cfg.frames && ret.has_value() ? 0 : 1;
}

template<gr::scheduler::ExecutionPolicy P>
static int run_policy(gr::Graph&& flow) {
    if (g_cfg.sched == "bfs") return run<gr::scheduler::BreadthFirst<P>>(std::move(flow));
    if (g_cfg.sched == "dfs") return run<gr::scheduler::DepthFirst<P>>(std::move(flow));
    return run<gr::scheduler::Simple<P>>(std::move(flow));
}

int main(int argc, char** argv) {
    if (argc < 11) { fprintf(stderr, "bad args\n"); return 2; }
    g_cfg.port = atoi(argv[1]); g_cfg.n = atoi(argv[2]); g_cfg.m = atoi(argv[3]);
    g_cfg.tiles = atoi(argv[4]); g_cfg.guard = atoi(argv[5]); g_cfg.train = atoi(argv[6]);
    g_cfg.scale = (float)atof(argv[7]); g_cfg.frames = atoi(argv[8]);
    g_cfg.det_path = argv[9]; g_cfg.lat_path = argv[10];
    for (int i = 11; i + 1 < argc; i += 2) {
        std::string k = argv[i], v = argv[i + 1];
        if (k == "--topology") g_cfg.topology = v;
        else if (k == "--sched") g_cfg.sched = v;
        else if (k == "--policy") g_cfg.policy = v;
        else if (k == "--threads") g_cfg.threads = std::stoi(v);
        else if (k == "--buf") g_cfg.buf = std::stoi(v);
        else if (k == "--src") g_cfg.src = v;
        else if (k == "--max-work") g_cfg.max_work = std::stol(v);
        else if (k == "--slots") g_cfg.slots = std::stoi(v);
        else if (k == "--pin-pool") { sscanf(v.c_str(), "%d-%d", &g_cfg.pin_lo, &g_cfg.pin_hi); }
        else if (k == "--pin-threads") { sscanf(v.c_str(), "%d-%d", &g_cfg.pint_lo, &g_cfg.pint_hi); }
        else { fprintf(stderr, "unknown option %s\n", k.c_str()); return 2; }
    }

    if (g_cfg.threads > 0) {
        // A fresh pool of exactly N CPU-bound workers (setThreadBounds on the default
        // pool does not retire its already-spawned workers, and its affinity striping
        // then fails for the surplus threads at this GR4 revision).
        gr::thread_pool::Manager::instance().replacePool(std::string(gr::thread_pool::kDefaultCpuPoolId),
            std::make_shared<gr::thread_pool::ThreadPoolWrapper>(
                std::make_unique<gr::thread_pool::BasicThreadPool>(gr::thread_pool::kDefaultCpuPoolId,
                    gr::thread_pool::TaskType::CPU_BOUND, (uint32_t)g_cfg.threads, (uint32_t)g_cfg.threads),
                "CPU"));
    }
    auto pool = gr::thread_pool::Manager::defaultCpuPool();
    if (g_cfg.pin_lo >= 0) {
        std::vector<bool> mask(std::thread::hardware_concurrency(), false);
        for (int c = g_cfg.pin_lo; c <= g_cfg.pin_hi; c++) mask[c] = true;
        if (auto* w = dynamic_cast<gr::thread_pool::ThreadPoolWrapper*>(pool.get())) {
            w->impl().setAffinityMask(mask);
        }
    }
    fprintf(stderr, "gnuradio4: topology=%s sched=%s policy=%s threads=%u buf=%d src=%s\n", g_cfg.topology.c_str(),
            g_cfg.sched.c_str(), g_cfg.policy.c_str(), pool->maxThreads(), g_cfg.buf, g_cfg.src.c_str());

    const char* trace_path = getenv("E5_STAGE_TRACE");
    if (trace_path && *trace_path && g_cfg.topology == "tiled") g_trace.init(g_cfg.frames, g_cfg.tiles);
    if (g_cfg.pint_lo >= 0) start_thread_pinner(g_cfg.pint_lo, g_cfg.pint_hi);
    gr::Graph flow;
    bool ok = g_cfg.topology == "tiled" ? build_tiled(flow) : build_serial(flow);
    if (!ok) { fprintf(stderr, "connect failed\n"); return 1; }

    using gr::scheduler::ExecutionPolicy;
    const int rc = g_cfg.policy == "st" ? run_policy<ExecutionPolicy::singleThreaded>(std::move(flow))
                                        : run_policy<ExecutionPolicy::multiThreaded>(std::move(flow));
    if (g_trace.on) g_trace.dump(trace_path);
    return rc;
}
