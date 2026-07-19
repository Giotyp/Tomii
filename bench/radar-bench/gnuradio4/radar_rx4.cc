// GNU Radio 4.0 baseline for the radar-pipeline workload (C++23).
//
// Same UDP chirp stream and DSP as the 3.10 baselines: source strips packet
// headers -> RangeFft (FFTW + Hann, per 1024-sample chirp) -> DetectSink
// (corner turn + Doppler/CFAR/cluster via the same libradar_kernels.so).
// Blocks are native gr::Block<> types run by the GR4 scheduler
// (ExecutionPolicy::multiThreaded); per-frame latency = first-packet arrival
// -> detections written.
//
// Usage: radar_rx4 <port> <n_samples> <n_chirps> <tiles> <guard> <train>
//                  <pfa_scale> <frames> <det_path> <lat_path>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <chrono>
#include <cmath>
#include <complex>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <set>
#include <vector>

#include <fftw3.h>
#include <gnuradio-4.0/Graph.hpp>
#include <gnuradio-4.0/Scheduler.hpp>

extern "C" {
struct rk_detection { uint32_t range_bin, doppler_bin; float power; };
void* rk_init(uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, float, uint32_t);
void* rk_make_doppler_ws(void*);
void* rk_alloc_rd(void*);
void* rk_alloc_power(void*);
void* rk_alloc_dets(void*);
void rk_doppler_fft(void*, void*, uint32_t, const void*, void*, uint32_t);
uint32_t rk_cfar(void*, uint32_t, const void*, void*, uint32_t);
uint32_t rk_cluster(void*, const void*, uint32_t, rk_detection*, uint32_t);
}

// Bench-app configuration/state shared between blocks (set in main before start).
struct Cfg {
    int port{}, n{}, m{}, tiles{}, guard{}, train{}, frames{};
    float scale{};
    const char *det_path{}, *lat_path{};
} g_cfg;

// frame_id + arrival time of each frame's first packet, in arrival order.
static std::mutex g_ts_mu;
static std::deque<std::pair<uint32_t, uint64_t>> g_ts;
static std::atomic<int> g_frames_done{0};

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct ChirpSource : gr::Block<ChirpSource> {
    gr::PortOut<std::complex<float>> out;
    GR_MAKE_REFLECTABLE(ChirpSource, out);

    int fd = -1, off = 0;
    bool done = false;
    std::deque<std::vector<std::complex<float>>> chunks;
    std::set<uint32_t> seen;
    std::vector<uint8_t> pkt;

    void start() {
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
        pkt.resize(64 + 4 * static_cast<size_t>(g_cfg.n));
    }

    gr::work::Status processBulk(gr::OutputSpanLike auto& outSpan) {
        size_t produced = 0;
        while (produced < outSpan.size()) {
            if (!chunks.empty()) {
                auto& c = chunks.front();
                size_t nn = std::min(outSpan.size() - produced, c.size() - off);
                std::memcpy(outSpan.data() + produced, c.data() + off,
                            nn * sizeof(std::complex<float>));
                produced += nn;
                off += nn;
                if (off == (int)c.size()) { chunks.pop_front(); off = 0; }
                continue;
            }
            if (done) break;
            ssize_t r = recv(fd, pkt.data(), pkt.size(), 0);
            if (r < 0) {
                if (!seen.empty() && (int)seen.size() >= g_cfg.frames) done = true;
                break;
            }
            uint64_t t = now_ns();
            uint32_t fid, chirp;
            std::memcpy(&fid, pkt.data(), 4);
            std::memcpy(&chirp, pkt.data() + 4, 4);
            const int16_t* iq = (const int16_t*)(pkt.data() + 64);
            std::vector<std::complex<float>> s(g_cfg.n);
            for (int i = 0; i < g_cfg.n; i++) s[i] = {(float)iq[2 * i], (float)iq[2 * i + 1]};
            if (chirp == 0) {
                std::lock_guard lk(g_ts_mu);
                g_ts.emplace_back(fid, t);
            }
            seen.insert(fid);
            chunks.push_back(std::move(s));
        }
        outSpan.publish(produced);
        if (done && chunks.empty()) return gr::work::Status::DONE;
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
        for (int i = 0; i < n; i++)
            win[i] = 0.5f * (1.0f - cosf(2.0f * (float)M_PI * i / (n - 1)));
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
            pending.insert(pending.end(), (std::complex<float>*)fout,
                           (std::complex<float>*)fout + n);
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

    void start() {
        ctx = rk_init(g_cfg.n, g_cfg.m, 1, g_cfg.tiles, g_cfg.guard, g_cfg.train,
                      g_cfg.scale, 64);
        dws = rk_make_doppler_ws(ctx);
        rd = rk_alloc_rd(ctx);
        power = rk_alloc_power(ctx);
        dets = rk_alloc_dets(ctx);
        det_f = fopen(g_cfg.det_path, "w");
        lat_f = fopen(g_cfg.lat_path, "w");
        fprintf(lat_f, "frame_id,latency_us\n");
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
        std::sort(out, out + nd, [](auto& a, auto& b) {
            return a.range_bin != b.range_bin ? a.range_bin < b.range_bin
                                              : a.doppler_bin < b.doppler_bin;
        });
        uint64_t fid = g_frames_done.load(), t_arr = 0;
        {
            std::lock_guard lk(g_ts_mu);
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
        if (t_arr) fprintf(lat_f, "%lu,%.2f\n", (unsigned long)fid, (now_ns() - t_arr) / 1e3);
        fflush(det_f);
        fflush(lat_f);
        g_frames_done.fetch_add(1);
    }
};

int main(int argc, char** argv) {
    if (argc != 11) { fprintf(stderr, "bad args\n"); return 2; }
    g_cfg = {atoi(argv[1]), atoi(argv[2]), atoi(argv[3]), atoi(argv[4]),
             atoi(argv[5]), atoi(argv[6]), atoi(argv[8]), (float)atof(argv[7]),
             argv[9], argv[10]};

    gr::Graph flow;
    auto& src = flow.emplaceBlock<ChirpSource>();
    auto& rfft = flow.emplaceBlock<RangeFft>();
    auto& sink = flow.emplaceBlock<DetectSink>();
    if (!flow.connect<"out", "in">(src, rfft) || !flow.connect<"out", "in">(rfft, sink)) {
        fprintf(stderr, "connect failed\n");
        return 1;
    }

    gr::scheduler::Simple<gr::scheduler::ExecutionPolicy::multiThreaded> sched;
    if (auto r = sched.exchange(std::move(flow)); !r) { fprintf(stderr, "exchange failed\n"); return 1; }
    auto ret = sched.runAndWait();
    printf("gnuradio4: processed %d frames\n", g_frames_done.load());
    return g_frames_done.load() >= g_cfg.frames && ret.has_value() ? 0 : 1;
}
