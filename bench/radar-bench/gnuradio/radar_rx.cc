// C++ GNU Radio 3.10 baseline for the radar-pipeline workload.
//
// Two flowgraph topologies over the same UDP chirp stream and verifier:
//
//   serial (default, the original baseline; same flowgraph as radar_rx.py):
//     chirp_udp_source (gr_complex stream) -> stream_to_vector(n)
//       -> fft_vcc (FFTW + Hann) -> stream_to_vector(m) -> radar_detect
//          (corner turn + Doppler/CFAR/cluster, all tiles serially, via the
//           shared libradar_kernels.so)
//
//   tiled (--topology tiled, "GR 3.10 at its best": the Tomii graph's
//   intra-frame data parallelism as native GR3 blocks under the
//   thread-per-block scheduler):
//     tok_source (uint32 token per chirp; payload in a packet ring)
//       -> range_blk (rk_range_fft per chirp, emits a frame token)
//       -> T x doppler_blk(t) -> join(T) -> T x cfar_blk(t) -> cluster_sink(T)
//     so every DSP stage calls exactly the kernels Tomii calls.
//
// Latency is logged per frame as frame_id,latency_us,done_ns (see radar_rx4.cc).
//
// Usage: radar_rx_cpp <port> <n_samples> <n_chirps> <tiles> <guard> <train>
//                     <pfa_scale> <frames> <det_path> <lat_path> [options]
// Options (defaults reproduce the original baseline):
//   --topology serial|tiled   flowgraph shape                       [serial]
//   --src fill|nb|poll        source recv policy (see radar_rx4.cc)  [fill]
//   --max-noutput N           tb->start(max_noutput_items)           [GR default]
//   --min-buf N               set_min_output_buffer(N items) on every block
//   --fft-threads N           fft_vcc FFTW threads (serial topology) [1]
//   --slots K                 rd/power ring depth (tiled)            [8]
//   --pin LO-HI               pin each block's thread to its own core in LO..HI
//                             (round robin; GR3's block::set_processor_affinity)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdio>
#include <cstring>
#include <deque>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gnuradio/block.h>
#include <gnuradio/blocks/stream_to_vector.h>
#include <gnuradio/fft/fft_v.h>
#include <gnuradio/io_signature.h>
#include <gnuradio/sync_block.h>
#include <gnuradio/top_block.h>
#include <pmt/pmt.h>

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

enum class SrcMode { Fill, Nb, Poll };

static uint64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static std::atomic<int> g_frames_done{0};

// UDP receive helper shared by both sources.
struct Udp {
    int fd = -1, expected = 0;
    std::set<uint32_t> seen;
    uint64_t last_pkt_ns = 0;
    bool done = false;

    void open(int port, int frames) {
        expected = frames;
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        int buf = 32 << 20;
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
        timeval tv{0, 200000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        bind(fd, (sockaddr*)&addr, sizeof(addr));
    }
    ssize_t recv_one(uint8_t* p, size_t len, bool block, uint32_t* fid, uint32_t* chirp) {
        ssize_t r = recv(fd, p, len, block ? 0 : MSG_DONTWAIT);
        uint64_t t = now_ns();
        if (r < 0) {
            if (!seen.empty() && (int)seen.size() >= expected &&
                (block || (last_pkt_ns && t - last_pkt_ns > 200000000ull)))
                done = true;
            return r;
        }
        last_pkt_ns = t;
        std::memcpy(fid, p, 4);
        std::memcpy(chirp, p + 4, 4);
        seen.insert(*fid);
        return r;
    }
};

// Per-frame chirp-0 arrival time, indexed by frame_id (for the internal metric).
static constexpr size_t kTsRing = 1 << 16;
static std::atomic<uint64_t> g_arrival[kTsRing];

static void write_frame(FILE* det_f, FILE* lat_f, uint64_t fid, rk_detection* out, uint32_t nd) {
    std::sort(out, out + nd, [](auto& a, auto& b) {
        return a.range_bin != b.range_bin ? a.range_bin < b.range_bin : a.doppler_bin < b.doppler_bin;
    });
    fprintf(det_f, "frame %lu:", (unsigned long)fid);
    for (uint32_t k = 0; k < nd; k++)
        fprintf(det_f, " %u,%u,%.3e", out[k].range_bin, out[k].doppler_bin, out[k].power);
    fprintf(det_f, "\n");
    fflush(det_f);
    uint64_t t_done = now_ns(), t_arr = g_arrival[fid % kTsRing].load(std::memory_order_acquire);
    if (t_arr) fprintf(lat_f, "%lu,%.2f,%lu\n", (unsigned long)fid, (t_done - t_arr) / 1e3, (unsigned long)t_done);
    fflush(lat_f);
    g_frames_done.fetch_add(1);
}

// ===========================================================================
// serial topology (original baseline)
// ===========================================================================
class chirp_udp_source : public gr::sync_block {
public:
    chirp_udp_source(int port, int n_samples, int frames, SrcMode mode)
        : gr::sync_block("chirp_udp_source", gr::io_signature::make(0, 0, 0),
                         gr::io_signature::make(1, 1, sizeof(gr_complex))),
          n_(n_samples), mode_(mode) {
        udp_.open(port, frames);
        pkt_.resize(64 + 4 * n_samples);
    }

    int work(int nout, gr_vector_const_void_star&, gr_vector_void_star& out_v) override {
        gr_complex* out = (gr_complex*)out_v[0];
        int produced = 0;
        bool got_any = false;
        while (produced < nout) {
            if (!chunks_.empty()) {
                auto& c = chunks_.front();
                int avail = (int)c.size() - off_, n = std::min(nout - produced, avail);
                std::memcpy(out + produced, c.data() + off_, n * sizeof(gr_complex));
                produced += n;
                off_ += n;
                if (off_ == (int)c.size()) { chunks_.pop_front(); off_ = 0; }
                continue;
            }
            if (udp_.done) break;
            bool block = mode_ == SrcMode::Fill || (mode_ == SrcMode::Nb && !got_any);
            uint32_t fid, chirp;
            if (udp_.recv_one(pkt_.data(), pkt_.size(), block, &fid, &chirp) < 0) break;
            got_any = true;
            const int16_t* iq = (const int16_t*)(pkt_.data() + 64);
            std::vector<gr_complex> s(n_);
            for (int i = 0; i < n_; i++) s[i] = {(float)iq[2 * i], (float)iq[2 * i + 1]};
            if (chirp == 0) {
                g_arrival[fid % kTsRing].store(udp_.last_pkt_ns, std::memory_order_release);
                tags_.push_back({abs_in_, fid});
            }
            abs_in_ += n_;
            chunks_.push_back(std::move(s));
        }
        while (!tags_.empty() && tags_.front().idx < abs_out_ + produced) {
            auto& tg = tags_.front();
            add_item_tag(0, nitems_written(0) + (tg.idx - abs_out_), pmt::intern("frame"), pmt::from_uint64(tg.fid));
            tags_.pop_front();
        }
        abs_out_ += produced;
        return (produced == 0 && udp_.done) ? -1 : produced;
    }

private:
    struct Tag { uint64_t idx; uint32_t fid; };
    int n_, off_ = 0;
    SrcMode mode_;
    Udp udp_;
    uint64_t abs_in_ = 0, abs_out_ = 0;
    std::deque<std::vector<gr_complex>> chunks_;
    std::deque<Tag> tags_;
    std::vector<uint8_t> pkt_;
};

class radar_detect : public gr::sync_block {
public:
    radar_detect(int n, int m, int tiles, int guard, int train, float scale, const char* det_path, const char* lat_path)
        : gr::sync_block("radar_detect", gr::io_signature::make(1, 1, sizeof(gr_complex) * n * m),
                         gr::io_signature::make(0, 0, 0)),
          n_(n), m_(m), tiles_(tiles) {
        ctx_ = rk_init(n, m, 1, tiles, guard, train, scale, 64);
        dws_ = rk_make_doppler_ws(ctx_);
        rd_ = rk_alloc_rd(ctx_);
        power_ = rk_alloc_power(ctx_);
        dets_ = rk_alloc_dets(ctx_);
        det_f_ = fopen(det_path, "w");
        lat_f_ = fopen(lat_path, "w");
        fprintf(lat_f_, "frame_id,latency_us,done_ns\n");
    }

    int work(int nin, gr_vector_const_void_star& in_v, gr_vector_void_star&) override {
        const gr_complex* in = (const gr_complex*)in_v[0];
        std::vector<gr::tag_t> tags;
        get_tags_in_window(tags, 0, 0, nin);
        for (int i = 0; i < nin; i++) {
            const gr_complex* frame = in + (size_t)i * n_ * m_;
            std::complex<float>* rd = (std::complex<float>*)rd_;
            for (int r = 0; r < n_; r++)  // corner turn [chirp][range] -> [range][chirp]
                for (int c = 0; c < m_; c++) rd[(size_t)r * m_ + c] = frame[(size_t)c * n_ + r];
            for (int t = 0; t < tiles_; t++) rk_doppler_fft(ctx_, dws_, t, rd_, power_, 0);
            for (int t = 0; t < tiles_; t++) rk_cfar(ctx_, t, power_, dets_, 0);
            rk_detection out[1024];
            uint32_t nd = rk_cluster(ctx_, dets_, 0, out, 1024);
            uint64_t fid = g_frames_done.load();
            if (i < (int)tags.size()) fid = pmt::to_uint64(tags[i].value);
            write_frame(det_f_, lat_f_, fid, out, nd);
        }
        return nin;
    }

private:
    int n_, m_, tiles_;
    void *ctx_, *dws_, *rd_, *power_, *dets_;
    FILE *det_f_, *lat_f_;
};

// ===========================================================================
// tiled topology
// ===========================================================================
struct Shared {
    int n = 0, m = 0, slots = 8;
    void *ctx{}, *rd{}, *power{}, *dets{};
    std::vector<uint8_t> ring;
    size_t pkt_len = 0, ring_pkts = 0;
    std::atomic<uint64_t> frames_retired{0};
} g_sh;

static uint8_t* ring_at(uint32_t idx) { return g_sh.ring.data() + (size_t)(idx % g_sh.ring_pkts) * g_sh.pkt_len; }

class tok_source : public gr::sync_block {
public:
    tok_source(int port, int frames, SrcMode mode)
        : gr::sync_block("tok_source", gr::io_signature::make(0, 0, 0), gr::io_signature::make(1, 1, sizeof(uint32_t))),
          mode_(mode) {
        udp_.open(port, frames);
    }
    int work(int nout, gr_vector_const_void_star&, gr_vector_void_star& out_v) override {
        uint32_t* out = (uint32_t*)out_v[0];
        int produced = 0;
        const int cap = std::min<int>(nout, g_sh.ring_pkts / 2);
        while (produced < cap && !udp_.done) {
            bool block = mode_ == SrcMode::Fill || (mode_ == SrcMode::Nb && produced == 0);
            uint32_t fid, chirp;
            if (udp_.recv_one(ring_at(widx_), g_sh.pkt_len, block, &fid, &chirp) < 0) break;
            if (chirp == 0) g_arrival[fid % kTsRing].store(udp_.last_pkt_ns, std::memory_order_release);
            out[produced++] = widx_++;
        }
        return (produced == 0 && udp_.done) ? -1 : produced;
    }

private:
    SrcMode mode_;
    Udp udp_;
    uint32_t widx_ = 0;
};

class range_blk : public gr::block {
public:
    range_blk()
        : gr::block("range_blk", gr::io_signature::make(1, 1, sizeof(uint32_t)), gr::io_signature::make(1, 1, sizeof(uint32_t))) {
        ws_ = rk_make_range_ws(g_sh.ctx);
        count_.assign(g_sh.slots, 0);
    }
    void forecast(int, gr_vector_int& need) override { need[0] = 1; }
    int general_work(int nout, gr_vector_int& nin, gr_vector_const_void_star& in_v, gr_vector_void_star& out_v) override {
        const uint32_t* in = (const uint32_t*)in_v[0];
        uint32_t* out = (uint32_t*)out_v[0];
        int used = 0, emitted = 0;
        for (; used < nin[0]; used++) {
            const uint8_t* p = ring_at(in[used]);
            uint32_t fid, chirp;
            std::memcpy(&fid, p, 4);
            std::memcpy(&chirp, p + 4, 4);
            if (fid >= g_sh.frames_retired.load(std::memory_order_acquire) + (uint64_t)g_sh.slots) break;
            if (emitted >= nout) break;
            uint32_t slot = fid % g_sh.slots;
            rk_range_fft(g_sh.ctx, ws_, (const int16_t*)(p + 64), g_sh.n, chirp, g_sh.rd, slot);
            if (++count_[slot] == g_sh.m) {
                count_[slot] = 0;
                out[emitted++] = fid;
            }
        }
        consume_each(used);
        return emitted;
    }

private:
    void* ws_;
    std::vector<int> count_;
};

class doppler_blk : public gr::sync_block {
public:
    explicit doppler_blk(int tile)
        : gr::sync_block("doppler_blk", gr::io_signature::make(1, 1, sizeof(uint32_t)), gr::io_signature::make(1, 1, sizeof(uint32_t))),
          tile_(tile) {
        ws_ = rk_make_doppler_ws(g_sh.ctx);
    }
    int work(int n, gr_vector_const_void_star& in_v, gr_vector_void_star& out_v) override {
        const uint32_t* in = (const uint32_t*)in_v[0];
        uint32_t* out = (uint32_t*)out_v[0];
        for (int i = 0; i < n; i++) {
            rk_doppler_fft(g_sh.ctx, ws_, tile_, g_sh.rd, g_sh.power, in[i] % g_sh.slots);
            out[i] = in[i];
        }
        return n;
    }

private:
    int tile_;
    void* ws_;
};

class cfar_blk : public gr::sync_block {
public:
    explicit cfar_blk(int tile)
        : gr::sync_block("cfar_blk", gr::io_signature::make(1, 1, sizeof(uint32_t)), gr::io_signature::make(1, 1, sizeof(uint32_t))),
          tile_(tile) {}
    int work(int n, gr_vector_const_void_star& in_v, gr_vector_void_star& out_v) override {
        const uint32_t* in = (const uint32_t*)in_v[0];
        uint32_t* out = (uint32_t*)out_v[0];
        for (int i = 0; i < n; i++) {
            rk_cfar(g_sh.ctx, tile_, g_sh.power, g_sh.dets, in[i] % g_sh.slots);
            out[i] = in[i];
        }
        return n;
    }

private:
    int tile_;
};

// Barrier: a sync block with T inputs only runs once every input has the token.
class join_blk : public gr::sync_block {
public:
    explicit join_blk(int t)
        : gr::sync_block("join_blk", gr::io_signature::make(t, t, sizeof(uint32_t)), gr::io_signature::make(1, 1, sizeof(uint32_t))) {}
    int work(int n, gr_vector_const_void_star& in_v, gr_vector_void_star& out_v) override {
        std::memcpy(out_v[0], in_v[0], n * sizeof(uint32_t));
        return n;
    }
};

class cluster_sink : public gr::sync_block {
public:
    cluster_sink(int t, const char* det_path, const char* lat_path)
        : gr::sync_block("cluster_sink", gr::io_signature::make(t, t, sizeof(uint32_t)), gr::io_signature::make(0, 0, 0)) {
        det_f_ = fopen(det_path, "w");
        lat_f_ = fopen(lat_path, "w");
        fprintf(lat_f_, "frame_id,latency_us,done_ns\n");
    }
    int work(int n, gr_vector_const_void_star& in_v, gr_vector_void_star&) override {
        const uint32_t* in = (const uint32_t*)in_v[0];
        for (int i = 0; i < n; i++) {
            rk_detection out[1024];
            uint32_t nd = rk_cluster(g_sh.ctx, g_sh.dets, in[i] % g_sh.slots, out, 1024);
            write_frame(det_f_, lat_f_, in[i], out, nd);
            g_sh.frames_retired.store(in[i] + 1, std::memory_order_release);
        }
        return n;
    }

private:
    FILE *det_f_, *lat_f_;
};

int main(int argc, char** argv) {
    if (argc < 11) { fprintf(stderr, "bad args\n"); return 2; }
    int port = atoi(argv[1]), n = atoi(argv[2]), m = atoi(argv[3]), tiles = atoi(argv[4]);
    int guard = atoi(argv[5]), train = atoi(argv[6]);
    float scale = atof(argv[7]);
    int frames = atoi(argv[8]);
    std::string topology = "serial", src = "fill";
    int max_nout = 0, min_buf = 0, fft_threads = 1, slots = 8, pin_lo = -1, pin_hi = -1;
    for (int i = 11; i + 1 < argc; i += 2) {
        std::string k = argv[i], v = argv[i + 1];
        if (k == "--topology") topology = v;
        else if (k == "--src") src = v;
        else if (k == "--max-noutput") max_nout = std::stoi(v);
        else if (k == "--min-buf") min_buf = std::stoi(v);
        else if (k == "--fft-threads") fft_threads = std::stoi(v);
        else if (k == "--slots") slots = std::stoi(v);
        else if (k == "--pin") sscanf(v.c_str(), "%d-%d", &pin_lo, &pin_hi);
        else { fprintf(stderr, "unknown option %s\n", k.c_str()); return 2; }
    }
    SrcMode mode = src == "poll" ? SrcMode::Poll : src == "nb" ? SrcMode::Nb : SrcMode::Fill;
    fprintf(stderr, "gnuradio-cpp: topology=%s src=%s max_noutput=%d min_buf=%d fft_threads=%d\n", topology.c_str(),
            src.c_str(), max_nout, min_buf, fft_threads);

    auto tb = gr::make_top_block("radar_rx_cpp");
    std::vector<gr::basic_block_sptr> all;
    std::function<bool()> finished;
    if (topology == "tiled") {
        g_sh.n = n; g_sh.m = m; g_sh.slots = slots;
        g_sh.ctx = rk_init(n, m, slots, tiles, guard, train, scale, 64);
        g_sh.rd = rk_alloc_rd(g_sh.ctx);
        g_sh.power = rk_alloc_power(g_sh.ctx);
        g_sh.dets = rk_alloc_dets(g_sh.ctx);
        g_sh.pkt_len = 64 + 4 * (size_t)n;
        g_sh.ring_pkts = 1u << 15;
        g_sh.ring.assign(g_sh.ring_pkts * g_sh.pkt_len, 0);
        auto s = std::make_shared<tok_source>(port, frames, mode);
        auto r = std::make_shared<range_blk>();
        auto j = std::make_shared<join_blk>(tiles);
        auto c = std::make_shared<cluster_sink>(tiles, argv[9], argv[10]);
        tb->connect(s, 0, r, 0);
        all = {s, r, j, c};
        for (int t = 0; t < tiles; t++) {
            auto d = std::make_shared<doppler_blk>(t);
            auto f = std::make_shared<cfar_blk>(t);
            tb->connect(r, 0, d, 0);
            tb->connect(d, 0, j, t);
            tb->connect(j, 0, f, 0);
            tb->connect(f, 0, c, t);
            all.push_back(d);
            all.push_back(f);
        }
    } else {
        std::vector<float> win(n);
        for (int i = 0; i < n; i++) win[i] = 0.5f * (1.0f - cosf(2.0f * M_PI * i / (n - 1)));
        auto s = std::make_shared<chirp_udp_source>(port, n, frames, mode);
        auto to_vec = gr::blocks::stream_to_vector::make(sizeof(gr_complex), n);
        auto rfft = gr::fft::fft_v<gr_complex, true>::make(n, win, false, fft_threads);
        auto to_frame = gr::blocks::stream_to_vector::make(sizeof(gr_complex) * n, m);
        auto sink = std::make_shared<radar_detect>(n, m, tiles, guard, train, scale, argv[9], argv[10]);
        tb->connect(s, 0, to_vec, 0);
        tb->connect(to_vec, 0, rfft, 0);
        tb->connect(rfft, 0, to_frame, 0);
        tb->connect(to_frame, 0, sink, 0);
        all = {s, to_vec, rfft, to_frame, sink};
    }
    if (min_buf > 0)
        for (auto& b : all)
            if (auto bb = std::dynamic_pointer_cast<gr::block>(b)) bb->set_min_output_buffer(min_buf);

    if (pin_lo >= 0) {
        int core = pin_lo;
        for (auto& b : all)
            if (auto bb = std::dynamic_pointer_cast<gr::block>(b)) {
                bb->set_processor_affinity({core});
                fprintf(stderr, "pin: %s -> core %d\n", bb->alias().c_str(), core);
                core = core >= pin_hi ? pin_lo : core + 1;
            }
    }
    auto t0 = std::chrono::steady_clock::now();
    if (max_nout > 0) tb->start(max_nout);
    else tb->start();
    while (g_frames_done.load() < frames && std::chrono::steady_clock::now() - t0 < std::chrono::seconds(600))
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    tb->stop();
    tb->wait();
    printf("gnuradio-cpp: processed %d frames\n", g_frames_done.load());
    return g_frames_done.load() >= frames ? 0 : 1;
}
