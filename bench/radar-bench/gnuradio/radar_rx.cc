// C++ GNU Radio 3.10 baseline — same flowgraph as radar_rx.py with the two
// custom blocks (UDP chirp source, radar detect sink) as native C++
// gr::sync_block subclasses. DSP identical: gr::fft::fft_vcc (FFTW) range FFT,
// Doppler/CFAR/cluster via libradar_kernels.so. Latency = first-packet
// arrival -> detections written, per frame.
//
// Usage: radar_rx_cpp <port> <n_samples> <n_chirps> <tiles> <guard> <train>
//                     <pfa_scale> <frames> <det_path> <lat_path>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <chrono>
#include <cmath>
#include <complex>
#include <cstdio>
#include <cstring>
#include <deque>
#include <set>
#include <thread>
#include <vector>

#include <gnuradio/blocks/stream_to_vector.h>
#include <gnuradio/fft/fft_v.h>
#include <gnuradio/io_signature.h>
#include <gnuradio/sync_block.h>
#include <gnuradio/top_block.h>
#include <pmt/pmt.h>

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

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class chirp_udp_source : public gr::sync_block {
public:
    chirp_udp_source(int port, int n_samples, int frames)
        : gr::sync_block("chirp_udp_source",
                         gr::io_signature::make(0, 0, 0),
                         gr::io_signature::make(1, 1, sizeof(gr_complex))),
          n_(n_samples), expected_(frames) {
        fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        int buf = 32 << 20;
        setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
        timeval tv{0, 200000};
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        bind(fd_, (sockaddr*)&addr, sizeof(addr));
        pkt_.resize(64 + 4 * n_samples);
    }

    int work(int nout, gr_vector_const_void_star&, gr_vector_void_star& out_v) override {
        gr_complex* out = (gr_complex*)out_v[0];
        int produced = 0;
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
            if (done_) break;
            ssize_t r = recv(fd_, pkt_.data(), pkt_.size(), 0);
            if (r < 0) {
                if (!seen_.empty() && (int)seen_.size() >= expected_) done_ = true;
                break;
            }
            uint64_t t = now_ns();
            uint32_t fid, chirp;
            std::memcpy(&fid, pkt_.data(), 4);
            std::memcpy(&chirp, pkt_.data() + 4, 4);
            const int16_t* iq = (const int16_t*)(pkt_.data() + 64);
            std::vector<gr_complex> s(n_);
            for (int i = 0; i < n_; i++) s[i] = {(float)iq[2 * i], (float)iq[2 * i + 1]};
            if (chirp == 0) tags_.push_back({abs_in_, fid, t});
            abs_in_ += n_;
            seen_.insert(fid);
            chunks_.push_back(std::move(s));
        }
        while (!tags_.empty() && tags_.front().idx < abs_out_ + produced) {
            auto& tg = tags_.front();
            add_item_tag(0, nitems_written(0) + (tg.idx - abs_out_), pmt::intern("frame"),
                         pmt::make_tuple(pmt::from_uint64(tg.fid), pmt::from_uint64(tg.t)));
            tags_.pop_front();
        }
        abs_out_ += produced;
        return (produced == 0 && done_) ? -1 : produced;
    }

private:
    struct Tag { uint64_t idx; uint32_t fid; uint64_t t; };
    int n_, expected_, fd_, off_ = 0;
    uint64_t abs_in_ = 0, abs_out_ = 0;
    bool done_ = false;
    std::deque<std::vector<gr_complex>> chunks_;
    std::deque<Tag> tags_;
    std::set<uint32_t> seen_;
    std::vector<uint8_t> pkt_;
};

class radar_detect : public gr::sync_block {
public:
    volatile int frames_done = 0;

    radar_detect(int n, int m, int tiles, int guard, int train, float scale,
                 const char* det_path, const char* lat_path)
        : gr::sync_block("radar_detect",
                         gr::io_signature::make(1, 1, sizeof(gr_complex) * n * m),
                         gr::io_signature::make(0, 0, 0)),
          n_(n), m_(m), tiles_(tiles) {
        ctx_ = rk_init(n, m, 1, tiles, guard, train, scale, 64);
        dws_ = rk_make_doppler_ws(ctx_);
        rd_ = rk_alloc_rd(ctx_);
        power_ = rk_alloc_power(ctx_);
        dets_ = rk_alloc_dets(ctx_);
        det_f_ = fopen(det_path, "w");
        lat_f_ = fopen(lat_path, "w");
        fprintf(lat_f_, "frame_id,latency_us\n");
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
            std::sort(out, out + nd, [](auto& a, auto& b) {
                return a.range_bin != b.range_bin ? a.range_bin < b.range_bin
                                                  : a.doppler_bin < b.doppler_bin;
            });
            uint64_t fid = frames_done, t_arr = 0;
            if (i < (int)tags.size()) {
                fid = pmt::to_uint64(pmt::tuple_ref(tags[i].value, 0));
                t_arr = pmt::to_uint64(pmt::tuple_ref(tags[i].value, 1));
            }
            fprintf(det_f_, "frame %lu:", (unsigned long)fid);
            for (uint32_t k = 0; k < nd; k++)
                fprintf(det_f_, " %u,%u,%.3e", out[k].range_bin, out[k].doppler_bin, out[k].power);
            fprintf(det_f_, "\n");
            if (t_arr) fprintf(lat_f_, "%lu,%.2f\n", (unsigned long)fid, (now_ns() - t_arr) / 1e3);
            frames_done++;
        }
        fflush(det_f_);
        fflush(lat_f_);
        return nin;
    }

private:
    int n_, m_, tiles_;
    void *ctx_, *dws_, *rd_, *power_, *dets_;
    FILE *det_f_, *lat_f_;
};

int main(int argc, char** argv) {
    if (argc != 11) { fprintf(stderr, "bad args\n"); return 2; }
    int port = atoi(argv[1]), n = atoi(argv[2]), m = atoi(argv[3]), tiles = atoi(argv[4]);
    int guard = atoi(argv[5]), train = atoi(argv[6]);
    float scale = atof(argv[7]);
    int frames = atoi(argv[8]);

    std::vector<float> win(n);
    for (int i = 0; i < n; i++) win[i] = 0.5f * (1.0f - cosf(2.0f * M_PI * i / (n - 1)));

    auto tb = gr::make_top_block("radar_rx_cpp");
    auto src = std::make_shared<chirp_udp_source>(port, n, frames);
    auto to_vec = gr::blocks::stream_to_vector::make(sizeof(gr_complex), n);
    auto rfft = gr::fft::fft_v<gr_complex, true>::make(n, win, false, 1);
    auto to_frame = gr::blocks::stream_to_vector::make(sizeof(gr_complex) * n, m);
    auto sink = std::make_shared<radar_detect>(n, m, tiles, guard, train, scale,
                                               argv[9], argv[10]);
    tb->connect(src, 0, to_vec, 0);
    tb->connect(to_vec, 0, rfft, 0);
    tb->connect(rfft, 0, to_frame, 0);
    tb->connect(to_frame, 0, sink, 0);

    auto t0 = std::chrono::steady_clock::now();
    tb->start();
    while (sink->frames_done < frames &&
           std::chrono::steady_clock::now() - t0 < std::chrono::seconds(600))
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    tb->stop();
    tb->wait();
    printf("gnuradio-cpp: processed %d frames\n", sink->frames_done);
    return sink->frames_done >= frames ? 0 : 1;
}
