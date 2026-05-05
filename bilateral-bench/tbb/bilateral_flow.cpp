/// TBB flow_graph bilateral image denoising benchmark.
///
/// Implements the same 2D wavefront DAG as taskflow/bilateral_wavefront.cpp:
/// each continue_node T(i,j) applies the bilateral filter to one image tile.
/// T(i,j) depends on T(i-1,j) and T(i,j-1) via make_edge.
///
/// Worker count is controlled via tbb::global_control (the native mechanism
/// for flow_graph, which bypasses task_arena).  No core pinning: arena-bound
/// PinningObserver does not observe global-pool threads.
///
/// CSV schema matches taskflow/bilateral_wavefront.cpp:
///   system,image_size,tile_size,kernel_radius,threads,time_ms,psnr_db,grid_n
///
/// Usage:
///   ./bilateral_flow \
///     --image-size 4096 --tile-size 256 --kernel-radius 4 \
///     --sigma-s 3.0 --sigma-r 0.1 \
///     --threads 8 --iterations 10 --warmup 2 \
///     --data-dir ../data --output results/tbb_flow_bilateral.csv

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <oneapi/tbb/flow_graph.h>
#include <oneapi/tbb/global_control.h>

namespace flow = tbb::flow;

// ---------------------------------------------------------------------------
// npy loader (float32, 2-D, C-order) — copied from taskflow reference
// ---------------------------------------------------------------------------

static std::vector<float> load_npy_f32(const std::string& path, int& H, int& W) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("Cannot open: " + path);

    char magic[6];
    f.read(magic, 6);
    if (std::string(magic, 6) != "\x93NUMPY")
        throw std::runtime_error("Not a .npy file: " + path);

    uint8_t major, minor;
    f.read(reinterpret_cast<char*>(&major), 1);
    f.read(reinterpret_cast<char*>(&minor), 1);

    uint16_t hdr_len16 = 0;
    uint32_t hdr_len32 = 0;
    size_t   hdr_len   = 0;
    if (major == 1) {
        f.read(reinterpret_cast<char*>(&hdr_len16), 2);
        hdr_len = hdr_len16;
    } else {
        f.read(reinterpret_cast<char*>(&hdr_len32), 4);
        hdr_len = hdr_len32;
    }

    std::string hdr(hdr_len, '\0');
    f.read(hdr.data(), hdr_len);

    size_t shape_pos = hdr.find("'shape'");
    if (shape_pos == std::string::npos) shape_pos = hdr.find("\"shape\"");
    if (shape_pos == std::string::npos) throw std::runtime_error("No shape in header");
    std::string shape_sub = hdr.substr(shape_pos);
    size_t lp = shape_sub.find('(');
    std::string nums = shape_sub.substr(lp + 1);
    {
        size_t p = 0;
        while (p < nums.size() && !std::isdigit(nums[p])) ++p;
        int v = 0;
        while (p < nums.size() && std::isdigit(nums[p])) v = v*10+(nums[p++]-'0');
        H = v;
        while (p < nums.size() && nums[p] == ',') ++p;
        while (p < nums.size() && !std::isdigit(nums[p])) ++p;
        v = 0;
        while (p < nums.size() && std::isdigit(nums[p])) v = v*10+(nums[p++]-'0');
        W = v;
    }

    size_t n = static_cast<size_t>(H) * W;
    std::vector<float> data(n);
    f.read(reinterpret_cast<char*>(data.data()), n * sizeof(float));
    if (!f) throw std::runtime_error("Short read from: " + path);
    return data;
}

// ---------------------------------------------------------------------------
// Benchmark state
// ---------------------------------------------------------------------------

struct BenchmarkState {
    std::vector<float> noisy_image;
    std::vector<float> output_image;
    std::vector<float> clean_image;
    int   image_height  = 0;
    int   image_width   = 0;
    int   tile_size     = 256;
    int   grid_n        = 0;
    float sigma_s       = 3.0f;
    float sigma_r       = 0.1f;
    int   kernel_radius = 4;
};

// ---------------------------------------------------------------------------
// Bilateral filter — one tile (identical to taskflow reference)
// ---------------------------------------------------------------------------

static void bilateral_filter_tile(BenchmarkState& st, int ti, int tj) {
    const int   T       = st.tile_size;
    const int   r       = st.kernel_radius;
    const int   H       = st.image_height;
    const int   W       = st.image_width;
    const float inv_2ss = 1.0f / (2.0f * st.sigma_s * st.sigma_s);
    const float inv_2sr = 1.0f / (2.0f * st.sigma_r * st.sigma_r);

    const int kw = 2 * r + 1;
    std::vector<float> spatial_w(static_cast<size_t>(kw) * kw);
    for (int di = -r; di <= r; ++di)
        for (int dj = -r; dj <= r; ++dj)
            spatial_w[(di + r) * kw + (dj + r)] =
                std::exp(-(static_cast<float>(di*di + dj*dj)) * inv_2ss);

    const int    row_start = ti * T;
    const int    col_start = tj * T;
    const float* src       = st.noisy_image.data();
    float*       dst       = st.output_image.data();

    for (int pi = row_start; pi < row_start + T; ++pi) {
        for (int pj = col_start; pj < col_start + T; ++pj) {
            const float Ip = src[pi * W + pj];
            float sum_w = 0.0f, sum_wI = 0.0f;
            for (int di = -r; di <= r; ++di) {
                for (int dj = -r; dj <= r; ++dj) {
                    const int   qi         = std::clamp(pi + di, 0, H - 1);
                    const int   qj         = std::clamp(pj + dj, 0, W - 1);
                    const float Iq         = src[qi * W + qj];
                    const float range_term = (Ip - Iq) * (Ip - Iq) * inv_2sr;
                    const float w          = spatial_w[(di + r) * kw + (dj + r)] *
                                             std::exp(-range_term);
                    sum_w  += w;
                    sum_wI += w * Iq;
                }
            }
            dst[pi * W + pj] = sum_wI / sum_w;
        }
    }
}

// ---------------------------------------------------------------------------
// PSNR
// ---------------------------------------------------------------------------

static double compute_psnr(const std::vector<float>& output,
                            const std::vector<float>& clean) {
    double mse = 0.0;
    for (size_t i = 0; i < output.size(); ++i) {
        double d = static_cast<double>(output[i]) - static_cast<double>(clean[i]);
        mse += d * d;
    }
    mse /= static_cast<double>(output.size());
    return 10.0 * std::log10(1.0 / mse);
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

static void append_csv(const std::string& path,
                        const std::string& system,
                        int image_size, int tile_size, int kernel_radius,
                        int threads, double time_ms, double psnr_db, int grid_n) {
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,image_size,tile_size,kernel_radius,threads,time_ms,psnr_db,grid_n\n";
    f << system << ',' << image_size << ',' << tile_size << ','
      << kernel_radius << ',' << threads << ','
      << time_ms << ',' << psnr_db << ',' << grid_n << '\n';
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli {
    int    image_size    = 4096;
    int    tile_size     = 256;
    int    kernel_radius = 4;
    float  sigma_s       = 3.0f;
    float  sigma_r       = 0.1f;
    int    threads       = 1;
    int    iterations    = 10;
    int    warmup        = 2;
    std::string data_dir = "../data";
    std::string output   = "results/tbb_flow_bilateral.csv";
};

static Cli parse_args(int argc, char** argv) {
    Cli c;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto val = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + a);
            return argv[++i];
        };
        if      (a == "--image-size")    c.image_size    = std::stoi(val());
        else if (a == "--tile-size")     c.tile_size     = std::stoi(val());
        else if (a == "--kernel-radius") c.kernel_radius = std::stoi(val());
        else if (a == "--sigma-s")       c.sigma_s       = std::stof(val());
        else if (a == "--sigma-r")       c.sigma_r       = std::stof(val());
        else if (a == "--threads")       c.threads       = std::stoi(val());
        else if (a == "--iterations")    c.iterations    = std::stoi(val());
        else if (a == "--warmup")        c.warmup        = std::stoi(val());
        else if (a == "--data-dir")      c.data_dir      = val();
        else if (a == "--output")        c.output        = val();
        else throw std::runtime_error("Unknown flag: " + a);
    }
    return c;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Cli cli = parse_args(argc, argv);

    const int S = cli.image_size;
    const int T = cli.tile_size;
    if (S % T != 0) {
        std::fprintf(stderr, "image_size (%d) must be divisible by tile_size (%d)\n", S, T);
        return 1;
    }
    const int N = S / T;

    std::printf("TBB flow_graph Bilateral | image=%dx%d tile=%d kernel_r=%d "
                "sigma_s=%.1f sigma_r=%.2f threads=%d\n",
                S, S, T, cli.kernel_radius, cli.sigma_s, cli.sigma_r, cli.threads);

    BenchmarkState st;
    st.image_height  = S;
    st.image_width   = S;
    st.tile_size     = T;
    st.grid_n        = N;
    st.sigma_s       = cli.sigma_s;
    st.sigma_r       = cli.sigma_r;
    st.kernel_radius = cli.kernel_radius;

    {
        int H2, W2;
        std::string noisy_path = cli.data_dir + "/noisy_" + std::to_string(S) + "x" + std::to_string(S) + ".npy";
        std::string clean_path = cli.data_dir + "/clean_" + std::to_string(S) + "x" + std::to_string(S) + ".npy";
        st.noisy_image = load_npy_f32(noisy_path, H2, W2);
        if (H2 != S || W2 != S)
            throw std::runtime_error("Noisy image size mismatch");
        st.clean_image = load_npy_f32(clean_path, H2, W2);
        st.output_image.assign(static_cast<size_t>(S) * S, 0.0f);
    }

    // Limit TBB's global pool to `threads` total (main + workers).
    tbb::global_control gc(tbb::global_control::max_allowed_parallelism, cli.threads);

    // Build the flow graph once — reset and re-trigger each iteration.
    using cnode = flow::continue_node<flow::continue_msg>;
    flow::graph g;
    flow::broadcast_node<flow::continue_msg> start(g);

    BenchmarkState* st_ptr = &st;
    std::vector<std::vector<std::unique_ptr<cnode>>> tiles(N);
    for (int i = 0; i < N; ++i) {
        tiles[i].resize(N);
        for (int j = 0; j < N; ++j) {
            tiles[i][j] = std::make_unique<cnode>(
                g,
                [st_ptr, i, j](flow::continue_msg) {
                    bilateral_filter_tile(*st_ptr, i, j);
                }
            );
        }
    }

    flow::make_edge(start, *tiles[0][0]);
    for (int i = 0; i < N; ++i) {
        for (int j = 0; j < N; ++j) {
            if (i > 0) flow::make_edge(*tiles[i-1][j], *tiles[i][j]);
            if (j > 0) flow::make_edge(*tiles[i][j-1], *tiles[i][j]);
        }
    }

    auto run_once = [&] {
        g.reset();
        start.try_put(flow::continue_msg{});
        g.wait_for_all();
    };

    for (int w = 0; w < cli.warmup; ++w) run_once();

    std::vector<double> times;
    times.reserve(cli.iterations);
    for (int it = 0; it < cli.iterations; ++it) {
        auto t0 = std::chrono::high_resolution_clock::now();
        run_once();
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        times.push_back(ms);
        std::printf("  iter %2d: %.2f ms\n", it + 1, ms);
    }

    double sum = 0.0;
    for (double t : times) sum += t;
    double mean_ms = sum / times.size();

    double psnr = compute_psnr(st.output_image, st.clean_image);
    std::printf("PSNR: %.2f dB\n", psnr);
    if (psnr < 28.0)
        std::fprintf(stderr, "WARNING: PSNR=%.2f dB below 28 dB threshold\n", psnr);

    std::printf("tbb_flow | image=%d tile=%d kr=%d threads=%d | mean=%.2f ms | PSNR=%.2f dB\n",
                S, T, cli.kernel_radius, cli.threads, mean_ms, psnr);

    append_csv(cli.output, "tbb_flow", S, T, cli.kernel_radius,
               cli.threads, mean_ms, psnr, N);
    return 0;
}
