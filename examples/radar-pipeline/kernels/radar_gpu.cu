/* CUDA (cuFFT) implementation of the radar kernel ABI in radar.h.
 *
 * Exports the same rk_* symbols as radar_cpu.c, so the Tomii plugin and graph
 * are unchanged — point RADAR_KERNELS_DIR at the gpu/ build directory and the
 * compute moves to the GPU. Buffer handles (rd/power/dets) are DEVICE
 * pointers; the plugin treats them as opaque and only kernels dereference
 * them. Chirp IQ arrives in host memory (H2D per chirp); detections return to
 * the host in rk_cluster.
 *
 * Concurrency model: each workspace (one per task instance) owns a CUDA
 * stream; every rk_ call synchronizes its stream before returning so Tomii's
 * barrier semantics (a successor runs only after its predecessors returned)
 * carry over to device work.
 *
 * Algorithm parity with radar_cpu.c / data/reference_check.py: symmetric Hann
 * windows, per-cell CA-CFAR with wrapping training window (direct window sums
 * on the GPU instead of the CPU's sliding-window formulation — same result),
 * strongest-cell-per-cluster grouping (on host, detection counts are tiny).
 *
 * First build/validation must happen on a CUDA machine (this file is drafted
 * off-GPU): make gpu && RADAR_KERNELS_DIR=$PWD/kernels/gpu python3 run_bench.py
 */
#include "radar.h"

#include <cuda_runtime.h>
#include <cufft.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CUDA_CK(x)                                                             \
    do {                                                                       \
        cudaError_t err_ = (x);                                                \
        if (err_ != cudaSuccess) {                                             \
            fprintf(stderr, "CUDA error %s:%d: %s\n", __FILE__, __LINE__,      \
                    cudaGetErrorString(err_));                                 \
            abort();                                                           \
        }                                                                      \
    } while (0)

#define CUFFT_CK(x)                                                            \
    do {                                                                       \
        cufftResult err_ = (x);                                                \
        if (err_ != CUFFT_SUCCESS) {                                           \
            fprintf(stderr, "cuFFT error %s:%d: %d\n", __FILE__, __LINE__,     \
                    (int)err_);                                                \
            abort();                                                           \
        }                                                                      \
    } while (0)

typedef struct {
    uint32_t n, m, frame_wnd, n_tiles, guard, train, max_dets;
    float pfa_scale;
    float *d_win_r; /* device Hann, length n */
    float *d_win_d; /* device Hann, length m */
} rk_ctx;

typedef struct {
    cufftHandle plan;
    cudaStream_t stream;
    int16_t *d_iq;         /* range ws: raw chirp staging (2n i16) */
    cufftComplex *d_work;  /* range: n; doppler: tile_rows * m     */
    const rk_ctx *ctx;
} rk_ws;

static float *make_hann_device(uint32_t len) {
    float *h = (float *)malloc(len * sizeof(float));
    const float two_pi = 6.28318530717958647692f;
    for (uint32_t i = 0; i < len; i++)
        h[i] = 0.5f * (1.0f - cosf(two_pi * i / (float)(len - 1)));
    float *d = NULL;
    CUDA_CK(cudaMalloc(&d, len * sizeof(float)));
    CUDA_CK(cudaMemcpy(d, h, len * sizeof(float), cudaMemcpyHostToDevice));
    free(h);
    return d;
}

extern "C" void *rk_init(uint32_t n_samples, uint32_t n_chirps,
                         uint32_t frame_wnd, uint32_t n_tiles, uint32_t guard,
                         uint32_t train, float pfa_scale,
                         uint32_t max_dets_per_tile) {
    rk_ctx *ctx = (rk_ctx *)calloc(1, sizeof(rk_ctx));
    ctx->n = n_samples;
    ctx->m = n_chirps;
    ctx->frame_wnd = frame_wnd;
    ctx->n_tiles = n_tiles;
    ctx->guard = guard;
    ctx->train = train;
    ctx->pfa_scale = pfa_scale;
    ctx->max_dets = max_dets_per_tile;
    ctx->d_win_r = make_hann_device(n_samples);
    ctx->d_win_d = make_hann_device(n_chirps);
    return ctx;
}

extern "C" void rk_free(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    cudaFree(ctx->d_win_r);
    cudaFree(ctx->d_win_d);
    free(ctx);
}

extern "C" void *rk_make_range_ws(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)calloc(1, sizeof(rk_ws));
    ws->ctx = ctx;
    CUDA_CK(cudaStreamCreateWithFlags(&ws->stream, cudaStreamNonBlocking));
    CUDA_CK(cudaMalloc(&ws->d_iq, 2u * ctx->n * sizeof(int16_t)));
    CUDA_CK(cudaMalloc(&ws->d_work, ctx->n * sizeof(cufftComplex)));
    CUFFT_CK(cufftPlan1d(&ws->plan, (int)ctx->n, CUFFT_C2C, 1));
    CUFFT_CK(cufftSetStream(ws->plan, ws->stream));
    return ws;
}

extern "C" void *rk_make_doppler_ws(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)calloc(1, sizeof(rk_ws));
    ws->ctx = ctx;
    const uint32_t rows = ctx->n / ctx->n_tiles;
    CUDA_CK(cudaStreamCreateWithFlags(&ws->stream, cudaStreamNonBlocking));
    CUDA_CK(cudaMalloc(&ws->d_work, (size_t)rows * ctx->m * sizeof(cufftComplex)));
    /* Batched m-point FFTs over `rows` contiguous rows of the range-major rd
     * matrix: stride 1, dist m — exactly the rd layout. */
    int nfft = (int)ctx->m;
    CUFFT_CK(cufftPlanMany(&ws->plan, 1, &nfft, NULL, 1, nfft, NULL, 1, nfft,
                           CUFFT_C2C, (int)rows));
    CUFFT_CK(cufftSetStream(ws->plan, ws->stream));
    return ws;
}

extern "C" void rk_free_ws(void *ws_p) {
    rk_ws *ws = (rk_ws *)ws_p;
    cufftDestroy(ws->plan);
    if (ws->d_iq)
        cudaFree(ws->d_iq);
    cudaFree(ws->d_work);
    cudaStreamDestroy(ws->stream);
    free(ws);
}

extern "C" void *rk_alloc_rd(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    void *d = NULL;
    CUDA_CK(cudaMalloc(
        &d, (size_t)ctx->frame_wnd * ctx->n * ctx->m * sizeof(cufftComplex)));
    return d;
}

extern "C" void *rk_alloc_power(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    void *d = NULL;
    CUDA_CK(cudaMalloc(&d,
                       (size_t)ctx->frame_wnd * ctx->n * ctx->m * sizeof(float)));
    return d;
}

/* dets layout matches radar_cpu.c: per (slot, tile) {u32 count; dets[max]} */
static size_t dets_stride(const rk_ctx *ctx) {
    return sizeof(uint32_t) + (size_t)ctx->max_dets * sizeof(rk_detection);
}

extern "C" void *rk_alloc_dets(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    void *d = NULL;
    size_t bytes = (size_t)ctx->frame_wnd * ctx->n_tiles * dets_stride(ctx);
    CUDA_CK(cudaMalloc(&d, bytes));
    CUDA_CK(cudaMemset(d, 0, bytes));
    return d;
}

extern "C" void rk_free_buf(void *buf) { cudaFree(buf); }

/* ---------------------------------------------------------------------- */

__global__ void k_window_i16(const int16_t *iq, const float *win,
                             cufftComplex *out, uint32_t n) {
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) {
        out[i].x = (float)iq[2 * i] * win[i];
        out[i].y = (float)iq[2 * i + 1] * win[i];
    }
}

/* Corner turn: FFT'd chirp -> strided column `chirp` of the range-major rd. */
__global__ void k_corner_turn(const cufftComplex *fft, cufftComplex *rd_col,
                              uint32_t n, uint32_t m) {
    uint32_t r = blockIdx.x * blockDim.x + threadIdx.x;
    if (r < n)
        rd_col[(size_t)r * m] = fft[r];
}

extern "C" void rk_range_fft(void *ctx_p, void *ws_p, const int16_t *iq,
                             uint32_t n_samples, uint32_t chirp_id, void *rd_p,
                             uint32_t slot) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)ws_p;
    (void)n_samples;
    const uint32_t n = ctx->n, m = ctx->m;

    CUDA_CK(cudaMemcpyAsync(ws->d_iq, iq, 2u * n * sizeof(int16_t),
                            cudaMemcpyHostToDevice, ws->stream));
    const uint32_t tpb = 256;
    k_window_i16<<<(n + tpb - 1) / tpb, tpb, 0, ws->stream>>>(
        ws->d_iq, ctx->d_win_r, ws->d_work, n);
    CUFFT_CK(cufftExecC2C(ws->plan, ws->d_work, ws->d_work, CUFFT_FORWARD));
    cufftComplex *rd_col =
        (cufftComplex *)rd_p + (size_t)slot * n * m + chirp_id;
    k_corner_turn<<<(n + tpb - 1) / tpb, tpb, 0, ws->stream>>>(ws->d_work,
                                                               rd_col, n, m);
    CUDA_CK(cudaStreamSynchronize(ws->stream));
}

__global__ void k_window_rows(const cufftComplex *src, const float *win,
                              cufftComplex *dst, uint32_t rows, uint32_t m) {
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < rows * m) {
        float w = win[i % m];
        dst[i].x = src[i].x * w;
        dst[i].y = src[i].y * w;
    }
}

__global__ void k_magnitude(const cufftComplex *src, float *dst,
                            uint32_t count) {
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count)
        dst[i] = src[i].x * src[i].x + src[i].y * src[i].y;
}

extern "C" void rk_doppler_fft(void *ctx_p, void *ws_p, uint32_t tile,
                               const void *rd_p, void *power_p, uint32_t slot) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)ws_p;
    const uint32_t n = ctx->n, m = ctx->m;
    const uint32_t rows = n / ctx->n_tiles;
    const uint32_t r0 = tile * rows;
    const size_t off = (size_t)slot * n * m + (size_t)r0 * m;
    const uint32_t count = rows * m;
    const uint32_t tpb = 256;

    k_window_rows<<<(count + tpb - 1) / tpb, tpb, 0, ws->stream>>>(
        (const cufftComplex *)rd_p + off, ctx->d_win_d, ws->d_work, rows, m);
    CUFFT_CK(cufftExecC2C(ws->plan, ws->d_work, ws->d_work, CUFFT_FORWARD));
    k_magnitude<<<(count + tpb - 1) / tpb, tpb, 0, ws->stream>>>(
        ws->d_work, (float *)power_p + off, count);
    CUDA_CK(cudaStreamSynchronize(ws->stream));
}

/* Per-cell CA-CFAR with wrap on both axes; direct window sums (the window is
 * (2*(guard+train)+1)^2 <= ~441 reads per thread). */
__global__ void k_cfar(const float *power, uint32_t n, uint32_t m, uint32_t r0,
                       uint32_t rows, int32_t K, int32_t G, float pfa_scale,
                       float n_train, uint8_t *tile_block, uint32_t max_dets) {
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= rows * m)
        return;
    uint32_t r = r0 + i / m;
    uint32_t d = i % m;

    float s_out = 0.0f, s_in = 0.0f;
    for (int32_t dr = -K; dr <= K; dr++) {
        uint32_t rr = (uint32_t)((int32_t)r + dr + (int32_t)n) % n;
        const float *prow = power + (size_t)rr * m;
        bool in_guard_r = dr >= -G && dr <= G;
        for (int32_t dd = -K; dd <= K; dd++) {
            uint32_t cc = (uint32_t)((int32_t)d + dd + (int32_t)m) % m;
            float v = prow[cc];
            s_out += v;
            if (in_guard_r && dd >= -G && dd <= G)
                s_in += v;
        }
    }
    float noise_sum = s_out - s_in;
    float p = power[(size_t)r * m + d];
    if (p > pfa_scale / n_train * noise_sum) {
        uint32_t *count = (uint32_t *)tile_block;
        rk_detection *out = (rk_detection *)(tile_block + sizeof(uint32_t));
        uint32_t idx = atomicAdd(count, 1u);
        if (idx < max_dets) {
            out[idx].range_bin = r;
            out[idx].doppler_bin = d;
            out[idx].power = p;
        }
    }
}

extern "C" uint32_t rk_cfar(void *ctx_p, uint32_t tile, const void *power_p,
                            void *dets_p, uint32_t slot) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    const uint32_t n = ctx->n, m = ctx->m;
    const uint32_t rows = n / ctx->n_tiles;
    const int32_t K = (int32_t)(ctx->guard + ctx->train);
    const int32_t G = (int32_t)ctx->guard;
    const float n_train =
        (float)((2 * K + 1) * (2 * K + 1) - (2 * G + 1) * (2 * G + 1));

    uint8_t *tile_block = (uint8_t *)dets_p +
                          ((size_t)slot * ctx->n_tiles + tile) * dets_stride(ctx);
    /* CFAR has no dedicated workspace/stream; use the default stream. Reset the
     * count, run, and read the count back (implicit sync via D2H copy). */
    CUDA_CK(cudaMemset(tile_block, 0, sizeof(uint32_t)));
    const uint32_t count_cells = rows * m, tpb = 256;
    k_cfar<<<(count_cells + tpb - 1) / tpb, tpb>>>(
        (const float *)power_p + (size_t)slot * n * m, n, m, tile * rows, rows,
        K, G, ctx->pfa_scale, n_train, tile_block, ctx->max_dets);
    CUDA_CK(cudaGetLastError());
    uint32_t cnt = 0;
    CUDA_CK(cudaMemcpy(&cnt, tile_block, sizeof(uint32_t),
                       cudaMemcpyDeviceToHost));
    return cnt < ctx->max_dets ? cnt : ctx->max_dets;
}

/* ---------------------------------------------------------------------- */

static inline uint32_t circ_diff(uint32_t a, uint32_t b, uint32_t size) {
    uint32_t d = a > b ? a - b : b - a;
    return d < size - d ? d : size - d;
}

extern "C" uint32_t rk_cluster(void *ctx_p, const void *dets_p, uint32_t slot,
                               rk_detection *out, uint32_t out_cap) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    const size_t stride = dets_stride(ctx);
    const size_t slot_bytes = (size_t)ctx->n_tiles * stride;
    uint8_t *host = (uint8_t *)malloc(slot_bytes);
    CUDA_CK(cudaMemcpy(host,
                       (const uint8_t *)dets_p + (size_t)slot * slot_bytes,
                       slot_bytes, cudaMemcpyDeviceToHost));

    const uint32_t cap = ctx->n_tiles * ctx->max_dets;
    rk_detection *all = (rk_detection *)malloc(cap * sizeof(rk_detection));
    uint32_t total = 0;
    for (uint32_t t = 0; t < ctx->n_tiles; t++) {
        const uint8_t *block = host + (size_t)t * stride;
        uint32_t cnt = *(const uint32_t *)block;
        if (cnt > ctx->max_dets)
            cnt = ctx->max_dets;
        const rk_detection *d = (const rk_detection *)(block + sizeof(uint32_t));
        for (uint32_t i = 0; i < cnt; i++)
            all[total++] = d[i];
    }

    /* Union-find, 8-connected with wrap; strongest cell per cluster. */
    uint32_t *parent = (uint32_t *)malloc(total * sizeof(uint32_t));
    for (uint32_t i = 0; i < total; i++)
        parent[i] = i;
    auto find = [&](uint32_t x) {
        while (parent[x] != x)
            x = parent[x] = parent[parent[x]];
        return x;
    };
    for (uint32_t i = 0; i < total; i++)
        for (uint32_t j = i + 1; j < total; j++)
            if (circ_diff(all[i].range_bin, all[j].range_bin, ctx->n) <= 1 &&
                circ_diff(all[i].doppler_bin, all[j].doppler_bin, ctx->m) <= 1)
                parent[find(i)] = find(j);

    uint32_t n_out = 0;
    for (uint32_t i = 0; i < total; i++) {
        if (find(i) != i)
            continue;
        rk_detection best = all[i];
        for (uint32_t j = 0; j < total; j++)
            if (find(j) == i && all[j].power > best.power)
                best = all[j];
        if (n_out < out_cap)
            out[n_out++] = best;
    }

    free(parent);
    free(all);
    free(host);
    return n_out;
}
