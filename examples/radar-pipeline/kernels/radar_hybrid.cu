/* Hybrid CPU+GPU implementation of the radar kernel ABI in radar.h.
 *
 * Placement experiment ("does splitting the graph across devices buy something"):
 *   range_fft   -> CPU / FFTW   (runs per chirp, streams in as packets arrive)
 *   doppler_fft -> GPU / cuFFT  (batched m-point FFTs over a tile's rows)
 *   cfar        -> GPU          (2D CA-CFAR)
 *   cluster     -> CPU          (tiny detection counts)
 *
 * Rationale: the all-GPU twin is capped by per-chirp H2D + kernel launch + stream
 * sync (n_chirps of them per frame). Doing range on the CPU as chirps arrive
 * removes those per-chirp launches; the rd matrix is then uploaded once per tile
 * (n_tiles H2D/frame instead of n_chirps) for the batched Doppler FFT + CFAR,
 * which is where the GPU is strongest. Same rd/power/dets layout as both twins
 * (radar.h), so the graph, plugin and verifier are unchanged.
 *
 * rd is PINNED HOST memory (CPU range writes it; fast async H2D into the Doppler
 * workspace). power and dets are DEVICE memory (GPU Doppler/CFAR). fftwf_complex
 * and cufftComplex share the interleaved float2 layout, so the host rd is read
 * verbatim on the device.
 */
#include "radar.h"

#include <cuda_runtime.h>
#include <cufft.h>
#include <fftw3.h>
#include <math.h>
#include <pthread.h>
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
    float *win_r;   /* HOST Hann, length n (CPU range) */
    float *d_win_d; /* device Hann, length m (GPU Doppler) */
    cudaStream_t *cfar_streams; /* [frame_wnd * n_tiles] */
} rk_ctx;

/* Tagged workspace: range is FFTW/host, doppler is cuFFT/device. */
enum { WS_RANGE = 0, WS_DOPPLER = 1 };
typedef struct {
    uint32_t kind;
    uint32_t nsub;
    const rk_ctx *ctx;
    /* range (FFTW) */
    fftwf_complex **in;
    fftwf_complex **out;
    fftwf_plan *fplan;
    /* doppler (cuFFT) */
    cufftHandle *plan;
    cudaStream_t *stream;
    cufftComplex **d_rd_in; /* [nsub] device tile input, rows*m */
    cufftComplex **d_work;  /* [nsub] device FFT scratch, rows*m */
} rk_ws;

static pthread_mutex_t plan_lock = PTHREAD_MUTEX_INITIALIZER;

static float *make_hann_host(uint32_t len) {
    float *h = (float *)malloc(len * sizeof(float));
    const float two_pi = 6.28318530717958647692f;
    for (uint32_t i = 0; i < len; i++)
        h[i] = 0.5f * (1.0f - cosf(two_pi * i / (float)(len - 1)));
    return h;
}

static float *make_hann_device(uint32_t len) {
    float *h = make_hann_host(len);
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
    ctx->win_r = make_hann_host(n_samples);
    ctx->d_win_d = make_hann_device(n_chirps);
    const uint32_t nstreams = (frame_wnd ? frame_wnd : 1) * n_tiles;
    ctx->cfar_streams = (cudaStream_t *)calloc(nstreams, sizeof(cudaStream_t));
    for (uint32_t i = 0; i < nstreams; i++)
        CUDA_CK(cudaStreamCreateWithFlags(&ctx->cfar_streams[i],
                                          cudaStreamNonBlocking));
    return ctx;
}

extern "C" void rk_free(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    const uint32_t nstreams =
        (ctx->frame_wnd ? ctx->frame_wnd : 1) * ctx->n_tiles;
    for (uint32_t i = 0; i < nstreams; i++)
        cudaStreamDestroy(ctx->cfar_streams[i]);
    free(ctx->cfar_streams);
    free(ctx->win_r);
    cudaFree(ctx->d_win_d);
    free(ctx);
}

extern "C" void *rk_make_range_ws(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)calloc(1, sizeof(rk_ws));
    ws->kind = WS_RANGE;
    ws->ctx = ctx;
    ws->nsub = ctx->frame_wnd ? ctx->frame_wnd : 1;
    ws->in = (fftwf_complex **)calloc(ws->nsub, sizeof(fftwf_complex *));
    ws->out = (fftwf_complex **)calloc(ws->nsub, sizeof(fftwf_complex *));
    ws->fplan = (fftwf_plan *)calloc(ws->nsub, sizeof(fftwf_plan));
    pthread_mutex_lock(&plan_lock);
    for (uint32_t s = 0; s < ws->nsub; s++) {
        ws->in[s] = (fftwf_complex *)fftwf_malloc(ctx->n * sizeof(fftwf_complex));
        ws->out[s] = (fftwf_complex *)fftwf_malloc(ctx->n * sizeof(fftwf_complex));
        ws->fplan[s] = fftwf_plan_dft_1d((int)ctx->n, ws->in[s], ws->out[s],
                                         FFTW_FORWARD, FFTW_MEASURE);
    }
    pthread_mutex_unlock(&plan_lock);
    return ws;
}

extern "C" void *rk_make_doppler_ws(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)calloc(1, sizeof(rk_ws));
    ws->kind = WS_DOPPLER;
    ws->ctx = ctx;
    ws->nsub = ctx->frame_wnd ? ctx->frame_wnd : 1;
    ws->plan = (cufftHandle *)calloc(ws->nsub, sizeof(cufftHandle));
    ws->stream = (cudaStream_t *)calloc(ws->nsub, sizeof(cudaStream_t));
    ws->d_rd_in = (cufftComplex **)calloc(ws->nsub, sizeof(cufftComplex *));
    ws->d_work = (cufftComplex **)calloc(ws->nsub, sizeof(cufftComplex *));
    const uint32_t rows = ctx->n / ctx->n_tiles;
    int nfft = (int)ctx->m;
    for (uint32_t s = 0; s < ws->nsub; s++) {
        CUDA_CK(cudaStreamCreateWithFlags(&ws->stream[s], cudaStreamNonBlocking));
        CUDA_CK(cudaMalloc(&ws->d_rd_in[s],
                           (size_t)rows * ctx->m * sizeof(cufftComplex)));
        CUDA_CK(cudaMalloc(&ws->d_work[s],
                           (size_t)rows * ctx->m * sizeof(cufftComplex)));
        CUFFT_CK(cufftPlanMany(&ws->plan[s], 1, &nfft, NULL, 1, nfft, NULL, 1,
                               nfft, CUFFT_C2C, (int)rows));
        CUFFT_CK(cufftSetStream(ws->plan[s], ws->stream[s]));
    }
    return ws;
}

extern "C" void rk_free_ws(void *ws_p) {
    rk_ws *ws = (rk_ws *)ws_p;
    if (ws->kind == WS_RANGE) {
        pthread_mutex_lock(&plan_lock);
        for (uint32_t s = 0; s < ws->nsub; s++) {
            fftwf_destroy_plan(ws->fplan[s]);
            fftwf_free(ws->in[s]);
            fftwf_free(ws->out[s]);
        }
        pthread_mutex_unlock(&plan_lock);
        free(ws->in);
        free(ws->out);
        free(ws->fplan);
    } else {
        for (uint32_t s = 0; s < ws->nsub; s++) {
            cufftDestroy(ws->plan[s]);
            cudaFree(ws->d_rd_in[s]);
            cudaFree(ws->d_work[s]);
            cudaStreamDestroy(ws->stream[s]);
        }
        free(ws->plan);
        free(ws->stream);
        free(ws->d_rd_in);
        free(ws->d_work);
    }
    free(ws);
}

/* rd: PINNED HOST (CPU writes, GPU reads via H2D). power/dets: DEVICE. */
extern "C" void *rk_alloc_rd(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    void *h = NULL;
    CUDA_CK(cudaMallocHost(
        &h, (size_t)ctx->frame_wnd * ctx->n * ctx->m * sizeof(cufftComplex)));
    return h;
}

extern "C" void *rk_alloc_power(void *ctx_p) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    void *d = NULL;
    CUDA_CK(cudaMalloc(&d,
                       (size_t)ctx->frame_wnd * ctx->n * ctx->m * sizeof(float)));
    return d;
}

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

extern "C" void rk_free_buf(void *buf) {
    /* rd is pinned host, power/dets are device. cudaFreeHost on a device ptr (or
     * vice versa) errors; the plugin frees each with the matching call is not
     * possible through one symbol, so probe the pointer's memory type. */
    cudaPointerAttributes attr;
    if (cudaPointerGetAttributes(&attr, buf) == cudaSuccess &&
        attr.type == cudaMemoryTypeHost) {
        cudaFreeHost(buf);
    } else {
        cudaGetLastError(); /* clear the error from probing a host-only ptr */
        cudaFree(buf);
    }
}

/* ------------------------------- range (CPU) ------------------------------- */

extern "C" void rk_range_fft(void *ctx_p, void *ws_p, const int16_t *iq,
                             uint32_t n_samples, uint32_t chirp_id, void *rd_p,
                             uint32_t slot) {
    rk_ctx *ctx = (rk_ctx *)ctx_p;
    rk_ws *ws = (rk_ws *)ws_p;
    (void)n_samples;
    fftwf_complex *rd = (fftwf_complex *)rd_p;
    const uint32_t sub = slot % ws->nsub;
    fftwf_complex *win = ws->in[sub];
    fftwf_complex *fft = ws->out[sub];
    for (uint32_t i = 0; i < ctx->n; i++) {
        win[i][0] = (float)iq[2 * i] * ctx->win_r[i];
        win[i][1] = (float)iq[2 * i + 1] * ctx->win_r[i];
    }
    fftwf_execute(ws->fplan[sub]);
    /* Corner turn into the range-major rd matrix (chirp = inner index). */
    fftwf_complex *base = rd + (size_t)slot * ctx->n * ctx->m + chirp_id;
    for (uint32_t r = 0; r < ctx->n; r++) {
        base[(size_t)r * ctx->m][0] = fft[r][0];
        base[(size_t)r * ctx->m][1] = fft[r][1];
    }
}

/* ----------------------------- doppler (GPU) ------------------------------ */

__global__ void k_window_rows(const cufftComplex *src, const float *win,
                              cufftComplex *dst, uint32_t rows, uint32_t m) {
    uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < rows * m) {
        float w = win[i % m];
        dst[i].x = src[i].x * w;
        dst[i].y = src[i].y * w;
    }
}

__global__ void k_magnitude(const cufftComplex *src, float *dst, uint32_t count) {
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
    const uint32_t sub = slot % ws->nsub;
    cudaStream_t stream = ws->stream[sub];
    /* Upload just this tile's rd rows (host -> device), then Doppler on device. */
    CUDA_CK(cudaMemcpyAsync(ws->d_rd_in[sub],
                            (const cufftComplex *)rd_p + off,
                            (size_t)count * sizeof(cufftComplex),
                            cudaMemcpyHostToDevice, stream));
    k_window_rows<<<(count + tpb - 1) / tpb, tpb, 0, stream>>>(
        ws->d_rd_in[sub], ctx->d_win_d, ws->d_work[sub], rows, m);
    CUFFT_CK(cufftExecC2C(ws->plan[sub], ws->d_work[sub], ws->d_work[sub],
                          CUFFT_FORWARD));
    k_magnitude<<<(count + tpb - 1) / tpb, tpb, 0, stream>>>(
        ws->d_work[sub], (float *)power_p + off, count);
    CUDA_CK(cudaStreamSynchronize(stream));
}

/* ------------------------------- cfar (GPU) ------------------------------- */

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
    cudaStream_t stream = ctx->cfar_streams[slot * ctx->n_tiles + tile];
    CUDA_CK(cudaMemsetAsync(tile_block, 0, sizeof(uint32_t), stream));
    const uint32_t count_cells = rows * m, tpb = 256;
    k_cfar<<<(count_cells + tpb - 1) / tpb, tpb, 0, stream>>>(
        (const float *)power_p + (size_t)slot * n * m, n, m, tile * rows, rows,
        K, G, ctx->pfa_scale, n_train, tile_block, ctx->max_dets);
    CUDA_CK(cudaGetLastError());
    uint32_t cnt = 0;
    CUDA_CK(cudaMemcpyAsync(&cnt, tile_block, sizeof(uint32_t),
                            cudaMemcpyDeviceToHost, stream));
    CUDA_CK(cudaStreamSynchronize(stream));
    return cnt < ctx->max_dets ? cnt : ctx->max_dets;
}

/* ----------------------------- cluster (CPU) ------------------------------ */

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
    CUDA_CK(cudaMemcpy(host, (const uint8_t *)dets_p + (size_t)slot * slot_bytes,
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
