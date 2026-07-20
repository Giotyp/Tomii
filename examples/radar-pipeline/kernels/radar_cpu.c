/* CPU (FFTW) implementation of the radar kernel ABI in radar.h.
 *
 * Algorithm parity: matches data/reference_check.py — symmetric Hann windows
 * on both FFT axes, cell-averaging CFAR with a wrapping training window, and
 * strongest-cell-per-cluster grouping.
 */
#include "radar.h"

#include <fftw3.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint32_t n;         /* samples per chirp = range bins  */
    uint32_t m;         /* chirps per frame  = Doppler bins */
    uint32_t frame_wnd; /* concurrent frames buffered       */
    uint32_t n_tiles;
    uint32_t guard;
    uint32_t train;
    uint32_t max_dets;  /* per tile */
    float pfa_scale;
    float *win_r; /* Hann, length n */
    float *win_d; /* Hann, length m */
} rk_ctx;

/* One workspace serves every slot sharing this (chirp|tile) index; up to
 * frame_wnd frames run concurrently, so each slot gets its OWN plan+scratch.
 * A shared fftwf plan executed with shared in/out arrays from two in-flight
 * frames silently corrupts (the GPU twin deadlocks instead — same root). */
typedef struct {
    uint32_t nsub; /* = frame_wnd */
    fftwf_complex **in;
    fftwf_complex **out;
    fftwf_plan *plan;
} rk_ws;

/* FFTW plan creation/destruction is not thread-safe. */
static pthread_mutex_t plan_lock = PTHREAD_MUTEX_INITIALIZER;

static float *hann(uint32_t len) {
    const float two_pi = 6.28318530717958647692f;
    float *w = malloc(len * sizeof(float));
    for (uint32_t i = 0; i < len; i++)
        w[i] = 0.5f * (1.0f - cosf(two_pi * i / (float)(len - 1)));
    return w;
}

void *rk_init(uint32_t n_samples, uint32_t n_chirps, uint32_t frame_wnd,
              uint32_t n_tiles, uint32_t guard, uint32_t train,
              float pfa_scale, uint32_t max_dets_per_tile) {
    rk_ctx *ctx = calloc(1, sizeof(rk_ctx));
    ctx->n = n_samples;
    ctx->m = n_chirps;
    ctx->frame_wnd = frame_wnd;
    ctx->n_tiles = n_tiles;
    ctx->guard = guard;
    ctx->train = train;
    ctx->pfa_scale = pfa_scale;
    ctx->max_dets = max_dets_per_tile;
    ctx->win_r = hann(n_samples);
    ctx->win_d = hann(n_chirps);
    return ctx;
}

void rk_free(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    free(ctx->win_r);
    free(ctx->win_d);
    free(ctx);
}

static void *make_ws(uint32_t len, uint32_t frame_wnd) {
    uint32_t nsub = frame_wnd ? frame_wnd : 1;
    rk_ws *ws = malloc(sizeof(rk_ws));
    ws->nsub = nsub;
    ws->in = malloc(nsub * sizeof(fftwf_complex *));
    ws->out = malloc(nsub * sizeof(fftwf_complex *));
    ws->plan = malloc(nsub * sizeof(fftwf_plan));
    for (uint32_t s = 0; s < nsub; s++) {
        ws->in[s] = fftwf_malloc(len * sizeof(fftwf_complex));
        ws->out[s] = fftwf_malloc(len * sizeof(fftwf_complex));
        pthread_mutex_lock(&plan_lock);
        ws->plan[s] = fftwf_plan_dft_1d((int)len, ws->in[s], ws->out[s],
                                        FFTW_FORWARD, FFTW_ESTIMATE);
        pthread_mutex_unlock(&plan_lock);
    }
    return ws;
}

void *rk_make_range_ws(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    return make_ws(ctx->n, ctx->frame_wnd);
}
void *rk_make_doppler_ws(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    return make_ws(ctx->m, ctx->frame_wnd);
}

void rk_free_ws(void *ws_p) {
    rk_ws *ws = ws_p;
    for (uint32_t s = 0; s < ws->nsub; s++) {
        pthread_mutex_lock(&plan_lock);
        fftwf_destroy_plan(ws->plan[s]);
        pthread_mutex_unlock(&plan_lock);
        fftwf_free(ws->in[s]);
        fftwf_free(ws->out[s]);
    }
    free(ws->in);
    free(ws->out);
    free(ws->plan);
    free(ws);
}

void *rk_alloc_rd(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    return fftwf_malloc((size_t)ctx->frame_wnd * ctx->n * ctx->m *
                        sizeof(fftwf_complex));
}

void *rk_alloc_power(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    return fftwf_malloc((size_t)ctx->frame_wnd * ctx->n * ctx->m *
                        sizeof(float));
}

/* dets layout: per (slot, tile) block of {u32 count; rk_detection[max_dets]} */
static size_t dets_stride(const rk_ctx *ctx) {
    return sizeof(uint32_t) + (size_t)ctx->max_dets * sizeof(rk_detection);
}

void *rk_alloc_dets(void *ctx_p) {
    rk_ctx *ctx = ctx_p;
    size_t bytes = (size_t)ctx->frame_wnd * ctx->n_tiles * dets_stride(ctx);
    void *buf = fftwf_malloc(bytes);
    memset(buf, 0, bytes);
    return buf;
}

void rk_free_buf(void *buf) { fftwf_free(buf); }

static uint32_t tile_start(const rk_ctx *ctx, uint32_t tile) {
    return tile * ctx->n / ctx->n_tiles;
}

void rk_range_fft(void *ctx_p, void *ws_p, const int16_t *iq,
                  uint32_t n_samples, uint32_t chirp_id, void *rd_p,
                  uint32_t slot) {
    rk_ctx *ctx = ctx_p;
    rk_ws *ws = ws_p;
    fftwf_complex *rd = rd_p;
    (void)n_samples;

    fftwf_complex *win = ws->in[slot % ws->nsub];
    fftwf_complex *fft = ws->out[slot % ws->nsub];
    for (uint32_t i = 0; i < ctx->n; i++) {
        win[i][0] = (float)iq[2 * i] * ctx->win_r[i];
        win[i][1] = (float)iq[2 * i + 1] * ctx->win_r[i];
    }
    fftwf_execute(ws->plan[slot % ws->nsub]);

    /* Corner turn: chirp chirp_id becomes a strided column of the
     * range-major rd matrix. Disjoint per-chirp writes; no locking. */
    fftwf_complex *base = rd + (size_t)slot * ctx->n * ctx->m + chirp_id;
    for (uint32_t r = 0; r < ctx->n; r++) {
        base[(size_t)r * ctx->m][0] = fft[r][0];
        base[(size_t)r * ctx->m][1] = fft[r][1];
    }
}

void rk_doppler_fft(void *ctx_p, void *ws_p, uint32_t tile, const void *rd_p,
                    void *power_p, uint32_t slot) {
    rk_ctx *ctx = ctx_p;
    rk_ws *ws = ws_p;
    const fftwf_complex *rd = rd_p;
    float *power = power_p;

    const size_t slot_off = (size_t)slot * ctx->n * ctx->m;
    const uint32_t r0 = tile_start(ctx, tile), r1 = tile_start(ctx, tile + 1);
    fftwf_complex *win = ws->in[slot % ws->nsub];
    fftwf_complex *fft = ws->out[slot % ws->nsub];
    for (uint32_t r = r0; r < r1; r++) {
        const fftwf_complex *row = rd + slot_off + (size_t)r * ctx->m;
        for (uint32_t c = 0; c < ctx->m; c++) {
            win[c][0] = row[c][0] * ctx->win_d[c];
            win[c][1] = row[c][1] * ctx->win_d[c];
        }
        fftwf_execute(ws->plan[slot % ws->nsub]);
        float *prow = power + slot_off + (size_t)r * ctx->m;
        for (uint32_t d = 0; d < ctx->m; d++)
            prow[d] = fft[d][0] * fft[d][0] + fft[d][1] * fft[d][1];
    }
}

static inline uint32_t wrap(int32_t v, uint32_t size) {
    int32_t s = (int32_t)size;
    return (uint32_t)(((v % s) + s) % s);
}

uint32_t rk_cfar(void *ctx_p, uint32_t tile, const void *power_p, void *dets_p,
                 uint32_t slot) {
    rk_ctx *ctx = ctx_p;
    const float *power = (const float *)power_p + (size_t)slot * ctx->n * ctx->m;
    const uint32_t n = ctx->n, m = ctx->m;
    const int32_t K = (int32_t)(ctx->guard + ctx->train);
    const int32_t G = (int32_t)ctx->guard;
    const float n_train =
        (float)((2 * K + 1) * (2 * K + 1) - (2 * G + 1) * (2 * G + 1));
    const float thresh = ctx->pfa_scale / n_train;

    uint8_t *block = (uint8_t *)dets_p +
                     ((size_t)slot * ctx->n_tiles + tile) * dets_stride(ctx);
    uint32_t *count = (uint32_t *)block;
    rk_detection *out = (rk_detection *)(block + sizeof(uint32_t));
    *count = 0;

    /* Per-row column sums over the outer/guard range spans, then a circular
     * sliding window along the Doppler axis. */
    float *col_out = malloc(m * sizeof(float));
    float *col_in = malloc(m * sizeof(float));

    const uint32_t r0 = tile_start(ctx, tile), r1 = tile_start(ctx, tile + 1);
    for (uint32_t r = r0; r < r1; r++) {
        memset(col_out, 0, m * sizeof(float));
        memset(col_in, 0, m * sizeof(float));
        for (int32_t dr = -K; dr <= K; dr++) {
            const float *prow = power + (size_t)wrap((int32_t)r + dr, n) * m;
            for (uint32_t d = 0; d < m; d++)
                col_out[d] += prow[d];
            if (dr >= -G && dr <= G)
                for (uint32_t d = 0; d < m; d++)
                    col_in[d] += prow[d];
        }

        float s_out = 0.0f, s_in = 0.0f;
        for (int32_t dd = -K; dd <= K; dd++)
            s_out += col_out[wrap(dd, m)];
        for (int32_t dd = -G; dd <= G; dd++)
            s_in += col_in[wrap(dd, m)];

        const float *prow = power + (size_t)r * m;
        for (uint32_t d = 0; d < m; d++) {
            float noise_sum = s_out - s_in;
            if (prow[d] > thresh * noise_sum && *count < ctx->max_dets) {
                out[*count].range_bin = r;
                out[*count].doppler_bin = d;
                out[*count].power = prow[d];
                (*count)++;
            }
            s_out += col_out[wrap((int32_t)d + K + 1, m)] -
                     col_out[wrap((int32_t)d - K, m)];
            s_in += col_in[wrap((int32_t)d + G + 1, m)] -
                    col_in[wrap((int32_t)d - G, m)];
        }
    }

    free(col_out);
    free(col_in);
    return *count;
}

static inline uint32_t circ_diff(uint32_t a, uint32_t b, uint32_t size) {
    uint32_t d = a > b ? a - b : b - a;
    return d < size - d ? d : size - d;
}

uint32_t rk_cluster(void *ctx_p, const void *dets_p, uint32_t slot,
                    rk_detection *out, uint32_t out_cap) {
    rk_ctx *ctx = ctx_p;
    const uint32_t cap = ctx->n_tiles * ctx->max_dets;
    rk_detection *all = malloc(cap * sizeof(rk_detection));
    uint32_t total = 0;

    for (uint32_t t = 0; t < ctx->n_tiles; t++) {
        const uint8_t *block = (const uint8_t *)dets_p +
                               ((size_t)slot * ctx->n_tiles + t) *
                                   dets_stride(ctx);
        uint32_t cnt = *(const uint32_t *)block;
        const rk_detection *d =
            (const rk_detection *)(block + sizeof(uint32_t));
        for (uint32_t i = 0; i < cnt; i++)
            all[total++] = d[i];
    }

    /* Union-find over detection cells; 8-connected with wrap on both axes. */
    uint32_t *parent = malloc(total * sizeof(uint32_t));
    for (uint32_t i = 0; i < total; i++)
        parent[i] = i;

#define FIND(x)                                                                \
    ({                                                                         \
        uint32_t _r = (x);                                                     \
        while (parent[_r] != _r)                                               \
            _r = parent[_r] = parent[parent[_r]];                              \
        _r;                                                                    \
    })

    for (uint32_t i = 0; i < total; i++)
        for (uint32_t j = i + 1; j < total; j++)
            if (circ_diff(all[i].range_bin, all[j].range_bin, ctx->n) <= 1 &&
                circ_diff(all[i].doppler_bin, all[j].doppler_bin, ctx->m) <= 1)
                parent[FIND(i)] = FIND(j);

    uint32_t n_out = 0;
    for (uint32_t i = 0; i < total; i++) {
        if (FIND(i) != i)
            continue;
        /* i is a root: pick the strongest cell of its cluster. */
        rk_detection best = all[i];
        for (uint32_t j = 0; j < total; j++)
            if (FIND(j) == i && all[j].power > best.power)
                best = all[j];
        if (n_out < out_cap)
            out[n_out++] = best;
    }
#undef FIND

    free(parent);
    free(all);
    return n_out;
}
