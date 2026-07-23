/* Radar kernel library ABI.
 *
 * The Tomii plugin (src/lib.rs) calls these through FFI; the same symbols will
 * be provided by a CUDA implementation (libradar_kernels_gpu.so) so the plugin
 * and graph stay unchanged when stages move to the GPU. All buffer/workspace
 * handles are opaque to the caller; only rk_detection crosses the boundary as
 * a concrete struct.
 *
 * Data layouts (CPU implementation):
 *   rd buffer:    complex float [frame_wnd][n_samples][n_chirps]
 *                 (range-major: the range FFT corner-turns on write so the
 *                  Doppler FFT reads contiguous chirp rows)
 *   power buffer: float [frame_wnd][n_samples][n_chirps]
 *   dets buffer:  per (slot, tile): u32 count + max_dets_per_tile rk_detection
 *
 * Tiles partition the n_samples range bins into n_tiles contiguous chunks;
 * doppler/cfar task instance `tile` owns rows [tile*n/n_tiles, (tile+1)*n/n_tiles).
 */
#ifndef RADAR_KERNELS_H
#define RADAR_KERNELS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint32_t range_bin;
    uint32_t doppler_bin;
    float power;
} rk_detection;

/* Context: dimensions, windows, CFAR parameters. */
void *rk_init(uint32_t n_samples, uint32_t n_chirps, uint32_t frame_wnd,
              uint32_t n_tiles, uint32_t guard, uint32_t train,
              float pfa_scale, uint32_t max_dets_per_tile);
void rk_free(void *ctx);

/* Per-task-instance FFT workspaces (plan + scratch; not thread-safe, one per
 * concurrent task instance). Plan creation is internally serialized. */
void *rk_make_range_ws(void *ctx);
void *rk_make_doppler_ws(void *ctx);
void rk_free_ws(void *ws);

/* Shared buffers, sized for frame_wnd concurrent frames. */
void *rk_alloc_rd(void *ctx);
void *rk_alloc_power(void *ctx);
void *rk_alloc_dets(void *ctx);
void rk_free_buf(void *buf);

/* Window + FFT one chirp (n_samples interleaved i16 IQ), corner-turned write
 * into the rd buffer column for chirp_id. */
void rk_range_fft(void *ctx, void *ws, const int16_t *iq, uint32_t n_samples,
                  uint32_t chirp_id, void *rd, uint32_t slot);

/* Window + FFT the chirp axis for every range row in `tile`; writes |.|^2
 * into the power buffer. */
void rk_doppler_fft(void *ctx, void *ws, uint32_t tile, const void *rd,
                    void *power, uint32_t slot);

/* 2D CA-CFAR over `tile`'s range rows (training window wraps on both axes,
 * reading the full power map). Appends to the tile's detection list; returns
 * the number of detections recorded for this tile. */
uint32_t rk_cfar(void *ctx, uint32_t tile, const void *power, void *dets,
                 uint32_t slot);

/* Cluster all tiles' detections for `slot` (8-connected, wrapping; strongest
 * cell per cluster). Fills `out` up to out_cap; returns the cluster count. */
uint32_t rk_cluster(void *ctx, const void *dets, uint32_t slot,
                    rk_detection *out, uint32_t out_cap);

#ifdef __cplusplus
}
#endif

#endif /* RADAR_KERNELS_H */
