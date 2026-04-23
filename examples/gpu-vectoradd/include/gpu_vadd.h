#ifndef GPU_VADD_H
#define GPU_VADD_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque host-side handle for a device buffer.
 * Heap-allocated (malloc/free); device pointer freed with cudaFree. */
typedef struct {
    float  *d_ptr;
    size_t  len;
} DevBuf;

/* ----------------------------------------------------------------------- *
 * Tomii-exported functions
 * ----------------------------------------------------------------------- */

/* Generate a host float array of length n filled deterministically from seed.
 * Returns a malloc'd array; the wrapper copies to Rust Vec<f32> and calls
 * free_host_f32 to release the C allocation. */
// @tomii_export(out_len=n, free=free_gen_vec)
float* generate_host_vec(size_t n, uint64_t seed);

/* Copy a host float slice to device memory.
 * Returns a malloc'd DevBuf* handle (opaque void*).
 * Caller (vadd_gpu) owns the free. */
// @tomii_export(host: array)
void* copy_h2d(const float* host, size_t host_len);

/* Launch element-wise vector add a + b on the GPU.
 * Consumes (frees) both input handles; returns a new DevBuf* for the result. */
// @tomii_export
void* vadd_gpu(void* a, void* b);

/* Copy a device buffer back to host, freeing the device buffer handle.
 * Returns a malloc'd float array of length n; wrapper copies to Rust Vec<f32>
 * and calls free_host_f32 to release the C allocation. */
// @tomii_export(out_len=n, free=free_d2h_vec)
float* copy_d2h(void* d, size_t n);

/* CPU-side validation: check gpu_result == host_a + host_b element-wise.
 * Prints "validate: OK" on success; calls abort() on mismatch. */
// @tomii_export(gpu_result: array, host_a: array, host_b: array)
void validate(const float* gpu_result, size_t gpu_result_len,
              const float* host_a,     size_t host_a_len,
              const float* host_b,     size_t host_b_len);

/* ----------------------------------------------------------------------- *
 * Memory helpers (not exported to the graph; used via free= annotations)
 * ----------------------------------------------------------------------- */
void free_gen_vec(void* p);   /* frees generate_host_vec output */
void free_d2h_vec(void* p);   /* frees copy_d2h output */

#ifdef __cplusplus
}
#endif

#endif /* GPU_VADD_H */
