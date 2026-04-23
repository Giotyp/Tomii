#include "gpu_vadd.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <cuda_runtime.h>

/* ----------------------------------------------------------------------- *
 * Error-checking macro — aborts on any CUDA failure.
 * ----------------------------------------------------------------------- */
#define CUDA_CHECK(call)                                                    \
    do {                                                                    \
        cudaError_t _e = (call);                                            \
        if (_e != cudaSuccess) {                                            \
            fprintf(stderr, "CUDA error %s:%d — %s: %s\n",                 \
                    __FILE__, __LINE__, #call, cudaGetErrorString(_e));      \
            abort();                                                        \
        }                                                                   \
    } while (0)

/* ----------------------------------------------------------------------- *
 * Kernel
 * ----------------------------------------------------------------------- */
__global__ static void vadd_kernel(const float* __restrict__ a,
                                   const float* __restrict__ b,
                                   float* __restrict__ c,
                                   size_t n)
{
    size_t i = blockIdx.x * (size_t)blockDim.x + threadIdx.x;
    if (i < n) c[i] = a[i] + b[i];
}

/* ----------------------------------------------------------------------- *
 * generate_host_vec
 * ----------------------------------------------------------------------- */
extern "C"
float* generate_host_vec(size_t n, uint64_t seed)
{
    float* data = (float*)malloc(n * sizeof(float));
    if (!data) { fprintf(stderr, "generate_host_vec: malloc failed\n"); abort(); }
    for (size_t i = 0; i < n; i++)
        data[i] = (float)(i + seed);
    return data;
}

/* ----------------------------------------------------------------------- *
 * copy_h2d
 * ----------------------------------------------------------------------- */
extern "C"
void* copy_h2d(const float* host, size_t host_len)
{
    DevBuf* buf = (DevBuf*)malloc(sizeof(DevBuf));
    if (!buf) { fprintf(stderr, "copy_h2d: malloc failed\n"); abort(); }
    buf->len = host_len;
    CUDA_CHECK(cudaMalloc(&buf->d_ptr, host_len * sizeof(float)));
    CUDA_CHECK(cudaMemcpyAsync(buf->d_ptr, host, host_len * sizeof(float),
                               cudaMemcpyHostToDevice, cudaStreamPerThread));
    CUDA_CHECK(cudaStreamSynchronize(cudaStreamPerThread));
    return (void*)buf;
}

/* ----------------------------------------------------------------------- *
 * vadd_gpu — consumes both input handles
 * ----------------------------------------------------------------------- */
extern "C"
void* vadd_gpu(void* a_ptr, void* b_ptr)
{
    DevBuf* a = (DevBuf*)a_ptr;
    DevBuf* b = (DevBuf*)b_ptr;
    if (a->len != b->len) {
        fprintf(stderr, "vadd_gpu: length mismatch (%zu vs %zu)\n", a->len, b->len);
        abort();
    }
    size_t n = a->len;

    DevBuf* out = (DevBuf*)malloc(sizeof(DevBuf));
    if (!out) { fprintf(stderr, "vadd_gpu: malloc failed\n"); abort(); }
    out->len = n;
    CUDA_CHECK(cudaMalloc(&out->d_ptr, n * sizeof(float)));

    int threads = 256;
    int blocks  = (int)((n + threads - 1) / threads);
    vadd_kernel<<<blocks, threads, 0, cudaStreamPerThread>>>(a->d_ptr, b->d_ptr, out->d_ptr, n);
    CUDA_CHECK(cudaStreamSynchronize(cudaStreamPerThread));

    CUDA_CHECK(cudaFree(a->d_ptr));
    free(a);
    CUDA_CHECK(cudaFree(b->d_ptr));
    free(b);

    return (void*)out;
}

/* ----------------------------------------------------------------------- *
 * copy_d2h — consumes the device handle
 * ----------------------------------------------------------------------- */
extern "C"
float* copy_d2h(void* d_ptr, size_t n)
{
    DevBuf* buf = (DevBuf*)d_ptr;
    if (buf->len != n) {
        fprintf(stderr, "copy_d2h: size mismatch (buf->len=%zu, n=%zu)\n", buf->len, n);
        abort();
    }
    float* host = (float*)malloc(n * sizeof(float));
    if (!host) { fprintf(stderr, "copy_d2h: malloc failed\n"); abort(); }
    CUDA_CHECK(cudaMemcpyAsync(host, buf->d_ptr, n * sizeof(float),
                               cudaMemcpyDeviceToHost, cudaStreamPerThread));
    CUDA_CHECK(cudaStreamSynchronize(cudaStreamPerThread));

    CUDA_CHECK(cudaFree(buf->d_ptr));
    free(buf);

    return host;
}

/* ----------------------------------------------------------------------- *
 * validate
 * ----------------------------------------------------------------------- */
extern "C"
void validate(const float* gpu_result, size_t gpu_result_len,
              const float* host_a,     size_t host_a_len,
              const float* host_b,     size_t host_b_len)
{
    if (gpu_result_len != host_a_len || gpu_result_len != host_b_len) {
        fprintf(stderr, "validate: length mismatch (%zu vs %zu vs %zu)\n",
                gpu_result_len, host_a_len, host_b_len);
        abort();
    }
    float max_err = 0.0f;
    for (size_t i = 0; i < gpu_result_len; i++) {
        float expected = host_a[i] + host_b[i];
        float err = fabsf(gpu_result[i] - expected);
        if (err > max_err) max_err = err;
    }
    if (max_err > 1e-4f) {
        fprintf(stderr, "validate: FAIL — max error %.6e (threshold 1e-4)\n", max_err);
        abort();
    }
    fprintf(stdout, "validate: OK (n=%zu, max_err=%.2e)\n", gpu_result_len, max_err);
    fflush(stdout);
}

/* ----------------------------------------------------------------------- *
 * Memory helpers
 * ----------------------------------------------------------------------- */
extern "C" void free_gen_vec(void* p) { free(p); }
extern "C" void free_d2h_vec(void* p) { free(p); }
