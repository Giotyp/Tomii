#ifndef MATCOMP_H
#define MATCOMP_H

#include <stddef.h>

/*
 * ABI-compatible with Rust's [f32; 2] / num_complex::Complex32.
 * FFTW's fftwf_complex is also float[2] with the same layout.
 */
typedef struct {
    float re;
    float im;
} complex_f32;

/* -----------------------------------------------------------------------
 * SynStream-exported functions
 * Annotated with // @synstream_export for the converter to generate wrappers.
 * ----------------------------------------------------------------------- */

/* Generate a vector of n complex samples. Returns a malloc'd array of n
 * complex_f32. The wrapper copies the data into a Rust Vec and calls
 * free_vector to release the C allocation. */
// @synstream_export(out_len=n, free=free_vector)
complex_f32* generate_vector(size_t n);

/* Create an FFTW single-precision FFT plan for the given buffer size.
 * Returns an opaque fftwf_plan handle. */
// @synstream_export
void* fft_planner(size_t buf_size);

/* Run an in-place FFT on `buffer` (length `buffer_len`) using the given plan.
 * `buffer` is annotated as mut_array so the wrapper borrows it mutably and
 * passes its raw pointer; `buffer_len` is auto-derived from the Vec's length. */
// @synstream_export(buffer: mut_array)
void compute_fft(void* planner, complex_f32* buffer, size_t buffer_len);

/* Convert a flat complex vector (length `vector_len`) into a square matrix.
 * Assumes vector_len is a perfect square. Returns an opaque Matrix* handle. */
// @synstream_export(vector: array)
void* vec_to_mat(const complex_f32* vector, size_t vector_len);

/* Multiply two square complex matrices (same dimensions). Returns a new
 * opaque Matrix* handle for the result. */
// @synstream_export
void* mat_mul(void* a, void* b);

/* Return the path of the output file (creating it if necessary).
 * Reads the directory from the environment variable `env_var` and appends
 * `out_file`. Returns a malloc'd C string freed after copying to Rust. */
// @synstream_export(free=free_string)
char* get_out_file(const char* env_var, const char* out_file);

/* Append the contents of `num_buffers` matrices to the file at `file_path`.
 * `buffers` is a pointer-array of Matrix* opaque handles. */
// @synstream_export(variadic)
void write_to_file(const char* file_path, void** buffers, size_t num_buffers);

/* -----------------------------------------------------------------------
 * Memory-management helpers (loaded by wrappers, not exposed to the graph)
 * ----------------------------------------------------------------------- */

void free_vector(void* vec);
void free_matrix(void* mat);
void free_string(void* s);

#endif /* MATCOMP_H */
