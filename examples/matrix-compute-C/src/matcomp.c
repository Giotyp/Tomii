#include "matcomp.h"

#include <cblas.h>
#include <fftw3.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------------- *
 * Internal Matrix representation
 * ---------------------------------------------------------------------- */

typedef struct {
    size_t rows;
    size_t cols;
    complex_f32 *data; /* row-major storage */
} Matrix;

/* ---------------------------------------------------------------------- *
 * generate_vector
 * ---------------------------------------------------------------------- */

complex_f32 *generate_vector(size_t n) {
    complex_f32 *data = (complex_f32 *)malloc(n * sizeof(complex_f32));
    if (!data) return NULL;
    for (size_t i = 0; i < n; i++) {
        data[i].re = (float)(i + 1);
        data[i].im = (float)(i + 1);
    }
    return data;
}

/* ---------------------------------------------------------------------- *
 * fft_planner
 * ---------------------------------------------------------------------- */

void *fft_planner(size_t buf_size) {
    /* Create a temporary buffer for plan estimation (FFTW_ESTIMATE does not
     * touch the buffer contents, so we can immediately free it). */
    fftwf_complex *tmp = (fftwf_complex *)fftwf_malloc(buf_size * sizeof(fftwf_complex));
    if (!tmp) return NULL;
    fftwf_plan plan =
        fftwf_plan_dft_1d((int)buf_size, tmp, tmp, FFTW_FORWARD, FFTW_ESTIMATE);
    fftwf_free(tmp);
    return (void *)plan;
}

/* ---------------------------------------------------------------------- *
 * compute_fft
 *
 * Executes an in-place complex FFT.
 * complex_f32 { float re; float im; } is layout-compatible with
 * fftwf_complex (float[2]), so we can cast directly.
 * ---------------------------------------------------------------------- */

void compute_fft(void *planner, complex_f32 *buffer, size_t buffer_len) {
    (void)buffer_len; /* length is implicit in the plan */
    fftwf_plan plan = (fftwf_plan)planner;
    fftwf_execute_dft(plan, (fftwf_complex *)buffer, (fftwf_complex *)buffer);
}

/* ---------------------------------------------------------------------- *
 * vec_to_mat
 * ---------------------------------------------------------------------- */

void *vec_to_mat(const complex_f32 *vector, size_t vector_len) {
    size_t n = (size_t)sqrt((double)vector_len);
    Matrix *mat = (Matrix *)malloc(sizeof(Matrix));
    if (!mat) return NULL;
    mat->rows = n;
    mat->cols = n;
    mat->data = (complex_f32 *)malloc(n * n * sizeof(complex_f32));
    if (!mat->data) {
        free(mat);
        return NULL;
    }
    memcpy(mat->data, vector, n * n * sizeof(complex_f32));
    return (void *)mat;
}

/* ---------------------------------------------------------------------- *
 * mat_mul
 *
 * Complex matrix multiply C = A * B using OpenBLAS cblas_cgemm.
 * Both matrices are assumed square and the same size.
 * ---------------------------------------------------------------------- */

void *mat_mul(void *a_ptr, void *b_ptr) {
    Matrix *A = (Matrix *)a_ptr;
    Matrix *B = (Matrix *)b_ptr;

    size_t n = A->rows;

    Matrix *C = (Matrix *)malloc(sizeof(Matrix));
    if (!C) return NULL;
    C->rows = n;
    C->cols = n;
    C->data = (complex_f32 *)calloc(n * n, sizeof(complex_f32));
    if (!C->data) {
        free(C);
        return NULL;
    }

    /* alpha = 1+0i, beta = 0+0i */
    float alpha[2] = {1.0f, 0.0f};
    float beta[2]  = {0.0f, 0.0f};

    cblas_cgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
                (int)n, (int)n, (int)n,
                alpha,
                (const void *)A->data, (int)n,
                (const void *)B->data, (int)n,
                beta,
                (void *)C->data, (int)n);

    return (void *)C;
}

/* ---------------------------------------------------------------------- *
 * get_out_file
 * ---------------------------------------------------------------------- */

char *get_out_file(const char *env_var, const char *out_file) {
    const char *dir = getenv(env_var);
    if (!dir) {
        fprintf(stderr, "get_out_file: environment variable '%s' not set\n", env_var);
        return NULL;
    }

    /* Build path: dir + "/" + out_file + '\0' */
    size_t len = strlen(dir) + 1 + strlen(out_file) + 1;
    char *path = (char *)malloc(len);
    if (!path) return NULL;
    snprintf(path, len, "%s/%s", dir, out_file);

    /* Create (or truncate) the file */
    FILE *f = fopen(path, "w");
    if (f) fclose(f);

    return path;
}

/* ---------------------------------------------------------------------- *
 * write_to_file
 *
 * Appends the contents of each matrix in `buffers` to the file at
 * `file_path`.  Each buffer entry is a Matrix*.
 * ---------------------------------------------------------------------- */

void write_to_file(const char *file_path, void **buffers, size_t num_buffers) {
    FILE *f = fopen(file_path, "a");
    if (!f) {
        fprintf(stderr, "write_to_file: failed to open '%s'\n", file_path);
        return;
    }

    for (size_t idx = 0; idx < num_buffers; idx++) {
        Matrix *mat = (Matrix *)buffers[idx];
        fprintf(f, "Buffer-%zu:\n{", idx);
        size_t total = mat->rows * mat->cols;
        for (size_t i = 0; i < total; i++) {
            if (i > 0) fprintf(f, ", ");
            fprintf(f, "%g+%gi", mat->data[i].re, mat->data[i].im);
        }
        fprintf(f, "}\n");
    }

    fclose(f);
}

/* ---------------------------------------------------------------------- *
 * Memory management helpers
 * ---------------------------------------------------------------------- */

void free_vector(void *vec) {
    free(vec);
}

void free_matrix(void *mat_ptr) {
    if (!mat_ptr) return;
    Matrix *mat = (Matrix *)mat_ptr;
    free(mat->data);
    free(mat);
}

void free_string(void *s) {
    free(s);
}
