#include "matcomp.h"

#include <fftw3.h>
#include <stdio.h>
#include <stdlib.h>

#define BUF_SIZE  100
#define NUM_NODES 200

int main(void) {
    const char *out_path = "validation.txt";

    /* Truncate output file — mirrors what get_out_file does for result.txt */
    FILE *f = fopen(out_path, "w");
    if (!f) { perror("validation: fopen"); return 1; }
    fclose(f);

    void *planner = fft_planner(BUF_SIZE);
    if (!planner) { fprintf(stderr, "validation: fft_planner failed\n"); return 1; }

    void *results[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
        complex_f32 *vec = generate_vector(BUF_SIZE);
        compute_fft(planner, vec, BUF_SIZE);
        void *mat = vec_to_mat(vec, BUF_SIZE);
        free_vector(vec);
        results[i] = mat_mul(mat, mat);
        free_matrix(mat);
    }

    /* Write all buffers at once — matches the variadic write_to_file call in the graph */
    write_to_file(out_path, results, NUM_NODES);

    for (int i = 0; i < NUM_NODES; i++) free_matrix(results[i]);
    fftwf_destroy_plan((fftwf_plan)planner);

    return 0;
}
