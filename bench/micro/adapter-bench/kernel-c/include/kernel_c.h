#ifndef KERNEL_C_H
#define KERNEL_C_H

#include <stddef.h>

/* Sum of an f32 buffer — the C-side twin of kernel-rust's buf_sum, wrapped
 * by tomii-converter's C-header path (libloading dynamic dispatch). `buf`
 * is annotated `array` so the converter emits the ArrayPtr extraction
 * (with_any::<Vec<f32>> -> raw ptr + len) matching examples/matrix-compute-C. */
// @tomii_export(buf: array)
float buf_sum_c(const float* buf, size_t buf_len);

#endif /* KERNEL_C_H */
