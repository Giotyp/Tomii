#include "kernel_c.h"

/* O(1): first + last element, matching kernel-rust's buf_sum so the
 * per-call cost is flat in buffer size and isolates dispatch overhead. */
float buf_sum_c(const float *buf, size_t buf_len) {
    return buf[0] + buf[buf_len - 1];
}
