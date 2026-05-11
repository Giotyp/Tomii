// use crate::bindings::phy_ldpc_encoder_5gnr_bindings::*;

// const PROC_BYTES: usize = 64;
// const I_LS_NUM: usize = 8;
// const ZC_MAX: usize = 384;

const BG1_COL_TOTAL: usize = 68;
const BG1_ROW_TOTAL: usize = 46;
const BG1_COL_INF_NUM: usize = 22;
//const BG1_NONZERO_NUM: usize = 307;

const BG2_COL_TOTAL: usize = 52;
const BG2_ROW_TOTAL: usize = 42;
const BG2_COL_INF_NUM: usize = 10;
//const BG2_NONZERO_NUM: usize = 188;

// Return the number of bytes needed to store n_bits bits
pub fn BitsToBytes(nbits: usize) -> usize {
    (nbits + 7) / 8
}

// Return the number of non-expanded base graph columns used for information
// bits for this base graph
pub fn LdpcNumInputCols(base_graph: usize) -> usize {
    if base_graph == 1 {
        BG1_COL_INF_NUM
    } else {
        BG2_COL_INF_NUM
    }
}

// Return the maximum number of rows in this non-expanded base graph
pub fn LdpcMaxNumRows(base_graph: usize) -> usize {
    if base_graph == 1 {
        BG1_ROW_TOTAL
    } else {
        BG2_ROW_TOTAL
    }
}

// Return the number of input information bits per codeword with this base
// graph and expansion factor
pub fn LdpcNumInputBits(base_graph: usize, expansion_factor: usize) -> usize {
    LdpcNumInputCols(base_graph) * expansion_factor
}

// Return the number of parity bits per codeword with this base graph and
// expansion factor
pub fn LdpcMaxNumParityBits(base_graph: usize, expansion_factor: usize) -> usize {
    LdpcMaxNumRows(base_graph) * expansion_factor
}

pub fn LdpcMaxNumEncodedBits(base_graph: usize, zc: usize) -> usize {
    let num_punctured_cols = 2;
    let factor = if base_graph == 1 {
        BG1_COL_TOTAL - num_punctured_cols
    } else {
        BG2_COL_TOTAL - num_punctured_cols
    };
    zc * factor
}

pub fn LdpcNumEncodedBits(base_graph: usize, zc: usize, nRows: usize) -> usize {
    let num_punctured_cols = 2;
    zc * (LdpcNumInputCols(base_graph) + nRows - num_punctured_cols)
}
