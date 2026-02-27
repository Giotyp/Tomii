use super::utils_ldpc::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct LDPCconfig {
    base_graph: u16,
    expansion_factor: u16,
    max_decoder_iter: i16,
    early_termination: bool,
    num_rows: usize,
    num_cb_len: u32,
    num_cb_codew_len: u32,
    num_blocks_in_symbol: usize,
}

impl LDPCconfig {
    pub fn new(
        base_graph: u16,
        expansion_factor: u16,
        max_decoder_iter: i16,
        early_termination: bool,
        num_cb_len: u32,
        num_cb_codew_len: u32,
        num_rows: usize,
        num_blocks_in_symbol: usize,
    ) -> Self {
        Self {
            base_graph,
            expansion_factor,
            max_decoder_iter,
            early_termination,
            num_rows,
            num_cb_len,
            num_cb_codew_len,
            num_blocks_in_symbol,
        }
    }

    pub fn NumInputBytes(&self) -> usize {
        BitsToBytes(LdpcNumInputBits(
            self.base_graph as usize,
            self.expansion_factor as usize,
        ))
    }

    pub fn NumBlocksInSymbol(&mut self, num_blocks: usize) {
        self.num_blocks_in_symbol = num_blocks;
    }

    pub fn GetNumBlocksInSymbol(&self) -> usize {
        self.num_blocks_in_symbol
    }
}

// Getters
impl LDPCconfig {
    pub fn base_graph(&self) -> u16 {
        self.base_graph
    }

    pub fn expansion_factor(&self) -> u16 {
        self.expansion_factor
    }

    pub fn max_decoder_iter(&self) -> i16 {
        self.max_decoder_iter
    }

    pub fn early_termination(&self) -> bool {
        self.early_termination
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_cb_len(&self) -> u32 {
        self.num_cb_len
    }

    pub fn num_cb_codew_len(&self) -> u32 {
        self.num_cb_codew_len
    }

    pub fn num_blocks_in_symbol(&self) -> usize {
        self.num_blocks_in_symbol
    }
}
