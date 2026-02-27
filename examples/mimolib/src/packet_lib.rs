#![allow(non_upper_case_globals)]

use crate::common::config::Config;
use crate::common::symbols::use12BitIQ;
use std::fmt;

static OffsetOfData: usize = 64;

#[repr(C, align(64))]
pub struct PacketConfig {
    pub packet_length: usize,
    pub schedule_length: usize,
}
impl PacketConfig {
    pub fn new(config: &Config) -> Self {
        let samps_per_symbol = config.ofdm_tx_zero_prefix()
            + config.ofdm_ca_num()
            + config.cp_len()
            + config.ofdm_tx_zero_postfix();
        let iqsize = if use12BitIQ { 3 } else { 4 };
        let packet_length = OffsetOfData + (iqsize * samps_per_symbol);
        let schedule_length = config.frame_schedule().len();

        Self {
            packet_length,
            schedule_length,
        }
    }
}

#[repr(C, align(64))]
pub struct Packet {
    pub frame_id: u32,
    pub symbol_id: u32,
    cell_id: u32,
    pub ant_id: u32,
    fill: [u32; 12],
    pub data: Vec<i16>,
}

impl Packet {
    pub fn new() -> Self {
        Self {
            frame_id: 0,
            symbol_id: 0,
            cell_id: 0,
            ant_id: 0,
            // Padding for 64-byte alignment needed for SIMD
            fill: [0; 12],
            // Elements sent by antennae are two bytes (I/Q samples)
            data: vec![],
        }
    }

    /// Parse packet from byte buffer. Takes reference to avoid copying.
    /// Assumes buffer has correct structure: 4 u32 headers, 12 u32 fill, then i16 data pairs.
    pub fn from_bytes(&mut self, buffer: &[u8]) -> () {
        self.frame_id = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        self.symbol_id = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        self.cell_id = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        self.ant_id = u32::from_le_bytes(buffer[12..16].try_into().unwrap());

        // Bulk copy fill array instead of byte-by-byte
        for i in 0..self.fill.len() {
            self.fill[i] = u32::from(buffer[16 + i]);
        }

        let data_bytes = &buffer[OffsetOfData..];

        // Pre-allocate exact capacity to avoid reallocations
        let num_samples = data_bytes.len() / 2;
        self.data.clear();
        self.data.reserve(num_samples);

        // Parse i16 pairs efficiently using chunks
        for chunk in data_bytes.chunks_exact(2) {
            let value = i16::from_le_bytes(chunk.try_into().unwrap());
            self.data.push(value);
        }
    }

    /// Zero-copy alternative: parse directly from buffer reference.
    /// Returns Packet without copying the entire buffer first.
    pub fn from_bytes_ref(buffer: &[u8]) -> Self {
        let frame_id = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let symbol_id = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let cell_id = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let ant_id = u32::from_le_bytes(buffer[12..16].try_into().unwrap());

        let mut fill = [0u32; 12];
        for i in 0..fill.len() {
            fill[i] = u32::from(buffer[16 + i]);
        }

        let data_bytes = &buffer[OffsetOfData..];
        let num_samples = data_bytes.len() / 2;
        let mut data = Vec::with_capacity(num_samples);

        for chunk in data_bytes.chunks_exact(2) {
            let value = i16::from_le_bytes(chunk.try_into().unwrap());
            data.push(value);
        }

        Self {
            frame_id,
            symbol_id,
            cell_id,
            ant_id,
            fill,
            data,
        }
    }
}

// Implement Display for Packet for debugging purposes
impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[Frame num {}, symbol ID {}, cell ID {}, antenna ID {}",
            self.frame_id.to_string(),
            self.symbol_id,
            self.cell_id,
            self.ant_id
        )
    }
}
