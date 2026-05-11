#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
use std::collections::HashMap;

pub enum Direction {
    Downlink,
    Uplink,
}

#[repr(C)]
#[derive(PartialEq, Clone, Copy)]
pub enum SymbolType {
    kBeacon,
    kControl,
    kUL,
    kDL,
    kPilot,
    kCalDL,
    kCalUL,
    kGuard,
    kUnknown,
}

pub fn SymbolMap() -> HashMap<char, SymbolType> {
    let mut symbol_map = HashMap::new();
    symbol_map.insert('B', SymbolType::kBeacon);
    symbol_map.insert('C', SymbolType::kControl);
    symbol_map.insert('U', SymbolType::kUL);
    symbol_map.insert('D', SymbolType::kDL);
    symbol_map.insert('P', SymbolType::kPilot);
    symbol_map.insert('L', SymbolType::kCalDL);
    symbol_map.insert('l', SymbolType::kCalUL);
    symbol_map.insert('G', SymbolType::kGuard);
    symbol_map.insert('?', SymbolType::kUnknown);
    symbol_map
}

pub enum StageType {
    kFFT,
    kCSI,
    kBeam,
    kDemul,
    kDecode,
    kEncode,
    kIFFT,
    kBroadcast,
    kPrecode,
    kRC,
}

// Constants

// Maximum number of antennas supported
pub const MaxAntennas: usize = 64;

// Maximum number of transceiver channels per radio
pub const MaxChannels: usize = 2;

// Maximum number of UEs supported
pub const MaxUEs: usize = 64;

// Use 12-bit IQ sample to reduce network throughput
pub const use12BitIQ: bool = false;

// Frame Window size
pub const FrameWnd: usize = 2;

// Maximum number of symbols per frame allowed
pub const MaxSymbols: usize = 140;

// Number of subcarriers in a partial transpose block
pub const TransposeBlockSize: usize = 8;

// Number of subcarriers in one cache line, when represented as complex floats
pub const SCsPerCacheline: usize = 64 / (2 * std::mem::size_of::<f32>());

// Demodulation
pub const DownlinkHardDemod: bool = false;
pub const UplinkHardDemod: bool = false;
pub const MaxModType: usize = 8;
pub const DefaultMcsIndex: usize = 10;
