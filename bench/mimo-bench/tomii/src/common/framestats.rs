#![allow(dead_code)]

use super::config::*;
use super::symbols::*;
pub struct FrameStats {
    frame_identifier: String,

    beacon_symbols: Vec<usize>,
    pilot_symbols: Vec<usize>,
    ul_symbols: Vec<usize>,
    ul_cal_symbols: Vec<usize>,
    dl_symbols: Vec<usize>,
    dl_cal_symbols: Vec<usize>,
    dl_control_symbols: Vec<usize>,
}

impl FrameStats {
    pub fn new(frame_identifier: String) -> Self {
        let mut beacon_symbols = Vec::new();
        let mut pilot_symbols = Vec::new();
        let mut ul_symbols = Vec::new();
        let mut ul_cal_symbols = Vec::new();
        let mut dl_symbols = Vec::new();
        let mut dl_cal_symbols = Vec::new();
        let mut dl_control_symbols = Vec::new();

        for i in 0..frame_identifier.len() {
            let symbol = frame_identifier.chars().nth(i).unwrap();
            match symbol {
                'B' => beacon_symbols.push(i),
                'P' => pilot_symbols.push(i),
                'U' => ul_symbols.push(i),
                'L' => ul_cal_symbols.push(i),
                'D' => dl_symbols.push(i),
                'C' => dl_cal_symbols.push(i),
                'S' => dl_control_symbols.push(i),
                'G' => {
                    // Guard symbol - no action needed (matches C++ behavior)
                }
                _ => {
                    eprintln!("Unknown symbol {} in frame: {}", symbol, frame_identifier);
                }
            }
        }

        Self {
            frame_identifier,
            beacon_symbols,
            pilot_symbols,
            ul_symbols,
            ul_cal_symbols,
            dl_symbols,
            dl_cal_symbols,
            dl_control_symbols,
        }
    }
}

impl FrameStats {
    pub fn UlSymbols(&self) -> &Vec<usize> {
        &self.ul_symbols
    }

    pub fn GetUlSymbol(&self, location: usize) -> usize {
        self.ul_symbols[location]
    }

    pub fn GetSymbolType(&self, symbol_id: usize) -> SymbolType {
        let symbol = self
            .frame_identifier
            .as_bytes()
            .get(symbol_id)
            .copied()
            .unwrap_or(b'?') as char;
        match symbol {
            'B' => SymbolType::kBeacon,
            'C' => SymbolType::kControl,
            'U' => SymbolType::kUL,
            'D' => SymbolType::kDL,
            'P' => SymbolType::kPilot,
            'L' => SymbolType::kCalUL,
            'G' => SymbolType::kGuard,
            _ => SymbolType::kUnknown,
        }
    }

    pub fn GetSymbolIdx(&self, search_vector: &Vec<usize>, symbol_number: usize) -> usize {
        // Find the index where symbol_number appears in the search_vector
        // This matches the C++ std::equal_range behavior
        match search_vector.binary_search(&symbol_number) {
            Ok(index) => index,
            Err(_) => usize::MAX,
        }
    }

    pub fn GetPilotSymbolIdx(&self, symbol_number: usize) -> usize {
        self.GetSymbolIdx(&self.pilot_symbols, symbol_number)
    }

    pub fn GetUlSymbolIdx(&self, symbol_number: usize) -> usize {
        self.GetSymbolIdx(&self.ul_symbols, symbol_number)
    }

    pub fn NumPilotSyms(&self) -> usize {
        self.pilot_symbols.len()
    }

    pub fn NumBeaconSyms(&self) -> usize {
        self.beacon_symbols.len()
    }

    pub fn NumDlSyms(&self) -> usize {
        self.dl_symbols.len()
    }

    pub fn NumUlSyms(&self) -> usize {
        self.ul_symbols.len()
    }

    pub fn NumUlCalSyms(&self) -> usize {
        self.ul_cal_symbols.len()
    }

    pub fn NumDlCalSyms(&self) -> usize {
        self.dl_cal_symbols.len()
    }

    pub fn NumDlControlSyms(&self) -> usize {
        self.dl_control_symbols.len()
    }

    pub fn NumUlDataSyms(&self, config: &Config) -> usize {
        self.NumUlSyms() - config.client_ul_pilot_symbols()
    }
}
