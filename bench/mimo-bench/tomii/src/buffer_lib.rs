use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::structures::{Cube, Grid, Table};
use crate::common::symbols::{self, MaxModType};
use num_complex::Complex;
use tomii_types::Sliceable;

pub struct FftBuffer {
    buffer: Table<Complex<f32>>,
}

impl FftBuffer {
    pub fn new(config: &Config, framestats: &FrameStats) -> Self {
        let symbols_ul = framestats.NumUlSyms() * symbols::FrameWnd;
        let buffer = Table::new(symbols_ul, config.bs_ant_num() * config.ofdm_data_num());
        FftBuffer { buffer }
    }

    pub fn get_mut(&mut self) -> &mut Table<Complex<f32>> {
        &mut self.buffer
    }

    pub fn get(&self) -> &Table<Complex<f32>> {
        &self.buffer
    }

    /// Raw `*mut` to a row from a shared `&self` (concurrent disjoint writes;
    /// avoids aliased `&mut` UB under W>1). SAFETY: callers write disjoint rows.
    pub fn row_ptr(&self, row: usize) -> *mut Complex<f32> {
        self.buffer.row_ptr(row)
    }
}

impl Sliceable<Complex<f32>> for FftBuffer {
    fn as_mut_slice(&mut self) -> &mut [Complex<f32>] {
        self.buffer.as_mut_slice()
    }
}

pub struct CsiBuffer {
    buffer: Grid<Complex<f32>>,
}

impl CsiBuffer {
    pub fn new(config: &Config) -> Self {
        let buffer = Grid::new(
            symbols::FrameWnd,
            config.ue_ant_num(),
            config.bs_ant_num() * config.ofdm_data_num(),
        );
        CsiBuffer { buffer }
    }

    pub fn get_mut(&mut self) -> &mut Grid<Complex<f32>> {
        &mut self.buffer
    }

    pub fn get(&self) -> &Grid<Complex<f32>> {
        &self.buffer
    }

    /// Raw `*mut` to a (frame_slot, pilot) cell from a shared `&self`
    /// (concurrent disjoint writes; avoids aliased `&mut` UB). SAFETY: disjoint.
    pub fn cell_ptr(&self, frame_slot: usize, pilot: usize) -> *mut Complex<f32> {
        self.buffer.cell_ptr(frame_slot, pilot)
    }
}

impl Sliceable<Complex<f32>> for CsiBuffer {
    fn as_mut_slice(&mut self) -> &mut [Complex<f32>] {
        self.buffer.as_mut_slice()
    }
}

pub struct UlBeamMatrix {
    buffer: Grid<Complex<f32>>,
}

impl UlBeamMatrix {
    pub fn new(config: &Config) -> Self {
        let buffer = Grid::new(
            symbols::FrameWnd,
            config.ofdm_data_num(),
            config.bs_ant_num() * config.ue_ant_num(),
        );
        UlBeamMatrix { buffer }
    }

    pub fn get_mut(&mut self) -> &mut Grid<Complex<f32>> {
        &mut self.buffer
    }

    pub fn get(&self) -> &Grid<Complex<f32>> {
        &self.buffer
    }

    /// Raw `*mut` to a (frame_slot, sc) beam-matrix cell from a shared `&self`
    /// (concurrent disjoint writes; avoids aliased `&mut` UB). SAFETY: disjoint.
    pub fn cell_ptr(&self, frame_slot: usize, sc: usize) -> *mut Complex<f32> {
        self.buffer.cell_ptr(frame_slot, sc)
    }
}

pub struct DemodBuffer {
    buffer: Cube<i8>,
}

impl DemodBuffer {
    pub fn new(config: &Config, framestats: &FrameStats) -> Self {
        let demod_buffer = Cube::new(
            symbols::FrameWnd,
            framestats.NumUlDataSyms(config),
            config.num_spatial_streams(),
            MaxModType * config.ofdm_data_num(),
        );
        DemodBuffer {
            buffer: demod_buffer,
        }
    }

    pub fn get_mut(&mut self) -> &mut Cube<i8> {
        &mut self.buffer
    }

    pub fn get(&self) -> &Cube<i8> {
        &self.buffer
    }

    /// Raw `*mut i8` to the (frame_slot, data_symbol, stream) cell, from a shared
    /// `&self`. Concurrent demul tasks write disjoint subcarrier ranges of these
    /// cells; using a raw pointer (instead of `&mut DemodBuffer`) avoids the
    /// aliased-`&mut` UB that miscompiled demod output under W>1.
    /// SAFETY: callers must write disjoint regions (guaranteed by the
    /// node_index → (symbol, base_sc) bijection across demul tasks).
    pub fn cell_ptr(&self, frame_slot: usize, data_symbol: usize, ss: usize) -> *mut i8 {
        self.buffer.cell_ptr(frame_slot, data_symbol, ss)
    }

    /// Serialise just the frame-window slot for `frame_id` (= frame_id % FrameWnd).
    /// The demod buffer is dimensioned over FrameWnd, so the full buffer holds
    /// several frames at once; for a deterministic per-frame verification we
    /// extract only the slot belonging to the dumped frame. Every fully-received
    /// frame carries identical content for identical input, so this is
    /// byte-stable across runs (unlike hashing the whole multi-frame buffer).
    pub fn frame_bytes(&self, frame_id: usize) -> Vec<u8> {
        let frame_slot = frame_id % symbols::FrameWnd;
        self.buffer
            .d1_plane(frame_slot)
            .into_iter()
            .map(|b| b as u8)
            .collect()
    }
}
