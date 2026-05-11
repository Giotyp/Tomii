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
}
