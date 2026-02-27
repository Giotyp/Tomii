use std::collections::HashMap;
use std::sync::RwLock;

use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::structures::{Cube, Grid, Table};
use crate::common::symbols::MaxModType;
use crate::common::symbols::{self, Direction};
use crate::common::utils::roundup;
use crate::common::utils_rdtsc::*;
use crate::packet_lib::Packet;
use num_complex::Complex;
use synstream_types::Sliceable;

pub struct PacketBuffer {
    buffer: RwLock<HashMap<u32, Vec<Packet>>>,
}

impl PacketBuffer {
    pub fn new() -> Self {
        PacketBuffer {
            buffer: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert_packet(&self, frame_id: u32, packet: Packet) {
        let mut buffer = self.buffer.write().unwrap();
        if let Some(packets) = buffer.get_mut(&frame_id) {
            packets.push(packet);
        } else {
            buffer.insert(frame_id, vec![packet]);
        }
    }

    pub fn get_packet_ptr(&self, frame_id: u32, packet_id: usize, offset: usize) -> *const i16 {
        let buffer = self.buffer.read().unwrap();
        if let Some(packets) = buffer.get(&frame_id) {
            let packet = &packets[packet_id];
            unsafe { packet.data.as_ptr().add(offset) as *const i16 }
        } else {
            panic!("Packet not found");
        }
    }
}

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

pub struct DecodedBuffer {
    buffer: Cube<i8>,
}

impl DecodedBuffer {
    pub fn new(config: &Config, framestats: &FrameStats) -> Self {
        let num_blocks_in_symbol = config.LdpcConfig(Direction::Uplink).GetNumBlocksInSymbol();
        let decoded_buffer = Cube::new(
            symbols::FrameWnd,
            framestats.NumUlDataSyms(config),
            config.ue_ant_num(),
            num_blocks_in_symbol * roundup(config.NumBytesPerCb(Direction::Uplink), 64),
        );
        DecodedBuffer {
            buffer: decoded_buffer,
        }
    }

    pub fn get_mut(&mut self) -> &mut Cube<i8> {
        &mut self.buffer
    }

    pub fn get(&self) -> &Cube<i8> {
        &self.buffer
    }
}

pub struct Buffers {
    packet_buffer: HashMap<u32, Vec<Packet>>,
    fft_buffer: Table<Complex<f32>>,
    csi_buffer: Grid<Complex<f32>>,
    ul_beam_matrix: Grid<Complex<f32>>,
    demod_buffer: Cube<i8>,
    decoded_buffer: Cube<i8>,
}

impl Buffers {
    pub fn new(config: &Config, framestats: &FrameStats) -> Self {
        let packet_buffer: HashMap<u32, Vec<Packet>> = HashMap::new();

        let symbols_ul = framestats.NumUlSyms() * symbols::FrameWnd;
        let fft_buffer: Table<Complex<f32>> =
            Table::new(symbols_ul, config.bs_ant_num() * config.ofdm_data_num());

        let csi_buffer = Grid::new(
            symbols::FrameWnd,
            config.ue_ant_num(),
            config.bs_ant_num() * config.ofdm_data_num(),
        );
        let ul_beam_matrix = Grid::new(
            symbols::FrameWnd,
            config.ofdm_data_num(),
            config.bs_ant_num() * config.ue_ant_num(),
        );

        let demod_buffer = Cube::new(
            symbols::FrameWnd,
            framestats.NumUlDataSyms(config),
            config.ue_ant_num(),
            MaxModType * config.ofdm_data_num(),
        );

        let num_blocks_in_symbol = config.LdpcConfig(Direction::Uplink).GetNumBlocksInSymbol();
        let decoded_buffer = Cube::new(
            symbols::FrameWnd,
            framestats.NumUlDataSyms(config),
            config.ue_ant_num(),
            num_blocks_in_symbol * roundup(config.NumBytesPerCb(Direction::Uplink), 64),
        );

        Buffers {
            packet_buffer,
            fft_buffer,
            csi_buffer,
            ul_beam_matrix,
            demod_buffer,
            decoded_buffer,
        }
    }
}

// Getters
impl Buffers {
    pub fn packet_buffer_mut(&mut self) -> &mut HashMap<u32, Vec<Packet>> {
        &mut self.packet_buffer
    }

    pub fn packet_buffer(&self) -> &HashMap<u32, Vec<Packet>> {
        &self.packet_buffer
    }

    pub fn fft_buffer(&mut self) -> &mut Table<Complex<f32>> {
        &mut self.fft_buffer
    }

    pub fn csi_buffer(&mut self) -> &mut Grid<Complex<f32>> {
        &mut self.csi_buffer
    }

    pub fn ul_beam_matrix(&mut self) -> &mut Grid<Complex<f32>> {
        &mut self.ul_beam_matrix
    }

    pub fn demod_buffer(&mut self) -> &mut Cube<i8> {
        &mut self.demod_buffer
    }

    pub fn decoded_buffer(&mut self) -> &mut Cube<i8> {
        &mut self.decoded_buffer
    }
}

const MAX_STAT_BREAKDOWN: usize = 4;
pub struct TimeBuffer {
    // vector of cycles for each task
    time_buffers: HashMap<String, Vec<[u64; MAX_STAT_BREAKDOWN]>>,
    frame_cycles: HashMap<String, Vec<[f64; MAX_STAT_BREAKDOWN]>>,
    freq_ghz: f64,
    task_count: HashMap<String, Vec<Vec<usize>>>,
    workers: usize,
}

impl TimeBuffer {
    pub fn new(freq_ghz: f64, workers: usize) -> Self {
        TimeBuffer {
            time_buffers: HashMap::new(),
            frame_cycles: HashMap::new(),
            freq_ghz,
            task_count: HashMap::new(),
            workers,
        }
    }

    pub fn init_task(&mut self, task: &str, time_buffer_len: usize) {
        // If task with same name exists, ommit
        if self.time_buffers.contains_key(task) {
            return;
        } else {
            self.time_buffers.insert(
                task.to_string(),
                vec![[0; MAX_STAT_BREAKDOWN]; time_buffer_len],
            );
            self.frame_cycles.insert(
                task.to_string(),
                vec![[0.0; MAX_STAT_BREAKDOWN]; time_buffer_len],
            );
            self.task_count.insert(
                task.to_string(),
                vec![vec![0; time_buffer_len]; self.workers],
            );
        }
    }

    pub fn increase_task(&mut self, task: &str, worker_id: usize, frame_id: usize, task_no: usize) {
        if let Some(buffer) = self.task_count.get_mut(task) {
            let buffer = buffer.get_mut(worker_id).unwrap();
            if frame_id < buffer.len() {
                buffer[frame_id] += task_no;
            }
        }
    }

    pub fn add_time(&mut self, task: &str, frame_id: usize, index: usize, time: u64) {
        if let Some(buffer) = self.time_buffers.get_mut(task) {
            if frame_id < buffer.len() && index < MAX_STAT_BREAKDOWN {
                buffer[frame_id][index] += time;
            }
        }
    }

    pub fn frame_summary(&mut self, task: &str, frame_id: usize, threads: usize) {
        if let Some(buffer) = self.time_buffers.get(task) {
            if frame_id < buffer.len() {
                for i in 0..MAX_STAT_BREAKDOWN {
                    let cycles = cycles_to_us(buffer[frame_id][i], self.freq_ghz);
                    self.frame_cycles.get_mut(task).unwrap()[frame_id][i] = cycles / threads as f64;
                }
            }
        }
    }

    pub fn frame_average_us(
        &self,
        task: &str,
        index: usize,
        frame_offset: usize,
        frames: usize,
    ) -> f64 {
        if let Some(buffer) = self.frame_cycles.get(task) {
            let mut average: f64 = 0.0;

            for frame_id in frame_offset..frames {
                average += buffer[frame_id][index];
            }

            // Average for frames
            average / (frames - frame_offset) as f64
        } else {
            0.0
        }
    }

    pub fn get_task_count(&self, task: &str, worker_id: usize, frame_id: usize) -> usize {
        if let Some(buffer) = self.task_count.get(task) {
            let buffer = buffer.get(worker_id).unwrap();
            if frame_id < buffer.len() {
                buffer[frame_id]
            } else {
                0
            }
        } else {
            0
        }
    }

    pub fn get_total_worker_tasks(&self, task: &str, worker_id: usize) -> usize {
        if let Some(buffer) = self.task_count.get(task) {
            let buffer = buffer.get(worker_id).unwrap();
            buffer.iter().sum()
        } else {
            0
        }
    }
}
