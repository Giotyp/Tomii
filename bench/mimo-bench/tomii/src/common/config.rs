#![allow(non_upper_case_globals)]

use num_complex::Complex;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};
use std::f64::consts::PI;
use std::fs::File;
use std::io::Read;

use super::comms_constants::*;
use super::comms_lib::*;
use super::framestats::FrameStats;
use super::ldpc_config::*;
use super::symbols;
use super::utils::*;
use super::utils_ldpc::*;
use super::utils_rdtsc::*;
use crate::modulation::{map_mod_to_str, map_mod_to_usize};

pub const MaxSupportedZc: usize = 256;
pub const CbPerSymbol: usize = 1;

#[derive(Serialize, Deserialize)]
pub struct Config {
    // System configuration
    freq_ghz: f64,
    avx512: bool,
    avx2: bool,
    core_offset: usize,
    exclude_cores: Vec<usize>,
    worker_thread_num: usize,
    socket_thread_num: usize,

    // UDP Configuration
    bs_server_port: i32,
    bs_server_addr: String,
    bs_rru_addr: String,

    // Subcarriers
    ofdm_tx_zero_prefix: usize,
    ofdm_rx_zero_prefix_bs: usize,
    ofdm_data_num: usize,
    ofdm_ca_num: usize,
    cp_len: usize,
    ofdm_tx_zero_postfix: usize,

    // Frame Schedule
    frame_schedule: String,
    max_frame: usize,

    // Antenna Configuration
    num_channels: usize,
    num_ue_channels: usize,

    bs_ant_num: usize, // * num_channels
    ue_ant_num: usize, // * num_ue_channels
    num_spatial_streams: usize,

    // Pilot buffers
    freq_orthogonal_pilot: bool,
    pilot_sc_group_size: usize,
    #[serde(skip_deserializing, skip_serializing)]
    pilots: Vec<Complex<f32>>,
    #[serde(skip_deserializing, skip_serializing)]
    pilots_sgn: Vec<Complex<f32>>,
    #[serde(skip_deserializing, skip_serializing)]
    common_pilot: Vec<Complex<f32>>,
    client_ul_pilot_symbols: usize,
    client_dl_pilot_symbols: usize,

    // Task sizes
    fft_block_size: usize,
    beam_block_size: usize,
    beam_events_per_symbol: usize,
    demul_block_size: usize,
    demul_events_per_symbol: usize,
    encode_block_size: usize,

    // Modulation
    ul_mcs: ULMcsConf,
    ul_mcs_index: usize,
    ul_mod_order_bits: usize,
    ul_modulation: String,
    ul_code_rate: usize,

    dl_mod_order_bits: usize,

    // LDPC
    ul_ldpc_config: LDPCconfig,
    ul_num_bytes_per_cb: usize,
    dl_ldpc_config: LDPCconfig,
    dl_num_bytes_per_cb: usize,
    scramble_enabled: bool,

    // Scheduling
    schedule_buffer_index: Vec<usize>,
    sched_rows: usize,
    sched_cols: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ULMcsConf {
    modulation: String,
    code_rate: f32,
    mcs_index: Option<usize>,
    base_graph: u16,
    earlyTermination: bool,
    decoderIter: i16,
}

// Constructor for Config
impl Config {
    pub fn new(config_file: &str) -> Self {
        let mut file = File::open(config_file).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let json_value: Value = serde_json::from_str(&contents).unwrap();

        // get CPU frequency
        let freq_ghz = measure_rdtsc_freq();

        // define if AVX512 or AVX2 is supported
        let mut avx512 = false;
        let mut avx2 = false;
        if is_x86_feature_detected!("avx512f") {
            println!("AVX-512 is supported");
            avx512 = true;
        } else if is_x86_feature_detected!("avx2") {
            avx2 = true;
        } else {
            println!("No AVX support");
        }

        // Channels and UE configuration
        let channel = Config::get_value("channel", "A".to_string(), &json_value);
        let ue_channel = Config::get_value("ue_channel", channel.clone(), &json_value);
        let num_channels = std::cmp::min(channel.len(), symbols::MaxChannels);
        let num_ue_channels = std::cmp::min(ue_channel.len(), symbols::MaxChannels);

        let ue_ant_num = {
            let num_radios = Config::get_value("ue_radio_num", 8, &json_value);
            num_channels * num_radios
        };

        // Subcarriers
        let ofdm_data_num = Config::get_value("ofdm_data_num", 1200, &json_value);

        // Task size
        let demul_block_size = Config::get_value("demul_block_size", 48, &json_value);
        let encode_block_size = Config::get_value("encode_block_size", 1, &json_value);

        // Pilot
        let freq_orthogonal_pilot = Config::get_value("freq_orthogonal_pilot", false, &json_value);
        let pilot_sc_group_size = Config::get_value(
            "pilot_sc_group_size",
            symbols::TransposeBlockSize,
            &json_value,
        );

        let beam_block_size = {
            let block_size = Config::get_value("beam_block_size", 1, &json_value);
            if freq_orthogonal_pilot && block_size == 1 {
                pilot_sc_group_size
            } else {
                block_size
            }
        };

        Config {
            // System configuration
            freq_ghz: freq_ghz,
            avx512: avx512,
            avx2: avx2,
            core_offset: Config::get_value("core_offset", 0, &json_value),
            exclude_cores: Config::get_value("exclude_cores", vec![0], &json_value),
            worker_thread_num: Config::get_value("worker_thread_num", 10, &json_value),
            socket_thread_num: Config::get_value("socket_thread_num", 1, &json_value),

            // UDP Configuration
            bs_server_port: Config::get_value("bs_server_port", 8000, &json_value),
            bs_server_addr: Config::get_value(
                "bs_server_addr",
                "127.0.0.1".to_string(),
                &json_value,
            ),
            bs_rru_addr: Config::get_value("bs_rru_addr", "127.0.0.1".to_string(), &json_value),

            // Subcarriers
            ofdm_tx_zero_prefix: Config::get_value("ofdm_tx_zero_prefix", 0, &json_value),
            ofdm_rx_zero_prefix_bs: Config::get_value("ofdm_rx_zero_prefix_bs", 0, &json_value),
            ofdm_tx_zero_postfix: Config::get_value("ofdm_tx_zero_postfix", 0, &json_value),
            ofdm_data_num: ofdm_data_num,
            ofdm_ca_num: Config::get_value("fft_size", 2048, &json_value),
            cp_len: Config::get_value("cp_size", 0, &json_value),

            // Frame Schedule
            frame_schedule: {
                let frame_schedule_vec: Vec<String> =
                    Config::get_value("frame_schedule", vec!["PUUU".to_string()], &json_value);
                frame_schedule_vec[0].clone()
            },
            max_frame: Config::get_value("max_frame", 10, &json_value),

            // Antenna Configuration
            num_channels: num_channels,
            num_ue_channels: num_ue_channels,

            bs_ant_num: {
                let num_radios = Config::get_value("bs_radio_num", 8, &json_value);
                num_channels * num_radios
            },
            ue_ant_num: ue_ant_num,

            num_spatial_streams: Config::get_value("num_spatial_streams", ue_ant_num, &json_value),

            // Pilot buffers
            freq_orthogonal_pilot: freq_orthogonal_pilot,
            pilot_sc_group_size: pilot_sc_group_size,
            pilots: Vec::new(),
            pilots_sgn: Vec::new(),
            common_pilot: Vec::new(),
            client_ul_pilot_symbols: Config::get_value("client_ul_pilot_syms", 0, &json_value),
            client_dl_pilot_symbols: Config::get_value("client_dl_pilot_syms", 0, &json_value),

            // Task Sizes
            fft_block_size: Config::get_value("fft_block_size", 1, &json_value),
            beam_block_size,
            beam_events_per_symbol: 1 + (ofdm_data_num - 1) / beam_block_size,

            demul_block_size: demul_block_size,
            demul_events_per_symbol: 1 + (ofdm_data_num - 1) / demul_block_size,
            encode_block_size: encode_block_size,

            // Modulation
            ul_mcs: {
                let ul_mcs_json = json_value.get("ul_mcs");

                if let Some(ul_mcs_json) = ul_mcs_json {
                    let modulation = ul_mcs_json["modulation"]
                        .as_str()
                        .unwrap_or("16QAM")
                        .to_string();
                    let code_rate = ul_mcs_json["code_rate"].as_f64().unwrap_or(0.333) as f32;
                    // mcs_index defines QAM, so if it doesn not exist in conf file, none value is given
                    let mcs_index: Option<usize> =
                        ul_mcs_json["mcs_index"].as_u64().map(|x| x as usize);

                    let base_graph = ul_mcs_json["base_graph"].as_u64().unwrap_or(1) as u16;
                    let earlyTermination =
                        ul_mcs_json["earlyTermination"].as_bool().unwrap_or(true);
                    let decoderIter = ul_mcs_json["decoderIter"].as_i64().unwrap_or(5) as i16;

                    ULMcsConf {
                        modulation,
                        code_rate,
                        mcs_index: mcs_index,
                        base_graph,
                        earlyTermination,
                        decoderIter,
                    }
                } else {
                    ULMcsConf {
                        modulation: "16QAM".to_string(),
                        code_rate: 0.333,
                        mcs_index: None,
                        base_graph: 1,
                        earlyTermination: true,
                        decoderIter: 5,
                    }
                }
            },

            // Decoding
            scramble_enabled: Config::get_value("wlan_scrambler", true, &json_value),

            // The following variables are initialized in UpdateUlMCS
            ul_mcs_index: 0,
            ul_mod_order_bits: 0,
            ul_modulation: String::new(),
            ul_code_rate: 0,
            dl_mod_order_bits: 0,

            // LDPC structures initialized in UpdateUlMCS
            ul_ldpc_config: LDPCconfig::new(0, 0, 0, false, 0, 0, 0, 0),
            ul_num_bytes_per_cb: 0,
            dl_ldpc_config: LDPCconfig::new(0, 0, 0, false, 0, 0, 0, 0),
            dl_num_bytes_per_cb: 0,

            // Schedule
            schedule_buffer_index: Vec::new(),
            sched_rows: 0,
            sched_cols: 0,
        }
    }

    // Get value from JSON data or use default
    fn get_value<T>(field_name: &str, default_value: T, json_value: &Value) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        json_value
            .get(field_name)
            .and_then(|v| from_value(v.clone()).ok())
            .unwrap_or(default_value)
    }
}

// Functions for Config
impl Config {
    pub fn Gen_pilots(&mut self) {
        // Generate common pilots based on Zadoff-Chu sequence for channel estimation
        let zc_seq_double = get_sequence(self.ofdm_data_num);
        let zc_seq = DoubleToCFloat(&zc_seq_double);

        self.common_pilot = seq_cyclic_shift(zc_seq, (PI / 4.0) as f32);

        for i in 0..self.ofdm_data_num {
            let val = Complex::new(self.common_pilot[i].re, self.common_pilot[i].im);
            self.pilots.push(val);

            let denom = self.common_pilot[i].norm_sqr();
            let pilot_sgn = self.common_pilot[i] / denom;

            let val = Complex::new(pilot_sgn.re, pilot_sgn.im);
            self.pilots_sgn.push(val);
        }
    }

    pub fn UpdateUlMCS(&mut self) {
        // Case that mcs_index is some in self.ul_mcs
        if let Some(mcs_idx) = self.ul_mcs.mcs_index {
            self.ul_mcs_index = mcs_idx;
            self.ul_mod_order_bits = get_mod_order_bits(mcs_idx);
            self.ul_modulation = map_mod_to_str(self.ul_mod_order_bits).to_string();
            self.ul_code_rate = get_code_rate(mcs_idx);
        } else {
            self.ul_modulation = self.ul_mcs.modulation.clone();
            self.ul_mod_order_bits = map_mod_to_usize(&self.ul_modulation);
            let ul_code_rate_usr = self.ul_mcs.code_rate;
            let code_rate_int: usize = (ul_code_rate_usr * 1024.0).round() as usize;

            self.ul_mcs_index = get_mcs_index(self.ul_mod_order_bits, code_rate_int);
            self.ul_code_rate = get_code_rate(self.ul_mcs_index);
        }

        let zc = SelectZc(
            self.ul_mcs.base_graph as usize,
            self.ul_code_rate as usize,
            self.ul_mod_order_bits as usize,
            self.ofdm_data_num,
            CbPerSymbol,
            "uplink".to_string(),
        );

        let num_rows = ((1024.0 * LdpcNumInputCols(self.ul_mcs.base_graph as usize) as f32
            / self.ul_code_rate as f32)
            .round() as usize)
            - (LdpcNumInputCols(self.ul_mcs.base_graph as usize) - 2);

        let num_cb_len: u32 = LdpcNumInputBits(self.ul_mcs.base_graph as usize, zc) as u32;
        let num_cb_codew_len: u32 =
            LdpcNumEncodedBits(self.ul_mcs.base_graph as usize, zc, num_rows) as u32;

        self.ul_ldpc_config = LDPCconfig::new(
            self.ul_mcs.base_graph,
            zc as u16,
            self.ul_mcs.decoderIter,
            self.ul_mcs.earlyTermination,
            num_cb_len,
            num_cb_codew_len,
            num_rows,
            0,
        );
        self.ul_ldpc_config.NumBlocksInSymbol(
            (self.ofdm_data_num * self.ul_mod_order_bits) / num_cb_codew_len as usize,
        );

        self.ul_num_bytes_per_cb = self.ul_ldpc_config.num_cb_len() as usize / 8;
    }

    pub fn ScheduleInit(&mut self) {
        let num_groups = if self.num_spatial_streams() == self.ue_ant_num() {
            1
        } else {
            self.ue_ant_num()
        };
        let rows = num_groups;
        let cols = self.ofdm_data_num * self.num_spatial_streams;
        self.schedule_buffer_index = vec![0; rows * cols];
        for gp in 0..num_groups {
            for sc in 0..self.ofdm_data_num {
                for ue in gp..(gp + self.num_spatial_streams()) {
                    let cur_ue = ue % self.ue_ant_num();
                    let offset = (gp * cols) + ((ue - gp) + self.num_spatial_streams * sc);
                    self.schedule_buffer_index[offset] = cur_ue;
                }
            }
        }
        self.sched_rows = rows;
        self.sched_cols = cols;
    }

    pub fn ScheduledUeList(&self, frame_id: usize, sc_id: usize) -> Vec<usize> {
        let gp = frame_id % self.sched_rows;
        let mut scheduled_ue_list: Vec<usize> = Vec::with_capacity(self.num_spatial_streams);

        for i in 0..self.num_spatial_streams {
            let index = self.schedule_buffer_index
                [gp * self.sched_cols + self.num_spatial_streams * sc_id + i];
            scheduled_ue_list.push(index);
        }
        scheduled_ue_list.sort();
        scheduled_ue_list
    }

    /// Non-allocating UE list: fills `buf[..n]` and returns `n`.
    /// Caller must provide buf with len >= num_spatial_streams.
    pub fn scheduled_ue_slice(&self, frame_id: usize, sc_id: usize, buf: &mut [usize]) -> usize {
        let gp = frame_id % self.sched_rows;
        let base = gp * self.sched_cols + self.num_spatial_streams * sc_id;
        let n = self.num_spatial_streams.min(buf.len());
        for i in 0..n {
            buf[i] = self.schedule_buffer_index[base + i];
        }
        buf[..n].sort_unstable();
        n
    }

    pub fn ModOrderBits(&self, dir: symbols::Direction) -> usize {
        match dir {
            symbols::Direction::Uplink => self.ul_mod_order_bits,
            symbols::Direction::Downlink => self.dl_mod_order_bits,
        }
    }

    pub fn ScheduledUeIndex(&self, frame_id: usize, sc_id: usize, sched_ue_id: usize) -> usize {
        self.ScheduledUeList(frame_id, sc_id)[sched_ue_id]
    }

    pub fn GetDataOffset(
        &self,
        frame_id: usize,
        symbol_id: usize,
        framestats: &FrameStats,
    ) -> usize {
        let frame_slot = frame_id % symbols::FrameWnd;
        let symbol_offset =
            (frame_slot * framestats.NumUlSyms()) + framestats.GetUlSymbolIdx(symbol_id);
        symbol_offset
    }

    pub fn GetTotalSymbolIdxUl(
        &self,
        frame_id: usize,
        symbol_idx_ul: usize,
        framestats: &FrameStats,
    ) -> usize {
        (frame_id % symbols::FrameWnd) * framestats.NumUlSyms() + symbol_idx_ul
    }

    pub fn GetTotalDataSymbolIdxUl(
        &self,
        frame_id: usize,
        data_symbol_idx_ul: usize,
        framestats: &FrameStats,
    ) -> usize {
        (frame_id % symbols::FrameWnd) * (framestats.NumUlSyms() - self.client_ul_pilot_symbols)
            + data_symbol_idx_ul
    }

    pub fn GetBeamScId(&self, sc_id: usize) -> usize {
        match self.freq_orthogonal_pilot {
            true => {
                let remain = sc_id % self.pilot_sc_group_size;
                sc_id - remain
            }
            false => sc_id,
        }
    }
}

fn SelectZc(
    base_graph: usize,
    code_rate: usize,
    mod_order_bits: usize,
    num_sc: usize,
    cb_per_sym: usize,
    direction: String,
) -> usize {
    let mut zc_vec: Vec<usize> = kZc.to_vec();
    zc_vec.sort();

    let max_zc_index = zc_vec.iter().position(|&r| r == MaxSupportedZc).unwrap();
    let max_uncoded_bits = (num_sc * code_rate * mod_order_bits) / 1024;
    let mut zc = usize::MAX;
    let mut i = 0;
    while i < max_zc_index {
        if zc_vec[i] * LdpcNumInputCols(base_graph) * cb_per_sym < max_uncoded_bits
            && zc_vec[i + 1] * LdpcNumInputCols(base_graph) * cb_per_sym > max_uncoded_bits
        {
            zc = zc_vec[i];
            break;
        }
        i += 1;
    }
    if zc == usize::MAX {
        println!(
            "Exceeded possible range of LDPC lifting Zc for {}!\
                Setting lifting size to max possible value({})",
            direction, MaxSupportedZc
        );
        zc = MaxSupportedZc;
    }
    zc
}

// One-line getters for Config class variables
impl Config {
    pub fn freq_ghz(&self) -> f64 {
        self.freq_ghz
    }
    pub fn avx512(&self) -> bool {
        self.avx512
    }
    pub fn avx2(&self) -> bool {
        self.avx2
    }

    pub fn worker_thread_num(&self) -> usize {
        self.worker_thread_num
    }
    pub fn socket_thread_num(&self) -> usize {
        self.socket_thread_num
    }
    pub fn core_offset(&self) -> usize {
        self.core_offset
    }

    pub fn bs_server_port(&self) -> i32 {
        self.bs_server_port
    }
    pub fn bs_server_addr(&self) -> &str {
        &self.bs_server_addr
    }

    pub fn ofdm_tx_zero_prefix(&self) -> usize {
        self.ofdm_tx_zero_prefix
    }
    pub fn ofdm_rx_zero_prefix_bs(&self) -> usize {
        self.ofdm_rx_zero_prefix_bs
    }
    pub fn ofdm_data_num(&self) -> usize {
        self.ofdm_data_num
    }
    pub fn ofdm_ca_num(&self) -> usize {
        self.ofdm_ca_num
    }
    pub fn ofdm_data_start(&self) -> usize {
        (self.ofdm_ca_num - self.ofdm_data_num) / 2
    }
    pub fn cp_len(&self) -> usize {
        self.cp_len
    }
    pub fn ofdm_tx_zero_postfix(&self) -> usize {
        self.ofdm_tx_zero_postfix
    }

    pub fn frame_schedule(&self) -> &str {
        &self.frame_schedule
    }
    pub fn max_frame(&self) -> usize {
        self.max_frame
    }

    pub fn num_channels(&self) -> usize {
        self.num_channels
    }
    pub fn num_ue_channels(&self) -> usize {
        self.num_ue_channels
    }
    pub fn bs_ant_num(&self) -> usize {
        self.bs_ant_num
    }
    pub fn ue_ant_num(&self) -> usize {
        self.ue_ant_num
    }
    pub fn num_spatial_streams(&self) -> usize {
        self.num_spatial_streams
    }

    pub fn freq_orth_pilot(&self) -> bool {
        self.freq_orthogonal_pilot
    }
    pub fn pilot_sc_group_size(&self) -> usize {
        self.pilot_sc_group_size
    }
    pub fn pilots(&self) -> &Vec<Complex<f32>> {
        &self.pilots
    }
    pub fn pilots_sgn(&self) -> &Vec<Complex<f32>> {
        &self.pilots_sgn
    }
    pub fn common_pilot(&self) -> &Vec<Complex<f32>> {
        &self.common_pilot
    }
    pub fn client_ul_pilot_symbols(&self) -> usize {
        self.client_ul_pilot_symbols
    }
    pub fn client_dl_pilot_symbols(&self) -> usize {
        self.client_dl_pilot_symbols
    }

    pub fn fft_block_size(&self) -> usize {
        self.fft_block_size
    }
    pub fn beam_block_size(&self) -> usize {
        self.beam_block_size
    }
    pub fn beam_events_per_symbol(&self) -> usize {
        self.beam_events_per_symbol
    }
    pub fn demul_block_size(&self) -> usize {
        self.demul_block_size
    }
    pub fn demul_events_per_symbol(&self) -> usize {
        self.demul_events_per_symbol
    }
    pub fn encode_block_size(&self) -> usize {
        self.encode_block_size
    }

    pub fn ul_mcs_index(&self) -> usize {
        self.ul_mcs_index
    }
    pub fn ul_mod_order_bits(&self) -> usize {
        self.ul_mod_order_bits
    }
    pub fn ul_modulation(&self) -> &str {
        &self.ul_modulation
    }
    pub fn ul_code_rate(&self) -> usize {
        self.ul_code_rate
    }

    pub fn dl_mod_order_bits(&self) -> usize {
        self.dl_mod_order_bits
    }

    pub fn scramble_enabled(&self) -> bool {
        self.scramble_enabled
    }

    pub fn LdpcConfig(&self, dir: symbols::Direction) -> &LDPCconfig {
        match dir {
            symbols::Direction::Uplink => &self.ul_ldpc_config,
            symbols::Direction::Downlink => &self.dl_ldpc_config,
        }
    }
    pub fn NumBytesPerCb(&self, dir: symbols::Direction) -> usize {
        match dir {
            symbols::Direction::Uplink => self.ul_num_bytes_per_cb,
            symbols::Direction::Downlink => self.dl_num_bytes_per_cb,
        }
    }
}
