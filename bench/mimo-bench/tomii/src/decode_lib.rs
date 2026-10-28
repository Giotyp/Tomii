#![allow(non_upper_case_globals)]
//! LDPC decode stage (FlexRAN 5GNR AVX-512 decoder), wired as a 5th MIMO node
//! fft → csi → beam → demul → **decode**. Each decode task decodes one code
//! block (batch 1) from demul's soft-LLR output into the DecodedBuffer. Matches
//! Agora's Table-3 decode: BG1, rate 1/3, Zc=104 (6864-bit codeword), ≤5 iters,
//! one code block per (UL data symbol, spatial stream) → 208 tasks/frame at
//! 64×16 (16 streams × 13 UL × 1 block).

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use crate::bindings::phy_ldpc_decoder_5gnr_bindings::*;
use crate::bindings::scrambler_bindings::*;
use crate::buffer_lib::*;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::ldpc_config::LDPCconfig;
use crate::common::structures::AlignedVec;
use crate::common::symbols::{frame_wnd, Direction};
use crate::common::utils::roundup;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

// Decoder scratch (variable-node buffer). 5GNR BG1/BG2 need ≤ 68·Zc i16; this
// is a generous upper bound. Thread-local so concurrent decode tasks never race
// on it (same rationale as demul's per-call working buffers) without re-allocating.
const VarNodesCap: usize = 1024 * 1024;

thread_local! {
    static RESP_VAR_NODES: RefCell<AlignedVec<i16>> =
        RefCell::new(AlignedVec::new(VarNodesCap, 64));
    // One WLAN descrambler object per worker thread (Agora creates one per worker).
    static SCRAMBLER: Cell<*mut libc::c_void> = const { Cell::new(std::ptr::null_mut()) };
}

fn scrambler_handle() -> *mut libc::c_void {
    SCRAMBLER.with(|s| {
        let mut p = s.get();
        if p.is_null() {
            p = unsafe { scrambler_new() };
            s.set(p);
        }
        p
    })
}

// ── Correctness / BLER sanity counters (printed once at process exit) ─────────
static DEC_TASKS: AtomicU64 = AtomicU64::new(0);
static DEC_PARITY_OK: AtomicU64 = AtomicU64::new(0);
static DEC_ITER_SUM: AtomicU64 = AtomicU64::new(0);
static DEC_ATEXIT: Once = Once::new();

extern "C" fn decode_stats_atexit() {
    let n = DEC_TASKS.load(Ordering::Relaxed);
    if n == 0 {
        return;
    }
    let ok = DEC_PARITY_OK.load(Ordering::Relaxed);
    let it = DEC_ITER_SUM.load(Ordering::Relaxed);
    eprintln!(
        "DECODE_STATS: tasks={n} parity_ok={ok} parity_ok_pct={:.4} avg_iters={:.3}",
        100.0 * ok as f64 / n as f64,
        it as f64 / n as f64,
    );
}

// Obtain a *mut T from CmTypes::Any / CmTypes::AnyHeld (mirrors demul_lib::raw_mut).
unsafe fn raw_mut<T: std::any::Any + Send + Sync + 'static>(cm: &CmTypes) -> *mut T {
    if let CmTypes::AnyHeld(data) = cm {
        return unsafe { data.downcast_ref::<T>() }
            .map(|r| r as *const T as *mut T)
            .unwrap_or_else(|| panic!("raw_mut AnyHeld: wrong type for {}", std::any::type_name::<T>()));
    }
    unsafe { cm.as_mut_ptr::<T>() }
        .map(|g| g.ptr)
        .unwrap_or_else(|| panic!("raw_mut Any: wrong type for {}", std::any::type_name::<T>()))
}

/// Decode tasks per UL symbol = spatial streams × code blocks per symbol.
#[tomii_export]
pub fn decode_tasks_per_symbol(config: &Config) -> usize {
    config.num_spatial_streams() * config.LdpcConfig(Direction::Uplink).GetNumBlocksInSymbol()
}

/// Total decode tasks per frame = UL symbols × decode_tasks_per_symbol
/// (64×16: 13 × 16 × 1 = 208, matching Agora Table 3).
#[tomii_export]
pub fn total_decode_tasks(config: &Config, framestats: &FrameStats) -> usize {
    framestats.NumUlSyms()
        * config.num_spatial_streams()
        * config.LdpcConfig(Direction::Uplink).GetNumBlocksInSymbol()
}

#[tomii_export]
pub fn create_decode_buffers(config: &Config, framestats: &FrameStats) -> DecodedBuffer {
    DecodedBuffer::new(config, framestats)
}

/// Optional terminal node (verify.py / decode-golden only): serialise this
/// frame's decoded-buffer slot to `$TOMII_DECODE_VERIFY_PATH` after all decode
/// tasks complete. Mirrors `dump_demod_if_env`; off for perf runs.
#[tomii_export]
pub fn dump_decoded_if_env(decode_buffer: &DecodedBuffer, frame_id: usize) {
    let path = match std::env::var("TOMII_DECODE_VERIFY_PATH") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    std::fs::write(&path, decode_buffer.frame_bytes(frame_id))
        .unwrap_or_else(|e| panic!("dump_decoded_if_env: write to {path} failed: {e}"));
}

/// Hand-coded `_cm` bridge (registry key `decode_op`): takes `decode_buffer` and
/// `demod_buffers` as `&CmTypes` and forms raw pointers via `raw_mut` — never an
/// aliased `&mut` to the whole buffer — so concurrent decode tasks that write
/// disjoint (symbol, ue, cb) regions are sound under W>1 (same discipline as
/// `demul_op_cm`). `symbol/stream/cb` are derived from `node_index`.
#[no_mangle]
pub fn decode_op_cm(
    config: &CmTypes,
    framestats: &CmTypes,
    decode_buffer: &CmTypes,
    demod_buffers: &CmTypes,
    frame_id: usize,
    node_index: usize,
) -> CmTypes {
    DEC_ATEXIT.call_once(|| unsafe {
        libc::atexit(decode_stats_atexit);
    });

    let config = unsafe { &*raw_mut::<Config>(config) };
    let framestats = unsafe { &*raw_mut::<FrameStats>(framestats) };
    // Shared `&` (read-only here) and shared `&` with raw cell writes — never `&mut`.
    let demod_buffers = unsafe { &*raw_mut::<DemodBuffer>(demod_buffers) };
    let decode_buffer = unsafe { &*raw_mut::<DecodedBuffer>(decode_buffer) };

    let frame_slot = frame_id % frame_wnd();
    let ldpc_config: &LDPCconfig = config.LdpcConfig(Direction::Uplink);
    let num_blocks = ldpc_config.GetNumBlocksInSymbol();
    let tasks_per_symbol = config.num_spatial_streams() * num_blocks;

    // Decompose node_index → (UL-symbol ordinal, spatial stream, code block).
    let total_decode = framestats.NumUlSyms() * tasks_per_symbol;

    let ul_ordinal = node_index / tasks_per_symbol;
    let within = node_index % tasks_per_symbol;
    let stream_id = within / num_blocks;
    let cur_cb_id = within % num_blocks;

    let symbol_id = framestats.GetUlSymbol(ul_ordinal);
    // Skip any client UL *pilot* symbols (data symbols only). For this config
    // client_ul_pilot_symbols == 0, so all UL symbols are decoded.
    if symbol_id < framestats.GetUlSymbol(config.client_ul_pilot_symbols()) {
        crate::e1probe::on_decode(frame_id, total_decode);
        return CmTypes::Usize(frame_id);
    }

    let ue_id = config.ScheduledUeIndex(frame_id, 0, stream_id);
    let num_bytes_per_cb = config.NumBytesPerCb(Direction::Uplink);
    let data_symbol_idx_ul =
        framestats.GetUlSymbolIdx(symbol_id) - config.client_ul_pilot_symbols();

    let num_filler_bits: i16 = 0;
    let num_channel_llrs: i16 = ldpc_config.num_cb_codew_len() as i16;
    let mod_bits = config.ModOrderBits(Direction::Uplink);
    let llr_offset = mod_bits * (ldpc_config.num_cb_codew_len() as usize * cur_cb_id);

    // Input: demul's soft LLRs for (symbol, stream), offset by code block.
    let demod_cell = demod_buffers.cell_ptr(frame_slot, data_symbol_idx_ul, stream_id);
    let llr_buffer_ptr: *mut i8 = unsafe { demod_cell.add(llr_offset) };

    // Output: compacted message bytes for (symbol, ue), offset by code block.
    let decode_cell = decode_buffer.cell_ptr(frame_slot, data_symbol_idx_ul, ue_id);
    let out_offset = cur_cb_id * roundup(num_bytes_per_cb, 64);
    let decode_ptr = unsafe { decode_cell.add(out_offset) as *mut u8 };

    let num_msg_bits = ldpc_config.num_cb_len() - num_filler_bits as u32;

    RESP_VAR_NODES.with(|rv| {
        let mut rv = rv.borrow_mut();
        let mut request = bblib_ldpc_decoder_5gnr_request {
            Zc: ldpc_config.expansion_factor(),
            baseGraph: ldpc_config.base_graph() as i32,
            nRows: ldpc_config.num_rows() as i32,
            varNodes: llr_buffer_ptr,
            numChannelLlrs: num_channel_llrs,
            numFillerBits: num_filler_bits,
            maxIterations: ldpc_config.max_decoder_iter(),
            enableEarlyTermination: ldpc_config.early_termination(),
        };
        let mut response = bblib_ldpc_decoder_5gnr_response {
            varNodes: rv.get_mut().as_mut_ptr(),
            numMsgBits: num_msg_bits as i32,
            compactedMessageBytes: decode_ptr,
            iterationAtTermination: Default::default(),
            parityPassedAtTermination: Default::default(),
        };
        unsafe {
            let res = bblib_ldpc_decoder_5gnr(&mut request, &mut response);
            if res != 0 {
                panic!("LDPC decoder failed (frame {frame_id}, node {node_index})");
            }
            if config.scramble_enabled() {
                scrambler_descramble(
                    scrambler_handle(),
                    decode_ptr as *mut libc::c_void,
                    num_bytes_per_cb,
                );
            }
        }
        DEC_TASKS.fetch_add(1, Ordering::Relaxed);
        DEC_ITER_SUM.fetch_add(response.iterationAtTermination as u64, Ordering::Relaxed);
        if response.parityPassedAtTermination {
            DEC_PARITY_OK.fetch_add(1, Ordering::Relaxed);
        }
    });

    crate::e1probe::on_decode(frame_id, total_decode);
    CmTypes::Usize(frame_id)
}
