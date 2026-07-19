//! Tomii plugin for the FMCW radar pipeline.
//!
//! Packet handling and network callbacks live here in Rust; the DSP kernels
//! (range/Doppler FFT, CFAR, clustering) live in kernels/libradar_kernels.so
//! behind the C ABI in kernels/radar.h, so a CUDA build of the same symbols
//! swaps the compute onto the GPU without touching this file or the graph.
//!
//! Topology (see build_graph.py):
//!   $network -> range_fft (factor n_chirps) -> doppler_fft (factor n_tiles)
//!            -> cfar (factor n_tiles) -> cluster (factor 1)

// make_radar_config's generated _cm wrapper inherits its 10 params.
#![allow(clippy::too_many_arguments)]

use std::ffi::c_void;
use std::io::Write;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

// ---------------------------------------------------------------------------
// FFI to the kernel library (kernels/radar.h)
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct RkDetection {
    pub range_bin: u32,
    pub doppler_bin: u32,
    pub power: f32,
}

extern "C" {
    fn rk_init(
        n_samples: u32,
        n_chirps: u32,
        frame_wnd: u32,
        n_tiles: u32,
        guard: u32,
        train: u32,
        pfa_scale: f32,
        max_dets_per_tile: u32,
    ) -> *mut c_void;
    fn rk_free(ctx: *mut c_void);
    fn rk_make_range_ws(ctx: *mut c_void) -> *mut c_void;
    fn rk_make_doppler_ws(ctx: *mut c_void) -> *mut c_void;
    fn rk_free_ws(ws: *mut c_void);
    fn rk_alloc_rd(ctx: *mut c_void) -> *mut c_void;
    fn rk_alloc_power(ctx: *mut c_void) -> *mut c_void;
    fn rk_alloc_dets(ctx: *mut c_void) -> *mut c_void;
    fn rk_free_buf(buf: *mut c_void);
    fn rk_range_fft(
        ctx: *mut c_void,
        ws: *mut c_void,
        iq: *const i16,
        n_samples: u32,
        chirp_id: u32,
        rd: *mut c_void,
        slot: u32,
    );
    fn rk_doppler_fft(
        ctx: *mut c_void,
        ws: *mut c_void,
        tile: u32,
        rd: *const c_void,
        power: *mut c_void,
        slot: u32,
    );
    fn rk_cfar(
        ctx: *mut c_void,
        tile: u32,
        power: *const c_void,
        dets: *mut c_void,
        slot: u32,
    ) -> u32;
    fn rk_cluster(
        ctx: *mut c_void,
        dets: *const c_void,
        slot: u32,
        out: *mut RkDetection,
        out_cap: u32,
    ) -> u32;
}

// ---------------------------------------------------------------------------
// Opaque handles stored in CmTypes::Any. The raw pointers are only touched
// from tasks the graph serializes correctly; Send/Sync moves them between
// worker threads.
// ---------------------------------------------------------------------------

macro_rules! handle {
    ($name:ident, $free:expr) => {
        pub struct $name(pub *mut c_void);
        unsafe impl Send for $name {}
        unsafe impl Sync for $name {}
        impl Drop for $name {
            fn drop(&mut self) {
                unsafe { $free(self.0) }
            }
        }
    };
}

handle!(KernelCtx, rk_free);
handle!(FftWs, rk_free_ws);
handle!(RdBuffer, rk_free_buf);
handle!(PowerBuffer, rk_free_buf);
handle!(DetsBuffer, rk_free_buf);

// Obtain a *mut T from CmTypes::Any or CmTypes::AnyHeld (zero-lock bulk path).
unsafe fn raw_mut<T: std::any::Any + Send + Sync + 'static>(cm: &CmTypes) -> *mut T {
    if let CmTypes::AnyHeld(data) = cm {
        return unsafe { data.downcast_ref::<T>() }
            .map(|r| r as *const T as *mut T)
            .unwrap_or_else(|| {
                panic!(
                    "raw_mut AnyHeld: wrong type for {}",
                    std::any::type_name::<T>()
                )
            });
    }
    unsafe { cm.as_mut_ptr::<T>() }
        .map(|g| g.ptr)
        .unwrap_or_else(|| panic!("raw_mut Any: wrong type for {}", std::any::type_name::<T>()))
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct RadarConfig {
    pub n_samples: usize,
    pub n_chirps: usize,
    pub n_tiles: usize,
    pub frame_wnd: usize,
    pub guard: usize,
    pub train: usize,
    pub pfa_scale: f64,
    pub max_dets_per_tile: usize,
    pub address: String,
    pub port: usize,
}

const HEADER_LEN: usize = 64;
const MAX_CLUSTER_OUT: usize = 1024;

#[tomii_export]
pub fn make_radar_config(
    n_samples: usize,
    n_chirps: usize,
    n_tiles: usize,
    frame_wnd: usize,
    guard: usize,
    train: usize,
    pfa_scale: f64,
    max_dets_per_tile: usize,
    address: String,
    port: usize,
) -> RadarConfig {
    assert!(
        n_samples.is_multiple_of(n_tiles),
        "n_tiles must divide n_samples"
    );
    RadarConfig {
        n_samples,
        n_chirps,
        n_tiles,
        frame_wnd,
        guard,
        train,
        pfa_scale,
        max_dets_per_tile,
        address,
        port,
    }
}

#[tomii_export]
pub fn get_packet_length(config: &RadarConfig) -> usize {
    HEADER_LEN + 4 * config.n_samples
}

#[tomii_export]
pub fn get_frame_packets(config: &RadarConfig) -> usize {
    config.n_chirps
}

#[tomii_export]
pub fn get_n_chirps(config: &RadarConfig) -> usize {
    config.n_chirps
}

#[tomii_export]
pub fn get_n_tiles(config: &RadarConfig) -> usize {
    config.n_tiles
}

#[tomii_export]
pub fn get_num_channels(_config: &RadarConfig) -> usize {
    1
}

/// Server address for the network receiver (network_config `address`).
#[no_mangle]
pub fn get_address(config: &CmTypes) -> CmTypes {
    config
        .with_any(|c: &RadarConfig| CmTypes::new_string(c.address.clone()))
        .expect("get_address: wrong type")
}

/// Base UDP port for the network receiver (network_config `start_port`).
#[no_mangle]
pub fn get_port(config: &CmTypes) -> CmTypes {
    config
        .with_any(|c: &RadarConfig| CmTypes::Usize(c.port))
        .expect("get_port: wrong type")
}

// ---------------------------------------------------------------------------
// Kernel context, workspaces, buffers (graph initializations)
// ---------------------------------------------------------------------------

#[tomii_export]
pub fn create_kernel_ctx(config: &RadarConfig) -> KernelCtx {
    KernelCtx(unsafe {
        rk_init(
            config.n_samples as u32,
            config.n_chirps as u32,
            config.frame_wnd as u32,
            config.n_tiles as u32,
            config.guard as u32,
            config.train as u32,
            config.pfa_scale as f32,
            config.max_dets_per_tile as u32,
        )
    })
}

#[tomii_export]
pub fn create_range_ws(ctx: &KernelCtx) -> FftWs {
    FftWs(unsafe { rk_make_range_ws(ctx.0) })
}

#[tomii_export]
pub fn create_doppler_ws(ctx: &KernelCtx) -> FftWs {
    FftWs(unsafe { rk_make_doppler_ws(ctx.0) })
}

#[tomii_export]
pub fn create_rd_buffer(ctx: &KernelCtx) -> RdBuffer {
    RdBuffer(unsafe { rk_alloc_rd(ctx.0) })
}

#[tomii_export]
pub fn create_power_buffer(ctx: &KernelCtx) -> PowerBuffer {
    PowerBuffer(unsafe { rk_alloc_power(ctx.0) })
}

#[tomii_export]
pub fn create_dets_buffer(ctx: &KernelCtx) -> DetsBuffer {
    DetsBuffer(unsafe { rk_alloc_dets(ctx.0) })
}

// ---------------------------------------------------------------------------
// Chirp packet + network callbacks
// ---------------------------------------------------------------------------

/// One UDP packet = one chirp. Layout mirrors sender.py:
/// u32 frame_id, u32 chirp_id, u32 channel_id, u32 n_samples,
/// 48 pad bytes, then n_samples interleaved LE i16 I/Q pairs.
pub struct ChirpPacket {
    pub frame_id: u32,
    pub chirp_id: u32,
    pub channel_id: u32,
    pub n_samples: u32,
    pub data: Vec<i16>,
}

impl ChirpPacket {
    pub fn from_bytes_ref(buffer: &[u8]) -> Self {
        let frame_id = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let chirp_id = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let channel_id = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let n_samples = u32::from_le_bytes(buffer[12..16].try_into().unwrap());

        let data_bytes = &buffer[HEADER_LEN..];
        let mut data = Vec::with_capacity(data_bytes.len() / 2);
        for chunk in data_bytes.chunks_exact(2) {
            data.push(i16::from_le_bytes(chunk.try_into().unwrap()));
        }
        Self {
            frame_id,
            chirp_id,
            channel_id,
            n_samples,
            data,
        }
    }
}

/// Parse raw packet bytes into a ChirpPacket (network extract_packet_func).
#[no_mangle]
pub fn process_packet(bytes_cm: &CmTypes) -> CmTypes {
    if let CmTypes::Bytes(arc) = bytes_cm {
        CmTypes::from_any(ChirpPacket::from_bytes_ref(arc.as_slice()))
    } else {
        panic!("process_packet: expected CmTypes::Bytes, got {:?}", bytes_cm)
    }
}

/// Frame (CPI) id for slot assignment (network id_function).
#[no_mangle]
pub fn get_frame_id(packet: &CmTypes) -> usize {
    packet
        .with_any(|p: &ChirpPacket| p.frame_id as usize)
        .expect("get_frame_id: wrong type")
}

/// Content-based packet index within the frame (network index_function).
#[no_mangle]
pub fn get_chirp_index(packet: &CmTypes) -> usize {
    packet
        .with_any(|p: &ChirpPacket| p.chirp_id as usize)
        .expect("get_chirp_index: wrong type")
}

// ---------------------------------------------------------------------------
// Pipeline ops (hand-coded `_cm` bridges; graph func names without `_cm`).
// Shared buffers are taken as `&CmTypes` and written through raw pointers —
// `&mut` to a buffer shared by concurrent tasks would be aliased-UB.
// ---------------------------------------------------------------------------

/// range_fft — window + FFT one chirp, corner-turned into the rd buffer.
/// Returns frame_id for downstream slot lookup.
#[no_mangle]
pub fn range_fft_cm(
    packet: &CmTypes,
    config: &CmTypes,
    ctx: &CmTypes,
    ws: &CmTypes,
    rd: &CmTypes,
    _index: usize,
) -> CmTypes {
    let packet = unsafe { &*raw_mut::<ChirpPacket>(packet) };
    let config = unsafe { &*raw_mut::<RadarConfig>(config) };
    let ctx = unsafe { &*raw_mut::<KernelCtx>(ctx) };
    let ws = unsafe { &*raw_mut::<FftWs>(ws) };
    let rd = unsafe { &*raw_mut::<RdBuffer>(rd) };

    assert_eq!(
        packet.n_samples as usize, config.n_samples,
        "packet n_samples mismatch"
    );
    assert!(packet.data.len() >= 2 * config.n_samples);
    let frame_id = packet.frame_id as usize;
    let slot = (frame_id % config.frame_wnd) as u32;

    unsafe {
        rk_range_fft(
            ctx.0,
            ws.0,
            packet.data.as_ptr(),
            packet.n_samples,
            packet.chirp_id,
            rd.0,
            slot,
        );
    }
    stage_done_idx(frame_id, 0, packet.chirp_id as usize);
    CmTypes::Usize(frame_id)
}

// Debug instrumentation (TOMII_RADAR_CHECK=1): count stage completions per
// frame and report when a stage starts before its predecessor barrier should
// have allowed it. Mutex acquire/release gives the happens-before edge, so a
// short count here means the runtime really did fire the successor early.
static STAGE_COUNTS: std::sync::Mutex<Option<std::collections::HashMap<(usize, u8), usize>>> =
    std::sync::Mutex::new(None);

fn check_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("TOMII_RADAR_CHECK").is_ok_and(|v| v == "1"))
}

// Per-(frame, stage) instance-completion times, for duplicate / late detection.
type InstanceMap =
    std::collections::HashMap<(usize, u8), std::collections::HashMap<usize, std::time::Instant>>;
static STAGE_INSTANCES: std::sync::Mutex<Option<InstanceMap>> = std::sync::Mutex::new(None);

fn stage_done(frame_id: usize, stage: u8) {
    stage_done_idx(frame_id, stage, usize::MAX);
}

fn stage_done_idx(frame_id: usize, stage: u8, instance: usize) {
    if !check_enabled() {
        return;
    }
    {
        let mut guard = STAGE_INSTANCES.lock().unwrap();
        let m = guard.get_or_insert_with(Default::default);
        if instance != usize::MAX {
            if let Some(first) = m
                .entry((frame_id, stage))
                .or_default()
                .insert(instance, std::time::Instant::now())
            {
                eprintln!(
                    "RADAR_CHECK DUPLICATE: stage {stage} frame {frame_id} instance {instance} \
                     completed again {:.1}us after first (thread {:?})",
                    first.elapsed().as_nanos() as f64 / 1e3,
                    std::thread::current().id(),
                );
            }
        }
        // Completion arriving after the NEXT stage already began on this frame
        // means this task raced a reader of its output buffer.
        if m.contains_key(&(frame_id, stage + 1)) {
            eprintln!(
                "RADAR_CHECK LATE: stage {stage} frame {frame_id} instance {instance} \
                 completed after stage {} started",
                stage + 1
            );
        }
    }
    let mut guard = STAGE_COUNTS.lock().unwrap();
    *guard
        .get_or_insert_with(Default::default)
        .entry((frame_id, stage))
        .or_insert(0) += 1;
}

fn stage_expect(frame_id: usize, prev_stage: u8, expected: usize, who: &str) {
    if !check_enabled() {
        return;
    }
    // Mark this stage as started so late predecessor completions are flagged.
    {
        let mut guard = STAGE_INSTANCES.lock().unwrap();
        guard
            .get_or_insert_with(Default::default)
            .entry((frame_id, prev_stage + 1))
            .or_default();
    }
    let guard = STAGE_COUNTS.lock().unwrap();
    let got = guard
        .as_ref()
        .and_then(|m| m.get(&(frame_id, prev_stage)).copied())
        .unwrap_or(0);
    if got != expected {
        eprintln!(
            "RADAR_CHECK VIOLATION: {who} frame {frame_id} started with {got}/{expected} \
             predecessor completions"
        );
    }
}

/// doppler_fft — chirp-axis FFT + power for this tile's range rows.
#[no_mangle]
pub fn doppler_fft_cm(
    config: &CmTypes,
    ctx: &CmTypes,
    ws: &CmTypes,
    rd: &CmTypes,
    power: &CmTypes,
    frame_id: usize,
    tile: usize,
) -> CmTypes {
    let config = unsafe { &*raw_mut::<RadarConfig>(config) };
    let ctx = unsafe { &*raw_mut::<KernelCtx>(ctx) };
    let ws = unsafe { &*raw_mut::<FftWs>(ws) };
    let rd = unsafe { &*raw_mut::<RdBuffer>(rd) };
    let power = unsafe { &*raw_mut::<PowerBuffer>(power) };

    let slot = (frame_id % config.frame_wnd) as u32;
    stage_expect(frame_id, 0, config.n_chirps, "doppler_fft");
    unsafe { rk_doppler_fft(ctx.0, ws.0, tile as u32, rd.0, power.0, slot) };
    stage_done_idx(frame_id, 1, tile);
    CmTypes::Usize(frame_id)
}

/// cfar — 2D CA-CFAR over this tile's range rows.
#[no_mangle]
pub fn cfar_cm(
    config: &CmTypes,
    ctx: &CmTypes,
    power: &CmTypes,
    dets: &CmTypes,
    frame_id: usize,
    tile: usize,
) -> CmTypes {
    let config = unsafe { &*raw_mut::<RadarConfig>(config) };
    let ctx = unsafe { &*raw_mut::<KernelCtx>(ctx) };
    let power = unsafe { &*raw_mut::<PowerBuffer>(power) };
    let dets = unsafe { &*raw_mut::<DetsBuffer>(dets) };

    let slot = (frame_id % config.frame_wnd) as u32;
    stage_expect(frame_id, 1, config.n_tiles, "cfar");
    let count = unsafe { rk_cfar(ctx.0, tile as u32, power.0, dets.0, slot) };
    stage_done_idx(frame_id, 2, tile);
    CmTypes::Usize(count as usize)
}

/// cluster — group this frame's detections; strongest cell per cluster.
/// Appends one line per frame to $TOMII_VERIFY_PATH when set (verify.py).
#[no_mangle]
pub fn cluster_cm(config: &CmTypes, ctx: &CmTypes, dets: &CmTypes, frame_id: usize) -> CmTypes {
    let config = unsafe { &*raw_mut::<RadarConfig>(config) };
    let ctx = unsafe { &*raw_mut::<KernelCtx>(ctx) };
    let dets = unsafe { &*raw_mut::<DetsBuffer>(dets) };

    let slot = (frame_id % config.frame_wnd) as u32;
    stage_expect(frame_id, 2, config.n_tiles, "cluster");
    let mut out = vec![RkDetection::default(); MAX_CLUSTER_OUT];
    let n = unsafe { rk_cluster(ctx.0, dets.0, slot, out.as_mut_ptr(), MAX_CLUSTER_OUT as u32) }
        as usize;
    out.truncate(n);
    out.sort_by_key(|d| (d.range_bin, d.doppler_bin));

    dump_detections_if_env(frame_id, &out);
    CmTypes::Usize(n)
}

fn dump_detections_if_env(frame_id: usize, dets: &[RkDetection]) {
    let path = match std::env::var("TOMII_VERIFY_PATH") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    static DUMP_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    let mut line = format!("frame {frame_id}:");
    for d in dets {
        line.push_str(&format!(" {},{},{:.3e}", d.range_bin, d.doppler_bin, d.power));
    }
    line.push('\n');

    let _guard = DUMP_LOCK.lock().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("dump_detections: open {path} failed: {e}"));
    file.write_all(line.as_bytes())
        .unwrap_or_else(|e| panic!("dump_detections: write {path} failed: {e}"));
}
