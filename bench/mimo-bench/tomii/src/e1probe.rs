//! E1 measurement probe (harness-side instrumentation, no runtime changes).
//!
//! Enabled only when `E1_FRAMES_OUT` is set; otherwise every hook is a single
//! `OnceLock` load + branch. Records, per frame id, on the same clock
//! (CLOCK_MONOTONIC) the C++ baselines use:
//!   first_rx  first packet of the frame decoded by the application
//!   last_rx   last packet of the frame decoded by the application
//!   done      last demul task of the frame finished
//!   overlap   FFT/CSI tasks that finished while the frame was still arriving
//!   verify    1 = demod output byte-identical to the golden, 2 = mismatch
//!
//! The same record format is written by `bench/mimo-bench/cpp/e1_common.hpp`,
//! so one analysis script (`e1/analyze.py`) scores every system identically.

use std::io::Write;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

const MAXF: usize = 1 << 16;

#[derive(Default)]
struct Rec {
    first_rx: AtomicU64,
    last_rx: AtomicU64,
    done: AtomicU64,
    rx: AtomicU32,
    overlap: AtomicU32,
    demul: AtomicU32,
    verify: AtomicU32,
    csi: AtomicU32,
    beam: AtomicU32,
    depviol: AtomicU32,
    exec: AtomicU32,    // FFT/CSI tasks finished (frame has started executing)
    winviol: AtomicU32, // started while frame - FrameWnd (same buffer window) was unfinished
}

const MAXSYM: usize = 32;

pub struct Probe {
    recs: Vec<Rec>,
    fft_sym: Vec<AtomicU32>, // [MAXF * MAXSYM] FFTs done per (frame, ul symbol)
    ppf: u32,
    out: String,
    golden: Option<Vec<u8>>,
    dump_golden: Option<String>,
    golden_frame: usize,
    n_expected: usize,
    done_count: AtomicUsize,
    verify_fail: AtomicUsize,
    last_activity: AtomicU64,
}

static PROBE: OnceLock<Option<Probe>> = OnceLock::new();

#[inline]
pub fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn env_usize(k: &str) -> Option<usize> {
    std::env::var(k).ok().and_then(|v| v.parse().ok())
}

fn init() -> Option<Probe> {
    let out = std::env::var("E1_FRAMES_OUT")
        .ok()
        .filter(|s| !s.is_empty())?;
    let ppf = env_usize("E1_PKTS_PER_FRAME").expect("E1_PKTS_PER_FRAME required") as u32;
    let golden = std::env::var("E1_GOLDEN")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|p| std::fs::read(&p).unwrap_or_else(|e| panic!("E1_GOLDEN {p}: {e}")));
    let mut recs = Vec::with_capacity(MAXF);
    recs.resize_with(MAXF, Rec::default);
    let mut fft_sym = Vec::with_capacity(MAXF * MAXSYM);
    fft_sym.resize_with(MAXF * MAXSYM, AtomicU32::default);
    let p = Probe {
        recs,
        fft_sym,
        ppf,
        out,
        golden,
        dump_golden: std::env::var("E1_DUMP_GOLDEN")
            .ok()
            .filter(|s| !s.is_empty()),
        golden_frame: env_usize("E1_GOLDEN_FRAME").unwrap_or(10),
        n_expected: env_usize("E1_NFRAMES").unwrap_or(usize::MAX),
        done_count: AtomicUsize::new(0),
        verify_fail: AtomicUsize::new(0),
        last_activity: AtomicU64::new(0),
    };
    // Flush at process exit too: Tomii exits as soon as done + dropped reaches
    // --max-frames, which can be before the writer thread's next flush.
    extern "C" fn flush_at_exit() {
        if let Some(Some(p)) = PROBE.get() {
            p.write();
        }
    }
    unsafe { libc::atexit(flush_at_exit) };
    // Writer thread: flushes the record file once all expected frames are done,
    // or after 2 s of inactivity (so a stalled/dropping run still leaves data).
    std::thread::Builder::new()
        .name("e1probe-writer".into())
        .spawn(|| {
            let mut written_at = usize::MAX;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let Some(Some(p)) = PROBE.get() else { continue };
                let done = p.done_count.load(Ordering::Acquire);
                let la = p.last_activity.load(Ordering::Relaxed);
                let idle = la != 0 && now_ns().saturating_sub(la) > 2_000_000_000;
                if (done >= p.n_expected || idle) && done != written_at {
                    p.write();
                    written_at = done;
                }
            }
        })
        .ok();
    Some(p)
}

#[inline]
fn probe() -> Option<&'static Probe> {
    PROBE.get_or_init(init).as_ref()
}

impl Probe {
    fn write(&self) {
        let mut s = String::new();
        s.push_str(&format!(
            "# {{\"system\":\"tomii\",\"ppf\":{},\"done\":{},\"verify_fail\":{},\"golden\":{}}}\n",
            self.ppf,
            self.done_count.load(Ordering::Acquire),
            self.verify_fail.load(Ordering::Acquire),
            self.golden.is_some()
        ));
        s.push_str("frame,first_rx_ns,last_rx_ns,done_ns,rx_pkts,overlap,verify,depviol,winviol,n_fftcsi,n_csi,n_beam,n_demul\n");
        for (f, r) in self.recs.iter().enumerate() {
            let fr = r.first_rx.load(Ordering::Acquire);
            if fr == 0 {
                continue;
            }
            s.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                f,
                fr,
                r.last_rx.load(Ordering::Acquire),
                r.done.load(Ordering::Acquire),
                r.rx.load(Ordering::Acquire),
                r.overlap.load(Ordering::Acquire),
                r.verify.load(Ordering::Acquire),
                r.depviol.load(Ordering::Acquire),
                r.winviol.load(Ordering::Acquire),
                r.exec.load(Ordering::Acquire),
                r.csi.load(Ordering::Acquire),
                r.beam.load(Ordering::Acquire),
                r.demul.load(Ordering::Acquire)
            ));
        }
        let tmp = format!("{}.tmp", self.out);
        if let Ok(mut f) = std::fs::File::create(&tmp) {
            let _ = f.write_all(s.as_bytes());
            let _ = std::fs::rename(&tmp, &self.out);
        }
    }
}

/// Called once per decoded packet (resolution thread, before admission).
#[inline]
pub fn on_packet(frame_id: usize) {
    let Some(p) = probe() else { return };
    let t = now_ns();
    let r = &p.recs[frame_id % MAXF];
    let _ = r
        .first_rx
        .compare_exchange(0, t, Ordering::AcqRel, Ordering::Relaxed);
    let n = r.rx.fetch_add(1, Ordering::AcqRel) + 1;
    if n == p.ppf {
        r.last_rx.store(t, Ordering::Release);
    }
    p.last_activity.store(t, Ordering::Relaxed);
}

/// Called at the end of every FFT/CSI task. `ul_sym` = Some(UL symbol index)
/// for FFT tasks, None for CSI (pilot) tasks.
#[inline]
pub fn on_fftcsi(frame_id: usize, ul_sym: Option<usize>) {
    let Some(p) = probe() else { return };
    let r = &p.recs[frame_id % MAXF];
    if r.rx.load(Ordering::Acquire) < p.ppf {
        r.overlap.fetch_add(1, Ordering::Relaxed);
    }
    r.exec.fetch_add(1, Ordering::Release);
    match ul_sym {
        Some(s) => {
            p.fft_sym[(frame_id % MAXF) * MAXSYM + s].fetch_add(1, Ordering::AcqRel);
        }
        None => {
            r.csi.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// Buffer-window check at the START of every FFT/CSI task: the plugin's shared
/// buffers hold FrameWnd frame windows (frame f -> window f % FrameWnd), so the
/// frame FrameWnd ids earlier must have finished before this frame writes.
#[inline]
pub fn on_task_start(frame_id: usize) {
    let Some(p) = probe() else { return };
    let fw = crate::common::symbols::frame_wnd();
    if frame_id < fw {
        return;
    }
    let g = &p.recs[(frame_id - fw) % MAXF];
    if g.exec.load(Ordering::Acquire) > 0 && g.done.load(Ordering::Acquire) == 0 {
        p.recs[frame_id % MAXF]
            .winviol
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Dependency check at the START of a beam task: every CSI task of the frame
/// must have finished (the graph's `csi.wait(0, total_pilot_symbols)` barrier).
/// A violation means the barrier fired early. With the Agora sender replaying
/// the same IQ every frame, an early-firing barrier after the first FrameWnd
/// frames would read the previous (identical) frame's data and could NOT be
/// caught by the output comparison alone — hence this explicit check.
#[inline]
pub fn on_beam_start<F: FnOnce() -> usize>(frame_id: usize, n_csi: F) {
    let Some(p) = probe() else { return };
    let r = &p.recs[frame_id % MAXF];
    if (r.csi.load(Ordering::Acquire) as usize) < n_csi() {
        r.depviol.fetch_add(1, Ordering::Relaxed);
    }
}
#[inline]
pub fn on_beam_end(frame_id: usize) {
    let Some(p) = probe() else { return };
    p.recs[frame_id % MAXF].beam.fetch_add(1, Ordering::AcqRel);
}

/// Dependency check at the START of a demul task: all FFTs of its UL symbol
/// (fft group_by antennas barrier) and all beam tasks must have finished.
#[inline]
pub fn on_demul_start(frame_id: usize, ul_sym: usize, n_ant: usize, n_beam: usize) {
    let Some(p) = probe() else { return };
    let r = &p.recs[frame_id % MAXF];
    let f = p.fft_sym[(frame_id % MAXF) * MAXSYM + ul_sym].load(Ordering::Acquire) as usize;
    let b = r.beam.load(Ordering::Acquire) as usize;
    if f < n_ant || b < n_beam {
        r.depviol.fetch_add(1, Ordering::Relaxed);
    }
}

/// Called at the end of every demul task. The task that completes the frame
/// stamps `done` first, then (off the measured interval) verifies the frame's
/// demod output against the golden. `cells` yields the (symbol, stream) cell
/// pointers in canonical order; each cell's first `cell_len` bytes are valid.
#[inline]
pub fn on_demul<F: Fn(usize, usize) -> *const i8>(
    frame_id: usize,
    total_demul: usize,
    n_sym: usize,
    n_ss: usize,
    cell_len: usize,
    cell: F,
) {
    let Some(p) = probe() else { return };
    let r = &p.recs[frame_id % MAXF];
    let n = r.demul.fetch_add(1, Ordering::AcqRel) + 1;
    if n as usize != total_demul {
        return;
    }
    let t = now_ns();
    r.done.store(t, Ordering::Release);
    p.last_activity.store(t, Ordering::Relaxed);

    let canon = |buf: &mut Vec<u8>| {
        for s in 0..n_sym {
            for ss in 0..n_ss {
                let ptr = cell(s, ss) as *const u8;
                buf.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, cell_len) });
            }
        }
    };
    if let Some(g) = &p.golden {
        let mut ok = g.len() == n_sym * n_ss * cell_len;
        if ok {
            'outer: for s in 0..n_sym {
                for ss in 0..n_ss {
                    let off = (s * n_ss + ss) * cell_len;
                    let got =
                        unsafe { std::slice::from_raw_parts(cell(s, ss) as *const u8, cell_len) };
                    if got != &g[off..off + cell_len] {
                        ok = false;
                        break 'outer;
                    }
                }
            }
        }
        r.verify.store(if ok { 1 } else { 2 }, Ordering::Release);
        if !ok {
            p.verify_fail.fetch_add(1, Ordering::AcqRel);
        }
    }
    if frame_id == p.golden_frame {
        if let Some(path) = &p.dump_golden {
            let mut buf = Vec::with_capacity(n_sym * n_ss * cell_len);
            canon(&mut buf);
            let _ = std::fs::write(path, &buf);
        }
    }
    p.done_count.fetch_add(1, Ordering::AcqRel);
}
