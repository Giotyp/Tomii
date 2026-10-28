//! Opt-in resolution-thread phase profiler (`TOMII_PHASE_PROF=1`).
//!
//! Zero-allocation rdtsc accumulators kept in a thread-local; each resolution thread
//! prints one `PHASE_PROF` line to stderr when its loop exits. When the env var is
//! unset every hook is a single cached-bool check.
use std::cell::RefCell;
use std::sync::OnceLock;
use std::time::Instant;

pub(crate) const LOOP: usize = 0; // whole loop iteration
pub(crate) const IDLE: usize = 1; // iterations that found no packets and no batch entries
pub(crate) const POLL: usize = 2; // poll_and_process_network_packets (total)
pub(crate) const DRAIN: usize = 3; // channel drain (cnt = packets drained)
pub(crate) const REINJECT: usize = 4; // re-admit parked packets
pub(crate) const TELEM: usize = 5; // per-packet "Packet Received" timing hook
pub(crate) const DECODE: usize = 6; // extract_packet plugin call (+ buffer return)
pub(crate) const IDFN: usize = 7; // id_function plugin call
pub(crate) const WINDOW: usize = 8; // admission-window check
pub(crate) const PARK: usize = 9; // park_pending_packet
pub(crate) const ADMIT: usize = 10; // admit_packet (total, includes 11..13)
pub(crate) const SLOTASSIGN: usize = 11; // assign_frame_to_available_slot
pub(crate) const INITDISP: usize = 12; // initial-node dispatch on slot activation
pub(crate) const INDEXFN: usize = 13; // parse_args + index_function + counters
pub(crate) const COALESCE: usize = 14; // DR3 coalesce
pub(crate) const NETDRIVE: usize = 15; // drive_batch for packets (cnt = entries)
pub(crate) const BQ: usize = 16; // drain_and_process_batch_queue (total)
pub(crate) const BQDRIVE: usize = 17; // drive_batch for compute completions (cnt = entries)
pub(crate) const CHECKSLOTS: usize = 18; // check_slots
pub(crate) const DISPATCH: usize = 19; // dispatch_nodes (scheduler submission; cnt = nodes)
const N: usize = 20;

const NAMES: [&str; N] = [
    "loop",
    "idle",
    "poll",
    "drain",
    "reinject",
    "telem",
    "decode",
    "idfn",
    "window",
    "park",
    "admit",
    "slotassign",
    "initdisp",
    "indexfn",
    "coalesce",
    "netdrive",
    "bq",
    "bqdrive",
    "checkslots",
    "dispatch",
];

struct Prof {
    cyc: [u64; N],
    cnt: [u64; N],
    t0: Option<(u64, Instant)>,
}

thread_local! {
    static PROF: RefCell<Prof> = const { RefCell::new(Prof { cyc: [0; N], cnt: [0; N], t0: None }) };
}

#[inline]
pub(crate) fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("TOMII_PHASE_PROF").is_some_and(|v| v != "0"))
}

#[inline]
fn rdtsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        0
    }
}

/// Start a timed section (0 when disabled).
#[inline]
pub(crate) fn now() -> u64 {
    if enabled() {
        rdtsc()
    } else {
        0
    }
}

/// Close a timed section started with [`now`], counting `n` items.
#[inline]
pub(crate) fn add_n(phase: usize, start: u64, n: u64) {
    if enabled() {
        let end = rdtsc();
        PROF.with(|p| {
            let mut p = p.borrow_mut();
            if p.t0.is_none() {
                p.t0 = Some((start, Instant::now()));
            }
            p.cyc[phase] += end.wrapping_sub(start);
            p.cnt[phase] += n;
        });
    }
}

#[inline]
pub(crate) fn add(phase: usize, start: u64) {
    add_n(phase, start, 1);
}

/// Items of work seen so far on this thread (drained packets + batch-queue entries);
/// the loop compares marks to classify an iteration as idle.
#[inline]
pub(crate) fn work_mark() -> u64 {
    if enabled() {
        PROF.with(|p| {
            let p = p.borrow();
            p.cnt[DRAIN] + p.cnt[BQDRIVE]
        })
    } else {
        0
    }
}

/// Print this thread's accumulators: per phase total ms, count, and ns per count.
pub(crate) fn dump(thread_id: usize) {
    if !enabled() {
        return;
    }
    PROF.with(|p| {
        let p = p.borrow();
        let Some((tsc0, inst0)) = p.t0 else { return };
        let wall_ns = inst0.elapsed().as_nanos() as f64;
        let tsc_span = rdtsc().wrapping_sub(tsc0) as f64;
        if wall_ns <= 0.0 || tsc_span <= 0.0 {
            return;
        }
        let ns_per_cyc = wall_ns / tsc_span;
        let mut line = format!(
            "PHASE_PROF thread={} wall_ms={:.1} ghz={:.3}",
            thread_id,
            wall_ns / 1e6,
            1.0 / ns_per_cyc
        );
        for (i, name) in NAMES.iter().enumerate() {
            let ns = p.cyc[i] as f64 * ns_per_cyc;
            let per = if p.cnt[i] > 0 {
                ns / p.cnt[i] as f64
            } else {
                0.0
            };
            line.push_str(&format!(
                " {}={:.1}ms/{}/{:.0}ns",
                name,
                ns / 1e6,
                p.cnt[i],
                per
            ));
        }
        eprintln!("{line}");
    });
}

// ---- worker-side per-node stats (global atomics; dumped by resolution thread 0) ----
use std::sync::atomic::{AtomicU64, Ordering};
const MAXN: usize = 8;
#[allow(clippy::declare_interior_mutable_const)]
const Z: AtomicU64 = AtomicU64::new(0);
static W_TASKS: [AtomicU64; MAXN] = [Z; MAXN];
static W_INST: [AtomicU64; MAXN] = [Z; MAXN];
static W_EXEC_NS: [AtomicU64; MAXN] = [Z; MAXN];
static W_WAIT_NS: [AtomicU64; MAXN] = [Z; MAXN];
static W_WAIT_MAX_NS: [AtomicU64; MAXN] = [Z; MAXN];
static W_KERNEL_NS: [AtomicU64; MAXN] = [Z; MAXN];

/// Add the kernel-only time (args built -> kernel returned) of the task just executed.
#[inline]
pub(crate) fn worker_kernel(node: usize) {
    if !enabled() || node >= MAXN {
        return;
    }
    let m = MARKS.with(|m| m.get());
    W_KERNEL_NS[node].fetch_add(m[1].saturating_sub(m[0]), Ordering::Relaxed);
}

/// Record one executed task: `wait_ns` = spawn→start, `exec_ns` = start→end (incl. successor resolution).
#[inline]
pub(crate) fn worker_task(node: usize, instances: u64, wait_ns: u64, exec_ns: u64) {
    if !enabled() || node >= MAXN {
        return;
    }
    W_TASKS[node].fetch_add(1, Ordering::Relaxed);
    W_INST[node].fetch_add(instances, Ordering::Relaxed);
    W_EXEC_NS[node].fetch_add(exec_ns, Ordering::Relaxed);
    W_WAIT_NS[node].fetch_add(wait_ns, Ordering::Relaxed);
    W_WAIT_MAX_NS[node].fetch_max(wait_ns, Ordering::Relaxed);
}

pub(crate) fn dump_workers() {
    if !enabled() {
        return;
    }
    let mut line = String::from("WORKER_PROF");
    for n in 0..MAXN {
        let t = W_TASKS[n].load(Ordering::Relaxed);
        if t == 0 {
            continue;
        }
        let i = W_INST[n].load(Ordering::Relaxed);
        let e = W_EXEC_NS[n].load(Ordering::Relaxed) as f64;
        let w = W_WAIT_NS[n].load(Ordering::Relaxed) as f64;
        line.push_str(&format!(
            " node{}: tasks={} inst={} exec_total_ms={:.0} exec_per_task_us={:.1} kernel_per_task_us={:.1} wait_mean_us={:.1} wait_max_ms={:.1} |",
            n,
            t,
            i,
            e / 1e6,
            e / t as f64 / 1e3,
            W_KERNEL_NS[n].load(Ordering::Relaxed) as f64 / t as f64 / 1e3,
            w / t as f64 / 1e3,
            W_WAIT_MAX_NS[n].load(Ordering::Relaxed) as f64 / 1e6
        ));
    }
    eprintln!("{line}");
}

// ---- event trace (TOMII_TRACE=<path>): preallocated, pre-faulted, lock-free ----
// Each event is EW u64 words; all times are CLOCK_MONOTONIC ns (same clock as e1probe).
//   kind 1 TASK : w0 = 1 | worker<<8 | shard<<24 | node<<32 | bulk<<40
//                 w1 = frame<<32 | index ; w2 spawn ; w3 start ; w4 end
//   kind 2 BLOCK: w0 = 2 | worker<<8 | shard<<24 | got_task<<32
//                 w1 = load_at_start<<32 | load_at_end ; w2 start ; w3 end
use std::cell::Cell;
use std::sync::atomic::AtomicUsize;
const EW: usize = 5;
struct Trace {
    buf: Vec<AtomicU64>,
    cur: AtomicUsize,
    cap: usize,
    path: String,
}
static TRACE: OnceLock<Option<Trace>> = OnceLock::new();
thread_local! {
    static WORKER: Cell<(u64, u64)> = const { Cell::new((0xFFFF, 0xFF)) };
}

#[inline]
fn trace() -> Option<&'static Trace> {
    TRACE
        .get_or_init(|| {
            let path = std::env::var("TOMII_TRACE")
                .ok()
                .filter(|s| !s.is_empty())?;
            let cap: usize = std::env::var("TOMII_TRACE_CAP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3_000_000);
            // collect() writes every word, so all pages are faulted here, not on the hot path.
            let buf: Vec<AtomicU64> = (0..cap * EW).map(|_| AtomicU64::new(0)).collect();
            Some(Trace {
                buf,
                cur: AtomicUsize::new(0),
                cap,
                path,
            })
        })
        .as_ref()
}

/// Force trace allocation (call at startup, before traffic).
pub(crate) fn trace_init() {
    let _ = trace();
}

#[inline]
pub(crate) fn trace_on() -> bool {
    trace().is_some()
}

#[inline]
pub(crate) fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

pub(crate) fn set_worker(worker: usize, shard: usize) {
    WORKER.with(|w| w.set((worker as u64 & 0xFFFF, shard as u64 & 0xFF)));
}

#[inline]
fn push(words: [u64; EW]) {
    if let Some(t) = trace() {
        let i = t.cur.fetch_add(1, Ordering::Relaxed);
        if i < t.cap {
            for (k, w) in words.iter().enumerate() {
                t.buf[i * EW + k].store(*w, Ordering::Relaxed);
            }
        }
    }
}

#[inline]
pub(crate) fn trace_task(
    node: usize,
    bulk: usize,
    frame: usize,
    index: usize,
    spawn: u64,
    start: u64,
    end: u64,
) {
    let (w, s) = WORKER.with(|c| c.get());
    push([
        1 | (w << 8) | (s << 24) | ((node as u64 & 0xFF) << 32) | ((bulk as u64 & 0xFFFF) << 40),
        ((frame as u64 & 0xFFFF_FFFF) << 32) | (index as u64 & 0xFFFF_FFFF),
        spawn,
        start,
        end,
    ]);
}

#[inline]
pub(crate) fn trace_block(got_task: bool, load0: usize, load1: usize, start: u64, end: u64) {
    let (w, s) = WORKER.with(|c| c.get());
    push([
        2 | (w << 8) | (s << 24) | ((got_task as u64) << 32),
        ((load0 as u64 & 0xFFFF_FFFF) << 32) | (load1 as u64 & 0xFFFF_FFFF),
        start,
        end,
        0,
    ]);
}

/// Write the trace as raw little-endian u64 words (header: n_events, EW).
pub(crate) fn trace_dump() {
    let Some(t) = trace() else { return };
    let n = t.cur.load(Ordering::Acquire).min(t.cap);
    let mut out: Vec<u8> = Vec::with_capacity(16 + n * EW * 8);
    out.extend_from_slice(&(n as u64).to_le_bytes());
    out.extend_from_slice(&(EW as u64).to_le_bytes());
    for w in &t.buf[..n * EW] {
        out.extend_from_slice(&w.load(Ordering::Relaxed).to_le_bytes());
    }
    match std::fs::write(&t.path, &out) {
        Ok(()) => eprintln!(
            "TRACE written {} events (dropped {}) to {}",
            n,
            t.cur.load(Ordering::Relaxed).saturating_sub(t.cap),
            t.path
        ),
        Err(e) => eprintln!("TRACE write failed: {e}"),
    }
}

/// kind 3 ADMIT event: w0 = 3 | sub<<8 ; w1 = frame ; w2 = time ; w3 = a ; w4 = b.
/// sub: 1 park(window) a=completed  2 park(no slot)  3 reinject a=n_packets  4 slot activated a=slot
///      5 packet routed to slot BUFFER a=slot  6 frame complete a=slot  7 frame dropped
#[inline]
pub(crate) fn trace_admit(sub: u64, frame: usize, a: u64, b: u64) {
    if trace_on() {
        push([3 | (sub << 8), frame as u64, mono_ns(), a, b]);
    }
}

// ---- spin_wait_for_result accounting (by predecessor node id) ----
static SW_CALLS: [AtomicU64; MAXN] = [Z; MAXN];
static SW_NS: [AtomicU64; MAXN] = [Z; MAXN];
static SW_MAX_NS: [AtomicU64; MAXN] = [Z; MAXN];

#[inline]
pub(crate) fn spin_wait_done(pred_node: usize, ns: u64) {
    if !enabled() || pred_node >= MAXN {
        return;
    }
    SW_CALLS[pred_node].fetch_add(1, Ordering::Relaxed);
    SW_NS[pred_node].fetch_add(ns, Ordering::Relaxed);
    SW_MAX_NS[pred_node].fetch_max(ns, Ordering::Relaxed);
}

pub(crate) fn dump_spin_wait() {
    if !enabled() {
        return;
    }
    let mut line = String::from("SPINWAIT_PROF");
    for n in 0..MAXN {
        let c = SW_CALLS[n].load(Ordering::Relaxed);
        if c > 0 {
            line.push_str(&format!(
                " pred{}: calls={} total_ms={:.1} max_ms={:.2} |",
                n,
                c,
                SW_NS[n].load(Ordering::Relaxed) as f64 / 1e6,
                SW_MAX_NS[n].load(Ordering::Relaxed) as f64 / 1e6
            ));
        }
    }
    eprintln!("{line}");
}

// ---- intra-task marks (trace only): 0 = args built, 1 = kernel returned ----
thread_local! {
    static MARKS: Cell<[u64; 4]> = const { Cell::new([0; 4]) };
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Marks 0/1 record wall time; marks 2/3 the thread CPU time at the same points.
#[inline]
pub(crate) fn mark(i: usize) {
    if enabled() || trace_on() {
        let t = mono_ns();
        let c = thread_cpu_ns();
        MARKS.with(|m| {
            let mut v = m.get();
            v[i] = t;
            v[i + 2] = c;
            m.set(v);
        });
    }
}

/// kind 4 SLOW TASK: w0 = 4 | node<<8 | worker<<16 ; w1 = start ; w2 = args_done ;
/// w3 = kernel_done ; w4 = end. Emitted only for tasks longer than 200 us.
#[inline]
pub(crate) fn trace_slow_task(node: usize, start: u64, end: u64) {
    if end.saturating_sub(start) < 200_000 {
        return;
    }
    let (w, _) = WORKER.with(|c| c.get());
    let m = MARKS.with(|m| m.get());
    let kcpu_us = (m[3].saturating_sub(m[2]) / 1000) & 0xFFFF_FFFF;
    push([
        4 | ((node as u64 & 0xFF) << 8) | (w << 16) | (kcpu_us << 32),
        start,
        m[0],
        m[1],
        end,
    ]);
}
