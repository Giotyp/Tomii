//! Opt-in kernel sub-phase timer (`TOMII_KPROF=1`); totals printed at process exit.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

pub const FFT_CONV: usize = 0;
pub const FFT_DFTI: usize = 1;
pub const FFT_SHIFT: usize = 2;
pub const FFT_TRANSPOSE: usize = 3;
pub const DEMUL_ALLOC: usize = 4;
pub const DEMUL_GATHER: usize = 5;
pub const DEMUL_EQ: usize = 6;
pub const DEMUL_DEMOD: usize = 7;
const N: usize = 8;
const NAMES: [&str; N] = [
    "fft_conv",
    "fft_dfti",
    "fft_shift",
    "fft_transpose",
    "demul_alloc",
    "demul_gather",
    "demul_eq",
    "demul_demod",
];

#[allow(clippy::declare_interior_mutable_const)]
const Z: AtomicU64 = AtomicU64::new(0);
static NS: [AtomicU64; N] = [Z; N];
static CNT: [AtomicU64; N] = [Z; N];

fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let on = std::env::var("TOMII_KPROF").is_ok_and(|v| v == "1");
        if on {
            unsafe { libc::atexit(dump) };
        }
        on
    })
}

#[inline]
fn now() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[inline]
pub fn t() -> u64 {
    if enabled() {
        now()
    } else {
        0
    }
}

#[inline]
pub fn add(phase: usize, t0: u64) {
    if t0 != 0 {
        NS[phase].fetch_add(now() - t0, Ordering::Relaxed);
        CNT[phase].fetch_add(1, Ordering::Relaxed);
    }
}

extern "C" fn dump() {
    let mut s = String::from("KPROF");
    for i in 0..N {
        let c = CNT[i].load(Ordering::Relaxed);
        if c > 0 {
            s.push_str(&format!(
                " {}={:.3}us(n={})",
                NAMES[i],
                NS[i].load(Ordering::Relaxed) as f64 / c as f64 / 1e3,
                c
            ));
        }
    }
    eprintln!("{s}");
}
