extern crate libc;

use libc::{clock_gettime, timespec, CLOCK_REALTIME};
use std::arch::asm;
use std::mem::zeroed;
use std::sync::OnceLock;

static RDTSC_FREQ_GHZ: OnceLock<f64> = OnceLock::new();

/// Read the timestamp counter with serialization (prevents instruction reordering).
/// `rdtscp` waits for all prior instructions to complete before reading the counter,
/// giving more accurate per-task measurements than plain `rdtsc`.
pub fn rdtsc() -> u64 {
    let rax: u32;
    let rdx: u32;
    // SAFETY: `rdtscp` is a valid x86_64 read-only instruction; all output registers are
    // explicitly declared, so no undefined register state is introduced.
    unsafe {
        asm!(
            "rdtscp",
            out("rax") rax,
            out("rdx") rdx,
            out("rcx") _,  // rdtscp also writes processor ID to ecx
        );
    }
    ((rdx as u64) << 32) | (rax as u64)
}

/// Initialize the RDTSC frequency cache eagerly.
/// Call this once at startup (before any timing) to avoid calibration during hot paths.
pub fn init_rdtsc_freq() {
    RDTSC_FREQ_GHZ.get_or_init(measure_rdtsc_freq);
}

fn get_rdtsc_freq() -> f64 {
    *RDTSC_FREQ_GHZ.get_or_init(measure_rdtsc_freq)
}

/// Measure the frequency of RDTSC based by comparing against
/// CLOCK_REALTIME. This is a pretty function that should be called only
/// during initialization.
fn measure_rdtsc_freq() -> f64 {
    // SAFETY: `zeroed()` is valid for `timespec` (all-zeros is a defined bit pattern);
    // `clock_gettime` is a POSIX syscall that writes into the provided `timespec` pointers,
    // which are valid stack allocations for the duration of the call.
    unsafe {
        let mut start: timespec = zeroed();
        let mut end: timespec = zeroed();
        clock_gettime(CLOCK_REALTIME, &mut start);
        let rdtsc_start = rdtsc();

        // Do not change this loop! The hardcoded value below depends on this
        // loop and prevents it from being optimized out.
        let mut sum: u64 = 5;
        for k in 0..1_000_000 {
            let i = k as u64;
            sum = sum.wrapping_add(i.wrapping_add((sum.wrapping_add(i)).wrapping_mul(i % sum)));
        }
        if sum != 13_580_802_877_818_827_968u64 {
            panic!("Error in RDTSC freq measurement");
        }

        clock_gettime(CLOCK_REALTIME, &mut end);
        let clock_ns = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        let rdtsc_cycles = rdtsc() - rdtsc_start;

        let freq_ghz = rdtsc_cycles as f64 * 1.0 / clock_ns as f64;

        // RDTSC frequencies outside these ranges are rare
        if !(1.0..=4.0).contains(&freq_ghz) {
            panic!("Invalid RDTSC frequency {:.2}", freq_ghz);
        }
        freq_ghz
    }
}

pub fn cycles_to_sec(cycles: u64) -> f64 {
    let freq_ghz = get_rdtsc_freq();
    cycles as f64 / (freq_ghz * 1_000_000_000.0)
}

pub fn cycles_to_ms(cycles: u64) -> f64 {
    let freq_ghz = get_rdtsc_freq();
    cycles as f64 / (freq_ghz * 1_000_000.0)
}

pub fn cycles_to_us(cycles: u64) -> f64 {
    let freq_ghz = get_rdtsc_freq();
    cycles as f64 / (freq_ghz * 1000.0)
}

pub fn cycles_to_ns(cycles: u64) -> f64 {
    let freq_ghz = get_rdtsc_freq();
    cycles as f64 / freq_ghz
}
