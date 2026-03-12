//! SynStream plugin for the bilateral image denoising benchmark.
//!
//! Node functions (each has a `_cm` symbol + a `_wrap` function in wrappers.rs):
//!   init_bench_state       — loads images, allocates output buffer
//!   decompose_tiles        — validates dims (lightweight, ordering dep)
//!   bilateral_filter_tile  — bilateral filter on one tile (in-place shared buf)
//!   reassemble_tiles       — no-op (tiles wrote directly to shared output)
//!   compute_psnr           — PSNR vs clean image, prints result
//!
//! All nodes receive `Arc<BenchState>` as their first argument (shared state).
//! Tile nodes write to non-overlapping regions of `output_image`; the wavefront
//! DAG dependencies guarantee that predecessor tiles finish before their halo
//! pixels are read.

use std::sync::Arc;
use synstream_types::CmTypes;

// ---------------------------------------------------------------------------
// npy loader (float32, 2-D, C-order, little-endian)
// ---------------------------------------------------------------------------

fn load_npy_f32(path: &str) -> (Vec<f32>, usize, usize) {
    let data = std::fs::read(path).unwrap_or_else(|e| panic!("Cannot read {path}: {e}"));
    assert_eq!(&data[..6], b"\x93NUMPY", "Not a .npy file: {path}");
    let major = data[6];
    let (hdr_len, hdr_start) = if major == 1 {
        (u16::from_le_bytes([data[8], data[9]]) as usize, 10)
    } else {
        (
            u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize,
            12,
        )
    };
    let hdr =
        std::str::from_utf8(&data[hdr_start..hdr_start + hdr_len]).expect("npy header not utf8");

    // Parse shape=(H, W) from header dict
    let shape_pos = hdr
        .find("'shape'")
        .or_else(|| hdr.find("\"shape\""))
        .expect("no shape in npy header");
    let after_lp = &hdr[shape_pos..][hdr[shape_pos..].find('(').unwrap() + 1..];
    let dims: Vec<usize> = after_lp
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .take(2)
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(dims.len(), 2, "expected 2D npy array");
    let (h, w) = (dims[0], dims[1]);

    let data_start = hdr_start + hdr_len;
    let n = h * w;
    assert_eq!(
        (data.len() - data_start) / 4,
        n,
        "float32 data length mismatch"
    );

    let mut pixels = vec![0f32; n];
    for (i, chunk) in data[data_start..].chunks_exact(4).enumerate() {
        pixels[i] = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    (pixels, h, w)
}

// ---------------------------------------------------------------------------
// Shared benchmark state
// ---------------------------------------------------------------------------

pub struct BenchState {
    pub noisy_image: Vec<f32>,
    pub output_image: Vec<f32>,
    pub clean_image: Vec<f32>,
    pub image_height: usize,
    pub image_width: usize,
    pub tile_size: usize,
    pub grid_n: usize,
    pub sigma_s: f32,
    pub sigma_r: f32,
    pub kernel_radius: usize,
}

// Safety: BenchState contains Vec<f32> (heap-allocated, stable address).
// Concurrent writes to non-overlapping tile regions are safe given the
// wavefront dependency ordering.
unsafe impl Send for BenchState {}
unsafe impl Sync for BenchState {}

// ---------------------------------------------------------------------------
// init_bench_state — graph initialisation, called once before the DAG runs
//
// Args (from JSON init):
//   [0] noisy_path    (CmTypes::String)
//   [1] clean_path    (CmTypes::String)
//   [2] tile_size     (CmTypes::Usize)
//   [3] sigma_s       (CmTypes::F32)
//   [4] sigma_r       (CmTypes::F32)
//   [5] kernel_radius (CmTypes::Usize)
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn init_bench_state_cm(
    noisy_path: &CmTypes,
    clean_path: &CmTypes,
    tile_size: &CmTypes,
    sigma_s: &CmTypes,
    sigma_r: &CmTypes,
    kernel_radius: &CmTypes,
) -> CmTypes {
    let noisy_path = match noisy_path {
        CmTypes::String(s) => s.clone(),
        _ => panic!("init_bench_state_cm: expected String for noisy_path"),
    };
    let clean_path = match clean_path {
        CmTypes::String(s) => s.clone(),
        _ => panic!("init_bench_state_cm: expected String for clean_path"),
    };
    let tile_size = match tile_size {
        CmTypes::Usize(v) => *v,
        _ => panic!("init_bench_state_cm: expected Usize for tile_size"),
    };
    let sigma_s = match sigma_s {
        CmTypes::F32(v) => *v,
        _ => panic!("init_bench_state_cm: expected F32 for sigma_s"),
    };
    let sigma_r = match sigma_r {
        CmTypes::F32(v) => *v,
        _ => panic!("init_bench_state_cm: expected F32 for sigma_r"),
    };
    let kernel_radius = match kernel_radius {
        CmTypes::Usize(v) => *v,
        _ => panic!("init_bench_state_cm: expected Usize for kernel_radius"),
    };

    let (noisy, h, w) = load_npy_f32(&noisy_path);
    let (clean, h2, w2) = load_npy_f32(&clean_path);
    assert_eq!((h, w), (h2, w2), "noisy/clean image size mismatch");
    assert_eq!(h % tile_size, 0, "image_height not divisible by tile_size");
    assert_eq!(w % tile_size, 0, "image_width not divisible by tile_size");

    let grid_n = h / tile_size;
    let state = BenchState {
        output_image: vec![0f32; h * w],
        noisy_image: noisy,
        clean_image: clean,
        image_height: h,
        image_width: w,
        tile_size,
        grid_n,
        sigma_s,
        sigma_r,
        kernel_radius,
    };

    CmTypes::from_any(Arc::new(state))
}

// ---------------------------------------------------------------------------
// decompose_tiles — lightweight ordering node (no data copy)
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn decompose_tiles_cm(state_cm: &CmTypes) -> CmTypes {
    let state = state_cm
        .with_any(|s: &Arc<BenchState>| Arc::clone(s))
        .expect("decompose_tiles_cm: expected Arc<BenchState>");
    assert!(state.grid_n > 0, "grid_n must be positive");
    eprintln!(
        "[decompose_tiles] {}x{} image → {}x{} grid of {}x{} tiles",
        state.image_width,
        state.image_height,
        state.grid_n,
        state.grid_n,
        state.tile_size,
        state.tile_size
    );
    CmTypes::None
}

// ---------------------------------------------------------------------------
// bilateral_filter_tile — core compute node
//
// Reads from state.noisy_image (read-only, global image including halo).
// Writes to state.output_image at tile region [row_start..row_end, col..col_end].
//
// SAFETY: The wavefront DAG guarantees T(i-1,j) and T(i,j-1) have completed
// before T(i,j) executes, so halo reads from predecessor regions are safe.
// Each tile writes to an exclusive non-overlapping memory region.
//
// Args:
//   [0] state_cm  ($ref Arc<BenchState>)
//   [1] tile_i    (CmTypes::Usize)
//   [2] tile_j    (CmTypes::Usize)
//   [3..] ignored (ordering $res deps from predecessor tiles)
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn bilateral_filter_tile_cm(state_cm: &CmTypes, tile_i: &CmTypes, tile_j: &CmTypes) -> CmTypes {
    let state = state_cm
        .with_any(|s: &Arc<BenchState>| Arc::clone(s))
        .expect("bilateral_filter_tile_cm: expected Arc<BenchState>");

    let ti = match tile_i {
        CmTypes::Usize(v) => *v,
        _ => panic!("bilateral_filter_tile_cm: expected Usize for tile_i"),
    };
    let tj = match tile_j {
        CmTypes::Usize(v) => *v,
        _ => panic!("bilateral_filter_tile_cm: expected Usize for tile_j"),
    };

    let t = state.tile_size;
    let r = state.kernel_radius;
    let h = state.image_height;
    let w = state.image_width;
    let kw = 2 * r + 1;
    let inv_2ss = 1.0f32 / (2.0 * state.sigma_s * state.sigma_s);
    let inv_2sr = 1.0f32 / (2.0 * state.sigma_r * state.sigma_r);

    // Precompute spatial Gaussian weights (constant across pixels)
    let mut spatial_w = vec![0f32; kw * kw];
    for di in 0..kw {
        for dj in 0..kw {
            let di_i = di as isize - r as isize;
            let dj_i = dj as isize - r as isize;
            spatial_w[di * kw + dj] = f32::exp(-((di_i * di_i + dj_i * dj_i) as f32) * inv_2ss);
        }
    }

    let row_start = ti * t;
    let col_start = tj * t;

    // SAFETY: non-overlapping tile regions + wavefront ordering (see doc above)
    let src: *const f32 = state.noisy_image.as_ptr();
    let dst: *mut f32 = state.output_image.as_ptr() as *mut f32;

    unsafe {
        for pi in row_start..row_start + t {
            for pj in col_start..col_start + t {
                let ip = *src.add(pi * w + pj);
                let mut sum_w = 0f32;
                let mut sum_wi = 0f32;
                for di in 0..kw {
                    for dj in 0..kw {
                        let di_i = di as isize - r as isize;
                        let dj_i = dj as isize - r as isize;
                        let qi = (pi as isize + di_i).clamp(0, h as isize - 1) as usize;
                        let qj = (pj as isize + dj_i).clamp(0, w as isize - 1) as usize;
                        let iq = *src.add(qi * w + qj);
                        let range_term = (ip - iq) * (ip - iq) * inv_2sr;
                        let w_val = spatial_w[di * kw + dj] * f32::exp(-range_term);
                        sum_w += w_val;
                        sum_wi += w_val * iq;
                    }
                }
                *dst.add(pi * w + pj) = sum_wi / sum_w;
            }
        }
    }

    CmTypes::None
}

// ---------------------------------------------------------------------------
// reassemble_tiles — no-op (tiles wrote directly into shared output buffer)
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn reassemble_tiles_cm(_state_cm: &CmTypes) -> CmTypes {
    CmTypes::None
}

// ---------------------------------------------------------------------------
// compute_psnr — compares output_image vs clean_image, prints and returns PSNR
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn compute_psnr_cm(state_cm: &CmTypes) -> CmTypes {
    let state = state_cm
        .with_any(|s: &Arc<BenchState>| Arc::clone(s))
        .expect("compute_psnr_cm: expected Arc<BenchState>");

    let n = state.image_height * state.image_width;
    let out: *const f32 = state.output_image.as_ptr();
    let cln: *const f32 = state.clean_image.as_ptr();

    let mut mse = 0f64;
    unsafe {
        for i in 0..n {
            let d = *out.add(i) as f64 - *cln.add(i) as f64;
            mse += d * d;
        }
    }
    mse /= n as f64;
    let psnr = 10.0 * (1.0f64 / mse).log10();

    println!("PSNR: {:.2} dB", psnr);
    if psnr < 28.0 {
        eprintln!(
            "WARNING: PSNR={:.2} dB is below 28 dB correctness threshold",
            psnr
        );
    }

    CmTypes::F64(psnr)
}
