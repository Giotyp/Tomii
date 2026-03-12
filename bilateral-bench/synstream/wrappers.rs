// Wrappers for the bilateral-bench dynamic library.
// Each wrapper extracts typed values from the args slice and calls the _cm symbol.
use libloading::{Library, Symbol};
use once_cell::sync::Lazy;
use synstream_types::*;

static DYN_LIB: Lazy<Library> = Lazy::new(|| {
    let path = std::env::var("PLUGIN_LIB").expect("PLUGIN_LIB must be set to your .so/.dll");
    unsafe { Library::new(path).expect("Failed to open plugin library") }
});

pub fn init_wrappers() {
    Lazy::force(&DYN_LIB);
}

macro_rules! cache_sym {
    ($vis:vis static $sym:ident : $typ:ty = $name:expr;) => {
        $vis static $sym: Lazy<$typ> = Lazy::new(|| {
            let lib = &*DYN_LIB;
            let sym: Symbol<$typ> =
                unsafe { lib.get($name) }
                    .unwrap_or_else(|e| panic!("couldn't load symbol {:?}: {}", $name, e));
            *sym
        });
    };
}

// ---------------------------------------------------------------------------
// init_bench_state
// ---------------------------------------------------------------------------

cache_sym! {
    pub static INIT_BENCH_STATE_CM_SYM: fn(&CmTypes,&CmTypes,&CmTypes,&CmTypes,&CmTypes,&CmTypes) -> CmTypes
        = b"init_bench_state_cm";
}

pub fn init_bench_state_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = noisy_path (String)
    // args[1] = clean_path (String)
    // args[2] = tile_size  (Usize)
    // args[3] = sigma_s    (F32)
    // args[4] = sigma_r    (F32)
    // args[5] = kernel_radius (Usize)
    INIT_BENCH_STATE_CM_SYM(&args[0], &args[1], &args[2], &args[3], &args[4], &args[5])
}

// ---------------------------------------------------------------------------
// decompose_tiles
// ---------------------------------------------------------------------------

cache_sym! {
    pub static DECOMPOSE_TILES_CM_SYM: fn(&CmTypes) -> CmTypes = b"decompose_tiles_cm";
}

pub fn decompose_tiles_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = state ($ref Arc<BenchState>)
    DECOMPOSE_TILES_CM_SYM(&args[0])
}

// ---------------------------------------------------------------------------
// bilateral_filter_tile
// ---------------------------------------------------------------------------

cache_sym! {
    pub static BILATERAL_FILTER_TILE_CM_SYM: fn(&CmTypes,&CmTypes,&CmTypes) -> CmTypes
        = b"bilateral_filter_tile_cm";
}

pub fn bilateral_filter_tile_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = state  ($ref Arc<BenchState>)
    // args[1] = tile_i (Usize)
    // args[2] = tile_j (Usize)
    // args[3..] = ordering $res deps (None — ignored for computation)
    BILATERAL_FILTER_TILE_CM_SYM(&args[0], &args[1], &args[2])
}

// ---------------------------------------------------------------------------
// reassemble_tiles
// ---------------------------------------------------------------------------

cache_sym! {
    pub static REASSEMBLE_TILES_CM_SYM: fn(&CmTypes) -> CmTypes = b"reassemble_tiles_cm";
}

pub fn reassemble_tiles_cm_wrap(args: &[CmTypes]) -> CmTypes {
    REASSEMBLE_TILES_CM_SYM(&args[0])
}

// ---------------------------------------------------------------------------
// compute_psnr
// ---------------------------------------------------------------------------

cache_sym! {
    pub static COMPUTE_PSNR_CM_SYM: fn(&CmTypes) -> CmTypes = b"compute_psnr_cm";
}

pub fn compute_psnr_cm_wrap(args: &[CmTypes]) -> CmTypes {
    COMPUTE_PSNR_CM_SYM(&args[0])
}
