// Wrappers for the wavefront-bench dynamic library.
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

// --- Initialisation ---

cache_sym! {
    pub static INIT_GRID_CM_SYM: fn(&CmTypes) -> CmTypes = b"init_grid_cm";
}
pub fn init_grid_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = n (CmTypes::Usize resolved from $ref "n")
    INIT_GRID_CM_SYM(&args[0])
}

// --- Compute ---

cache_sym! {
    pub static WF_CELL_CM_SYM: fn(&CmTypes, usize, usize, usize) -> CmTypes = b"wf_cell_cm";
}
pub fn wf_cell_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = grid ($ref → CmTypes::Any(Vec<f64>))
    // args[1] = n    (CmTypes::Usize, from $ref "n")
    // args[2] = diag (CmTypes::Usize, literal hardcoded per diagonal node)
    // args[3] = idx  (CmTypes::Usize, resolved from $ref "$index" at runtime)
    // args[4] = barrier value from previous diagonal (ignored — synchronisation only)
    let n = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_cell_cm: expected CmTypes::Usize for n"),
    };
    let diag = match args[2] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_cell_cm: expected CmTypes::Usize for diag"),
    };
    let idx = match args[3] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_cell_cm: expected CmTypes::Usize for idx"),
    };
    WF_CELL_CM_SYM(&args[0], n, diag, idx)
}

cache_sym! {
    pub static WF_TILE_CM_SYM: fn(&CmTypes, usize, usize, usize, usize) -> CmTypes = b"wf_tile_cm";
}
pub fn wf_tile_cm_wrap(args: &[CmTypes]) -> CmTypes {
    // args[0] = grid      ($ref → CmTypes::Any(Vec<f64>))
    // args[1] = n         (CmTypes::Usize, from $ref "n")
    // args[2] = diag      (CmTypes::Usize, literal hardcoded per diagonal node)
    // args[3] = tile_idx  (CmTypes::Usize, resolved from $ref "$index" at runtime)
    // args[4] = tile_size (CmTypes::Usize, compile-time constant from graph)
    // args[5] = barrier   (ignored — synchronisation only)
    let n = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_tile_cm: expected CmTypes::Usize for n"),
    };
    let diag = match args[2] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_tile_cm: expected CmTypes::Usize for diag"),
    };
    let tile_idx = match args[3] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_tile_cm: expected CmTypes::Usize for tile_idx"),
    };
    let tile_size = match args[4] {
        CmTypes::Usize(x) => x,
        _ => panic!("wf_tile_cm: expected CmTypes::Usize for tile_size"),
    };
    WF_TILE_CM_SYM(&args[0], n, diag, tile_idx, tile_size)
}
