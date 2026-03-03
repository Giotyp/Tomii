// Wrapper file used by SynStream to load functions from the cost-bench dynamic library
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

// --- Initialization functions ---

cache_sym! {
    pub static LOAD_GRAPH_CM_SYM: fn(&CmTypes) -> CmTypes = b"load_graph_cm";
}
pub fn load_graph_cm_wrap(args: &[CmTypes]) -> CmTypes {
    LOAD_GRAPH_CM_SYM(&args[0])
}

cache_sym! {
    pub static CREATE_RANKS_CM_SYM: fn(&CmTypes) -> CmTypes = b"create_ranks_cm";
}
pub fn create_ranks_cm_wrap(args: &[CmTypes]) -> CmTypes {
    CREATE_RANKS_CM_SYM(&args[0])
}

cache_sym! {
    pub static GET_PARTITION_CM_SYM: fn(&CmTypes, usize, usize) -> CmTypes = b"get_partition_cm";
}
pub fn get_partition_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("get_partition_cm: expected Usize for idx"),
    };
    let n_parts = match args[2] {
        CmTypes::Usize(x) => x,
        _ => panic!("get_partition_cm: expected Usize for n_parts"),
    };
    GET_PARTITION_CM_SYM(&args[0], idx, n_parts)
}

cache_sym! {
    pub static GET_ALL_PARTITIONS_CM_SYM: fn(&CmTypes, usize) -> CmTypes = b"get_all_partitions_cm";
}
pub fn get_all_partitions_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n_parts = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("get_all_partitions_cm: expected Usize for n_parts"),
    };
    GET_ALL_PARTITIONS_CM_SYM(&args[0], n_parts)
}

// --- Compute functions ---

cache_sym! {
    pub static PR_SCATTER_CM_SYM: fn(&CmTypes, usize, &CmTypes, &CmTypes) -> CmTypes = b"pr_scatter_cm";
}
pub fn pr_scatter_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("pr_scatter_cm: expected Usize for idx"),
    };
    PR_SCATTER_CM_SYM(&args[0], idx, &args[2], &args[3])
}

cache_sym! {
    pub static PR_GATHER_CM_SYM: fn(&CmTypes, f64, &[CmTypes]) -> CmTypes = b"pr_gather_cm";
}
pub fn pr_gather_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let damping = match args[1] {
        CmTypes::F64(x) => x,
        _ => panic!("pr_gather_cm: expected F64 for damping"),
    };
    // args[0] = ranks, args[1] = damping, args[2..] = scatter contributions
    PR_GATHER_CM_SYM(&args[0], damping, &args[2..])
}

cache_sym! {
    pub static PR_PARTIAL_GATHER_CM_SYM: fn(usize, usize, &[CmTypes]) -> CmTypes
        = b"pr_partial_gather_cm";
}
pub fn pr_partial_gather_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n_workers = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("pr_partial_gather_cm: expected Usize for n_workers"),
    };
    let idx = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("pr_partial_gather_cm: expected Usize for idx"),
    };
    // args[0] = n_workers, args[1] = idx, args[2..] = scatter contributions
    PR_PARTIAL_GATHER_CM_SYM(n_workers, idx, &args[2..])
}

cache_sym! {
    pub static PR_REDUCE_CM_SYM: fn(&CmTypes, f64, &[CmTypes]) -> CmTypes = b"pr_reduce_cm";
}
pub fn pr_reduce_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let damping = match args[1] {
        CmTypes::F64(x) => x,
        _ => panic!("pr_reduce_cm: expected F64 for damping"),
    };
    // args[0] = ranks, args[1] = damping, args[2..] = partial sums
    PR_REDUCE_CM_SYM(&args[0], damping, &args[2..])
}

cache_sym! {
    pub static PR_REDUCE_PARTIAL_CM_SYM: fn(usize, usize, &CmTypes, f64, &[CmTypes]) -> CmTypes
        = b"pr_reduce_partial_cm";
}
pub fn pr_reduce_partial_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n_workers = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("pr_reduce_partial_cm: expected Usize for n_workers"),
    };
    let idx = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("pr_reduce_partial_cm: expected Usize for idx"),
    };
    let damping = match args[3] {
        CmTypes::F64(x) => x,
        _ => panic!("pr_reduce_partial_cm: expected F64 for damping"),
    };
    // args[0]=n_workers, args[1]=idx, args[2]=ranks, args[3]=damping, args[4..]=partial sums
    PR_REDUCE_PARTIAL_CM_SYM(n_workers, idx, &args[2], damping, &args[4..])
}
