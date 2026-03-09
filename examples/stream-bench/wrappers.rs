// Wrapper file used by SynStream to load functions from the stream-bench dynamic library
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

cache_sym! {
    pub static GENERATE_ARRAY_CM_SYM: fn(usize, f64) -> CmTypes = b"generate_array_cm";
}
pub fn generate_array_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("generate_array_cm: expected Usize for n"),
    };
    let fill = match args[1] {
        CmTypes::F64(x) => x,
        _ => panic!("generate_array_cm: expected F64 for fill"),
    };
    GENERATE_ARRAY_CM_SYM(n, fill)
}

cache_sym! {
    pub static STREAM_COPY_CM_SYM: fn(&CmTypes) -> CmTypes = b"stream_copy_cm";
}
pub fn stream_copy_cm_wrap(args: &[CmTypes]) -> CmTypes {
    STREAM_COPY_CM_SYM(&args[0])
}

cache_sym! {
    pub static STREAM_SCALE_CM_SYM: fn(&CmTypes, f64) -> CmTypes = b"stream_scale_cm";
}
pub fn stream_scale_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let scalar = match args[1] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_scale_cm: expected F64 for scalar"),
    };
    STREAM_SCALE_CM_SYM(&args[0], scalar)
}

cache_sym! {
    pub static STREAM_ADD_CM_SYM: fn(&CmTypes, &CmTypes) -> CmTypes = b"stream_add_cm";
}
pub fn stream_add_cm_wrap(args: &[CmTypes]) -> CmTypes {
    STREAM_ADD_CM_SYM(&args[0], &args[1])
}

cache_sym! {
    pub static STREAM_TRIAD_CM_SYM: fn(&CmTypes, &CmTypes, f64) -> CmTypes = b"stream_triad_cm";
}
pub fn stream_triad_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let scalar = match args[2] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_triad_cm: expected F64 for scalar"),
    };
    STREAM_TRIAD_CM_SYM(&args[0], &args[1], scalar)
}

cache_sym! {
    pub static SINK_CM_SYM: fn(&CmTypes) -> CmTypes = b"sink_cm";
}
pub fn sink_cm_wrap(args: &[CmTypes]) -> CmTypes {
    SINK_CM_SYM(&args[0])
}

cache_sym! {
    pub static GENERATE_MUT_ARRAY_CM_SYM: fn(usize) -> CmTypes = b"generate_mut_array_cm";
}
pub fn generate_mut_array_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("generate_mut_array_cm: expected Usize for n"),
    };
    GENERATE_MUT_ARRAY_CM_SYM(n)
}

cache_sym! {
    pub static STREAM_COPY_INIT_POOLED_CM_SYM: fn(&CmTypes, &CmTypes) -> CmTypes = b"stream_copy_init_pooled_cm";
}
pub fn stream_copy_init_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    STREAM_COPY_INIT_POOLED_CM_SYM(&args[0], &args[1])
}

cache_sym! {
    pub static STREAM_SCALE_INIT_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, f64) -> CmTypes = b"stream_scale_init_pooled_cm";
}
pub fn stream_scale_init_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let scalar = match args[2] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_scale_init_pooled_cm: expected F64 for scalar"),
    };
    STREAM_SCALE_INIT_POOLED_CM_SYM(&args[0], &args[1], scalar)
}

cache_sym! {
    pub static STREAM_ADD_INIT_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, &CmTypes) -> CmTypes = b"stream_add_init_pooled_cm";
}
pub fn stream_add_init_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    STREAM_ADD_INIT_POOLED_CM_SYM(&args[0], &args[1], &args[2])
}

cache_sym! {
    pub static STREAM_TRIAD_INIT_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, &CmTypes, f64) -> CmTypes = b"stream_triad_init_pooled_cm";
}
pub fn stream_triad_init_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let scalar = match args[3] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_triad_init_pooled_cm: expected F64 for scalar"),
    };
    STREAM_TRIAD_INIT_POOLED_CM_SYM(&args[0], &args[1], &args[2], scalar)
}

cache_sym! {
    pub static CREATE_BUFFER_POOL_CM_SYM: fn(usize, usize, f64) -> CmTypes = b"create_buffer_pool_cm";
}
pub fn create_buffer_pool_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n_workers = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("create_buffer_pool_cm: expected Usize for n_workers"),
    };
    let array_size = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("create_buffer_pool_cm: expected Usize for array_size"),
    };
    let fill = match args[2] {
        CmTypes::F64(x) => x,
        _ => panic!("create_buffer_pool_cm: expected F64 for fill"),
    };
    CREATE_BUFFER_POOL_CM_SYM(n_workers, array_size, fill)
}

cache_sym! {
    pub static CREATE_MUTABLE_BUFFER_POOL_CM_SYM: fn(usize, usize) -> CmTypes = b"create_mutable_buffer_pool_cm";
}
pub fn create_mutable_buffer_pool_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n_workers = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("create_mutable_buffer_pool_cm: expected Usize for n_workers"),
    };
    let array_size = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("create_mutable_buffer_pool_cm: expected Usize for array_size"),
    };
    CREATE_MUTABLE_BUFFER_POOL_CM_SYM(n_workers, array_size)
}

cache_sym! {
    pub static STREAM_COPY_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, usize) -> CmTypes = b"stream_copy_pooled_cm";
}
pub fn stream_copy_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[2] {
        CmTypes::Usize(x) => x,
        _ => panic!("stream_copy_pooled_cm: expected Usize for idx"),
    };
    STREAM_COPY_POOLED_CM_SYM(&args[0], &args[1], idx)
}

cache_sym! {
    pub static STREAM_SCALE_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, usize, f64) -> CmTypes = b"stream_scale_pooled_cm";
}
pub fn stream_scale_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[2] {
        CmTypes::Usize(x) => x,
        _ => panic!("stream_scale_pooled_cm: expected Usize for idx"),
    };
    let scalar = match args[3] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_scale_pooled_cm: expected F64 for scalar"),
    };
    STREAM_SCALE_POOLED_CM_SYM(&args[0], &args[1], idx, scalar)
}

cache_sym! {
    pub static STREAM_ADD_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, &CmTypes, usize) -> CmTypes = b"stream_add_pooled_cm";
}
pub fn stream_add_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[3] {
        CmTypes::Usize(x) => x,
        _ => panic!("stream_add_pooled_cm: expected Usize for idx"),
    };
    STREAM_ADD_POOLED_CM_SYM(&args[0], &args[1], &args[2], idx)
}

cache_sym! {
    pub static STREAM_TRIAD_POOLED_CM_SYM: fn(&CmTypes, &CmTypes, &CmTypes, usize, f64) -> CmTypes = b"stream_triad_pooled_cm";
}
pub fn stream_triad_pooled_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let idx = match args[3] {
        CmTypes::Usize(x) => x,
        _ => panic!("stream_triad_pooled_cm: expected Usize for idx"),
    };
    let scalar = match args[4] {
        CmTypes::F64(x) => x,
        _ => panic!("stream_triad_pooled_cm: expected F64 for scalar"),
    };
    STREAM_TRIAD_POOLED_CM_SYM(&args[0], &args[1], &args[2], idx, scalar)
}
