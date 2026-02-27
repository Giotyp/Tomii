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
