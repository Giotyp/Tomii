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
    pub static GENERATE_VECTOR_CM_SYM: fn(usize) -> CmTypes = b"generate_vector_cm";
}
pub fn generate_vector_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let n = match args[0] { CmTypes::Usize(x) => x, _ => panic!("generate_vector_cm: expected Usize for n") };
    GENERATE_VECTOR_CM_SYM(n)
}

cache_sym! {
    pub static FFT_PLANNER_CM_SYM: fn(usize) -> CmTypes = b"fft_planner_cm";
}
pub fn fft_planner_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let buf_size = match args[0] { CmTypes::Usize(x) => x, _ => panic!("fft_planner_cm: expected Usize for buf_size") };
    FFT_PLANNER_CM_SYM(buf_size)
}

cache_sym! {
    pub static COMPUTE_FFT_CM_SYM: fn(&CmTypes, &CmTypes) -> CmTypes = b"compute_fft_cm";
}
pub fn compute_fft_cm_wrap(args: &[CmTypes]) -> CmTypes {
    COMPUTE_FFT_CM_SYM(&args[0], &args[1])
}

cache_sym! {
    pub static VEC_TO_MAT_CM_SYM: fn(&CmTypes) -> CmTypes = b"vec_to_mat_cm";
}
pub fn vec_to_mat_cm_wrap(args: &[CmTypes]) -> CmTypes {
    VEC_TO_MAT_CM_SYM(&args[0])
}

cache_sym! {
    pub static MAT_MUL_CM_SYM: fn(&CmTypes, &CmTypes) -> CmTypes = b"mat_mul_cm";
}
pub fn mat_mul_cm_wrap(args: &[CmTypes]) -> CmTypes {
    MAT_MUL_CM_SYM(&args[0], &args[1])
}

cache_sym! {
    pub static GET_OUT_FILE_CM_SYM: fn(&str, &str) -> CmTypes = b"get_out_file_cm";
}
pub fn get_out_file_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let env_var_s = match &args[0] { CmTypes::String(x) => x.to_string(), _ => panic!("get_out_file_cm: expected String for env_var") };
    let env_var = env_var_s.as_str();
    let out_file_s = match &args[1] { CmTypes::String(x) => x.to_string(), _ => panic!("get_out_file_cm: expected String for out_file") };
    let out_file = out_file_s.as_str();
    GET_OUT_FILE_CM_SYM(env_var, out_file)
}

cache_sym! {
    pub static WRITE_TO_FILE_CM_SYM: fn(&str, &[CmTypes]) -> CmTypes = b"write_to_file_cm";
}
pub fn write_to_file_cm_wrap(args: &[CmTypes]) -> CmTypes {
    let file_path_s = match &args[0] { CmTypes::String(x) => x.to_string(), _ => panic!("write_to_file_cm: expected String for file_path") };
    let file_path = file_path_s.as_str();
    WRITE_TO_FILE_CM_SYM(file_path, &args[1..])
}

