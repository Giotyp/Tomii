// Wrapper file used by SynStream to load functions from a dynamic library
// No need to be compiled in our library
// In future versions, this file will be auto-generated based on the functions used in the graph
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
pub fn generate_vector_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let buf_size = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for packet length"),
    };
    GENERATE_VECTOR_CM_SYM(buf_size)
}

cache_sym! {
    pub static FFT_PLANNER_CM_SYM: fn(usize) -> CmTypes = b"fft_planner_cm";
}
pub fn fft_planner_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let buf_size = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for packet length"),
    };
    FFT_PLANNER_CM_SYM(buf_size)
}

cache_sym! {
    pub static COMPUTE_FFT_CM_SYM: fn(&CmTypes, &mut Vec<Complex32>) = b"compute_fft_cm";
}
pub fn compute_fft_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let fft_planner = &args[0];

    let buffer = match &args[1] {
        CmTypes::VecCmt(x) => x.clone(),
        _ => panic!("Expected Vec<CmTypes> argument"),
    };
    let mut vec: Vec<Complex32> = Vec::new();
    for i in 0..buffer.len() {
        let x = match buffer[i] {
            CmTypes::Complex32(x) => x.into(),
            _ => panic!("Invalid argument type"),
        };
        vec.push(x);
    }
    COMPUTE_FFT_CM_SYM(fft_planner, &mut vec);
    CmTypes::None
}

cache_sym! {
    pub static VEC_TO_MAT_CM_SYM: fn(&Vec<Complex32>) -> CmTypes = b"vec_to_mat_cm";
}
pub fn vec_to_mat_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let vector = match &args[0] {
        CmTypes::VecCmt(x) => x.clone(),
        _ => panic!("Expected Vec<Complex32> argument"),
    };
    let mut vec: Vec<Complex32> = Vec::new();
    for i in 0..vector.len() {
        let x = match vector[i] {
            CmTypes::Complex32(x) => x.into(),
            _ => panic!("Invalid argument type"),
        };
        vec.push(x);
    }
    VEC_TO_MAT_CM_SYM(&vec)
}

cache_sym! {
    pub static MAT_MUL_CM_SYM: fn(&CmTypes, &CmTypes) -> CmTypes = b"mat_mul_cm";
}
pub fn mat_mul_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    MAT_MUL_CM_SYM(&args[0], &args[1])
}

cache_sym! {
    pub static GET_OUT_FILE_SYM: fn(&str, &str) -> String = b"get_out_file";
}
pub fn get_out_file_wrap(args: Vec<CmTypes>) -> CmTypes {
    let env_var = match &args[0] {
        CmTypes::String(x) => x.clone(),
        _ => panic!("Expected string argument"),
    };

    let out_file = match &args[1] {
        CmTypes::String(x) => x.clone(),
        _ => panic!("Expected string argument"),
    };
    CmTypes::String(GET_OUT_FILE_SYM(&env_var, &out_file))
}

cache_sym! {
    pub static WRITE_TO_FILE_SYM: fn(&str, &Vec<CmTypes>) = b"write_to_file";
}
pub fn write_to_file_wrap(args: Vec<CmTypes>) -> CmTypes {
    let file_path = match &args[0] {
        CmTypes::String(x) => x.clone(),
        _ => panic!("Expected string argument"),
    };

    let buffers = &args[1..]
        .iter()
        .map(|x| x.clone())
        .collect::<Vec<CmTypes>>();
    WRITE_TO_FILE_SYM(&file_path, &buffers);
    CmTypes::None
}
