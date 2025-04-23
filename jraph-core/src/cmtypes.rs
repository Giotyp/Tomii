use serde::Deserialize;
use std::fmt;
use std::sync::{Arc, Mutex};
use nalgebra::*;
use num_complex::Complex32;
use crate::init_funcs;

#[derive(Deserialize, Clone)]
pub enum CmTypes {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Char(char),
    Usize(usize),
    VecUsize(Vec<usize>),
    String(String),
    VecC32(Vec<Complex32>),
    #[serde(with = "dvector_arc_serde")]
    DVectorC32(Arc<DVector<Complex32>>),
    #[serde(with = "dmatrix_arc_serde")]
    DMatrixC32(Arc<DMatrix<Complex32>>),
    Ref(String),
    Res(String),
    None(),
    // Space for Custom structs/types
    #[serde[skip]]
    Fft(Arc<Mutex<init_funcs::Fft>>)
}

mod dvector_arc_serde {
    use super::*;
    use serde::Deserializer;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<DVector<Complex32>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        DVector::deserialize(deserializer).map(Arc::new)
    }
}

mod dmatrix_arc_serde {
    use super::*;
    use serde::Deserializer;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<DMatrix<Complex32>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        DMatrix::deserialize(deserializer).map(Arc::new)
    }
}

impl CmTypes {
    pub fn arg_name(&self) -> String {
        match self {
            CmTypes::Bool(_) => "bool".to_string(),
            CmTypes::I8(_) => "i8".to_string(),
            CmTypes::I16(_) => "i16".to_string(),
            CmTypes::I32(_) => "i32".to_string(),
            CmTypes::I64(_) => "i64".to_string(),
            CmTypes::I128(_) => "i128".to_string(),
            CmTypes::U8(_) => "u8".to_string(),
            CmTypes::U16(_) => "u16".to_string(),
            CmTypes::U32(_) => "u32".to_string(),
            CmTypes::U64(_) => "u64".to_string(),
            CmTypes::U128(_) => "u128".to_string(),
            CmTypes::F32(_) => "f32".to_string(),
            CmTypes::F64(_) => "f64".to_string(),
            CmTypes::Char(_) => "char".to_string(),
            CmTypes::Usize(_) => "usize".to_string(),
            CmTypes::String(_) => "String".to_string(),
            CmTypes::Ref(_) => "$ref".to_string(),
            CmTypes::Res(_) => "$res".to_string(),
            CmTypes::None() => "None".to_string(),
            _ => "Unsupported type".to_string(),
        }
    }
}

impl std::fmt::Debug for CmTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CmTypes::Bool(val) => write!(f, "Bool({:?})", val),
            CmTypes::I8(val) => write!(f, "I8({:?})", val),
            CmTypes::I16(val) => write!(f, "I16({:?})", val),
            CmTypes::I32(val) => write!(f, "I32({:?})", val),
            CmTypes::I64(val) => write!(f, "I64({:?})", val),
            CmTypes::I128(val) => write!(f, "I128({:?})", val),
            CmTypes::U8(val) => write!(f, "U8({:?})", val),
            CmTypes::U16(val) => write!(f, "U16({:?})", val),
            CmTypes::U32(val) => write!(f, "U32({:?})", val),
            CmTypes::U64(val) => write!(f, "U64({:?})", val),
            CmTypes::U128(val) => write!(f, "U128({:?})", val),
            CmTypes::F32(val) => write!(f, "F32({:?})", val),
            CmTypes::F64(val) => write!(f, "F64({:?})", val),
            CmTypes::Char(val) => write!(f, "Char({:?})", val),
            CmTypes::Usize(val) => write!(f, "Usize({:?})", val),
            CmTypes::VecUsize(val) => write!(f, "VecUsize({:?})", val),
            CmTypes::String(val) => write!(f, "String({:?})", val),
            CmTypes::VecC32(val) => write!(f, "VecC32({:?})", val),
            CmTypes::DVectorC32(val) => write!(f, "DVectorC32({:?})", val),
            CmTypes::DMatrixC32(val) => write!(f, "DMatrixC32({:?})", val),
            CmTypes::Ref(val) => write!(f, "Ref({:?})", val),
            CmTypes::Res(val) => write!(f, "Res({:?})", val),
            CmTypes::None() => write!(f, "None"),
            CmTypes::Fft(_) => write!(f, "Fft(<excluded>)"), // Custom debug output or omit entirely
        }
    }
}

// implement Display for CmTypes
impl fmt::Display for CmTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CmTypes::Bool(x) => write!(f, "{}", x),
            CmTypes::I8(x) => write!(f, "{}", x),
            CmTypes::I16(x) => write!(f, "{}", x),
            CmTypes::I32(x) => write!(f, "{}", x),
            CmTypes::I64(x) => write!(f, "{}", x),
            CmTypes::I128(x) => write!(f, "{}", x),
            CmTypes::U8(x) => write!(f, "{}", x),
            CmTypes::U16(x) => write!(f, "{}", x),
            CmTypes::U32(x) => write!(f, "{}", x),
            CmTypes::U64(x) => write!(f, "{}", x),
            CmTypes::U128(x) => write!(f, "{}", x),
            CmTypes::F32(x) => write!(f, "{}", x),
            CmTypes::F64(x) => write!(f, "{}", x),
            CmTypes::Char(x) => write!(f, "{}", x),
            CmTypes::Usize(x) => write!(f, "{}", x),
            CmTypes::String(x) => write!(f, "{}", x),
            CmTypes::Ref(x) => write!(f, "{}", x),
            CmTypes::Res(x) => write!(f, "{}", x),
            CmTypes::None() => write!(f, "None"),
            _ => write!(f, "Unsupported type"),
        }
    }
}

pub type CmPtr = fn(Vec<CmTypes>) -> CmTypes;

#[derive(Debug)]
pub struct CustomError {
    details: String,
}

impl CustomError {
    fn new(msg: &str) -> CustomError {
        CustomError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for CustomError {
    fn description(&self) -> &str {
        &self.details
    }
}

pub fn string_to_primitive(tp: String, arg: String) -> Result<CmTypes, CustomError> {
    let tp = tp.as_str();
    match tp {
        "bool" => arg
            .parse::<bool>()
            .map(CmTypes::Bool)
            .map_err(|_| CustomError::new("Failed to parse bool")),
        "i8" => arg
            .parse::<i8>()
            .map(CmTypes::I8)
            .map_err(|_| CustomError::new("Failed to parse i8")),
        "i16" => arg
            .parse::<i16>()
            .map(CmTypes::I16)
            .map_err(|_| CustomError::new("Failed to parse i16")),
        "i32" => arg
            .parse::<i32>()
            .map(CmTypes::I32)
            .map_err(|_| CustomError::new("Failed to parse i32")),
        "i64" => arg
            .parse::<i64>()
            .map(CmTypes::I64)
            .map_err(|_| CustomError::new("Failed to parse i64")),
        "i128" => arg
            .parse::<i128>()
            .map(CmTypes::I128)
            .map_err(|_| CustomError::new("Failed to parse i128")),
        "u8" => arg
            .parse::<u8>()
            .map(CmTypes::U8)
            .map_err(|_| CustomError::new("Failed to parse u8")),
        "u16" => arg
            .parse::<u16>()
            .map(CmTypes::U16)
            .map_err(|_| CustomError::new("Failed to parse u16")),
        "u32" => arg
            .parse::<u32>()
            .map(CmTypes::U32)
            .map_err(|_| CustomError::new("Failed to parse u32")),
        "u64" => arg
            .parse::<u64>()
            .map(CmTypes::U64)
            .map_err(|_| CustomError::new("Failed to parse u64")),
        "u128" => arg
            .parse::<u128>()
            .map(CmTypes::U128)
            .map_err(|_| CustomError::new("Failed to parse u128")),
        "f32" => arg
            .parse::<f32>()
            .map(CmTypes::F32)
            .map_err(|_| CustomError::new("Failed to parse f32")),
        "f64" => arg
            .parse::<f64>()
            .map(CmTypes::F64)
            .map_err(|_| CustomError::new("Failed to parse f64")),
        "char" => arg
            .chars()
            .next()
            .map(CmTypes::Char)
            .ok_or_else(|| CustomError::new("Failed to parse char")),
        "usize" => arg
            .parse::<usize>()
            .map(CmTypes::Usize)
            .map_err(|_| CustomError::new("Failed to parse usize")),
        "String" => arg
            .parse::<String>()
            .map(CmTypes::String)
            .map_err(|_| CustomError::new("Failed to parse String")),
        "$ref" => arg
            .parse::<String>()
            .map(CmTypes::Ref)
            .map_err(|_| CustomError::new("Failed to parse Ref")),
        "$res" => arg
            .parse::<String>()
            .map(CmTypes::Res)
            .map_err(|_| CustomError::new("Failed to parse Res")),
        _ => Err(CustomError::new(&format!("Unsupported type: {}", tp))),
    }
}