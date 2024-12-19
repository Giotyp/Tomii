use serde::Deserialize;
use std::fmt;

#[derive(Debug, Deserialize, Clone)]
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
  String(String),
  None()
}

pub type CmPtr = fn(Vec<CmTypes>) -> CmTypes;

#[derive(Debug)]
pub struct CustomError {
    details: String,
}

impl CustomError {
    fn new(msg: &str) -> CustomError {
        CustomError{details: msg.to_string()}
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
      "bool" => arg.parse::<bool>()
          .map(CmTypes::Bool)
          .map_err(|_| CustomError::new("Failed to parse bool")),
      "i8" => arg.parse::<i8>()
          .map(CmTypes::I8)
          .map_err(|_| CustomError::new("Failed to parse i8")),
      "i16" => arg.parse::<i16>()
          .map(CmTypes::I16)
          .map_err(|_| CustomError::new("Failed to parse i16")),
      "i32" => arg.parse::<i32>()
          .map(CmTypes::I32)
          .map_err(|_| CustomError::new("Failed to parse i32")),
      "i64" => arg.parse::<i64>()
          .map(CmTypes::I64)
          .map_err(|_| CustomError::new("Failed to parse i64")),
      "i128" => arg.parse::<i128>()
          .map(CmTypes::I128)
          .map_err(|_| CustomError::new("Failed to parse i128")),
      "u8" => arg.parse::<u8>()
          .map(CmTypes::U8)
          .map_err(|_| CustomError::new("Failed to parse u8")),
      "u16" => arg.parse::<u16>()
          .map(CmTypes::U16)
          .map_err(|_| CustomError::new("Failed to parse u16")),
      "u32" => arg.parse::<u32>()
          .map(CmTypes::U32)
          .map_err(|_| CustomError::new("Failed to parse u32")),
      "u64" => arg.parse::<u64>()
          .map(CmTypes::U64)
          .map_err(|_| CustomError::new("Failed to parse u64")),
      "u128" => arg.parse::<u128>()
          .map(CmTypes::U128)
          .map_err(|_| CustomError::new("Failed to parse u128")),
      "f32" => arg.parse::<f32>()
          .map(CmTypes::F32)
          .map_err(|_| CustomError::new("Failed to parse f32")),
      "f64" => arg.parse::<f64>()
          .map(CmTypes::F64)
          .map_err(|_| CustomError::new("Failed to parse f64")),
      "char" => arg.chars().next()
          .map(CmTypes::Char)
          .ok_or_else(|| CustomError::new("Failed to parse char")),
      "usize" => arg.parse::<usize>()
          .map(CmTypes::Usize)
          .map_err(|_| CustomError::new("Failed to parse usize")),
      "String" => arg.parse::<String>()
          .map(CmTypes::String)
          .map_err(|_| CustomError::new("Failed to parse String")),
      _ => Err(CustomError::new(&format!("Unsupported type: {}", tp))),
  }
}