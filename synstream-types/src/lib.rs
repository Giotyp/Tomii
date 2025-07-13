use lazy_static::lazy_static;
use serde::Deserialize;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

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
    Isize(isize),
    String(String),
    Ref(String),
    Res(String),
    Barrier(String),
    VecCmt(Vec<CmTypes>),
    None(),
    #[serde(skip)]
    Any(Arc<RwLock<Box<dyn Any + Send + Sync>>>),
}

impl PartialEq for CmTypes {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CmTypes::Bool(a), CmTypes::Bool(b)) => a == b,
            (CmTypes::I8(a), CmTypes::I8(b)) => a == b,
            (CmTypes::I16(a), CmTypes::I16(b)) => a == b,
            (CmTypes::I32(a), CmTypes::I32(b)) => a == b,
            (CmTypes::I64(a), CmTypes::I64(b)) => a == b,
            (CmTypes::I128(a), CmTypes::I128(b)) => a == b,
            (CmTypes::U8(a), CmTypes::U8(b)) => a == b,
            (CmTypes::U16(a), CmTypes::U16(b)) => a == b,
            (CmTypes::U32(a), CmTypes::U32(b)) => a == b,
            (CmTypes::U64(a), CmTypes::U64(b)) => a == b,
            (CmTypes::U128(a), CmTypes::U128(b)) => a == b,
            (CmTypes::F32(a), CmTypes::F32(b)) => a == b,
            (CmTypes::F64(a), CmTypes::F64(b)) => a == b,
            (CmTypes::Char(a), CmTypes::Char(b)) => a == b,
            (CmTypes::Usize(a), CmTypes::Usize(b)) => a == b,
            (CmTypes::String(a), CmTypes::String(b)) => a == b,
            (CmTypes::VecCmt(a), CmTypes::VecCmt(b)) => a == b,
            (CmTypes::Ref(a), CmTypes::Ref(b)) => a == b,
            (CmTypes::Res(a), CmTypes::Res(b)) => a == b,
            (CmTypes::Barrier(a), CmTypes::Barrier(b)) => a == b,
            _ => false,
        }
    }
}

impl CmTypes {
    pub fn from_any<T: Any + Send + Sync>(value: T) -> CmTypes {
        CmTypes::Any(Arc::new(RwLock::new(Box::new(value))))
    }

    /// Read-only borrow
    pub fn with_any<T: Any + Send + Sync, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        if let CmTypes::Any(lock) = self {
            let guard = lock.read().unwrap();
            guard.downcast_ref::<T>().map(f)
        } else {
            None
        }
    }

    /// Mutable borrow
    pub fn with_any_mut<T: Any + Send + Sync, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        if let CmTypes::Any(lock) = self {
            let mut guard = lock.write().unwrap();
            guard.downcast_mut::<T>().map(f)
        } else {
            None
        }
    }

    pub fn valid_number_to_usize(&self) -> Option<usize> {
        match self {
            CmTypes::Usize(x) => Some(*x),
            CmTypes::U8(x) => Some(*x as usize),
            CmTypes::U16(x) => Some(*x as usize),
            CmTypes::U32(x) => Some(*x as usize),
            CmTypes::U64(x) => Some(*x as usize),
            CmTypes::U128(x) => Some(*x as usize),
            CmTypes::I8(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I16(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I32(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I64(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I128(x) if *x >= 0 => Some(*x as usize),
            _ => None,
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
            CmTypes::Isize(val) => write!(f, "Isize({:?})", val),
            CmTypes::VecCmt(val) => write!(f, "VecCmt({:?})", val),
            CmTypes::String(val) => write!(f, "String({:?})", val),
            CmTypes::Ref(val) => write!(f, "Ref({:?})", val),
            CmTypes::Res(val) => write!(f, "Res({:?})", val),
            CmTypes::Barrier(val) => write!(f, "Barrier({:?})", val),
            CmTypes::None() => write!(f, "None"),
            CmTypes::Any(_) => write!(f, "CustomType"),
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
            CmTypes::Isize(x) => write!(f, "{}", x),
            CmTypes::String(x) => write!(f, "{}", x),
            CmTypes::Ref(x) => write!(f, "{}", x),
            CmTypes::Res(x) => write!(f, "{}", x),
            CmTypes::Barrier(x) => write!(f, "{}", x),
            CmTypes::None() => write!(f, "None"),
            CmTypes::VecCmt(x) => {
                write!(f, "[")?;
                for (i, item) in x.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            CmTypes::Any(_) => write!(f, "{}", "CustomType"),
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

type ParserFn = fn(&str) -> Result<CmTypes, CustomError>;

lazy_static! {
    /// Parsers for every explicit type
    static ref PARSERS: HashMap<&'static str, ParserFn> = {
        let mut entry_map: HashMap<&'static str, ParserFn> = HashMap::new();
        macro_rules! add {
            ($ty:expr, $p:expr) => { entry_map.insert($ty, $p as ParserFn); };
        }
        add!("bool",    |s| s.parse::<bool>().map(CmTypes::Bool).map_err(|_| CustomError::new("invalid bool")));
        add!("i8",      |s| s.parse::<i8>().map( CmTypes::I8  ).map_err(|_| CustomError::new("invalid i8")));
        add!("i16",     |s| s.parse::<i16>().map(CmTypes::I16 ).map_err(|_| CustomError::new("invalid i16")));
        add!("i32",     |s| s.parse::<i32>().map(CmTypes::I32 ).map_err(|_| CustomError::new("invalid i32")));
        add!("i64",     |s| s.parse::<i64>().map(CmTypes::I64 ).map_err(|_| CustomError::new("invalid i64")));
        add!("i128",    |s| s.parse::<i128>().map(CmTypes::I128).map_err(|_| CustomError::new("invalid i128")));
        add!("u8",      |s| s.parse::<u8>().map( CmTypes::U8  ).map_err(|_| CustomError::new("invalid u8")));
        add!("u16",     |s| s.parse::<u16>().map(CmTypes::U16 ).map_err(|_| CustomError::new("invalid u16")));
        add!("u32",     |s| s.parse::<u32>().map(CmTypes::U32 ).map_err(|_| CustomError::new("invalid u32")));
        add!("u64",     |s| s.parse::<u64>().map(CmTypes::U64 ).map_err(|_| CustomError::new("invalid u64")));
        add!("u128",    |s| s.parse::<u128>().map(CmTypes::U128).map_err(|_| CustomError::new("invalid u128")));
        add!("f32",     |s| s.parse::<f32>().map(CmTypes::F32 ).map_err(|_| CustomError::new("invalid f32")));
        add!("f64",     |s| s.parse::<f64>().map(CmTypes::F64 ).map_err(|_| CustomError::new("invalid f64")));
        add!("char",    |s| s.chars().next().map(CmTypes::Char).ok_or_else(|| CustomError::new("invalid char")));
        add!("usize",   |s| s.parse::<usize>().map(CmTypes::Usize).map_err(|_| CustomError::new("invalid usize")));
        add!("isize",   |s| s.parse::<isize>().map(CmTypes::Isize).map_err(|_| CustomError::new("invalid isize")));
        add!("String",  |s| Ok(CmTypes::String(s.to_string())));
        add!("$ref",    |s| Ok(CmTypes::Ref(s.to_string())));
        add!("$res",    |s| Ok(CmTypes::Res(s.to_string())));
        add!("$barrier", |s| Ok(CmTypes::Barrier(s.to_string())));
        add!("None",    |_| Ok(CmTypes::None()));
        entry_map
    };
}

pub fn defined_type(tp: &str) -> bool {
    PARSERS.contains_key(tp)
}

pub fn string_to_cmtype(tp: String, arg: String) -> Result<CmTypes, CustomError> {
    // 1) explicit table
    if let Some(parser) = PARSERS.get(tp.as_str()) {
        return parser(&arg);
    }

    if tp.starts_with("Vec") {
        // get type inside <> markers
        let tp = tp
            .strip_prefix("Vec<")
            .and_then(|s| s.strip_suffix(">"))
            .ok_or_else(|| CustomError::new(&format!("Invalid Vec format: {}", tp)))?;

        let mut v: Vec<CmTypes> = Vec::new();
        // arg contains tp values separated by commas
        let values: Vec<&str> = arg.split(',').collect();
        for value in values {
            if let Some(parser) = PARSERS.get(tp) {
                v.push(parser(value.trim())?);
            } else {
                return Err(CustomError::new(&format!("Unable to parse type '{}'", tp)));
            }
        }
        // Return the vector of CmTypes
        return Ok(CmTypes::VecCmt(v));
    } else {
        // Return error
        return Err(CustomError::new(&format!("Unable to parse type '{}'", tp)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_cmtypes_from_any() {
        let value = 42;
        let cm_type = CmTypes::from_any(value);
        cm_type.with_any_mut(|v: &mut i32| {
            assert_eq!(*v, 42);
            *v += 1;
        });
        cm_type.with_any(|v: &i32| {
            println!("Value: {}", v);
            assert_eq!(*v, 43);
        });
    }

    #[test]
    fn test_boxed_type() {
        let value: Box<dyn Any + Send + Sync> = Box::new(42);
        let cm_type = CmTypes::from_any(value);
        cm_type.with_any(|v: &i32| {
            assert_eq!(*v, 42);
        });
    }

    #[test]
    fn test_boxed_fn() {
        let fun: Box<dyn Fn(usize) + Send + Sync> = Box::new(|value| println!("Value: {}", value));
        let cm_type = CmTypes::from_any(fun);
        cm_type.with_any(|fun: &Box<dyn Fn(usize) + Send + Sync>| {
            fun(10);
        });
    }

    #[test]
    fn test_boxed_fnmut() {
        let fun: Box<dyn FnMut(usize) + Send + Sync> =
            Box::new(|value| println!("Value: {}", value));
        let cm_type = CmTypes::from_any(fun);
        cm_type.with_any_mut(|fun: &mut Box<dyn FnMut(usize) + Send + Sync>| {
            fun(20);
        });
    }
}
