use lazy_static::lazy_static;
use num_complex::Complex32;
use serde::Deserialize;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

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
    String(String),
    C32(Complex32),
    Ref(String),
    Res(String),
    VecCmt(Vec<CmTypes>),
    None(),
    // "Mutable Any"
    #[serde(skip)]
    AnyMut(Arc<Mutex<Box<dyn Any + Send + Sync>>>),
    // "Immutable Any"
    #[serde(skip)]
    Any(Arc<Box<dyn Any + Send + Sync>>),
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
            _ => false,
        }
    }
}

impl CmTypes {
    // wrap any `T: Any + Send + Sync`
    pub fn from_any_mut<T: Any + Send + Sync>(value: T) -> CmTypes {
        CmTypes::AnyMut(Arc::new(Mutex::new(Box::new(value))))
    }

    // downcast and consume the catch-all, panic if wrong type or shared
    pub fn into_any_mut<T: Any + Send + Sync>(self) -> T {
        match self {
            CmTypes::AnyMut(arc_mutex) => {
                let boxed = Arc::try_unwrap(arc_mutex)
                    .expect("Multiple Arc refs on Any")
                    .into_inner()
                    .expect("Mutex poisoned");
                *boxed.downcast::<T>().unwrap_or_else(|_| {
                    panic!(
                        "Type mismatch in into_any_mut<{}>",
                        std::any::type_name::<T>()
                    )
                })
            }
            _ => panic!("into_any_mut() called on non-AnyMut variant"),
        }
    }

    pub fn as_any_mut(&self) -> MutexGuard<'_, Box<dyn Any + Send + Sync>> {
        match self {
            CmTypes::AnyMut(arc_mutex) => arc_mutex.lock().expect("Mutex poisoned in as_any_mut()"),
            other => panic!("as_any_mut() called on non-AnyMut variant: {:?}", other),
        }
    }

    pub fn from_any<T: Any + Send + Sync>(value: T) -> CmTypes {
        CmTypes::Any(Arc::new(Box::new(value)))
    }

    pub fn as_any(&self) -> &Box<dyn Any + Send + Sync> {
        match self {
            CmTypes::Any(arc_box) => arc_box.as_ref(),
            other => panic!("as_any_shared() called on non-Any variant: {:?}", other),
        }
    }

    pub fn into_any<T: Any + Send + Sync>(self) -> T {
        match self {
            CmTypes::Any(arc_box) => {
                let boxed_any = Arc::try_unwrap(arc_box)
                    .expect("Multiple Arc references exist for AnyShared")
                    .downcast::<T>()
                    .unwrap_or_else(|_| {
                        panic!("Type mismatch in into_any<{}>", std::any::type_name::<T>())
                    });
                *boxed_any
            }
            _ => panic!("into_any() called on non-Any variant"),
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
            CmTypes::C32(val) => write!(f, "C32({:?})", val),
            CmTypes::F64(val) => write!(f, "F64({:?})", val),
            CmTypes::Char(val) => write!(f, "Char({:?})", val),
            CmTypes::Usize(val) => write!(f, "Usize({:?})", val),
            CmTypes::VecCmt(val) => write!(f, "VecCmt({:?})", val),
            CmTypes::String(val) => write!(f, "String({:?})", val),
            CmTypes::Ref(val) => write!(f, "Ref({:?})", val),
            CmTypes::Res(val) => write!(f, "Res({:?})", val),
            CmTypes::None() => write!(f, "None"),
            CmTypes::AnyMut(_) => write!(f, "CustomTypeMut"),
            CmTypes::Any(_) => write!(f, "CustomTypeShared"),
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
            CmTypes::C32(x) => write!(f, "{}", x),
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
            CmTypes::AnyMut(_) => write!(f, "CustomTypeMut"),
            CmTypes::Any(_) => write!(f, "CustomTypeShared"),
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
        add!("String",  |s| Ok(CmTypes::String(s.to_string())));
        add!("$ref",    |s| Ok(CmTypes::Ref(s.to_string())));
        add!("$res",    |s| Ok(CmTypes::Res(s.to_string())));
        entry_map
    };
}

pub fn defined_type(tp: &str) -> bool {
    PARSERS.contains_key(tp)
}

/// Converts a type‐name & argument‐string into a CmTypes, with:
///  - explicit parsers for known primitives/strings
///  - JSON→Vec<CmTypes> for `VecCmt`
///  - generic `from_any` fallback for everything else
pub fn string_to_cmtype(
    tp: String,
    arg: String,
    mutable: Option<bool>,
) -> Result<CmTypes, CustomError> {
    // 1) explicit table
    if let Some(parser) = PARSERS.get(tp.as_str()) {
        return parser(&arg);
    }

    if tp == "Custom" {
        // Handle "Custom" type, which is a special case
        if let Some(is_mut) = mutable {
            if !is_mut {
                return Ok(CmTypes::from_any(arg.to_string()));
            } else {
                return Ok(CmTypes::from_any_mut(arg.to_string()));
            }
        } else {
            // by default, return immutable
            return Ok(CmTypes::from_any(arg.to_string()));
        }
    } else {
        // Return error
        return Err(CustomError::new(&format!(
            "No parser for type '{}', use 'Custom' or 'VecCmt'",
            tp
        )));
    }
}
