//! Generic Python bridge plugin for Tomii.
//!
//! Exports four `#[no_mangle]` `_cm` functions that the tomii-converter
//! picks up as-is (no `#[tomii_export]` macro needed):
//!
//!   py_load_callable   — init: import module, cache a Python callable as CmTypes::Any
//!   py_call_any        — compute: call a cached callable; args/return are CmTypes::Any
//!   py_call_void       — variadic sink: call a callable, collect trailing args into a list
//!   py_call_bytes      — NumPy fast path: bytes in, bytes out, zero extra allocation
//!
//! Also exports `tomii_python_bridge_abi() -> u32` so the embedding `tomii-core`
//! binary (built with `--features embed-python`) can verify Python version
//! compatibility before the graph starts.
//!
//! GIL strategy (auto-detected at startup):
//!   Tier 1 (default, stock 3.11/3.12): Python::with_gil per call. NumPy/BLAS
//!     release the GIL internally, so matmul/FFT-heavy graphs scale with worker count.
//!   Tier 3 (python3.13t, PEP 703): Python::with_gil is a no-op in the free-threaded
//!     build (compiled with Py_GIL_DISABLED). Full parallelism for all Python code.
//!
//! Python initialization is owned by the embedding binary; the bridge uses the
//! `extension-module` PyO3 feature so it does NOT link its own libpython copy.
//!
//! Sub-interpreter isolation (Tier 2, PEP 684) is not implemented here because
//! NumPy 2.x has partial sub-interpreter support and C extensions that lack
//! Py_mod_multiple_interpreters silently fall back to the main GIL. For most
//! NumPy-heavy graphs Tier 1 already gives adequate scaling; use python3.13t for more.

#![allow(improper_ctypes_definitions)]

use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule, PyTuple};
use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;
use tomii_types::CmTypes;

// PYTHON_BRIDGE_ABI_BASE: u32 — (major<<24)|(minor<<16), no GIL bit.
// Generated at bridge compile time by build.rs using pyo3-build-config.
include!(concat!(env!("OUT_DIR"), "/python_abi.rs"));

/// Return the packed Python ABI for this bridge: (major<<24)|(minor<<16)|(gil_disabled<<15).
///
/// The GIL_DISABLED bit is set via compile-time cfg so that the build script
/// does not need pyo3-build-config >= 0.23 for the `gil_disabled` field.
///
/// The embedding binary (`tomii-core --features embed-python`) calls this
/// immediately after loading the bridge dylib and aborts on mismatch,
/// preventing silent runtime corruption from interpreter version skew.
#[no_mangle]
pub extern "C" fn tomii_python_bridge_abi() -> u32 {
    let gil_bit: u32 = if cfg!(Py_GIL_DISABLED) { 1 << 15 } else { 0 };
    PYTHON_BRIDGE_ABI_BASE | gil_bit
}

// --------------------------------------------------------------------------- //
// Startup logging (printed once on first call)
// --------------------------------------------------------------------------- //

static BRIDGE_INIT: OnceLock<()> = OnceLock::new();

fn ensure_logged() {
    BRIDGE_INIT.get_or_init(|| {
        #[cfg(Py_GIL_DISABLED)]
        eprintln!("tomii-python-bridge: tier 3 — free-threaded Python 3.13t (no GIL)");

        #[cfg(not(Py_GIL_DISABLED))]
        {
            if std::env::var("TOMII_PY_SUBINTERPRETERS").is_ok() {
                eprintln!(
                    "tomii-python-bridge: TOMII_PY_SUBINTERPRETERS requested but not yet \
                     implemented; falling back to tier 1"
                );
            }
            eprintln!(
                "tomii-python-bridge: tier 1 — standard GIL \
                 (NumPy/BLAS ops release GIL; use python3.13t for tier 3)"
            );
        }
    });
}

// --------------------------------------------------------------------------- //
// CmTypes ↔ Python conversion helpers
// --------------------------------------------------------------------------- //

fn cm_to_py<'py>(py: Python<'py>, cm: &CmTypes) -> Bound<'py, PyAny> {
    use pyo3::types::PyString;
    match cm {
        CmTypes::Any(arc) => {
            let guard = arc.read();
            let py_obj = guard
                .downcast_ref::<Py<PyAny>>()
                .expect("py bridge: Any slot does not contain Py<PyAny>");
            // clone_ref increments refcount under the GIL and returns a new Py<PyAny>
            py_obj.clone_ref(py).into_bound(py)
        }
        CmTypes::Bool(b) => b.into_py(py).into_bound(py),
        CmTypes::I8(n) => n.into_py(py).into_bound(py),
        CmTypes::I16(n) => n.into_py(py).into_bound(py),
        CmTypes::I32(n) => n.into_py(py).into_bound(py),
        CmTypes::I64(n) => n.into_py(py).into_bound(py),
        CmTypes::U8(n) => n.into_py(py).into_bound(py),
        CmTypes::U16(n) => n.into_py(py).into_bound(py),
        CmTypes::U32(n) => n.into_py(py).into_bound(py),
        CmTypes::U64(n) => n.into_py(py).into_bound(py),
        CmTypes::Usize(n) => (*n as u64).into_py(py).into_bound(py),
        CmTypes::Isize(n) => (*n as i64).into_py(py).into_bound(py),
        CmTypes::F32(f) => f.into_py(py).into_bound(py),
        CmTypes::F64(f) => f.into_py(py).into_bound(py),
        CmTypes::String(s) => PyString::new_bound(py, s.as_ref()).into_any(),
        CmTypes::Bytes(data) => pyo3::types::PyBytes::new_bound(py, data.as_slice()).into_any(),
        // Barrier sentinels — not a real value; convert to Python None so callers
        // can filter them with `if arg is None` if needed.
        CmTypes::None => py.None().into_bound(py),
        other => panic!(
            "tomii-python-bridge: cannot convert CmTypes variant to Python (discriminant {:?})",
            std::mem::discriminant(other)
        ),
    }
}

fn py_to_cm(_py: Python<'_>, obj: Bound<'_, PyAny>) -> CmTypes {
    let py_obj: Py<PyAny> = obj.unbind();
    let boxed: Box<dyn Any + Send + Sync> = Box::new(py_obj);
    CmTypes::Any(Arc::new(RwLock::new(boxed)))
}

/// Extract the callable from a CmTypes::Any slot, incrementing its refcount.
fn get_callable(py: Python<'_>, arc: &Arc<RwLock<Box<dyn Any + Send + Sync>>>) -> Py<PyAny> {
    let guard = arc.read();
    guard
        .downcast_ref::<Py<PyAny>>()
        .expect("py bridge: callable slot does not contain Py<PyAny>")
        .clone_ref(py)
}

// --------------------------------------------------------------------------- //
// Bridge entry points — taken as-is by tomii-converter (no macro needed)
// --------------------------------------------------------------------------- //

/// Init function: import `args[0]` (module name) and cache `getattr(args[1])` (fn name).
///
/// Graph DSL name: "py_load_callable"
/// Returns: CmTypes::Any wrapping a Py<PyAny> callable
#[no_mangle]
pub fn py_load_callable_cm(args: &[CmTypes]) -> CmTypes {
    ensure_logged();
    let module_name = match &args[0] {
        CmTypes::String(s) => s.clone(),
        _ => panic!("py_load_callable: args[0] must be String (module name)"),
    };
    let fn_name = match &args[1] {
        CmTypes::String(s) => s.clone(),
        _ => panic!("py_load_callable: args[1] must be String (function name)"),
    };

    Python::with_gil(|py| {
        let module = PyModule::import_bound(py, module_name.as_ref()).unwrap_or_else(|e| {
            panic!(
                "py_load_callable: cannot import module '{}': {}",
                module_name, e
            )
        });
        let func = module.getattr(fn_name.as_ref()).unwrap_or_else(|e| {
            panic!(
                "py_load_callable: '{}' not found in module '{}': {}",
                fn_name, module_name, e
            )
        });
        let callable: Py<PyAny> = func.unbind();
        let boxed: Box<dyn Any + Send + Sync> = Box::new(callable);
        CmTypes::Any(Arc::new(RwLock::new(boxed)))
    })
}

/// Compute function: call a cached callable with positional args.
///
/// args[0]: CmTypes::Any  — callable handle (from py_load_callable init)
/// args[1..]: any CmTypes — passed as positional Python args.
///            CmTypes::None entries (barrier sentinels) are filtered out.
///
/// Graph DSL name: "py_call_any"
/// Returns: CmTypes::Any wrapping the Python result
#[no_mangle]
pub fn py_call_any_cm(args: &[CmTypes]) -> CmTypes {
    let callable_arc = match &args[0] {
        CmTypes::Any(arc) => arc.clone(),
        _ => panic!("py_call_any: args[0] must be CmTypes::Any (callable handle)"),
    };

    Python::with_gil(|py| {
        let callable = get_callable(py, &callable_arc);
        let callable_bound = callable.bind(py);

        // Collect non-None args (filter out barrier sentinels)
        let py_args: Vec<Bound<'_, PyAny>> = args[1..]
            .iter()
            .filter(|cm| !matches!(cm, CmTypes::None))
            .map(|cm| cm_to_py(py, cm))
            .collect();

        let tuple = PyTuple::new_bound(py, &py_args);
        let result = callable_bound
            .call1(tuple)
            .unwrap_or_else(|e| panic!("py_call_any: call failed: {}", e));

        py_to_cm(py, result)
    })
}

/// Variadic sink: call a callable with `(first_arg, [rest_args...])`.
///
/// Mirrors #[tomii_export(variadic)] on the Rust side. The first non-callable
/// arg is passed directly; all subsequent args are collected into a Python list
/// and passed as the second positional argument. Useful for terminal write nodes.
///
/// args[0]: CmTypes::Any  — callable handle
/// args[1]: any CmTypes   — first positional arg (e.g. output file path)
/// args[2..]: any CmTypes — collected into a Python list for the second arg
///            CmTypes::None barrier sentinels are filtered before collection.
///
/// Graph DSL name: "py_call_void"
/// Returns: CmTypes::None
#[no_mangle]
pub fn py_call_void_cm(args: &[CmTypes]) -> CmTypes {
    if args.len() < 2 {
        panic!("py_call_void: need at least 2 args (callable, first_arg)");
    }
    let callable_arc = match &args[0] {
        CmTypes::Any(arc) => arc.clone(),
        _ => panic!("py_call_void: args[0] must be CmTypes::Any (callable handle)"),
    };

    Python::with_gil(|py| {
        let callable = get_callable(py, &callable_arc);
        let callable_bound = callable.bind(py);

        let first_arg = cm_to_py(py, &args[1]);

        // Collect remaining args (filtering barrier sentinels) into a Python list
        let rest: Vec<Bound<'_, PyAny>> = args[2..]
            .iter()
            .filter(|cm| !matches!(cm, CmTypes::None))
            .map(|cm| cm_to_py(py, cm))
            .collect();
        let py_list = PyList::new_bound(py, &rest);

        let tuple = PyTuple::new_bound(py, &[first_arg, py_list.into_any()]);
        callable_bound
            .call1(tuple)
            .unwrap_or_else(|e| panic!("py_call_void: call failed: {}", e));

        CmTypes::None
    })
}

/// NumPy fast path: pass raw bytes (zero-copy via Arc) and a JSON shape/dtype hint.
///
/// The caller is expected to reconstruct a numpy array from the bytes buffer.
/// The Python function receives (array_bytes, metadata_str) and must return bytes.
/// Use np.frombuffer(data, dtype=...) inside the Python function.
///
/// args[0]: CmTypes::Any    — callable handle
/// args[1]: CmTypes::Bytes  — raw array bytes
/// args[2]: CmTypes::String — dtype+shape metadata as a JSON string
///
/// Graph DSL name: "py_call_bytes"
/// Returns: CmTypes::Bytes
#[no_mangle]
pub fn py_call_bytes_cm(args: &[CmTypes]) -> CmTypes {
    let callable_arc = match &args[0] {
        CmTypes::Any(arc) => arc.clone(),
        _ => panic!("py_call_bytes: args[0] must be CmTypes::Any (callable handle)"),
    };
    let data = match &args[1] {
        CmTypes::Bytes(b) => b.clone(),
        _ => panic!("py_call_bytes: args[1] must be CmTypes::Bytes"),
    };
    let meta = match &args[2] {
        CmTypes::String(s) => s.clone(),
        _ => panic!("py_call_bytes: args[2] must be CmTypes::String (metadata)"),
    };

    Python::with_gil(|py| {
        use pyo3::types::PyString;
        let callable = get_callable(py, &callable_arc);
        let callable_bound = callable.bind(py);

        let py_bytes = pyo3::types::PyBytes::new_bound(py, data.as_slice());
        let py_meta = PyString::new_bound(py, meta.as_ref()).into_any();
        let tuple = PyTuple::new_bound(py, &[py_bytes.into_any(), py_meta]);

        let result = callable_bound
            .call1(tuple)
            .unwrap_or_else(|e| panic!("py_call_bytes: call failed: {}", e));

        // Expect a bytes-like object back
        let py_bytes_result = result
            .downcast::<pyo3::types::PyBytes>()
            .unwrap_or_else(|_| panic!("py_call_bytes: function must return bytes"));
        let bytes: Vec<u8> = py_bytes_result.as_bytes().to_vec();
        CmTypes::Bytes(Arc::new(bytes))
    })
}
