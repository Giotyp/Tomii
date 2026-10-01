//! Dynamic kernel registration (PoC).
//!
//! Instead of baking per-kernel marshalling wrappers and lookup tables into the
//! runtime binary at build time (the static `func_reg` path), this module loads
//! a plugin `.so` that *self-describes* its exported kernels via a C-ABI symbol
//! `__tomii_exports` (a [`tomii_types::ExportTable`]), and builds the kernel
//! lookup table at process start by dlsym.
//!
//! Because the runtime binary then contains no plugin-specific codegen, adding a
//! new exported kernel to a plugin rebuilds only the plugin `.so` — the runtime
//! binary stays byte-identical. This closes the "new-stage requires a runtime
//! rebuild" caveat in the evolution study.
//!
//! The runtime and the plugin link the SAME `tomii-types`, so the `CmPtr` /
//! `CmBulkPtr` / `CmTypes` layouts match. [`tomii_types::TOMII_EXPORT_ABI_VERSION`]
//! guards against a plugin built against an incompatible `tomii-types`: a version
//! mismatch is rejected at load rather than risking a call through a wrapper with
//! a differently-laid-out `CmTypes`.

use std::collections::HashMap;
use std::sync::OnceLock;
use tomii_types::{
    CmBulkPtr, CmPtr, ExportArgSpec, ExportEntry, TomiiExportsFn, TOMII_EXPORT_ABI_VERSION,
};

/// One registered kernel resolved from a plugin's export table. The function
/// pointers point into the loaded `.so` image, which is leaked (kept mapped for
/// the process lifetime) so they remain valid; the `&'static str` metadata
/// borrows `'static` string data in that same image.
struct Kernel {
    wrap: CmPtr,
    unchecked: Option<CmPtr>,
    bulk: Option<CmBulkPtr>,
    argspec: Option<&'static [&'static str]>,
    ret_variant: Option<&'static str>,
}

static REGISTRY: OnceLock<HashMap<String, Kernel>> = OnceLock::new();

/// Reconstruct a `'static` string from a plugin-provided (ptr, len). The bytes
/// live in the leaked `.so` image, so the `'static` lifetime is sound. Returns
/// an error on invalid UTF-8 (defensive against a malformed/incompatible `.so`).
///
/// # Safety
///
/// `ptr` must point to `len` readable bytes that live for the process lifetime
/// (satisfied by the leaked plugin image).
unsafe fn str_from_raw(ptr: *const u8, len: usize) -> Result<&'static str, String> {
    if ptr.is_null() {
        return Err("null string pointer in plugin export table".to_string());
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    std::str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8 in plugin export table: {e}"))
}

/// Load `plugin_path`, read its `__tomii_exports` table, verify the ABI version,
/// and populate the global kernel registry. Must be called once, before the
/// graph is built. Idempotent-unsafe: a second successful call errors.
///
/// The expected ABI version is [`TOMII_EXPORT_ABI_VERSION`], unless the
/// `TOMII_SIMULATE_ABI_MISMATCH` environment variable is set — a test hook that
/// forces a deliberate mismatch so the version guard can be triggered from the
/// CLI against an otherwise-valid plugin.
///
/// Errors (all reject the plugin rather than proceed):
/// - the library cannot be opened,
/// - it exports no `__tomii_exports` symbol (not a dynamic-registration plugin),
/// - the table is null or reports an ABI version != expected,
/// - a descriptor contains invalid UTF-8.
pub fn init(plugin_path: &str) -> Result<(), String> {
    let expected = if std::env::var_os("TOMII_SIMULATE_ABI_MISMATCH").is_some() {
        // Force a mismatch against any real plugin so the guard is observable.
        TOMII_EXPORT_ABI_VERSION.wrapping_add(1)
    } else {
        TOMII_EXPORT_ABI_VERSION
    };
    let map = load_table(plugin_path, expected)?;
    REGISTRY
        .set(map)
        .map_err(|_| "dynamic-registration: registry already initialized".to_string())
}

/// Open a plugin and build its kernel map, validating against `expected_version`.
/// Pure (does not touch the global [`REGISTRY`]), so it is reusable by tests for
/// both the success and the ABI-mismatch paths without global-state contention.
fn load_table(plugin_path: &str, expected_version: u32) -> Result<HashMap<String, Kernel>, String> {
    // Leak the library: its code/data back the kernel function pointers and the
    // `'static` metadata strings, which must stay mapped for the process life.
    let lib = unsafe { libloading::Library::new(plugin_path) }
        .map_err(|e| format!("dynamic-registration: cannot open plugin '{plugin_path}': {e}"))?;
    let lib: &'static libloading::Library = Box::leak(Box::new(lib));

    let exports: libloading::Symbol<TomiiExportsFn> = unsafe { lib.get(b"__tomii_exports\0") }
        .map_err(|e| {
            format!(
                "dynamic-registration: plugin '{plugin_path}' exports no __tomii_exports symbol \
                 ({e}); build it with the self-contained converter mode"
            )
        })?;

    let table_ptr = unsafe { exports() };
    if table_ptr.is_null() {
        return Err("dynamic-registration: __tomii_exports returned a null table".to_string());
    }
    let table = unsafe { &*table_ptr };

    if table.abi_version != expected_version {
        return Err(format!(
            "dynamic-registration: plugin export-table ABI version {} != runtime {} — \
             rebuild the plugin against this tomii-types",
            table.abi_version, expected_version
        ));
    }

    let entries: &[ExportEntry] = if table.entries.is_null() || table.entries_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(table.entries, table.entries_len) }
    };

    let mut map: HashMap<String, Kernel> = HashMap::with_capacity(entries.len());
    for e in entries {
        let name = unsafe { str_from_raw(e.name, e.name_len) }?.to_string();

        let argspec: Option<&'static [&'static str]> = if e.argspec.is_null() {
            None
        } else {
            let specs: &[ExportArgSpec] =
                unsafe { std::slice::from_raw_parts(e.argspec, e.argspec_len) };
            let mut v: Vec<&'static str> = Vec::with_capacity(specs.len());
            for s in specs {
                v.push(unsafe { str_from_raw(s.ptr, s.len) }?);
            }
            // Leak into a `'static` slice so `get_func_argspec` can hand it out.
            Some(&*Box::leak(v.into_boxed_slice()))
        };

        let ret_variant: Option<&'static str> = if e.ret_variant.is_null() {
            None
        } else {
            Some(unsafe { str_from_raw(e.ret_variant, e.ret_variant_len) }?)
        };

        map.insert(
            name,
            Kernel {
                wrap: e.wrap,
                unchecked: e.unchecked,
                bulk: e.bulk,
                argspec,
                ret_variant,
            },
        );
    }

    Ok(map)
}

/// `true` once [`init`] has populated the registry.
pub fn is_initialized() -> bool {
    REGISTRY.get().is_some()
}

#[inline]
fn registry() -> Option<&'static HashMap<String, Kernel>> {
    REGISTRY.get()
}

pub(crate) fn get_func(name: &str) -> Option<CmPtr> {
    registry()?.get(name).map(|k| k.wrap)
}

pub(crate) fn get_bulk_func(name: &str) -> Option<CmBulkPtr> {
    registry()?.get(name).and_then(|k| k.bulk)
}

/// # Safety
///
/// Returns the unchecked wrapper twin, which skips per-argument variant checks.
/// The caller must only invoke it on nodes whose argument variants are provably
/// constant — the same contract the static path discharges via
/// `select_unchecked_wrappers`.
pub(crate) unsafe fn get_unchecked_func(name: &str) -> Option<CmPtr> {
    registry()?.get(name).and_then(|k| k.unchecked)
}

pub(crate) fn get_func_argspec(name: &str) -> Option<&'static [&'static str]> {
    registry()?.get(name).and_then(|k| k.argspec)
}

pub(crate) fn get_func_ret_variant(name: &str) -> Option<&'static str> {
    registry()?.get(name).and_then(|k| k.ret_variant)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Path to the radar plugin built with `--features dynamic-registration`.
    fn radar_so() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/radar-pipeline/target/debug/libradar_pipeline_tomii.so")
    }

    #[test]
    fn load_radar_exports_resolves_kernels() {
        let so = radar_so();
        if !so.exists() {
            eprintln!(
                "skipping: {} not built (build radar with --features dynamic-registration)",
                so.display()
            );
            return;
        }
        let map = load_table(so.to_str().unwrap(), TOMII_EXPORT_ABI_VERSION)
            .expect("radar exports should load at the matching ABI version");
        for k in ["range_fft", "doppler_fft", "cfar", "cluster"] {
            assert!(
                map.contains_key(k),
                "kernel {k} missing from radar export table"
            );
        }
        // At least one kernel must carry an argspec (the unchecked-twin metadata
        // that preserves the soundness contract across the dynamic boundary).
        assert!(
            map.values().any(|k| k.argspec.is_some()),
            "no kernel carried an argspec"
        );
    }

    #[test]
    fn abi_version_mismatch_is_rejected() {
        let so = radar_so();
        if !so.exists() {
            eprintln!("skipping: {} not built", so.display());
            return;
        }
        let err = match load_table(
            so.to_str().unwrap(),
            TOMII_EXPORT_ABI_VERSION.wrapping_add(1),
        ) {
            Ok(_) => panic!("a wrong expected ABI version must be rejected"),
            Err(e) => e,
        };
        assert!(err.contains("ABI version"), "unexpected error: {err}");
    }
}
