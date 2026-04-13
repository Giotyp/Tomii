//! C header parser for SynStream plugins.
//!
//! Parses C function declarations annotated with `// @synstream_export` and
//! produces `ExportedFn` entries that drive the same code-generation pipeline
//! as Rust entries, but using `libloading` dynamic dispatch instead of static
//! linking.
//!
//! # Annotation format
//!
//! ```c
//! // @synstream_export
//! void* fft_planner(size_t buf_size);
//!
//! // @synstream_export(out_len=n, free=free_matrix)
//! complex_f32* generate_vector(size_t n);
//!
//! // @synstream_export(variadic)
//! void write_to_file(const char* path, void** buffers, size_t num_buffers);
//!
//! // @synstream_export(buffer: mut_array)
//! void compute_fft(void* planner, complex_f32* buffer, size_t buffer_len);
//! ```

use crate::{CmParam, CmParamKind, CmRet, ExportedFn, PrimKind, SourceLang};

// ---------------------------------------------------------------------------
// Annotation key-value parsing
// ---------------------------------------------------------------------------

/// Parsed attributes from a `// @synstream_export(...)` annotation line.
#[derive(Debug, Default)]
struct Annotation {
    /// `(variadic)` flag — last `void**, size_t` pair becomes `SliceCmTypes` + auto.
    variadic: bool,
    /// `(out_len=PARAM)` — return is an allocated array; length from param `PARAM`.
    out_len: Option<String>,
    /// `(free=FUNC)` — free the returned pointer with `FUNC` after copy.
    free_fn: Option<String>,
    /// `(PARAM: array)` — param `PARAM` is a const array pointer.
    array_params: Vec<String>,
    /// `(PARAM: mut_array)` — param `PARAM` is a mutable array pointer.
    mut_array_params: Vec<String>,
}

/// Parse the optional `(...)` body from `// @synstream_export(...)`.
///
/// The grammar is a comma-separated list of:
/// - `variadic`
/// - `out_len=IDENT`
/// - `free=IDENT`
/// - `IDENT: array`
/// - `IDENT: mut_array`
fn parse_annotation(attr_body: &str) -> Annotation {
    let mut ann = Annotation::default();

    let body = attr_body.trim();
    if body.is_empty() {
        return ann;
    }

    for token in body.split(',') {
        let token = token.trim();
        if token == "variadic" {
            ann.variadic = true;
        } else if let Some(rhs) = token.strip_prefix("out_len=") {
            ann.out_len = Some(rhs.trim().to_string());
        } else if let Some(rhs) = token.strip_prefix("free=") {
            ann.free_fn = Some(rhs.trim().to_string());
        } else if let Some(lhs) = token.strip_suffix(": array") {
            ann.array_params.push(lhs.trim().to_string());
        } else if let Some(lhs) = token.strip_suffix(": mut_array") {
            ann.mut_array_params.push(lhs.trim().to_string());
        }
        // Unknown tokens are silently ignored
    }

    ann
}

// ---------------------------------------------------------------------------
// C declaration parsing
// ---------------------------------------------------------------------------

/// A raw C parameter before classification.
#[derive(Debug)]
struct RawParam {
    name: String,
    /// Normalised type string (e.g. `"void*"`, `"const char*"`, `"size_t"`).
    c_type: String,
}

/// Normalise a C type string for consistent matching.
/// Removes extra whitespace and collapses `const T *` → `const T*`.
fn normalise_c_type(raw: &str) -> String {
    // Collapse whitespace
    let s: String = raw
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    // Move trailing `*` adjacent to token before it
    // e.g. "const char *" → "const char*", "void * *" → "void**"
    let mut result = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch == '*' {
            // Remove any preceding space before `*`
            if result.ends_with(' ') {
                result.pop();
            }
            result.push('*');
        } else {
            result.push(ch);
        }
    }

    result.trim().to_string()
}

/// Split a raw C parameter string (e.g. `"complex_f32* buffer"`) into
/// `(c_type, name)`.
///
/// Handles:
/// - `void* name`
/// - `const char* name`
/// - `size_t name`
/// - `complex_f32* name`
/// - `void** name`
fn split_c_param(raw: &str) -> Option<(String, String)> {
    let raw = raw.trim();
    if raw == "void" || raw.is_empty() {
        return None; // no-argument `void`
    }

    // Find the last token (name), the rest is type.
    // This works because C names can't contain spaces or `*`.
    let last_space = raw.rfind(|c: char| c == ' ' || c == '*');

    if let Some(pos) = last_space {
        let (type_part, name_part) = raw.split_at(pos + 1);
        let name = name_part.trim().to_string();
        let c_type = normalise_c_type(type_part);
        if name.is_empty() {
            return None;
        }
        Some((c_type, name))
    } else {
        // No space — treat as a named param with unknown type (shouldn't happen)
        None
    }
}

/// Parse a full C function declaration line (after the annotation line):
/// `return_type func_name(param_list);`
///
/// Returns `(return_type, func_name, Vec<RawParam>)` or `None` if the line
/// doesn't match the expected pattern.
fn parse_c_declaration(decl: &str) -> Option<(String, String, Vec<RawParam>)> {
    let decl = decl.trim().trim_end_matches(';').trim();

    // Find the opening paren
    let open = decl.find('(')?;
    let close = decl.rfind(')')?;

    let before_paren = decl[..open].trim();
    let params_str = decl[open + 1..close].trim();

    // Split `return_type func_name` — last whitespace-separated token is func_name
    let last_space_or_star = before_paren
        .rfind(|c: char| c == ' ' || c == '*')?;
    let (ret_part, fn_name) = before_paren.split_at(last_space_or_star + 1);
    let fn_name = fn_name.trim().to_string();
    let ret_type = normalise_c_type(ret_part);

    // Parse parameters
    let params: Vec<RawParam> = if params_str.is_empty() || params_str == "void" {
        vec![]
    } else {
        params_str
            .split(',')
            .filter_map(|p| {
                let p = p.trim();
                let (c_type, name) = split_c_param(p)?;
                Some(RawParam { name, c_type })
            })
            .collect()
    };

    Some((ret_type, fn_name, params))
}

// ---------------------------------------------------------------------------
// Type classification
// ---------------------------------------------------------------------------

/// Classify a C type string into a `CmParamKind`.
///
/// `ann_array` and `ann_mut_array` contain param names explicitly annotated
/// as array or mut_array. `is_variadic_size` marks companion `size_t` params
/// that follow a `void**` in variadic mode.
fn classify_c_param(
    raw: &RawParam,
    ann: &Annotation,
    variadic_void_ptr_ptr_seen: bool,
) -> Option<CmParamKind> {
    let t = raw.c_type.as_str();
    let name = raw.name.as_str();

    // Explicit annotation overrides type-based classification
    if ann.mut_array_params.iter().any(|s| s == name) {
        // Determine len_param: look for companion `size_t NAME_len`
        let len_param = format!("{}_len", name);
        let c_element_type = normalise_element_type(t);
        return Some(CmParamKind::MutArrayPtr {
            c_element_type,
            len_param,
        });
    }
    if ann.array_params.iter().any(|s| s == name) {
        let len_param = format!("{}_len", name);
        let c_element_type = normalise_element_type(t);
        return Some(CmParamKind::ArrayPtr {
            c_element_type,
            len_param,
        });
    }

    // Variadic companion size_t → auto-derived (caller marks in auto_params)
    if variadic_void_ptr_ptr_seen && (t == "size_t" || t == "usize") {
        return None; // signal: auto-param, skip
    }

    match t {
        "size_t" | "usize" => Some(CmParamKind::Primitive(PrimKind::Usize)),
        "int" | "int32_t" => Some(CmParamKind::Primitive(PrimKind::I32)),
        "int64_t" => Some(CmParamKind::Primitive(PrimKind::I64)),
        "uint32_t" | "unsigned int" => Some(CmParamKind::Primitive(PrimKind::U32)),
        "uint64_t" => Some(CmParamKind::Primitive(PrimKind::U64)),
        "float" => Some(CmParamKind::Primitive(PrimKind::F32)),
        "double" => Some(CmParamKind::Primitive(PrimKind::F64)),
        "bool" | "_Bool" => Some(CmParamKind::Primitive(PrimKind::Bool)),
        "const char*" | "char const*" | "char*" => Some(CmParamKind::StrRef),
        "void*" => Some(CmParamKind::OpaquePtr),
        "void**" => {
            // variadic pointer array — becomes SliceCmTypes
            Some(CmParamKind::SliceCmTypes)
        }
        other => {
            // T* (pointer to custom type) — treat as OpaquePtr unless annotated
            if other.ends_with('*') {
                Some(CmParamKind::OpaquePtr)
            } else {
                // Unknown value type — map to I32 as a safe default
                Some(CmParamKind::Primitive(PrimKind::I32))
            }
        }
    }
}

/// Extract the element type name from a pointer type, e.g. `complex_f32*` → `complex_f32`.
fn normalise_element_type(c_type: &str) -> String {
    c_type.trim_end_matches('*').trim().to_string()
}

/// Classify a C return type.
fn classify_c_return(
    ret_type: &str,
    ann: &Annotation,
) -> CmRet {
    match ret_type {
        "void" => CmRet::Void,
        "size_t" | "usize" => CmRet::Primitive(PrimKind::Usize),
        "int" | "int32_t" => CmRet::Primitive(PrimKind::I32),
        "int64_t" => CmRet::Primitive(PrimKind::I64),
        "uint32_t" | "unsigned int" => CmRet::Primitive(PrimKind::U32),
        "uint64_t" => CmRet::Primitive(PrimKind::U64),
        "float" => CmRet::Primitive(PrimKind::F32),
        "double" => CmRet::Primitive(PrimKind::F64),
        "bool" | "_Bool" => CmRet::Primitive(PrimKind::Bool),
        "char*" => CmRet::AllocatedString {
            free_fn: ann.free_fn.clone(),
        },
        "void*" => {
            if let Some(ref len_param) = ann.out_len {
                CmRet::AllocatedArray {
                    c_element_type: "void".to_string(),
                    len_from_param: len_param.clone(),
                    free_fn: ann.free_fn.clone(),
                }
            } else {
                CmRet::OpaquePtr
            }
        }
        other if other.ends_with('*') => {
            // e.g. `complex_f32*`
            let c_element_type = normalise_element_type(other);
            if let Some(ref len_param) = ann.out_len {
                CmRet::AllocatedArray {
                    c_element_type,
                    len_from_param: len_param.clone(),
                    free_fn: ann.free_fn.clone(),
                }
            } else {
                CmRet::OpaquePtr
            }
        }
        _ => CmRet::Primitive(PrimKind::I32), // safe fallback
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Parse C header source text and return all `@synstream_export`-annotated
/// function entries.
pub(crate) fn collect_c_entries(source: &str) -> Vec<ExportedFn> {
    let mut entries = Vec::new();

    let lines: Vec<&str> = source.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Match annotation line
        if let Some(after_at) = line.strip_prefix("// @synstream_export") {
            // Extract optional `(...)` body
            let attr_body = if let Some(paren_content) = after_at.trim().strip_prefix('(') {
                // Find the closing paren
                if let Some(close_pos) = paren_content.rfind(')') {
                    &paren_content[..close_pos]
                } else {
                    ""
                }
            } else {
                ""
            };

            let ann = parse_annotation(attr_body);
            i += 1;

            // Collect the next non-empty, non-comment line(s) until we see a `;`
            let mut decl_buf = String::new();
            while i < lines.len() {
                let next = lines[i].trim();
                if next.is_empty() || next.starts_with("//") {
                    i += 1;
                    continue;
                }
                decl_buf.push_str(next);
                decl_buf.push(' ');
                i += 1;
                if next.contains(';') {
                    break;
                }
            }

            let decl_str = decl_buf.trim().to_string();
            if decl_str.is_empty() {
                continue;
            }

            if let Some(entry) = build_entry(decl_str.as_str(), &ann) {
                entries.push(entry);
            }
        } else {
            i += 1;
        }
    }

    entries
}

/// Build an `ExportedFn` from a parsed C declaration and its annotation.
fn build_entry(decl: &str, ann: &Annotation) -> Option<ExportedFn> {
    let (ret_type, fn_name, raw_params) = parse_c_declaration(decl)?;

    let cm_ret = classify_c_return(&ret_type, ann);

    let mut cm_params: Vec<CmParam> = Vec::new();
    let mut auto_params: Vec<String> = Vec::new();

    // Track whether we've seen void** (variadic mode).
    // The annotation drives variadic classification; we also detect void** inline
    // so that the size_t companion is correctly auto-derived.
    let mut seen_void_ptr_ptr = false;

    for raw in &raw_params {
        let t = raw.c_type.as_str();

        // Check if this is a companion size_t for an ArrayPtr/MutArrayPtr param
        // i.e. its name matches `{prev_array_param}_len`
        let is_companion_len = cm_params.last().map_or(false, |prev: &CmParam| {
            match &prev.kind {
                CmParamKind::ArrayPtr { len_param, .. }
                | CmParamKind::MutArrayPtr { len_param, .. } => len_param == &raw.name,
                _ => false,
            }
        });

        if is_companion_len {
            // Mark as auto-derived; don't add to cm_params
            auto_params.push(raw.name.clone());
            continue;
        }

        // In variadic mode, size_t after void** is auto-derived
        let void_ptr_ptr_before = seen_void_ptr_ptr && ann.variadic;
        if t == "void**" {
            seen_void_ptr_ptr = true;
        }

        match classify_c_param(raw, ann, void_ptr_ptr_before) {
            Some(kind) => {
                cm_params.push(CmParam {
                    name: raw.name.clone(),
                    kind,
                });
            }
            None => {
                // auto-param: size_t companion to variadic void**
                auto_params.push(raw.name.clone());
            }
        }
    }

    Some(ExportedFn {
        cm_name: fn_name.clone(),
        registry_key: fn_name,
        cm_params,
        cm_ret,
        source_lang: SourceLang::C,
        auto_params,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_opaque_return() {
        let src = r#"
// @synstream_export
void* fft_planner(size_t buf_size);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cm_name, "fft_planner");
        assert_eq!(entries[0].registry_key, "fft_planner");
        assert_eq!(entries[0].cm_params.len(), 1);
        assert!(
            matches!(entries[0].cm_params[0].kind, CmParamKind::Primitive(PrimKind::Usize)),
            "expected Primitive(Usize), got {:?}",
            entries[0].cm_params[0].kind
        );
        assert!(
            matches!(entries[0].cm_ret, CmRet::OpaquePtr),
            "expected OpaquePtr, got {:?}",
            entries[0].cm_ret
        );
        assert_eq!(entries[0].source_lang, SourceLang::C);
    }

    #[test]
    fn test_alloc_array_with_free() {
        let src = r#"
// @synstream_export(out_len=n, free=free_vector)
complex_f32* generate_vector(size_t n);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.cm_name, "generate_vector");
        assert!(
            matches!(
                &e.cm_ret,
                CmRet::AllocatedArray { len_from_param, free_fn, .. }
                    if len_from_param == "n" && free_fn.as_deref() == Some("free_vector")
            ),
            "unexpected cm_ret: {:?}",
            e.cm_ret
        );
    }

    #[test]
    fn test_variadic() {
        let src = r#"
// @synstream_export(variadic)
void write_to_file(const char* file_path, void** buffers, size_t num_buffers);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.cm_params.len(), 2, "expected 2 CmTypes params (file_path + buffers)");
        assert!(
            matches!(e.cm_params[0].kind, CmParamKind::StrRef),
            "expected StrRef for file_path, got {:?}",
            e.cm_params[0].kind
        );
        assert!(
            matches!(e.cm_params[1].kind, CmParamKind::SliceCmTypes),
            "expected SliceCmTypes for buffers, got {:?}",
            e.cm_params[1].kind
        );
        assert!(
            e.auto_params.contains(&"num_buffers".to_string()),
            "expected num_buffers in auto_params, got {:?}",
            e.auto_params
        );
    }

    #[test]
    fn test_mut_array_param() {
        let src = r#"
// @synstream_export(buffer: mut_array)
void compute_fft(void* planner, complex_f32* buffer, size_t buffer_len);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(
            e.cm_params.len(),
            2,
            "expected 2 params (planner + buffer); got {:?}",
            e.cm_params.iter().map(|p| &p.name).collect::<Vec<_>>()
        );
        assert!(
            matches!(e.cm_params[0].kind, CmParamKind::OpaquePtr),
            "expected OpaquePtr for planner, got {:?}",
            e.cm_params[0].kind
        );
        assert!(
            matches!(&e.cm_params[1].kind, CmParamKind::MutArrayPtr { .. }),
            "expected MutArrayPtr for buffer, got {:?}",
            e.cm_params[1].kind
        );
        assert!(
            e.auto_params.contains(&"buffer_len".to_string()),
            "expected buffer_len in auto_params, got {:?}",
            e.auto_params
        );
    }

    #[test]
    fn test_annotation_parsing_combined() {
        let body = "vector: array, free=free_matrix, out_len=n";
        let ann = parse_annotation(body);
        assert_eq!(ann.out_len.as_deref(), Some("n"));
        assert_eq!(ann.free_fn.as_deref(), Some("free_matrix"));
        assert!(ann.array_params.contains(&"vector".to_string()));
        assert!(!ann.variadic);
    }

    #[test]
    fn test_plain_void_return() {
        let src = r#"
// @synstream_export
void destroy_planner(void* planner);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert!(matches!(e.cm_ret, CmRet::Void));
        assert!(matches!(e.cm_params[0].kind, CmParamKind::OpaquePtr));
    }

    #[test]
    fn test_alloc_string_with_free() {
        let src = r#"
// @synstream_export(free=free_str)
char* get_version_string(void* ctx);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert!(
            matches!(&e.cm_ret, CmRet::AllocatedString { free_fn }
                if free_fn.as_deref() == Some("free_str")),
            "unexpected cm_ret: {:?}",
            e.cm_ret
        );
    }

    #[test]
    fn test_multiline_declaration() {
        let src = r#"
// @synstream_export
void* fft_planner(
    size_t buf_size
);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cm_name, "fft_planner");
    }

    #[test]
    fn test_multiple_exports() {
        let src = r#"
// @synstream_export
void* fft_planner(size_t n);

// @synstream_export
void destroy_planner(void* planner);
"#;
        let entries = collect_c_entries(src);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].cm_name, "fft_planner");
        assert_eq!(entries[1].cm_name, "destroy_planner");
    }

    #[test]
    fn test_normalise_c_type() {
        assert_eq!(normalise_c_type("const char *"), "const char*");
        assert_eq!(normalise_c_type("void *"), "void*");
        assert_eq!(normalise_c_type("void **"), "void**");
        assert_eq!(normalise_c_type("size_t"), "size_t");
    }
}
