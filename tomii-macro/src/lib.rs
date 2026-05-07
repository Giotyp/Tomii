//! Procedural macro for Τομί plugin export.
//!
//! Annotating a function with `#[tomii_export]` causes the macro to emit the
//! original function unchanged **plus** a companion `_cm` function whose signature
//! is fully type-erased via `CmTypes`.
//!
//! ## Variadic last parameter
//!
//! Use `#[tomii_export(variadic)]` when the last parameter is `Vec<T>` and
//! the graph passes multiple `$res` values that should be collected into it:
//!
//! ```rust,ignore
//! #[tomii_export(variadic)]
//! pub fn write_to_file(file_path: &str, buffers: Vec<DMatrix<Complex32>>) { ... }
//! ```
//!
//! The generated `_cm` receives `buffers: &[CmTypes]` and extracts each element
//! via `with_any::<T>()` before calling the original function.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    parse_macro_input, FnArg, GenericArgument, Ident, ItemFn, Pat, PatType, PathArguments,
    ReturnType, Type, TypePath, TypeReference,
};

// ---------------------------------------------------------------------------
// Attribute parsing
// ---------------------------------------------------------------------------

/// Returns true if the attribute token stream contains the word `variadic`.
fn has_variadic_attr(attr: &TokenStream) -> bool {
    attr.clone().into_iter().any(|t| {
        if let proc_macro::TokenTree::Ident(id) = t {
            id.to_string() == "variadic"
        } else {
            false
        }
    })
}

/// Returns true if the attribute token stream contains the word `bulk`.
fn has_bulk_attr(attr: &TokenStream) -> bool {
    attr.clone().into_iter().any(|t| {
        if let proc_macro::TokenTree::Ident(id) = t {
            id.to_string() == "bulk"
        } else {
            false
        }
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_primitive_or_string(ty: &Type) -> bool {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 {
            let name = path.segments[0].ident.to_string();
            return matches!(
                name.as_str(),
                "bool"
                    | "i8"
                    | "i16"
                    | "i32"
                    | "i64"
                    | "i128"
                    | "u8"
                    | "u16"
                    | "u32"
                    | "u64"
                    | "u128"
                    | "f32"
                    | "f64"
                    | "usize"
                    | "isize"
                    | "char"
                    | "String"
            );
        }
    }
    false
}

fn ref_is_str(ty: &TypeReference) -> bool {
    if let Type::Path(TypePath { qself: None, path }) = ty.elem.as_ref() {
        path.segments.len() == 1 && path.segments[0].ident == "str"
    } else {
        false
    }
}

/// If `ty` is `Vec<T>`, return `T`. Otherwise `None`.
fn vec_element_type(ty: &Type) -> Option<Type> {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 && path.segments[0].ident == "Vec" {
            if let PathArguments::AngleBracketed(ab) = &path.segments[0].arguments {
                if ab.args.len() == 1 {
                    if let GenericArgument::Type(elem_ty) = &ab.args[0] {
                        return Some(elem_ty.clone());
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Parameter classification
// ---------------------------------------------------------------------------

enum ParamKind {
    /// Keep as-is: bool, i32, usize, &str, String, …
    Passthrough,
    /// `&T` (non-primitive) → `&CmTypes`, use `with_any`
    SharedRef(Type),
    /// `&mut T` (non-primitive) → `&CmTypes`, use `with_any_mut`
    MutRef(Type),
    /// Owned non-primitive `T` → `&CmTypes`, use `with_any` + clone
    OwnedNonPrim(Type),
    /// Last parameter `Vec<T>` with `variadic` attribute →
    /// `&[CmTypes]` in `_cm`, iterate + `with_any::<T>()` to collect
    Variadic(Type),
}

fn classify(ty: &Type) -> ParamKind {
    match ty {
        Type::Reference(r) => {
            if ref_is_str(r) {
                ParamKind::Passthrough
            } else if r.mutability.is_some() {
                ParamKind::MutRef((*r.elem).clone())
            } else {
                ParamKind::SharedRef((*r.elem).clone())
            }
        }
        other => {
            if is_primitive_or_string(other) {
                ParamKind::Passthrough
            } else {
                ParamKind::OwnedNonPrim(other.clone())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Return type classification
// ---------------------------------------------------------------------------

enum RetKind {
    Void,
    Primitive(String),
    StringOwned,
    CmTypesDirect,
    Other,
}

fn classify_return(ty: &Type) -> RetKind {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 {
            let name = path.segments[0].ident.to_string();
            return match name.as_str() {
                "bool" => RetKind::Primitive("Bool".into()),
                "i8" => RetKind::Primitive("I8".into()),
                "i16" => RetKind::Primitive("I16".into()),
                "i32" => RetKind::Primitive("I32".into()),
                "i64" => RetKind::Primitive("I64".into()),
                "i128" => RetKind::Primitive("I128".into()),
                "u8" => RetKind::Primitive("U8".into()),
                "u16" => RetKind::Primitive("U16".into()),
                "u32" => RetKind::Primitive("U32".into()),
                "u64" => RetKind::Primitive("U64".into()),
                "u128" => RetKind::Primitive("U128".into()),
                "f32" => RetKind::Primitive("F32".into()),
                "f64" => RetKind::Primitive("F64".into()),
                "usize" => RetKind::Primitive("Usize".into()),
                "isize" => RetKind::Primitive("Isize".into()),
                "char" => RetKind::Primitive("Char".into()),
                "String" => RetKind::StringOwned,
                "CmTypes" => RetKind::CmTypesDirect,
                _ => RetKind::Other,
            };
        }
        if let Some(last) = path.segments.last() {
            if last.ident == "CmTypes" {
                return RetKind::CmTypesDirect;
            }
        }
    }
    RetKind::Other
}

// ---------------------------------------------------------------------------
// Macro entry point
// ---------------------------------------------------------------------------

#[proc_macro_attribute]
pub fn tomii_export(attr: TokenStream, item: TokenStream) -> TokenStream {
    let variadic = has_variadic_attr(&attr);
    let bulk = has_bulk_attr(&attr);
    let func = parse_macro_input!(item as ItemFn);
    let companion = build_companion(&func, variadic);
    let output = if bulk {
        let bulk_companion = build_bulk_companion(&func);
        quote! {
            #func
            #companion
            #bulk_companion
        }
    } else {
        quote! {
            #func
            #companion
        }
    };
    output.into()
}

// ---------------------------------------------------------------------------
// Build the _bulk_cm companion
// ---------------------------------------------------------------------------

/// Build the `_bulk_cm` companion for functions annotated with `#[tomii_export(bulk)]`.
///
/// The bulk companion has signature `fn(start: usize, end: usize, args: &[CmTypes]) -> CmTypes`.
/// It extracts all static (non-index) arguments once before the loop, then calls the original
/// function for each instance in `start..end`.
///
/// Parameter classification:
/// - Parameters named `idx`, `index`, or `i` are treated as the per-instance index and
///   are supplied by the loop variable `__bulk_inst` instead of extracted from `args`.
/// - Primitive / `&str` parameters are extracted from `args` once before the loop.
/// - Non-primitive reference parameters (`&T`, `&mut T`) are accessed via `with_any` closures
///   that wrap the entire loop body, amortising any lock costs across all iterations.
///   When the Tier 2 prologue has upgraded `Any` slots to `AnyHeld`, these closures are
///   lock-free.
fn build_bulk_companion(func: &ItemFn) -> proc_macro2::TokenStream {
    let vis = &func.vis;
    let fn_name = &func.sig.ident;
    let bulk_cm_name = Ident::new(&format!("{}_bulk_cm", fn_name), Span::call_site());
    let fn_name_str = fn_name.to_string();

    struct Param {
        name: Ident,
        orig_ty: Type,
        kind: ParamKind,
        /// Index into the `args` slice consumed by this param.
        /// `None` for the per-instance index param (replaced by the loop variable).
        arg_slot: Option<usize>,
    }

    // Classify each parameter, assigning consecutive arg slots.
    let mut params: Vec<Param> = Vec::new();
    let mut slot = 0usize;
    for arg in func.sig.inputs.iter() {
        if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
            if let Pat::Ident(pi) = pat.as_ref() {
                let kind = classify(ty);
                let name_str = pi.ident.to_string();
                // The per-instance index param is supplied by the loop variable.
                let is_index_param = matches!(name_str.as_str(), "idx" | "index" | "i");
                let arg_slot = if is_index_param {
                    None
                } else {
                    let s = slot;
                    slot += 1;
                    Some(s)
                };
                params.push(Param {
                    name: pi.ident.clone(),
                    orig_ty: (*ty.as_ref()).clone(),
                    kind,
                    arg_slot,
                });
            }
        }
    }

    // Call arguments for the original function inside the loop.
    let call_args = params.iter().map(|p| {
        let n = &p.name;
        if p.arg_slot.is_none() {
            quote! { __bulk_inst }
        } else {
            quote! { #n }
        }
    });
    let raw_call = quote! { #fn_name(#(#call_args),*) };

    // The innermost body is the loop that calls the original function.
    let loop_body = quote! {
        for __bulk_inst in __bulk_start..__bulk_end {
            let _ = #raw_call;
        }
    };

    // Non-primitive params: wrap the loop body in nested `with_any` closures.
    // Iterate in reverse so the outermost closure binds the first such param.
    let closure_params: Vec<&Param> = params
        .iter()
        .filter(|p| {
            p.arg_slot.is_some()
                && matches!(
                    p.kind,
                    ParamKind::SharedRef(_) | ParamKind::MutRef(_) | ParamKind::OwnedNonPrim(_)
                )
        })
        .collect();

    let mut body: proc_macro2::TokenStream = loop_body;
    for p in closure_params.iter().rev() {
        let name = &p.name;
        let slot_idx = p.arg_slot.unwrap();
        let msg = format!("{}_bulk_cm: failed to access {}", fn_name_str, name);
        match &p.kind {
            ParamKind::SharedRef(inner_ty) => {
                body = quote! {
                    args[#slot_idx].with_any(|#name: &#inner_ty| { #body }).expect(#msg)
                };
            }
            ParamKind::MutRef(inner_ty) => {
                body = quote! {
                    args[#slot_idx].with_any_mut(|#name: &mut #inner_ty| { #body }).expect(#msg)
                };
            }
            ParamKind::OwnedNonPrim(inner_ty) => {
                body = quote! {
                    args[#slot_idx].with_any(|#name: &#inner_ty| {
                        let #name = #name.clone();
                        #body
                    }).expect(#msg)
                };
            }
            _ => unreachable!(),
        }
    }

    // Primitive / &str params: emit `let name: ty = ...` extractions before the closures.
    let passthrough_params: Vec<&Param> = params
        .iter()
        .filter(|p| p.arg_slot.is_some() && matches!(p.kind, ParamKind::Passthrough))
        .collect();

    let primitive_extractions = passthrough_params.iter().map(|p| {
        let name = &p.name;
        let slot_idx = p.arg_slot.unwrap();
        let ty = &p.orig_ty;
        // &str requires a temporary String owner.
        if let Type::Reference(r) = ty {
            if ref_is_str(r) {
                let tmp = Ident::new(&format!("{}_s", name), Span::call_site());
                return quote! {
                    let #tmp = match &args[#slot_idx] {
                        ::tomii_types::CmTypes::String(s) => s.to_string(),
                        _ => panic!(
                            concat!(stringify!(#fn_name), "_bulk_cm: expected String for ",
                                    stringify!(#name))
                        ),
                    };
                    let #name: &str = #tmp.as_str();
                };
            }
        }
        // Numeric primitive — cast from whichever CmTypes numeric variant is present.
        let fn_name_str2 = fn_name_str.clone();
        quote! {
            let #name: #ty = match &args[#slot_idx] {
                ::tomii_types::CmTypes::Bool(x)   => *x as #ty,
                ::tomii_types::CmTypes::I8(x)     => *x as #ty,
                ::tomii_types::CmTypes::I16(x)    => *x as #ty,
                ::tomii_types::CmTypes::I32(x)    => *x as #ty,
                ::tomii_types::CmTypes::I64(x)    => *x as #ty,
                ::tomii_types::CmTypes::U8(x)     => *x as #ty,
                ::tomii_types::CmTypes::U16(x)    => *x as #ty,
                ::tomii_types::CmTypes::U32(x)    => *x as #ty,
                ::tomii_types::CmTypes::U64(x)    => *x as #ty,
                ::tomii_types::CmTypes::F32(x)    => *x as #ty,
                ::tomii_types::CmTypes::F64(x)    => *x as #ty,
                ::tomii_types::CmTypes::Usize(x)  => *x as #ty,
                ::tomii_types::CmTypes::Isize(x)  => *x as #ty,
                _ => panic!("{}_bulk_cm: unexpected CmTypes variant for {}",
                            #fn_name_str2, stringify!(#name)),
            };
        }
    });

    quote! {
        #[no_mangle]
        #vis fn #bulk_cm_name(
            __bulk_start: usize,
            __bulk_end: usize,
            args: &[::tomii_types::CmTypes],
        ) -> ::tomii_types::CmTypes {
            #(#primitive_extractions)*
            #body;
            ::tomii_types::CmTypes::None
        }
    }
}

// ---------------------------------------------------------------------------
// Build the _cm companion
// ---------------------------------------------------------------------------

fn build_companion(func: &ItemFn, variadic: bool) -> proc_macro2::TokenStream {
    let vis = &func.vis;
    let fn_name = &func.sig.ident;
    let cm_name = Ident::new(&format!("{}_cm", fn_name), Span::call_site());
    let fn_name_str = fn_name.to_string();

    struct Param {
        name: Ident,
        orig_ty: Type,
        kind: ParamKind,
    }

    let mut params: Vec<Param> = func
        .sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
                if let Pat::Ident(pi) = pat.as_ref() {
                    let kind = classify(ty);
                    return Some(Param {
                        name: pi.ident.clone(),
                        orig_ty: (*ty.as_ref()).clone(),
                        kind,
                    });
                }
            }
            None
        })
        .collect();

    // If variadic: reclassify the last parameter as Variadic(elem_ty).
    // It must be Vec<T>.
    if variadic {
        if let Some(last) = params.last_mut() {
            if let Some(elem_ty) = vec_element_type(&last.orig_ty) {
                last.kind = ParamKind::Variadic(elem_ty);
            } else {
                panic!(
                    "#[tomii_export(variadic)]: last parameter of `{}` must be `Vec<T>`",
                    fn_name
                );
            }
        }
    }

    // --- Build _cm parameter list ---
    let cm_params = params.iter().map(|p| {
        let name = &p.name;
        match &p.kind {
            ParamKind::Passthrough => {
                let ty = &p.orig_ty;
                quote! { #name: #ty }
            }
            ParamKind::Variadic(_) => quote! { #name: &[::tomii_types::CmTypes] },
            _ => quote! { #name: &::tomii_types::CmTypes },
        }
    });

    // --- Return kind ---
    let ret_kind = match &func.sig.output {
        ReturnType::Default => RetKind::Void,
        ReturnType::Type(_, ty) => classify_return(ty),
    };

    // --- Build call args (names as-is; closures shadow non-prim names) ---
    let call_args = params.iter().map(|p| {
        let n = &p.name;
        quote! { #n }
    });
    let raw_call = quote! { #fn_name(#(#call_args),*) };

    // --- Innermost expression: call + return wrapping ---
    let inner_expr = match &ret_kind {
        RetKind::Void => quote! { { #raw_call; ::tomii_types::CmTypes::None } },
        RetKind::Primitive(v) => {
            let vi = Ident::new(v, Span::call_site());
            quote! { ::tomii_types::CmTypes::#vi(#raw_call) }
        }
        RetKind::StringOwned => quote! {
            { let __r = #raw_call; ::tomii_types::CmTypes::String(::std::sync::Arc::from(__r.as_str())) }
        },
        RetKind::CmTypesDirect => quote! { #raw_call },
        RetKind::Other => quote! { ::tomii_types::CmTypes::from_any(#raw_call) },
    };

    // --- Collect the non-primitive, non-variadic params that need closures ---
    let closure_params: Vec<&Param> = params
        .iter()
        .filter(|p| {
            matches!(
                p.kind,
                ParamKind::SharedRef(_) | ParamKind::MutRef(_) | ParamKind::OwnedNonPrim(_)
            )
        })
        .collect();

    // --- Wrap inner_expr in closures (innermost first, so iterate reversed) ---
    let mut body = inner_expr;
    for p in closure_params.iter().rev() {
        let name = &p.name;
        let msg = format!("{}: failed to access {}", fn_name_str, name);
        match &p.kind {
            ParamKind::SharedRef(inner_ty) => {
                body = quote! {
                    #name.with_any(|#name: &#inner_ty| { #body }).expect(#msg)
                };
            }
            ParamKind::MutRef(inner_ty) => {
                body = quote! {
                    #name.with_any_mut(|#name: &mut #inner_ty| { #body }).expect(#msg)
                };
            }
            ParamKind::OwnedNonPrim(inner_ty) => {
                body = quote! {
                    #name.with_any(|#name: &#inner_ty| { let #name = #name.clone(); #body }).expect(#msg)
                };
            }
            _ => unreachable!(),
        }
    }

    // --- Prepend variadic collection if needed ---
    let full_body = if let Some(vp) = params
        .iter()
        .find(|p| matches!(p.kind, ParamKind::Variadic(_)))
    {
        let vname = &vp.name;
        let elem_ty = match &vp.kind {
            ParamKind::Variadic(t) => t,
            _ => unreachable!(),
        };
        let msg = format!("{}: failed to extract variadic element", fn_name_str);
        // For numeric primitives: extract directly from CmTypes numeric variants
        // (no Arc/RwLock overhead from with_any).
        // String and non-primitive types fall through to the with_any path.
        let is_numeric = if let Type::Path(TypePath { qself: None, path }) = elem_ty {
            if path.segments.len() == 1 {
                matches!(
                    path.segments[0].ident.to_string().as_str(),
                    "bool" | "i8" | "i16" | "i32" | "i64" | "i128"
                        | "u8" | "u16" | "u32" | "u64" | "u128"
                        | "f32" | "f64" | "usize" | "isize"
                )
            } else {
                false
            }
        } else {
            false
        };
        if is_numeric {
            let fn_name_str2 = fn_name_str.clone();
            quote! {
                let #vname: Vec<#elem_ty> = #vname.iter()
                    .map(|__v| match __v {
                        ::tomii_types::CmTypes::Bool(x)   => (*x as u8) as #elem_ty,
                        ::tomii_types::CmTypes::I8(x)     => *x as #elem_ty,
                        ::tomii_types::CmTypes::I16(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::I32(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::I64(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::I128(x)   => **x as #elem_ty,
                        ::tomii_types::CmTypes::U8(x)     => *x as #elem_ty,
                        ::tomii_types::CmTypes::U16(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::U32(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::U64(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::U128(x)   => **x as #elem_ty,
                        ::tomii_types::CmTypes::F32(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::F64(x)    => *x as #elem_ty,
                        ::tomii_types::CmTypes::Usize(x)  => *x as #elem_ty,
                        ::tomii_types::CmTypes::Isize(x)  => *x as #elem_ty,
                        _ => panic!("{}: expected numeric CmTypes variant for variadic element",
                                    #fn_name_str2),
                    })
                    .collect();
                #body
            }
        } else {
            quote! {
                let #vname: Vec<#elem_ty> = #vname.iter()
                    .map(|__v| __v.with_any(|__v: &#elem_ty| __v.clone()).expect(#msg))
                    .collect();
                #body
            }
        }
    } else {
        body
    };

    quote! {
        #[no_mangle]
        #vis extern "C" fn #cm_name(#(#cm_params),*) -> ::tomii_types::CmTypes {
            #full_body
        }
    }
}
