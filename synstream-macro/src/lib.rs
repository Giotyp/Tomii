//! Procedural macro for SynStream plugin export.
//!
//! Annotating a function with `#[synstream_export]` causes the macro to emit the
//! original function unchanged **plus** a companion `_cm` function whose signature
//! is fully type-erased via `CmTypes`.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    parse_macro_input, FnArg, Ident, ItemFn, Pat, PatType, ReturnType, Type, TypePath,
    TypeReference,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns true if `ty` is a primitive or `String` that should be kept as-is
/// in the `_cm` signature.
fn is_primitive_or_string(ty: &Type) -> bool {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 {
            let name = path.segments[0].ident.to_string();
            matches!(
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
            )
        } else {
            false
        }
    } else {
        false
    }
}

/// Returns true if a reference type (`&T` or `&mut T`) has `T = str`.
fn ref_is_str(ty: &TypeReference) -> bool {
    if let Type::Path(TypePath { qself: None, path }) = ty.elem.as_ref() {
        path.segments.len() == 1 && path.segments[0].ident == "str"
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Parameter classification
// ---------------------------------------------------------------------------

enum ParamKind {
    /// Keep as-is (bool, i32, usize, &str, String, …)
    Passthrough,
    /// `&T` where T is non-primitive → `&CmTypes`, use `with_any`
    SharedRef(Type),
    /// `&mut T` where T is non-primitive → `&CmTypes`, use `with_any_mut`
    MutRef(Type),
    /// Owned non-primitive `T` → `&CmTypes`, use `with_any`, clone
    OwnedNonPrim(Type),
}

fn classify(ty: &Type) -> ParamKind {
    match ty {
        Type::Reference(r) => {
            if ref_is_str(r) {
                // &str — passthrough
                ParamKind::Passthrough
            } else if r.mutability.is_some() {
                // &mut T (non-primitive)
                ParamKind::MutRef((*r.elem).clone())
            } else {
                // &T (non-primitive)
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
    Primitive(String), // variant name in CmTypes e.g. "Usize", "F64"
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
        // Multi-segment path — e.g. synstream_types::CmTypes
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

/// Derive a `_cm` companion function for a SynStream plugin function.
///
/// # Example
///
/// ```rust,ignore
/// use synstream_macro::synstream_export;
///
/// #[synstream_export]
/// pub fn compute_fft(planner: &Arc<dyn Fft<f32>>, buf: &mut Vec<Complex32>) {
///     // ...
/// }
/// ```
///
/// This emits `compute_fft` unchanged **and** a `compute_fft_cm` function
/// with all non-primitive parameters replaced by `&CmTypes`.
#[proc_macro_attribute]
pub fn synstream_export(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let companion = build_companion(&func);
    let output = quote! {
        #func
        #companion
    };
    output.into()
}

// ---------------------------------------------------------------------------
// Build the _cm companion
// ---------------------------------------------------------------------------

fn build_companion(func: &ItemFn) -> proc_macro2::TokenStream {
    let vis = &func.vis;
    let fn_name = &func.sig.ident;
    let cm_name = Ident::new(&format!("{}_cm", fn_name), Span::call_site());
    let fn_name_str = fn_name.to_string();

    // Collect parameters, skipping `self`.
    struct Param {
        name: Ident,
        orig_ty: Type,
        kind: ParamKind,
    }

    let params: Vec<Param> = func
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

    // --- Build _cm parameter list ---
    let cm_params = params.iter().map(|p| {
        let name = &p.name;
        match &p.kind {
            ParamKind::Passthrough => {
                let ty = &p.orig_ty;
                quote! { #name: #ty }
            }
            _ => quote! { #name: &::synstream_types::CmTypes },
        }
    });

    // --- Determine return expression ---
    let ret_ty = match &func.sig.output {
        ReturnType::Default => None,
        ReturnType::Type(_, ty) => Some(ty.as_ref().clone()),
    };

    let ret_kind = match &ret_ty {
        None => RetKind::Void,
        Some(ty) => classify_return(ty),
    };

    // --- Build the call expression ---
    // Passthrough params are passed directly; non-primitive params are extracted
    // inside nested closures and then the function is called at the innermost level.

    // Collect non-primitive params in order (they need nesting).
    let non_prim_params: Vec<&Param> = params
        .iter()
        .filter(|p| !matches!(p.kind, ParamKind::Passthrough))
        .collect();

    // The innermost body: the actual function call + return wrapping.
    let call_args = params.iter().map(|p| {
        let name = &p.name;
        quote! { #name }
    });

    // Build the raw call
    let raw_call = quote! { #fn_name(#(#call_args),*) };

    // Wrap the call's result for returning CmTypes
    let inner_expr = match &ret_kind {
        RetKind::Void => quote! {
            { #raw_call; ::synstream_types::CmTypes::None }
        },
        RetKind::Primitive(variant) => {
            let variant_ident = Ident::new(variant, Span::call_site());
            quote! { ::synstream_types::CmTypes::#variant_ident(#raw_call) }
        }
        RetKind::StringOwned => quote! {
            {
                let __result = #raw_call;
                ::synstream_types::CmTypes::String(::std::sync::Arc::from(__result.as_str()))
            }
        },
        RetKind::CmTypesDirect => quote! { #raw_call },
        RetKind::Other => quote! { ::synstream_types::CmTypes::from_any(#raw_call) },
    };

    // Now wrap with nested closures for each non-primitive param, innermost first.
    let body = if non_prim_params.is_empty() {
        inner_expr
    } else {
        // Build from inside out: start with inner_expr, wrap with each closure.
        let mut acc = inner_expr;
        for p in non_prim_params.iter().rev() {
            let name = &p.name;
            let param_name_str = name.to_string();
            let expect_msg = format!("{}: failed to access {}", fn_name_str, param_name_str);
            match &p.kind {
                ParamKind::SharedRef(inner_ty) => {
                    acc = quote! {
                        #name.with_any(|#name: &#inner_ty| {
                            #acc
                        }).expect(#expect_msg)
                    };
                }
                ParamKind::MutRef(inner_ty) => {
                    acc = quote! {
                        #name.with_any_mut(|#name: &mut #inner_ty| {
                            #acc
                        }).expect(#expect_msg)
                    };
                }
                ParamKind::OwnedNonPrim(inner_ty) => {
                    acc = quote! {
                        #name.with_any(|#name: &#inner_ty| {
                            let #name = #name.clone();
                            #acc
                        }).expect(#expect_msg)
                    };
                }
                ParamKind::Passthrough => unreachable!(),
            }
        }
        acc
    };

    quote! {
        #[no_mangle]
        #vis fn #cm_name(#(#cm_params),*) -> ::synstream_types::CmTypes {
            #body
        }
    }
}
