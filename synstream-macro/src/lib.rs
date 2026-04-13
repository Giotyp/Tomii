//! Procedural macro for SynStream plugin export.
//!
//! Annotating a function with `#[synstream_export]` causes the macro to emit the
//! original function unchanged **plus** a companion `_cm` function whose signature
//! is fully type-erased via `CmTypes`.
//!
//! ## Variadic last parameter
//!
//! Use `#[synstream_export(variadic)]` when the last parameter is `Vec<T>` and
//! the graph passes multiple `$res` values that should be collected into it:
//!
//! ```rust,ignore
//! #[synstream_export(variadic)]
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
fn has_variadic_attr(attr: TokenStream) -> bool {
    attr.into_iter().any(|t| {
        if let proc_macro::TokenTree::Ident(id) = t {
            id.to_string() == "variadic"
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
pub fn synstream_export(attr: TokenStream, item: TokenStream) -> TokenStream {
    let variadic = has_variadic_attr(attr);
    let func = parse_macro_input!(item as ItemFn);
    let companion = build_companion(&func, variadic);
    let output = quote! {
        #func
        #companion
    };
    output.into()
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
                    "#[synstream_export(variadic)]: last parameter of `{}` must be `Vec<T>`",
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
            ParamKind::Variadic(_) => quote! { #name: &[::synstream_types::CmTypes] },
            _ => quote! { #name: &::synstream_types::CmTypes },
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
        RetKind::Void => quote! { { #raw_call; ::synstream_types::CmTypes::None } },
        RetKind::Primitive(v) => {
            let vi = Ident::new(v, Span::call_site());
            quote! { ::synstream_types::CmTypes::#vi(#raw_call) }
        }
        RetKind::StringOwned => quote! {
            { let __r = #raw_call; ::synstream_types::CmTypes::String(::std::sync::Arc::from(__r.as_str())) }
        },
        RetKind::CmTypesDirect => quote! { #raw_call },
        RetKind::Other => quote! { ::synstream_types::CmTypes::from_any(#raw_call) },
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
        quote! {
            let #vname: Vec<#elem_ty> = #vname.iter()
                .map(|__v| __v.with_any(|__v: &#elem_ty| __v.clone()).expect(#msg))
                .collect();
            #body
        }
    } else {
        body
    };

    quote! {
        #[no_mangle]
        #vis extern "C" fn #cm_name(#(#cm_params),*) -> ::synstream_types::CmTypes {
            #full_body
        }
    }
}
