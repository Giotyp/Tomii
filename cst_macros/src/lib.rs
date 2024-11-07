extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use std::env;
use syn::{parse::{Parse, ParseStream}, parse_macro_input, punctuated::Punctuated, Expr, LitStr, Token};

#[proc_macro]
pub fn execute_function(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ExecuteFunctionInput);

    let path = input.path.value();
    // break path in '/', take the last part and remove .rs
    let path_name = path.split('/').last().unwrap().split('.').next().unwrap();
    let name = input.name.value();

    // Use cargo manifest to point to the project's root directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let full_path = format!("{}/{}", manifest_dir, path);

    let module_name = syn::Ident::new(&path_name, proc_macro2::Span::call_site());
    let function_name = syn::Ident::new(&name, proc_macro2::Span::call_site());

    let expanded = quote! {
        {
            #[allow(dead_code)]
            #[path = #full_path]
            mod #module_name;
            #module_name::#function_name()
        }
    };

    expanded.into()
}

struct ExecuteFunctionInput {
    path: LitStr,
    name: LitStr,
}

impl syn::parse::Parse for ExecuteFunctionInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let path: LitStr = input.parse()?;
        let _: Token![,] = input.parse()?;
        let name: LitStr = input.parse()?;

        Ok(ExecuteFunctionInput { path, name })
    }
}

struct ExecuteFunctionArgsInput {
    path: LitStr,
    name: LitStr,
    args: Punctuated<Expr, Token![,]>,
}

impl Parse for ExecuteFunctionArgsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let path: LitStr = input.parse()?;
        let _: Token![,] = input.parse()?;
        let name: LitStr = input.parse()?;
        let _: Token![,] = input.parse()?;
        let args = Punctuated::<Expr, Token![,]>::parse_terminated(input)?;

        Ok(ExecuteFunctionArgsInput { path, name, args })
    }
}


#[proc_macro]
pub fn execute_function_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ExecuteFunctionArgsInput);

    let path = input.path.value();
    // break path in '/', take the last part and remove .rs
    let path_name = path.split('/').last().unwrap().split('.').next().unwrap();
    let name = input.name.value();

    // Use cargo manifest to point to the project's root directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let full_path = format!("{}/{}", manifest_dir, path);

    let module_name = syn::Ident::new(&path_name, proc_macro2::Span::call_site());
    let function_name = syn::Ident::new(&name, proc_macro2::Span::call_site());

    let args = input.args;

    let expanded = quote! {
        {
            #[allow(dead_code)]
            #[path = #full_path]
            mod #module_name;
            #module_name::#function_name(#args)
        }
    };

    TokenStream::from(expanded)
}