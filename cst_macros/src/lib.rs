extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr, Token, punctuated::Punctuated};

struct ImportCallArgs {
    fn_path: LitStr,
    fn_name: LitStr,
}

impl syn::parse::Parse for ImportCallArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let content: Punctuated<LitStr, Token![,]> = Punctuated::parse_terminated(input)?;
        let mut iter = content.into_iter();

        let fn_path = iter.next().ok_or_else(|| syn::Error::new(input.span(), "expected function path"))?;
        let fn_name = iter.next().ok_or_else(|| syn::Error::new(input.span(), "expected function name"))?;
        
        Ok(ImportCallArgs { fn_path, fn_name })
    }
}

#[proc_macro]
pub fn import_and_call(input: TokenStream) -> TokenStream {
    let ImportCallArgs { fn_path, fn_name } = parse_macro_input!(input as ImportCallArgs);

    let fn_path_value = fn_path.value();
    let fn_name_ident = syn::Ident::new(&fn_name.value(), fn_name.span());

    let generated_code = quote! {
        {
            let path = concat!(env!("CARGO_MANIFEST_DIR"), "/", #fn_path_value);
            println!("Importing and calling function from: {}", path);
            include!(path);
            #fn_name_ident()
        }
    };

    TokenStream::from(generated_code)
}