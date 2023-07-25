use std::collections::HashSet;

use proc_macro::TokenStream;
use quote::quote;
use serde::Deserialize;
use serde_syn::{config, from_stream};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
};

#[derive(Debug, Deserialize)]
struct Args {
    allow_list: HashSet<String>,
    deny_list: HashSet<String>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        from_stream(config::JSONY, input)
    }
}

pub(crate) fn get_allow_deny_lists(item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(item as Args);

    // verify no overlaps
    let res = args
        .allow_list
        .iter()
        .find(|v| args.deny_list.contains(v.as_str()));

    if let Some(res) = res {
        return quote! {compile_error!(std::stringify!(#res value appears on both, the allow list and the deny list.))}
            .into();
    };

    let allow_list: Vec<_> = args.allow_list.into_iter().collect();
    let deny_list: Vec<_> = args.deny_list.into_iter().collect();

    quote! {
        (
            [#(#allow_list, )*].into_iter().map(|v| v.to_owned()).collect::<std::collections::HashSet<String>>(),
            [#(#deny_list, )*].into_iter().map(|v| v.to_owned()).collect::<std::collections::HashSet<String>>()
        )
    }
    .into()
}
