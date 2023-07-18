use proc_macro::TokenStream;

mod allow_deny_lists;
mod js_api_function;

#[proc_macro]
pub fn get_allow_deny_lists(item: TokenStream) -> TokenStream {
    allow_deny_lists::get_allow_deny_lists(item)
}

#[proc_macro_attribute]
pub fn js_api_function(attr: TokenStream, item: TokenStream) -> TokenStream {
    js_api_function::js_api_function(attr, item)
}
