use proc_macro::TokenStream;

mod allow_deny_lists;

#[proc_macro]
pub fn get_allow_deny_lists(item: TokenStream) -> TokenStream {
    allow_deny_lists::get_allow_deny_lists(item)
}
