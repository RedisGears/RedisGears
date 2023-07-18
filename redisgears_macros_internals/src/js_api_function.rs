use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::{quote, ToTokens};
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use serde_syn::{config, from_stream};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, FnArg, ItemFn,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ApiVersion(pub u8, pub u8);

impl<'de> serde::Deserialize<'de> for ApiVersion {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let mut splitted = s.split(".");
        let major: u8 = splitted
            .next()
            .ok_or_else(|| D::Error::custom("Could not parse value as version"))?
            .parse()
            .map_err(|e| D::Error::custom(format!("Could not parse value as version, {e}.")))?;
        let minor: u8 = splitted
            .next()
            .ok_or_else(|| D::Error::custom("Could not parse value as version"))?
            .parse()
            .map_err(|e| D::Error::custom(format!("Could not parse value as version, {e}.")))?;
        Ok(ApiVersion(minor, major))
    }
}

impl From<ApiVersion> for String {
    fn from(value: ApiVersion) -> Self {
        format!("{}.{}", value.1, value.0)
    }
}

pub static ALL_VERSIONS: [ApiVersion; 2] = [ApiVersion(1, 0), ApiVersion(1, 1)];

#[derive(Debug, Deserialize)]
struct Args {
    api_name: Option<String>,
    available_since: ApiVersion,
    deprecated_on: Option<ApiVersion>,
    removed_on: Option<ApiVersion>,
    object: Option<String>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        from_stream(config::JSONY, &input)
    }
}

pub(crate) fn js_api_function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as Args);
    let function = parse_macro_input!(item as ItemFn);
    let original_function = function.clone();
    let function_name = function.sig.ident;
    let function_args: Vec<_> = function.sig.inputs.clone().into_iter().skip(1).collect();
    let function_args_names = match function_args
        .iter()
        .map(|v| match v {
            FnArg::Typed(t) => Ok(t.pat.to_token_stream()),
            _ => Err("Function arguments must by typed"),
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(res) => res,
        Err(e) => {
            return quote! {compile_error!(std::stringify!(Failed getting function arguments, #e.))}
                .into()
        }
    };

    let api_name = args.api_name.unwrap_or(function_name.to_string());

    let path = args
        .object
        .map_or(vec![], |v| v.split("/").map(|v| v.to_owned()).collect());

    let mut get_object_code = quote!(__ctx_scope.get_globals());
    path.into_iter().for_each(|v| {
        get_object_code = quote! {
            let __obj = {#get_object_code};
            __obj.get_str_field(__ctx_scope, #v).map_or_else(||{
                let obj = __isolate_scope.new_object();
                __obj.set(__ctx_scope, &__isolate_scope.new_string(#v).to_value(), &obj.to_value());
                obj
            }, |obj| {
                if obj.is_undefined() {
                    let obj = __isolate_scope.new_object();
                    __obj.set(__ctx_scope, &__isolate_scope.new_string(#v).to_value(), &obj.to_value());
                    obj
                } else {
                    assert!(obj.is_object());
                    obj.as_object()
                }
            })
        };
    });

    let register_functions: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|v| {
            if **v < args.available_since {
                return false;
            }
            if let Some(removed_on) = args.removed_on {
                if **v > removed_on {
                    return false;
                }
            }
            return true;
        })
        .map(|v| {
            let js_api_list_name =
                Ident::new(&format!("JS_API_V{}_{}", v.0, v.1), Span::call_site());
            let register_func_name = Ident::new(
                &format!(
                    "{}_register_for_v{}_{}",
                    function_name.to_string(),
                    v.0,
                    v.1
                ),
                Span::call_site(),
            );

            let deprecation_code = args.deprecated_on.map_or_else(|| quote!(), |deprecated_on| {
                if deprecated_on <= *v {
                    let deprecated_on_str: String = deprecated_on.into();
                    quote!(__script_ctx.compiled_library_api.log_warning(stringify!(#api_name api was deprecated on #deprecated_on_str.)))
                } else {
                    quote!()
                }
            });

            quote! {
                #[linkme::distributed_slice(crate::v8_api::#js_api_list_name)]
                fn #register_func_name(
                    __script_ctx: std::sync::Weak<crate::v8_script_ctx::V8ScriptCtx>,
                    __isolate_scope: &v8_rs::v8::isolate_scope::V8IsolateScope,
                    __ctx_scope: &v8_rs::v8::v8_context_scope::V8ContextScope)
                {
                    let __globals = __ctx_scope.get_globals();
                    let __object = {#get_object_code};
                    __object.set_native_function(__ctx_scope, #api_name, new_native_function!(move|__isolate_scope, __ctx_scope, #(#function_args, )*|{
                        let __script_ctx = __script_ctx.upgrade().ok_or(format!("Use of uninitialized script context for within {}", #api_name))?;
                        #deprecation_code
                        let __function_ctx = crate::v8_api::JSApiFunction {
                            api_name: #api_name,
                            script_ctx: __script_ctx,
                            isolate_scope: __isolate_scope,
                            ctx_scope: __ctx_scope,
                        };
                        #function_name(__function_ctx, #(#function_args_names, )*)
                    }));
                }
            }
        })
        .collect();

    let res = quote! {
        #original_function

        #(#register_functions)*
    };
    res.into()
}
