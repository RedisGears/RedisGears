use crate::redisgears_plugin_api::run_function_ctx::RunFunctionCtxInterface;
use crate::redisgears_plugin_api::FunctionCallResult;

pub trait FunctionCtxInterface {
    fn call(&self, run_ctx: &mut dyn RunFunctionCtxInterface) -> FunctionCallResult;
}
