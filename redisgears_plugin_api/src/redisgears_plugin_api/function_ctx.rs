/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisgears_plugin_api::run_function_ctx::RunFunctionCtxInterface;
use crate::redisgears_plugin_api::FunctionCallResult;

pub trait FunctionCtxInterface {
    fn call(&self, run_ctx: &dyn RunFunctionCtxInterface) -> FunctionCallResult;
}
