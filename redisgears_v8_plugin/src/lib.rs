/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{init_api, Context};
use v8_rs::v8::{
    isolate::V8Isolate, isolate_scope::V8IsolateScope, try_catch::V8TryCatch,
    v8_array::V8LocalArray, v8_context_scope::V8ContextScope, v8_value::V8LocalValue,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtxInterfaceUninitialised, load_library_ctx::FunctionFlags, GearsApiError,
};

mod v8_backend;
mod v8_function_ctx;
mod v8_native_functions;
mod v8_notifications_ctx;
mod v8_redisai;
mod v8_script_ctx;
mod v8_stream_ctx;

use crate::v8_backend::V8Backend;
use std::sync::{Arc, Mutex};

pub(crate) fn get_exception_msg(
    isolate: &V8Isolate,
    try_catch: V8TryCatch,
    ctx_scope: &V8ContextScope,
) -> GearsApiError {
    if try_catch.has_terminated() {
        isolate.cancel_terminate_execution();
        GearsApiError::new("Err Execution was terminated due to OOM or timeout")
    } else {
        let error_utf8 = try_catch.get_exception().to_utf8().unwrap();
        let trace_utf8 = try_catch.get_trace(ctx_scope).map(|v| v.to_utf8().unwrap());
        GearsApiError::new_verbose(
            error_utf8.as_str(),
            trace_utf8.as_ref().map(|v| v.as_str().replace('\n', "|")),
        )
    }
}

pub(crate) fn get_error_from_object(
    val: &V8LocalValue,
    ctx_scope: &V8ContextScope,
) -> GearsApiError {
    let msg = val.to_utf8().unwrap().as_str().to_string();
    let trace = if val.is_object() {
        let val = val.as_object();
        val.get_str_field(ctx_scope, "stack")
            .map(|v| v.to_utf8().unwrap().as_str().to_string())
    } else {
        None
    };
    GearsApiError::new_verbose(msg, trace.map(|v| v.replace('\n', "|")))
}

pub(crate) fn get_exception_v8_value<'isolate_scope, 'isolate>(
    isolate: &V8Isolate,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    try_catch: V8TryCatch<'isolate_scope, 'isolate>,
) -> V8LocalValue<'isolate_scope, 'isolate> {
    if try_catch.has_terminated() {
        isolate.cancel_terminate_execution();
        isolate_scope
            .new_string("Err Execution was terminated due to OOM or timeout")
            .to_value()
    } else {
        try_catch.get_exception()
    }
}

pub(crate) fn get_function_flags(
    curr_ctx_scope: &V8ContextScope,
    flags: &V8LocalArray,
) -> Result<FunctionFlags, String> {
    let mut flags_val = FunctionFlags::empty();
    for i in 0..flags.len() {
        let flag = flags.get(curr_ctx_scope, i);
        if !flag.is_string() {
            return Err("wrong type of string value".to_string());
        }
        let flag_str = flag.to_utf8().unwrap();
        match flag_str.as_str() {
            "no-writes" => flags_val.insert(FunctionFlags::NO_WRITES),
            "allow-oom" => flags_val.insert(FunctionFlags::ALLOW_OOM),
            "raw-arguments" => flags_val.insert(FunctionFlags::RAW_ARGUMENTS),
            _ => return Err(format!("Unknow flag '{}' was given", flag_str.as_str())),
        }
    }
    Ok(flags_val)
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn initialize_plugin(ctx: &Context) -> *mut dyn BackendCtxInterfaceUninitialised {
    init_api(ctx);
    Box::into_raw(Box::new(V8Backend {
        script_ctx_vec: Arc::new(Mutex::new(Vec::new())),
    }))
}
