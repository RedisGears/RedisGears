use v8_rs::v8::{
    isolate::V8Isolate, isolate_scope::V8IsolateScope, try_catch::V8TryCatch,
    v8_array::V8LocalArray, v8_context_scope::V8ContextScope, v8_value::V8LocalValue,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtxInterface, load_library_ctx::FUNCTION_FLAG_ALLOW_OOM,
    load_library_ctx::FUNCTION_FLAG_NO_WRITES, load_library_ctx::FUNCTION_FLAG_RAW_ARGUMENTS,
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

pub(crate) fn get_exception_msg(isolate: &V8Isolate, try_catch: V8TryCatch) -> String {
    if try_catch.has_terminated() {
        isolate.cancel_terminate_execution();
        "Err Execution was terminated due to OOM or timeout".to_string()
    } else {
        let error_utf8 = try_catch.get_exception().to_utf8().unwrap();
        error_utf8.as_str().to_string()
    }
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
) -> Result<u8, String> {
    let mut flags_val = 0;
    for i in 0..flags.len() {
        let flag = flags.get(curr_ctx_scope, i);
        if !flag.is_string() {
            return Err("wrong type of string value".to_string());
        }
        let flag_str = flag.to_utf8().unwrap();
        match flag_str.as_str() {
            "no-writes" => flags_val |= FUNCTION_FLAG_NO_WRITES,
            "allow-oom" => flags_val |= FUNCTION_FLAG_ALLOW_OOM,
            "raw-arguments" => flags_val |= FUNCTION_FLAG_RAW_ARGUMENTS,
            _ => return Err(format!("Unknow flag '{}' was given", flag_str.as_str())),
        }
    }
    Ok(flags_val)
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn initialize_plugin() -> *mut dyn BackendCtxInterface {
    Box::into_raw(Box::new(V8Backend {
        script_ctx_vec: Arc::new(Mutex::new(Vec::new())),
    }))
}
