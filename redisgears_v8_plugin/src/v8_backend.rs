/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::RedisValue;
use redisgears_macros_internals::get_allow_deny_lists;
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtxInterfaceInitialised;
use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, backend_ctx::BackendCtxInterfaceUninitialised,
    backend_ctx::CompiledLibraryInterface, backend_ctx::LibraryFatalFailurePolicy,
    load_library_ctx::LibraryCtxInterface, GearsApiError,
};
use v8_rs::v8::v8_version;

use crate::v8_native_functions::{initialize_globals_for_version, ApiVersionSupported};
use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init_with_error_handlers};

use crate::get_exception_msg;
use crate::v8_redisai::get_tensor_object_template;
use crate::v8_script_ctx::V8LibraryCtx;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::str;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
lazy_static::lazy_static! {
    static ref GLOBALS_ALLOW_DENY_LISTS: (HashSet<String>, HashSet<String>) = get_allow_deny_lists!({
        allow_list: [
            "Object",
            "Function",
            "Array",
            "Number",
            "parseFloat",
            "parseInt",
            "Infinity",
            "NaN",
            "undefined",
            "Boolean",
            "String",
            "Symbol",
            "Date",
            "Promise",
            "RegExp",
            "Error",
            "AggregateError",
            "RangeError",
            "ReferenceError",
            "SyntaxError",
            "TypeError",
            "URIError",
            "globalThis",
            "JSON",
            "Math",
            "Intl",
            "ArrayBuffer",
            "Uint8Array",
            "Int8Array",
            "Uint16Array",
            "Int16Array",
            "Uint32Array",
            "Int32Array",
            "Float32Array",
            "Float64Array",
            "Uint8ClampedArray",
            "BigUint64Array",
            "BigInt64Array",
            "DataView",
            "Map",
            "BigInt",
            "Set",
            "WeakMap",
            "WeakSet",
            "Proxy",
            "Reflect",
            "FinalizationRegistry",
            "WeakRef",
            "decodeURI",
            "decodeURIComponent",
            "encodeURI",
            "encodeURIComponent",
            "escape",
            "unescape",
            "isFinite",
            "isNaN",
            "console",
            "WebAssembly",
        ],
        deny_list: [
            "eval",              // Might be considered dangerous.
            "EvalError",         // Because we remove eval, this one is also not needed.
            "SharedArrayBuffer", // Needed for workers which we are not supporting
            "Atomics",           // Needed for workers which we are not supporting
        ]
    });
}

fn allow_list() -> &'static HashSet<String> {
    &GLOBALS_ALLOW_DENY_LISTS.0
}

fn deny_list() -> &'static HashSet<String> {
    &GLOBALS_ALLOW_DENY_LISTS.1
}

struct Globals {
    backend_ctx: Option<BackendCtx>,
}

unsafe impl GlobalAlloc for Globals {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        match self.backend_ctx.as_ref() {
            Some(a) => a.allocator.alloc(layout),
            None => System.alloc(layout),
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match self.backend_ctx.as_ref() {
            Some(a) => a.allocator.dealloc(ptr, layout),
            None => System.dealloc(ptr, layout),
        }
    }
}

#[global_allocator]
static mut GLOBAL: Globals = Globals { backend_ctx: None };

pub(crate) fn log(msg: &str) {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().log)(msg)
    };
    #[cfg(test)]
    println!("log message: {msg}");
}

pub(crate) fn get_fatal_failure_policy() -> LibraryFatalFailurePolicy {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_on_oom_policy)()
    }
    #[cfg(test)]
    LibraryFatalFailurePolicy::Abort
}

pub(crate) fn gil_lock_timeout() -> u128 {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_lock_timeout)()
    }
    #[cfg(test)]
    0u128
}

pub(crate) struct V8Backend {
    pub(crate) script_ctx_vec: Arc<Mutex<Vec<Weak<V8ScriptCtx>>>>,
}

impl V8Backend {
    fn isolates_gc(&mut self) {
        let mut l = self.script_ctx_vec.lock().unwrap();
        let indexes = l
            .iter()
            .enumerate()
            .filter(|(_i, v)| v.strong_count() == 0)
            .map(|(i, _v)| i)
            .collect::<Vec<usize>>();
        for i in indexes.iter().rev() {
            l.swap_remove(*i);
        }
    }
}

impl BackendCtxInterfaceUninitialised for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn initialize(
        self: Box<Self>,
        backend_ctx: BackendCtx,
    ) -> Result<Box<dyn BackendCtxInterfaceInitialised>, GearsApiError> {
        unsafe {
            GLOBAL.backend_ctx = Some(backend_ctx);
        }
        v8_init_with_error_handlers(
            Box::new(|line, msg| {
                let msg = format!("v8 fatal error on {}, {}", line, msg);
                log(&msg);
                panic!("{}", msg);
            }),
            Box::new(|line, is_heap_oom| {
                let isolate = V8Isolate::current_isolate();
                let msg = format!("v8 oom error on {}, is_heap_oom:{}", line, is_heap_oom);
                log(&msg);
                if let Some(i) = isolate {
                    log(&format!(
                        "used_heap_size={}, total_heap_size={}",
                        i.used_heap_size(),
                        i.total_heap_size()
                    ));
                }
                panic!("{}", msg);
            }),
            1,
        );

        let script_ctxs = Arc::clone(&self.script_ctx_vec);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let l = script_ctxs.lock().unwrap();
                for script_ctx_weak in l.iter() {
                    let script_ctx = match script_ctx_weak.upgrade() {
                        Some(s) => s,
                        None => continue,
                    };
                    if script_ctx
                        .is_running
                        .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        let interrupt_script_ctx_clone = Weak::clone(script_ctx_weak);
                        script_ctx.isolate.request_interrupt(move|isolate|{
                            let script_ctx = match interrupt_script_ctx_clone.upgrade() {
                                Some(s) => s,
                                None => return,
                            };
                            if script_ctx.is_gil_locked() && !script_ctx.is_lock_timedout() {
                                // gil is current locked. we should check for timeout.
                                // todo: call Redis back to reply to pings and some other commands.
                                let gil_lock_duration = script_ctx.git_lock_duration_ms();
                                let gil_lock_configured_timeout = gil_lock_timeout();
                                if gil_lock_duration > gil_lock_configured_timeout {
                                    script_ctx.set_lock_timedout();
                                    script_ctx.compiled_library_api.log(&format!("Script locks Redis for about {}ms which is more then the configured timeout {}ms.", gil_lock_duration, gil_lock_configured_timeout));
                                    match get_fatal_failure_policy() {
                                        LibraryFatalFailurePolicy::Kill => {
                                            script_ctx.compiled_library_api.log("Fatal error policy do not allow to abort the script, we will allow the script to continue running, best effort approach.");
                                        }
                                        LibraryFatalFailurePolicy::Abort => {
                                            script_ctx.compiled_library_api.log("Aborting script with timeout error.");
                                            isolate.terminate_execution();
                                        }
                                    }
                                }
                            }
                            script_ctx.before_run();
                        });
                    }
                }
            }
        });

        Ok(self)
    }
}

impl BackendCtxInterfaceInitialised for V8Backend {
    fn get_version(&self) -> String {
        format!("Version: {}, v8-rs: {}", v8_version(), v8_rs::GIT_SEMVER)
    }

    fn compile_library(
        &mut self,
        module_name: &str,
        code: &str,
        api_version: ApiVersion,
        config: Option<&String>,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError> {
        let isolate = V8Isolate::new_with_limits(
            8 * 1024 * 1024, /* 8M */
            compiled_library_api.get_maxmemory(),
        );

        let script_ctx = {
            let (ctx, script, tensor_obj_template) = {
                let isolate_scope = isolate.enter();
                let ctx = isolate_scope.new_context(None);
                let ctx_scope = ctx.enter(&isolate_scope);

                let globals = ctx_scope.get_globals();
                let propeties = globals.get_own_property_names(&ctx_scope);
                propeties.iter(&ctx_scope).try_for_each(|v| {
                    let s = v.to_utf8().ok_or(GearsApiError::new("Failed converting global property name to string"))?;
                    if !allow_list().contains(s.as_str()) {
                        if !deny_list().contains(s.as_str()) {
                            compiled_library_api.log(&format!(
                                "Found global '{}' which is not on the allowed list nor on the deny list.",
                                s.as_str()
                            ));
                        }
                        // property does not exists on the allow list. lets drop it.
                        if !globals.delete(&ctx_scope, &v) {
                            return Err(GearsApiError::new(format!("Failed deleting global '{}' which is not on the allowed list, can not load the library.", s.as_str())));
                        }
                    }
                    Ok(())
                })?;

                let v8code_str = isolate_scope.new_string(code);

                let trycatch = isolate_scope.new_try_catch();
                let script = match ctx_scope.compile(&v8code_str) {
                    Some(s) => s,
                    None => {
                        return Err(get_exception_msg(&isolate, trycatch, &ctx_scope));
                    }
                };

                let script = script.persist();
                let tensor_obj_template = get_tensor_object_template(&isolate_scope);
                (ctx, script, tensor_obj_template)
            };
            let script_ctx = Arc::new(V8ScriptCtx::new(
                isolate,
                ctx,
                script,
                tensor_obj_template,
                compiled_library_api,
            ));
            let len = {
                let mut l = self.script_ctx_vec.lock().unwrap();
                l.push(Arc::downgrade(&script_ctx));
                l.len()
            };
            if len > 100 {
                // let try to do some gc
                self.isolates_gc();
            }
            {
                let isolate_scope = script_ctx.isolate.enter();
                let ctx_scope = script_ctx.ctx.enter(&isolate_scope);
                let globals = ctx_scope.get_globals();

                let oom_script_ctx = Arc::downgrade(&script_ctx);

                script_ctx
                    .isolate
                    .set_near_oom_callback(move |curr_limit, initial_limit| {
                        let msg = format!(
                            "V8 near OOM notification arrive, curr_limit={curr_limit}, initial_limit={initial_limit}"
                        );
                        let script_ctx = match oom_script_ctx.upgrade() {
                            Some(s_c) => s_c,
                            None => {
                                log("V8 near OOM notification arrive after script was deleted");
                                log(&msg);
                                panic!("{}", msg);
                            }
                        };

                        let msg = format!("{msg}, used_heap_size={}, total_heap_size={}", script_ctx.isolate.used_heap_size(), script_ctx.isolate.total_heap_size());

                        script_ctx.compiled_library_api.log(&msg);

                        match get_fatal_failure_policy() {
                            LibraryFatalFailurePolicy::Kill => {
                                script_ctx.compiled_library_api.log("Fatal error policy do not allow to abort the script, server will be killed shortly.");
                                curr_limit
                            }
                            LibraryFatalFailurePolicy::Abort => {
                                let mut new_limit: usize = (curr_limit as f64 * 1.2 ) as usize;
                                if new_limit < script_ctx.isolate.total_heap_size() {
                                    new_limit = (script_ctx.isolate.total_heap_size() as f64 * 1.2) as usize;
                                }
                                script_ctx.isolate.request_interrupt(|isolate| {
                                    isolate.memory_pressure_notification();
                                });
                                script_ctx.isolate.terminate_execution();

                                script_ctx
                                    .compiled_library_api
                                    .log(&format!("Temporarily increasing max memory to {new_limit} memory and aborting the script"));

                                new_limit
                            }
                        }
                    });

                let api_version_supported: ApiVersionSupported = api_version.try_into()?;

                api_version_supported
                    .validate_code(code)
                    .iter()
                    .enumerate()
                    .map(|(index, error)| format!("\t{}. {}", index + 1, error.get_msg()))
                    .for_each(|message| {
                        script_ctx
                            .compiled_library_api
                            .log(&format!("Module \"{module_name}\": {message}"))
                    });

                let api_version_supported = api_version_supported.into_latest_compatible();
                initialize_globals_for_version(
                    api_version_supported,
                    &script_ctx,
                    &globals,
                    &isolate_scope,
                    &ctx_scope,
                    config,
                )?;
            }

            script_ctx
        };

        Ok(Box::new(V8LibraryCtx { script_ctx }))
    }

    fn debug(&mut self, args: &[&str]) -> Result<RedisValue, GearsApiError> {
        let mut args = args.iter();
        let sub_command = args
            .next()
            .map_or(Err(GearsApiError::new("Subcommand was not provided")), Ok)?
            .to_lowercase();
        match sub_command.as_ref() {
            "help" => Ok(RedisValue::Array(vec![
                RedisValue::BulkString("isolates_stats - statistics about isolates.".to_string()),
                RedisValue::BulkString(
                    "isolates_strong_count - For each isolate returns its strong ref count value."
                        .to_string(),
                ),
                RedisValue::BulkString(
                    "isolates_gc - Runs GC to clear none active isolates.".to_string(),
                ),
                RedisValue::BulkString("help - Print this message.".to_string()),
            ])),
            "isolates_stats" => {
                let l = self.script_ctx_vec.lock().unwrap();
                let active = l.iter().filter(|v| v.strong_count() > 0).count() as i64;
                let not_active = l.iter().filter(|v| v.strong_count() == 0).count() as i64;
                Ok(RedisValue::Array(vec![
                    RedisValue::BulkString("active".to_string()),
                    RedisValue::Integer(active),
                    RedisValue::BulkString("not_active".to_string()),
                    RedisValue::Integer(not_active),
                ]))
            }
            "isolates_strong_count" => {
                let l = self.script_ctx_vec.lock().unwrap();
                let isolates_strong_count = l
                    .iter()
                    .map(|v| RedisValue::Integer(v.strong_count() as i64))
                    .collect::<Vec<RedisValue>>();
                Ok(RedisValue::Array(isolates_strong_count))
            }
            "isolates_gc" => {
                self.isolates_gc();
                Ok(RedisValue::SimpleString("OK".to_string()))
            }
            _ => Err(GearsApiError::new(format!(
                "Unknown subcommand '{sub_command}'",
            ))),
        }
    }
}
