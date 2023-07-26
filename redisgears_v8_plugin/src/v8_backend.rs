/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use bitflags::bitflags;
use redis_module::redisvalue::RedisValueKey;
use redis_module::RedisValue;
use redisgears_macros_internals::get_allow_deny_lists;
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtxInterfaceInitialised;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::{InfoSectionData, ModuleInfo};
use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, backend_ctx::BackendCtxInterfaceUninitialised,
    backend_ctx::CompiledLibraryInterface, backend_ctx::LibraryFatalFailurePolicy,
    load_library_ctx::LibraryCtxInterface, GearsApiError,
};
use v8_rs::v8::isolate_scope::GarbageCollectionJobType;
use v8_rs::v8::{v8_init_platform, v8_version};

use crate::v8_native_functions::{initialize_globals_for_version, ApiVersionSupported};
use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init_with_error_handlers};

use crate::get_exception_msg;
use crate::v8_redisai::get_tensor_object_template;
use crate::v8_script_ctx::V8LibraryCtx;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::str;

use std::sync::atomic::{AtomicBool, Ordering};
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

type ScriptCtxVec = Arc<Mutex<Vec<Weak<V8ScriptCtx>>>>;

bitflags! {
    #[derive(Clone, Copy, Debug)]
    pub struct GlobalOptions : u32 {
        /// If enable, avoid filtering globals by the globals allow list.
        const AVOID_GLOBALS_ALLOW_LIST = 0b00000001;
    }
}

struct Globals {
    backend_ctx: Option<BackendCtx>,
    bypassed_memory_limit: Option<AtomicBool>,
    script_ctx_vec: Option<ScriptCtxVec>,
    global_options: GlobalOptions,
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
static mut GLOBAL: Globals = Globals {
    backend_ctx: None,
    bypassed_memory_limit: None,
    script_ctx_vec: None,
    global_options: GlobalOptions::empty(),
};

/// Log a generic info message which are not related to
/// a specific library.
/// Notice that logging messages which are library related
/// should be done using `CompiledLibraryInterface`
pub(crate) fn log_info(msg: &str) {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().log_info)(msg)
    };
    #[cfg(test)]
    println!("log message: {msg}");
}

/// Log a generic warning message which are not related to
/// a specific library.
/// Notice that logging messages which are library related
/// should be done using `CompiledLibraryInterface`
pub(crate) fn log_warning(msg: &str) {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().log_warning)(msg)
    };
    #[cfg(test)]
    println!("log message: {msg}");
}

/// Log a generic error message which are not related to
/// a specific library.
/// Notice that logging messages which are library related
/// should be done using `CompiledLibraryInterface`
pub(crate) fn log_error(msg: &str) {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().log_error)(msg)
    };
    #[cfg(test)]
    println!("log message: {msg}");
}

/// Return the fatal failure policy configuration value.
pub(crate) fn get_fatal_failure_policy() -> LibraryFatalFailurePolicy {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_on_oom_policy)()
    }
    #[cfg(test)]
    LibraryFatalFailurePolicy::Abort
}

/// Return the timeout for a script being loaded from an RDB.
pub(crate) fn gil_rdb_lock_timeout() -> u128 {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_rdb_lock_timeout)()
    }
    #[cfg(test)]
    0u128
}

/// Return the timeout for which a library is allowed to
/// take the Redis GIL. Once this timeout reached it is
/// considered a fatal failure.
pub(crate) fn gil_lock_timeout() -> u128 {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_lock_timeout)()
    }
    #[cfg(test)]
    0u128
}

/// Return the total memory limit that is allowed
/// to be used by all isolate combined.
/// Bypass this limit is considered a fatal failure.
pub(crate) fn max_memory_limit() -> usize {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL.backend_ctx.as_ref().unwrap().get_v8_maxmemory)()
    }
    #[cfg(test)]
    0usize
}

/// Return the initial memory usage to set for isolates.
pub(crate) fn initial_memory_usage() -> usize {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL
            .backend_ctx
            .as_ref()
            .unwrap()
            .get_v8_library_initial_memory)()
    }
    #[cfg(test)]
    0usize
}

/// Return the initial memory limit to set for isolates.
pub(crate) fn initial_memory_limit() -> usize {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL
            .backend_ctx
            .as_ref()
            .unwrap()
            .get_v8_library_initial_memory_limit)()
    }
    #[cfg(test)]
    0usize
}

/// Set the given globals option.
pub(crate) fn set_global_option(global_option: GlobalOptions) {
    unsafe { &mut GLOBAL }.global_options |= global_option;
}

/// Get the given globals option.
pub(crate) fn get_global_option() -> GlobalOptions {
    unsafe { &GLOBAL }.global_options
}

/// Get the given globals option.
pub(crate) fn get_v8_flags() -> String {
    unsafe { &GLOBAL }
        .backend_ctx
        .as_ref()
        .map_or_else(|| "".to_owned(), |v| (v.get_v8_flags)())
}

/// Return the delta by which we should increase
/// an isolate memory limit, as long as the max
/// memory did not yet reached.
pub(crate) fn memory_delta() -> usize {
    #[cfg(not(test))]
    unsafe {
        (GLOBAL
            .backend_ctx
            .as_ref()
            .unwrap()
            .get_v8_library_memory_delta)()
    }
    #[cfg(test)]
    0usize
}

/// Scan all active isolate and return to total heap memory usage.
pub(crate) fn calc_isolates_used_memory() -> usize {
    let script_ctxs = unsafe { GLOBAL.script_ctx_vec.as_ref().unwrap() };
    script_ctxs.lock().unwrap().iter().fold(0, |agg, v| {
        agg + v.upgrade().map_or(0, |v| v.isolate.total_heap_size())
    })
}

/// Return `true` if we bypass the memory limit otherwise `false`.
/// In case we bypass the memory limit. Check the current memory usage
/// in case GC cleaned some memory.
pub(crate) fn bypass_memory_limit() -> bool {
    let bypassed_memory_limit =
        unsafe { GLOBAL.bypassed_memory_limit.as_ref().unwrap() }.load(Ordering::Relaxed);
    if !bypassed_memory_limit {
        return false;
    }
    if calc_isolates_used_memory() >= max_memory_limit() {
        return true;
    }
    unsafe { GLOBAL.bypassed_memory_limit.as_ref().unwrap() }.store(false, Ordering::Relaxed);
    false
}

pub(crate) struct V8Backend {
    pub(crate) script_ctx_vec: ScriptCtxVec,
}

fn scan_for_isolates_timeout(script_ctx_vec: &ScriptCtxVec) {
    let l: std::sync::MutexGuard<Vec<Weak<V8ScriptCtx>>> = script_ctx_vec.lock().unwrap();
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
                    let gil_lock_duration = script_ctx.gil_lock_duration_ms();
                    let gil_lock_configured_timeout = if script_ctx.is_being_loaded_from_rdb() {
                        gil_rdb_lock_timeout()
                    } else {
                        gil_lock_timeout()
                    };
                    if gil_lock_duration > gil_lock_configured_timeout {
                        script_ctx.set_lock_timedout();
                        script_ctx.compiled_library_api.log_warning(&format!("Script locks Redis for about {}ms which is more then the configured timeout {}ms.", gil_lock_duration, gil_lock_configured_timeout));
                        match get_fatal_failure_policy() {
                            LibraryFatalFailurePolicy::Kill => {
                                script_ctx.compiled_library_api.log_warning("Fatal error policy do not allow to abort the script, we will allow the script to continue running, best effort approach.");
                            }
                            LibraryFatalFailurePolicy::Abort => {
                                script_ctx.compiled_library_api.log_warning("Aborting script with timeout error.");
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

fn check_isolates_memory_limit(
    script_ctx_vec: &ScriptCtxVec,
    detected_memory_pressure: bool,
) -> bool {
    if bypass_memory_limit() {
        if !detected_memory_pressure {
            log_warning("Detects OOM state on the JS engine, will send memory pressure notification to all libraries.");
        }
        script_ctx_vec
            .lock()
            .unwrap()
            .iter()
            .filter_map(|v| v.upgrade())
            .for_each(|v| v.isolate.memory_pressure_notification());
        true
    } else {
        if detected_memory_pressure {
            log_info("Exit OOM state, JS memory usage dropped bellow the max memory limit.");
        }
        false
    }
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

    fn initialize_v8_engine(&self) -> Result<(), GearsApiError> {
        v8_init_with_error_handlers(
            Box::new(|line, msg| {
                let msg = format!("v8 fatal error on {}, {}", line, msg);
                log_error(&msg);
                panic!("{}", msg);
            }),
            Box::new(|line, is_heap_oom| {
                let isolate = V8Isolate::current_isolate();
                let msg = format!("v8 oom error on {}, is_heap_oom:{}", line, is_heap_oom);
                log_error(&msg);
                if let Some(i) = isolate {
                    log_error(&format!(
                        "used_heap_size={}, total_heap_size={}",
                        i.used_heap_size(),
                        i.total_heap_size()
                    ));
                }
                panic!("{}", msg);
            }),
        )
        .map_err(GearsApiError::new)
    }

    fn spawn_background_maintenance_thread(&self) -> Result<(), GearsApiError> {
        let script_ctxs = Arc::clone(&self.script_ctx_vec);
        std::thread::Builder::new()
            .name("v8maintenance".to_string())
            .spawn(move || {
                let mut detected_memory_pressure = false;
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    scan_for_isolates_timeout(&script_ctxs);
                    detected_memory_pressure =
                        check_isolates_memory_limit(&script_ctxs, detected_memory_pressure);
                }
            })
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        Ok(())
    }
}

impl BackendCtxInterfaceUninitialised for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn on_load(&self, backend_ctx: BackendCtx) -> Result<(), GearsApiError> {
        unsafe {
            GLOBAL.backend_ctx = Some(backend_ctx);
            GLOBAL.bypassed_memory_limit = Some(AtomicBool::new(false));
            GLOBAL.script_ctx_vec = Some(Arc::clone(&self.script_ctx_vec));
        }

        std::panic::set_hook(Box::new(|panic_info| {
            log_error(&format!("Application panicked, {}", panic_info));
            let (file, line) = match panic_info.location() {
                Some(l) => (l.file(), l.line()),
                None => ("", 0),
            };
            let file = std::ffi::CString::new(file).unwrap();
            unsafe {
                redis_module::raw::RedisModule__Assert.unwrap()(
                    "Crashed on panic\0".as_ptr() as *const std::os::raw::c_char,
                    file.as_ptr(),
                    line as i32,
                );
            }
        }));

        let flags = get_v8_flags();
        let flags = if flags.starts_with('\'') && flags.len() > 1 {
            &flags[1..flags.len() - 1]
        } else {
            &flags
        };

        v8_init_platform(1, Some(flags)).map_err(GearsApiError::new)
    }

    fn initialize(
        self: Box<Self>,
    ) -> Result<Box<dyn BackendCtxInterfaceInitialised>, GearsApiError> {
        self.initialize_v8_engine()?;
        self.spawn_background_maintenance_thread()?;

        Ok(self)
    }
}

impl BackendCtxInterfaceInitialised for V8Backend {
    fn get_version(&self) -> String {
        format!(
            "Version: {}, v8-rs: {}, profile:{}",
            v8_version(),
            v8_rs::GIT_SEMVER,
            v8_rs::PROFILE
        )
    }

    fn compile_library(
        &mut self,
        module_name: &str,
        code: &str,
        api_version: ApiVersion,
        config: Option<&String>,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError> {
        if calc_isolates_used_memory() >= max_memory_limit() {
            return Err(GearsApiError::new(
                "JS engine reached OOM state and can not run any more code",
            ));
        }

        let isolate = V8Isolate::new_with_limits(initial_memory_usage(), initial_memory_limit());

        let script_ctx = {
            let (ctx, script, tensor_obj_template) = {
                let isolate_scope = isolate.enter();
                let ctx = isolate_scope.new_context(None);
                let ctx_scope = ctx.enter(&isolate_scope);

                let globals = ctx_scope.get_globals();
                if !(get_global_option().contains(GlobalOptions::AVOID_GLOBALS_ALLOW_LIST)) {
                    let propeties = globals.get_own_property_names(&ctx_scope);
                    propeties.iter(&ctx_scope).try_for_each(|v| {
                        let s = v.to_utf8().ok_or(GearsApiError::new("Failed converting global property name to string"))?;
                        if !allow_list().contains(s.as_str()) {
                            if !deny_list().contains(s.as_str()) {
                                compiled_library_api.log_warning(&format!(
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
                }

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
                module_name.to_owned(),
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
                            "V8 near OOM notification arrive, curr_limit={curr_limit}, initial_limit={initial_limit}",
                        );
                        let script_ctx = match oom_script_ctx.upgrade() {
                            Some(s_c) => s_c,
                            None => {
                                log_warning("V8 near OOM notification arrive after script was deleted");
                                log_warning(&msg);
                                panic!("{}", msg);
                            }
                        };

                        let msg = format!("{msg}, library={}, used_heap_size={}, total_heap_size={}", script_ctx.name, script_ctx.isolate.used_heap_size(), script_ctx.isolate.total_heap_size());
                        let memory_delta = memory_delta();
                        let new_isolate_limit = usize::max(script_ctx.isolate.total_heap_size(), curr_limit) + memory_delta;

                        if calc_isolates_used_memory() + memory_delta >= max_memory_limit() {
                            unsafe { GLOBAL.bypassed_memory_limit.as_ref().unwrap() }.store(true, Ordering::Relaxed);
                            script_ctx.compiled_library_api.log_warning(&msg);
                            // we are going to bypass the total memory limit, lets try to abort.
                            match get_fatal_failure_policy() {
                                LibraryFatalFailurePolicy::Kill => {
                                    script_ctx.compiled_library_api.log_warning("Fatal error policy do not allow to abort the script, server will be killed shortly.");
                                }
                                LibraryFatalFailurePolicy::Abort => {
                                    script_ctx.isolate.request_interrupt(|isolate| {
                                        isolate.memory_pressure_notification();
                                    });
                                    script_ctx.isolate.terminate_execution();
                                    script_ctx
                                        .compiled_library_api
                                        .log_warning(&format!("Temporarily increasing max memory to {new_isolate_limit} and aborting the script"));
                                }
                            }
                        } else {
                            script_ctx.compiled_library_api.log_trace(&msg);
                            script_ctx.compiled_library_api.log_trace(&format!("Increasing max memory to {new_isolate_limit}"));
                        }
                        new_isolate_limit
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
                            .log_info(&format!("Module \"{module_name}\": {message}"))
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
            "isolates_aggregated_stats" => {
                let (active, not_active) = {
                    let l = self.script_ctx_vec.lock().unwrap();
                    let active = l.iter().filter(|v| v.strong_count() > 0).count() as i64;
                    let not_active = l.iter().filter(|v| v.strong_count() == 0).count() as i64;
                    (active, not_active)
                };
                Ok(RedisValue::Array(vec![
                    RedisValue::BulkString("active".to_string()),
                    RedisValue::Integer(active),
                    RedisValue::BulkString("not_active".to_string()),
                    RedisValue::Integer(not_active),
                    RedisValue::BulkString("combined_memory_limit".to_string()),
                    RedisValue::Integer(calc_isolates_used_memory() as i64),
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
            "request_v8_gc_for_debugging" => {
                let l = self.script_ctx_vec.lock().unwrap();
                l.iter().filter_map(|v| v.upgrade()).for_each(|v| {
                    let isolate_scope = v.isolate.enter();
                    isolate_scope.request_gc_for_testing(GarbageCollectionJobType::Full);
                });
                Ok(RedisValue::SimpleString("OK".to_string()))
            }
            "avoid_global_allow_list" => {
                set_global_option(GlobalOptions::AVOID_GLOBALS_ALLOW_LIST);
                Ok(RedisValue::SimpleString("OK".to_string()))
            }
            "isolates_stats" => {
                let l = self.script_ctx_vec.lock().unwrap();
                let isolates_strong_count = l
                    .iter()
                    .filter_map(|v| v.upgrade())
                    .map(|v| {
                        (
                            RedisValueKey::String(v.name.clone()),
                            RedisValue::Map(HashMap::from([
                                (
                                    RedisValueKey::String("total_heap_size".to_owned()),
                                    RedisValue::Integer(v.isolate.total_heap_size() as i64),
                                ),
                                (
                                    RedisValueKey::String("used_heap_size".to_owned()),
                                    RedisValue::Integer(v.isolate.used_heap_size() as i64),
                                ),
                                (
                                    RedisValueKey::String("heap_size_limit".to_owned()),
                                    RedisValue::Integer(v.isolate.heap_size_limit() as i64),
                                ),
                            ])),
                        )
                    })
                    .collect();
                Ok(RedisValue::Map(isolates_strong_count))
            }
            _ => Err(GearsApiError::new(format!(
                "Unknown subcommand '{sub_command}'",
            ))),
        }
    }

    fn get_info(&mut self) -> Option<ModuleInfo> {
        let sections = {
            let aggregated_libraries_stats = InfoSectionData::KeyValuePairs({
                let (active, not_active) = {
                    let l = self.script_ctx_vec.lock().unwrap();
                    let active = l.iter().filter(|v| v.strong_count() > 0).count() as i64;
                    let not_active = l.iter().filter(|v| v.strong_count() == 0).count() as i64;
                    (active, not_active)
                };

                let mut data = HashMap::new();
                data.insert("active".to_owned(), active.to_string());
                data.insert("not_active".to_owned(), not_active.to_string());
                data.insert(
                    "combined_memory_limit".to_owned(),
                    calc_isolates_used_memory().to_string(),
                );

                {
                    let l = self.script_ctx_vec.lock().unwrap();
                    let (total_heap_size, used_heap_size) = l
                        .iter()
                        .filter_map(|v| v.upgrade())
                        .fold((0, 0), |mut acc, v| {
                            acc.0 += v.isolate.total_heap_size();
                            acc.1 += v.isolate.used_heap_size();
                            acc
                        });

                    data.insert("total_heap_size".to_owned(), total_heap_size.to_string());
                    data.insert("used_heap_size".to_owned(), used_heap_size.to_string());
                }

                data
            });

            let mut sections = HashMap::new();
            sections.insert(
                "V8AggregatedLibraryStatistics".to_owned(),
                aggregated_libraries_stats,
            );
            sections
        };
        Some(ModuleInfo { sections })
    }
}
