use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, backend_ctx::BackendCtxInterface,
    backend_ctx::CompiledLibraryInterface, backend_ctx::LibraryFatalFailurePolicy,
    load_library_ctx::LibraryCtxInterface, CallResult, GearsApiError,
};

use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init_with_error_handlers};

use crate::v8_native_functions::initialize_globals;

use crate::get_exception_msg;
use crate::v8_script_ctx::V8LibraryCtx;

use std::alloc::{GlobalAlloc, Layout, System};
use std::str;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};

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
    unsafe { (GLOBAL.backend_ctx.as_ref().unwrap().log)(msg) };
}

pub(crate) fn get_fatal_failure_policy() -> LibraryFatalFailurePolicy {
    unsafe { (GLOBAL.backend_ctx.as_ref().unwrap().get_on_oom_policy)() }
}

pub(crate) fn gil_lock_timeout() -> u128 {
    unsafe { (GLOBAL.backend_ctx.as_ref().unwrap().get_lock_timeout)() }
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

impl BackendCtxInterface for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn initialize(&self, backend_ctx: BackendCtx) -> Result<(), GearsApiError> {
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
                let msg = format!("v8 oom error on {}, is_heap_oom:{}", line, is_heap_oom);
                log(&msg);
                panic!("{}", msg);
            }),
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
                        let interrupt_script_ctx_clone = Weak::clone(&script_ctx_weak);
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

        Ok(())
    }

    fn compile_library(
        &mut self,
        blob: &str,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError> {
        let isolate = V8Isolate::new_with_limits(
            8 * 1024 * 1024, /* 8M */
            compiled_library_api.get_maxmemory(),
        );

        let script_ctx = {
            let (ctx, script) = {
                let isolate_scope = isolate.enter();
                let _handlers_scope = isolate.new_handlers_scope();

                let ctx = isolate_scope.new_context(None);
                let ctx_scope = ctx.enter();

                let v8code_str = isolate.new_string(blob);

                let trycatch = isolate.new_try_catch();
                let script = match ctx_scope.compile(&v8code_str) {
                    Some(s) => s,
                    None => {
                        let error_msg = get_exception_msg(&isolate, trycatch);
                        return Err(GearsApiError::Msg(format!(
                            "Failed compiling code, {}",
                            error_msg
                        )));
                    }
                };

                let script = script.persist(&isolate);
                (ctx, script)
            };
            let script_ctx = Arc::new(V8ScriptCtx::new(isolate, ctx, script, compiled_library_api));
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
                let _isolate_scope = script_ctx.isolate.enter();
                let _handlers_scope = script_ctx.isolate.new_handlers_scope();
                let ctx_scope = script_ctx.ctx.enter();
                let globals = ctx_scope.get_globals();

                let oom_script_ctx = Arc::downgrade(&script_ctx);

                script_ctx
                    .isolate
                    .set_near_oom_callback(move |curr_limit, initial_limit| {
                        let msg = format!(
                            "V8 near OOM notification arrive, curr_limit={}, initial_limit={}",
                            curr_limit, initial_limit
                        );
                        let script_ctx = match oom_script_ctx.upgrade() {
                            Some(s_c) => s_c,
                            None => {
                                log("V8 near OOM notification arrive after script was deleted");
                                log(&msg);
                                panic!("{}", msg);
                            }
                        };

                        script_ctx.compiled_library_api.log(&msg);

                        match get_fatal_failure_policy() {
                            LibraryFatalFailurePolicy::Kill => {
                                script_ctx.compiled_library_api.log("Fatal error policy do not allow to abort the script, server will be killed shortly.");
                                curr_limit as usize
                            }
                            LibraryFatalFailurePolicy::Abort => {
                                script_ctx.isolate.terminate_execution();

                                script_ctx
                                    .compiled_library_api
                                    .log("Temporarly increase max memory aborting the script");

                                (curr_limit as f64 * 1.2) as usize
                            }
                        }
                    });

                initialize_globals(&script_ctx, &globals, &ctx_scope);
            }

            script_ctx
        };

        Ok(Box::new(V8LibraryCtx {
            script_ctx: script_ctx,
        }))
    }

    fn debug(&mut self, args: &[&str]) -> Result<CallResult, GearsApiError> {
        let mut args = args.iter();
        let sub_command = args
            .next()
            .map_or(
                Err(GearsApiError::Msg(
                    "Subcommand was not provided".to_string(),
                )),
                |v| Ok(v),
            )?
            .to_lowercase();
        match sub_command.as_ref() {
            "isolates_stats" => {
                let l = self.script_ctx_vec.lock().unwrap();
                let active = l
                    .iter()
                    .filter(|v| v.strong_count() > 0)
                    .collect::<Vec<&Weak<V8ScriptCtx>>>()
                    .len() as i64;
                let not_active = l
                    .iter()
                    .filter(|v| v.strong_count() == 0)
                    .collect::<Vec<&Weak<V8ScriptCtx>>>()
                    .len() as i64;
                Ok(CallResult::Array(vec![
                    CallResult::BulkStr("active".to_string()),
                    CallResult::Long(active),
                    CallResult::BulkStr("not_active".to_string()),
                    CallResult::Long(not_active),
                ]))
            }
            "isolates_strong_count" => {
                let l = self.script_ctx_vec.lock().unwrap();
                let isolates_strong_count = l
                    .iter()
                    .map(|v| CallResult::Long(v.strong_count() as i64))
                    .collect::<Vec<CallResult>>();
                Ok(CallResult::Array(isolates_strong_count))
            }
            "isolates_gc" => {
                self.isolates_gc();
                Ok(CallResult::SimpleStr("OK".to_string()))
            }
            _ => Err(GearsApiError::Msg(format!(
                "Unknown subcommand '{}'",
                sub_command
            ))),
        }
    }
}
