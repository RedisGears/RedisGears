use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::CompiledLibraryInterface, load_library_ctx::LibraryCtxInterface,
    load_library_ctx::LoadLibraryCtxInterface, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_promise::V8PromiseState,
    v8_script::V8PersistedScript,
};

use redisgears_plugin_api::redisgears_plugin_api::RefCellWrapper;
use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use crate::get_exception_msg;

pub(crate) enum GilState {
    Lock,
    Unlock,
}

pub(crate) struct GilStateCtx {
    state: GilState,
    lock_time: SystemTime,
    lock_timed_out: bool,
}

impl GilStateCtx {
    fn new() -> GilStateCtx {
        GilStateCtx {
            state: GilState::Unlock,
            lock_time: SystemTime::now(),
            lock_timed_out: false,
        }
    }

    pub(crate) fn set_lock(&mut self) {
        self.state = GilState::Lock;
        self.lock_time = SystemTime::now();
    }

    pub(crate) fn set_unlock(&mut self) {
        self.state = GilState::Unlock;
        self.lock_timed_out = false;
    }

    pub(crate) fn is_locked(&self) -> bool {
        match self.state {
            GilState::Lock => true,
            GilState::Unlock => false,
        }
    }

    pub(crate) fn git_lock_duration_ms(&self) -> u128 {
        let duration = match SystemTime::now().duration_since(self.lock_time) {
            Ok(d) => d,
            Err(_) => return 0,
        };
        duration.as_millis()
    }

    pub(crate) fn set_lock_timedout(&mut self) {
        self.lock_timed_out = true;
    }

    pub(crate) fn is_lock_timedout(&self) -> bool {
        self.lock_timed_out
    }
}

pub(crate) struct V8ScriptCtx {
    pub(crate) script: V8PersistedScript,
    pub(crate) ctx: V8Context,
    pub(crate) isolate: V8Isolate,
    pub(crate) compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    pub(crate) is_running: AtomicBool,
    pub(crate) lock_state: RefCellWrapper<GilStateCtx>,
}

impl V8ScriptCtx {
    pub(crate) fn new(
        isolate: V8Isolate,
        ctx: V8Context,
        script: V8PersistedScript,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> V8ScriptCtx {
        V8ScriptCtx {
            isolate: isolate,
            ctx: ctx,
            script: script,
            compiled_library_api: compiled_library_api,
            is_running: AtomicBool::new(false),
            lock_state: RefCellWrapper {
                ref_cell: RefCell::new(GilStateCtx::new()),
            },
        }
    }

    pub(crate) fn before_run(&self) {
        self.is_running.store(true, Ordering::Relaxed);
    }

    pub(crate) fn after_run(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    pub(crate) fn after_lock_gil(&self) {
        self.lock_state.ref_cell.borrow_mut().set_lock();
    }

    pub(crate) fn before_release_gil(&self) {
        self.lock_state.ref_cell.borrow_mut().set_unlock();
    }

    pub(crate) fn is_gil_locked(&self) -> bool {
        self.lock_state.ref_cell.borrow().is_locked()
    }

    pub(crate) fn git_lock_duration_ms(&self) -> u128 {
        self.lock_state.ref_cell.borrow().git_lock_duration_ms()
    }

    pub(crate) fn set_lock_timedout(&self) {
        self.lock_state.ref_cell.borrow_mut().set_lock_timedout()
    }

    pub(crate) fn is_lock_timedout(&self) -> bool {
        self.lock_state.ref_cell.borrow().is_lock_timedout()
    }
}

pub(crate) struct V8LibraryCtx {
    pub(crate) script_ctx: Arc<V8ScriptCtx>,
}

impl LibraryCtxInterface for V8LibraryCtx {
    fn load_library(
        &self,
        load_library_ctx: &mut dyn LoadLibraryCtxInterface,
    ) -> Result<(), GearsApiError> {
        let _isolate_scope = self.script_ctx.isolate.enter();
        let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
        let ctx_scope = self.script_ctx.ctx.enter();
        let trycatch = self.script_ctx.isolate.new_try_catch();

        let script = self.script_ctx.script.to_local(&self.script_ctx.isolate);

        // set private content
        self.script_ctx
            .ctx
            .set_private_data(0, Some(&load_library_ctx));

        self.script_ctx.before_run();
        self.script_ctx.after_lock_gil();
        let res = script.run(&ctx_scope);
        self.script_ctx.before_release_gil();
        self.script_ctx.after_run();

        // reset private data
        self.script_ctx
            .ctx
            .set_private_data::<&mut dyn LoadLibraryCtxInterface>(0, None);

        if res.is_none() {
            let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
            return Err(GearsApiError::Msg(format!(
                "Failed evaluating module: {}",
                error_msg
            )));
        }
        let res = res.unwrap();
        if res.is_promise() {
            let promise = res.as_promise();
            if promise.state() == V8PromiseState::Rejected {
                let error = promise.get_result();
                let error_utf8 = error.to_utf8(&self.script_ctx.isolate).unwrap();
                return Err(GearsApiError::Msg(format!(
                    "Failed evaluating module: {}",
                    error_utf8.as_str()
                )));
            }
        }
        Ok(())
    }
}
