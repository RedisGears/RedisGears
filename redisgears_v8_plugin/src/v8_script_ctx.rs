/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::{InfoSectionData, ModuleInfo};
use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::CompiledLibraryInterface, load_library_ctx::LibraryCtxInterface,
    load_library_ctx::LoadLibraryCtxInterface, GearsApiError,
};

use v8_derive::new_native_function;
use v8_rs::v8::isolate_scope::V8IsolateScope;
use v8_rs::v8::v8_context_scope::V8ContextScope;
use v8_rs::v8::v8_promise::V8LocalPromise;
use v8_rs::v8::v8_resolver::V8LocalResolver;
use v8_rs::v8::v8_script::V8LocalScript;
use v8_rs::v8::v8_value::V8LocalValue;
use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_object_template::V8PersistedObjectTemplate,
    v8_promise::V8PromiseState, v8_script::V8PersistedScript,
};

use redisgears_plugin_api::redisgears_plugin_api::RefCellWrapper;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use crate::{get_error_from_object, get_exception_msg};

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
    fn new() -> Self {
        Self {
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
        SystemTime::now()
            .duration_since(self.lock_time)
            .unwrap_or_default()
            .as_millis()
    }

    pub(crate) fn set_lock_timedout(&mut self) {
        self.lock_timed_out = true;
    }

    pub(crate) fn is_lock_timedout(&self) -> bool {
        self.lock_timed_out
    }
}

pub(crate) enum GilStatus {
    Locked,
    Unlocked,
}

impl GilStatus {
    pub(crate) fn is_locked(&self) -> bool {
        matches!(self, Self::Locked)
    }
}

/// A struct, objects of which hold a reference to the parent
/// [`V8ScriptCtx`], which is designated as the one being loaded from
/// RDB at the time the object of this struct is alive.
#[repr(transparent)]
struct RdbLoadingGuard<'a>(&'a V8ScriptCtx);
impl<'a> RdbLoadingGuard<'a> {
    fn new(is_loading_rdb: bool, script_ctx: &'a V8ScriptCtx) -> Self {
        script_ctx
            .is_being_loaded_from_rdb
            .store(is_loading_rdb, Ordering::SeqCst);
        Self(script_ctx)
    }
}
impl<'a> Drop for RdbLoadingGuard<'a> {
    fn drop(&mut self) {
        self.0
            .is_being_loaded_from_rdb
            .store(false, Ordering::Release);
    }
}

pub(crate) struct V8ScriptCtx {
    /// The name of the library.
    pub(crate) name: String,

    /// The initialisation script to run.
    pub(crate) script: V8PersistedScript,

    /// Tensors API for RedisAI integrations.
    pub(crate) tensor_object_template: V8PersistedObjectTemplate,

    /// The V8 context
    pub(crate) ctx: V8Context,

    /// The V8 isolate
    pub(crate) isolate: V8Isolate,

    /// Api to interact back with Redis for operations like command invocation and logging.
    pub(crate) compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,

    /// A boolean value to determine if we are presently executing JavaScript code or not.
    /// This boolean is employed to ascertain whether we should prompt for JavaScript interruption
    /// and conduct timeout checks.
    pub(crate) is_running: AtomicBool,

    /// If set to [`true`], then the script is currently being loaded,
    /// or evaluated for the first time, from an RDB.
    pub(crate) is_being_loaded_from_rdb: AtomicBool,

    /// Signifies the present locking status of the running JavaScript code,
    /// enabling us to distinguish between background JS code execution and JS code that holds a lock on Redis.
    pub(crate) lock_state: RefCellWrapper<GilStateCtx>,
}

pub(crate) struct OnDoneCtx<'isolate_scope, 'isolate, 'ctx_scope> {
    pub(crate) isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    pub(crate) ctx_scope: &'ctx_scope V8ContextScope<'isolate_scope, 'isolate>,
    pub(crate) res: V8LocalValue<'isolate_scope, 'isolate>,
}

impl V8ScriptCtx {
    pub(crate) fn new(
        name: String,
        isolate: V8Isolate,
        ctx: V8Context,
        script: V8PersistedScript,
        tensor_object_template: V8PersistedObjectTemplate,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Self {
        Self {
            name,
            isolate,
            ctx,
            script,
            tensor_object_template,
            compiled_library_api,
            is_running: AtomicBool::new(false),
            is_being_loaded_from_rdb: AtomicBool::new(false),
            lock_state: RefCellWrapper {
                ref_cell: RefCell::new(GilStateCtx::new()),
            },
        }
    }

    /// Returns [`true`] if the script is currently being loaded from
    /// an RDB.
    pub(crate) fn is_being_loaded_from_rdb(&self) -> bool {
        self.is_being_loaded_from_rdb.load(Ordering::Relaxed)
    }

    /// Marks the [`Self`] as being (or not) loaded from RDB, returning
    /// a guard object, which will remove the mark upon destruction.
    fn mark_loading_rdb(&self, is_loading_rdb: bool) -> RdbLoadingGuard<'_> {
        RdbLoadingGuard::new(is_loading_rdb, self)
    }

    /// Perform necessary operation before running JS code.
    /// Currently, just set an atomic boolean indicating JS code is running.
    /// Returns [`true`] if JS code was already running or [`false`] otherwise.
    pub(crate) fn before_run(&self) -> bool {
        self.is_running.swap(true, Ordering::Relaxed)
    }

    /// Perform necessary operation after running JS code.
    /// Gets us input whether or not a JS code was already running before and set
    /// it to an atomic boolean indicating whether or not a JS code is running.
    pub(crate) fn after_run(&self, val: bool) {
        self.is_running.store(val, Ordering::Relaxed);
    }

    /// Perform necessary operation after locking Redis GIL like saving
    /// the current time for timeout purposes.
    pub(crate) fn after_lock_gil(&self) {
        self.lock_state.ref_cell.borrow_mut().set_lock();
    }

    /// Perform necessary operation before unlocking Redis GIL like unset
    /// the current time for timeout purposes.
    pub(crate) fn before_release_gil(&self) {
        self.lock_state.ref_cell.borrow_mut().set_unlock();
    }

    /// Return [`true`] if Redis GIL is locked and [`false`] otherwise.
    pub(crate) fn is_gil_locked(&self) -> bool {
        self.lock_state.ref_cell.borrow().is_locked()
    }

    /// Return the duration (in MS) we locked the Redis GIL.
    pub(crate) fn gil_lock_duration_ms(&self) -> u128 {
        self.lock_state.ref_cell.borrow().git_lock_duration_ms()
    }

    /// Set an indication that we bypass the allowed Redis GIL timeout.
    pub(crate) fn set_lock_timedout(&self) {
        self.lock_state.ref_cell.borrow_mut().set_lock_timedout();
    }

    /// If we have reached a timeout for the Redis Global Interpreter Lock (GIL) lock,
    /// the function should return [`true`], otherwise [`false`].
    pub(crate) fn is_lock_timedout(&self) -> bool {
        self.lock_state.ref_cell.borrow().is_lock_timedout()
    }

    /// The function calls the specified V8 function with the provided arguments.
    /// It performs the necessary operations before and after invoking the function,
    /// such as setting a variable to indicate that JavaScript code is currently running
    /// or recording the time at which the GIL (Global Interpreter Lock) is locked for timeout support.
    pub(crate) fn call<'isolate_scope, 'isolate>(
        &self,
        func: &V8LocalValue<'isolate_scope, 'isolate>,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        args: Option<&[&V8LocalValue<'isolate_scope, 'isolate>]>,
        gil_status: GilStatus,
    ) -> Option<V8LocalValue<'isolate_scope, 'isolate>> {
        let old_val = self.before_run();
        if gil_status.is_locked() {
            self.after_lock_gil();
        }
        let res = func.call(ctx_scope, args);
        if gil_status.is_locked() {
            self.before_release_gil();
        }
        self.after_run(old_val);
        res
    }

    /// The function calls the specified V8 script.
    /// It performs the necessary operations before and after invoking the function,
    /// such as setting a variable to indicate that JavaScript code is currently running
    /// or recording the time at which the GIL (Global Interpreter Lock) is locked for timeout support.
    pub(crate) fn run<'isolate_scope, 'isolate>(
        &self,
        script: &V8LocalScript<'isolate_scope, 'isolate>,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        gil_status: GilStatus,
    ) -> Option<V8LocalValue<'isolate_scope, 'isolate>> {
        let old_val = self.before_run();
        if gil_status.is_locked() {
            self.after_lock_gil();
        }
        let res = script.run(ctx_scope);
        if gil_status.is_locked() {
            self.before_release_gil();
        }
        self.after_run(old_val);
        res
    }

    /// Resolve the given promise object with the given value.
    /// It performs the necessary operations before and after invoking the function,
    /// such as setting a variable to indicate that JavaScript code is currently running.
    pub(crate) fn resolve(
        &self,
        resolver: &V8LocalResolver,
        ctx_scope: &V8ContextScope,
        val: &V8LocalValue,
    ) {
        let old_val = self.before_run();
        resolver.resolve(ctx_scope, val);
        self.after_run(old_val);
    }

    /// Reject the given promise object with the given value.
    /// It performs the necessary operations before and after invoking the function,
    /// such as setting a variable to indicate that JavaScript code is currently running.
    pub(crate) fn reject(
        &self,
        resolver: &V8LocalResolver,
        ctx_scope: &V8ContextScope,
        val: &V8LocalValue,
    ) {
        let old_val = self.before_run();
        resolver.reject(ctx_scope, val);
        self.after_run(old_val);
    }

    /// Runs the given closure if the given promise obejct was already resolved or rejected.
    fn run_on_done_promise<
        'isolate_scope,
        'isolate,
        T,
        Done: FnOnce(Result<OnDoneCtx, GearsApiError>) -> T,
    >(
        &self,
        isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        promise: &V8LocalPromise<'isolate_scope, 'isolate>,
        on_done: Done,
    ) -> T {
        let res = promise.get_result();
        if promise.state() == V8PromiseState::Fulfilled {
            on_done(Ok(OnDoneCtx {
                isolate_scope,
                ctx_scope,
                res,
            }))
        } else {
            let error = get_error_from_object(&res, ctx_scope);
            // v callback gets an error object and it can not assume the V8 is locked so there is no
            // reason not to release the isolate lock and this will also help to avoid deadlocks with
            // the Redis GIL.
            let _unlocker = isolate_scope.new_unlocker();
            on_done(Err(error))
        }
    }

    /// Return [`true`] if the given promise was already resolved or rejected, otherwise false.
    pub(crate) fn is_reject_or_fulfilled(&self, promise: &V8LocalPromise) -> bool {
        promise.state() == V8PromiseState::Fulfilled || promise.state() == V8PromiseState::Rejected
    }

    /// The function receives a promise and a closure as parameters,
    /// and invokes the closure if the promise has already been resolved or rejected.
    /// In such cases, the function returns Some(T),
    /// where T represents the return value of the closure.
    /// If the promise has neither been resolved nor rejected yet,
    /// the function returns None.
    pub(crate) fn promise_rejected_or_fulfilled<
        'isolate_scope,
        'isolate,
        T,
        Done: FnOnce(Result<OnDoneCtx, GearsApiError>) -> T,
    >(
        &self,
        isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        promise: &V8LocalPromise<'isolate_scope, 'isolate>,
        on_done: Done,
    ) -> Option<T> {
        if self.is_reject_or_fulfilled(promise) {
            return Some(self.run_on_done_promise(isolate_scope, ctx_scope, promise, on_done));
        }
        None
    }

    /// The function receives a promise and a closure as arguments,
    /// and assigns the closure as the resolve/reject callback of the promise,
    /// assuming that the promise has not been resolved or rejected yet.
    /// If the promise was already resolved or reject, the callback will be called
    /// on the next V8 minor task cicle.
    pub(crate) fn promise_rejected_or_fulfilled_async<
        'isolate_scope,
        'isolate,
        T,
        Done: 'static + FnOnce(Result<OnDoneCtx, GearsApiError>) -> T,
    >(
        &self,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        promise: &V8LocalPromise<'isolate_scope, 'isolate>,
        on_done: Done,
    ) {
        let on_done_resolve = Arc::new(RefCell::new(Some(on_done)));
        let on_done_reject = Arc::clone(&on_done_resolve);
        let on_done_dropped = Arc::clone(&on_done_resolve);
        let resolve = ctx_scope.new_native_function(new_native_function!(
            move |isolate_scope, ctx_scope, res: V8LocalValue| {
                let mut on_done = on_done_resolve.borrow_mut();
                if let Some(v) = on_done.take() {
                    v(Ok(OnDoneCtx {
                        isolate_scope,
                        ctx_scope,
                        res,
                    }));
                }
                Ok::<_, String>(None)
            }
        ));
        let reject = ctx_scope.new_native_function(new_native_function!(
            move |isolate_scope, ctx_scope, res: V8LocalValue| {
                let mut on_done = on_done_reject.borrow_mut();
                if let Some(v) = on_done.take() {
                    let res = Err(get_error_from_object(&res, ctx_scope));
                    // v callback gets an error object and it can not assume the V8 is locked so there is no
                    // reason not to release the isolate lock  and this will also help to avoid deadlocks with
                    // the Redis GIL.
                    let _unlocker = isolate_scope.new_unlocker();
                    v(res);
                };
                Ok::<_, String>(None)
            }
        ));
        promise.then(ctx_scope, &resolve, &reject);
        promise.to_value().on_dropped(move || {
            let mut on_done = on_done_dropped.borrow_mut();
            if let Some(v) = on_done.take() {
                v(Err(GearsApiError::new("Promise was dropped without been resolved. Usually happened because of timeout or OOM.")));
            };
        });
    }

    /// The function accepts a promise object and a closure,
    /// and invokes the closure when the promise is resolved.
    /// This invocation occurs immediately if the promise is
    /// already resolved or when it becomes resolved.
    /// The closure receives a [`Result<V8LocalValue, GearsApiError>`] parameter,
    /// which indicates whether the promise was successfully resolved or rejected.
    pub(crate) fn handle_promise<
        'isolate_scope,
        'isolate,
        T,
        Done: 'static + FnOnce(Result<OnDoneCtx, GearsApiError>) -> T,
    >(
        &self,
        isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
        ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
        promise: &V8LocalPromise<'isolate_scope, 'isolate>,
        on_done: Done,
    ) -> Option<T> {
        if self.is_reject_or_fulfilled(promise) {
            return Some(self.run_on_done_promise(isolate_scope, ctx_scope, promise, on_done));
        }
        self.promise_rejected_or_fulfilled_async(ctx_scope, promise, on_done);
        None
    }
}

pub(crate) struct V8LibraryCtx {
    pub(crate) script_ctx: Arc<V8ScriptCtx>,
}

impl LibraryCtxInterface for V8LibraryCtx {
    fn load_library(
        &self,
        load_library_ctx: &dyn LoadLibraryCtxInterface,
        is_being_loaded_from_rdb: bool,
    ) -> Result<(), GearsApiError> {
        let isolate_scope = self.script_ctx.isolate.enter();
        let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
        let trycatch = isolate_scope.new_try_catch();

        let script = self.script_ctx.script.to_local(&isolate_scope);

        let _rdb_loading_guard = self.script_ctx.mark_loading_rdb(is_being_loaded_from_rdb);

        // set private content
        let _load_library_guard = self.script_ctx.ctx.set_private_data(0, &load_library_ctx);

        let res = self.script_ctx.run(&script, &ctx_scope, GilStatus::Locked);

        let res =
            res.ok_or_else(|| get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope))?;

        if res.is_promise() {
            let promise = res.as_promise();
            if promise.state() == V8PromiseState::Rejected {
                let error = promise.get_result();
                let error_utf8 = error.to_utf8().unwrap();
                return Err(GearsApiError::new(format!(
                    "Failed evaluating module: {}",
                    error_utf8.as_str()
                )));
            }
        }

        Ok(())
    }

    fn get_info(&self) -> Option<ModuleInfo> {
        let sections = {
            let libraries_stats = {
                let mut isolate_stats_data = HashMap::new();

                isolate_stats_data.insert(
                    "total_heap_size".to_owned(),
                    self.script_ctx.isolate.total_heap_size().to_string(),
                );
                isolate_stats_data.insert(
                    "used_heap_size".to_owned(),
                    self.script_ctx.isolate.used_heap_size().to_string(),
                );
                isolate_stats_data.insert(
                    "heap_size_limit".to_owned(),
                    self.script_ctx.isolate.heap_size_limit().to_string(),
                );

                InfoSectionData::KeyValuePairs(isolate_stats_data)
            };

            let mut sections = HashMap::new();
            sections.insert("V8PerLibraryStatistics".to_owned(), libraries_stats);
            sections
        };
        Some(ModuleInfo { sections })
    }
}
