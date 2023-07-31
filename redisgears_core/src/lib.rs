/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
//! RedisGears core.
//! This crate contains the implementation of RedisGears Redis module.
//! RedisGears allows for custom functions to be executed within Redis.
//! The functions may be written in JavaScript or WebAssembly.

#![deny(missing_docs)]

use keys_notifications_ctx::KeySpaceNotificationsCtx;
use redis_module::redisvalue::RedisValueKey;
use redis_module::{
    BlockingCallOptions, CallOptionResp, CallOptionsBuilder, CallResult, ContextFlags, ErrorReply,
    PromiseCallReply, RedisGILGuard,
};
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtxInterfaceInitialised;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::{
    FunctionFlags, InfoSectionData, ModuleInfo,
};
use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
use redisgears_plugin_api::redisgears_plugin_api::run_function_ctx::PromiseReply;
use serde::{Deserialize, Serialize};

use config::{
    FatalFailurePolicyConfiguration, DB_LOADING_LOCK_REDIS_TIMEOUT, ENABLE_DEBUG_COMMAND,
    ERROR_VERBOSITY, EXECUTION_THREADS, FATAL_FAILURE_POLICY, LOCK_REDIS_TIMEOUT, V8_FLAGS,
    V8_LIBRARY_INITIAL_MEMORY_LIMIT, V8_LIBRARY_INITIAL_MEMORY_USAGE,
    V8_LIBRARY_MEMORY_USAGE_DELTA, V8_MAX_MEMORY, V8_PLUGIN_PATH,
};

use redis_module::raw::RedisModule__Assert;
use threadpool::ThreadPool;

use redis_module::{
    alloc::RedisAlloc, raw::KeyType::Stream, AclPermissions, CallOptions, Context, InfoContext,
    KeysCursor, NextArg, NotifyEvent, RedisError, RedisResult, RedisString, RedisValue, Status,
    ThreadSafeContext,
};

use redis_module::server_events::{
    FlushSubevent, LoadingSubevent, ModuleChangeSubevent, ServerRole,
};
use redis_module_macros::{
    command, config_changed_event_handler, cron_event_handler, flush_event_handler,
    info_command_handler, loading_event_handler, module_changed_event_handler,
    role_changed_event_handler,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, backend_ctx::BackendCtxInterfaceUninitialised,
    backend_ctx::LibraryFatalFailurePolicy, function_ctx::FunctionCtxInterface,
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    load_library_ctx::LibraryCtxInterface, load_library_ctx::LoadLibraryCtxInterface,
    load_library_ctx::RegisteredKeys, load_library_ctx::RemoteFunctionCtx,
    stream_ctx::StreamCtxInterface, GearsApiError,
};

use redisgears_plugin_api::redisgears_plugin_api::{FunctionCallResult, RefCellWrapper};

use crate::run_ctx::RunCtx;

use libloading::{Library, Symbol};

use std::collections::HashMap;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::stream_reader::{ConsumerData, StreamReaderCtx};
use std::iter::Skip;
use std::vec::IntoIter;

use crate::compiled_library_api::CompiledLibraryInternals;
use crate::keys_notifications::{KeysNotificationsCtx, NotificationCallback, NotificationConsumer};
use crate::stream_run_ctx::{GearsStreamConsumer, GearsStreamRecord};

use std::cell::RefCell;

use crate::keys_notifications::ConsumerKey;

use mr::libmr::mr_init;

mod background_run_ctx;
mod background_run_scope_guard;
mod compiled_library_api;
mod config;
mod function_del_command;
mod function_list_command;
mod function_load_command;
mod keys_notifications;
mod keys_notifications_ctx;
mod rdb;
mod run_ctx;
mod stream_reader;
mod stream_run_ctx;

/// GIT commit hash used for this build.
pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
/// Crate version (string) used for this build.
pub const VERSION_STR: Option<&str> = std::option_env!("VERSION_STR");
/// Crate version (number) used for this build.
pub const VERSION_NUM: Option<&str> = std::option_env!("VERSION_NUM");
/// The operating system used for building the crate.
pub const BUILD_OS: Option<&str> = std::option_env!("BUILD_OS");
/// The type of the operating system used for building the crate.
pub const BUILD_OS_NICK: Option<&str> = std::option_env!("BUILD_OS_NICK");
/// The CPU architeture of the operating system used for building the crate.
pub const BUILD_OS_ARCH: Option<&str> = std::option_env!("BUILD_OS_ARCH");
/// The build type of the crate.
pub const BUILD_TYPE: Option<&str> = std::option_env!("BUILD_TYPE");

const PSEUDO_SLAVE_READONLY_CONFIG_NAME: &str = "pseudo-slave-readonly";
const PSEUDO_SLAVE_CONFIG_NAME: &str = "pseudo-slave";

fn check_redis_version_compatible(ctx: &Context) -> Result<(), String> {
    use redis_module::Version;

    const VERSION: Version = Version {
        major: 7,
        minor: 1,
        patch: 242,
    };

    match ctx.get_redis_version() {
        Ok(v) => {
            if v.cmp(&VERSION) == std::cmp::Ordering::Less {
                return Err(format!(
                    "Redis version must be {}.{}.{} or greater",
                    VERSION.major, VERSION.minor, VERSION.patch
                ));
            }
        }
        Err(e) => {
            return Err(format!("Failed getting Redis version, version is probably to old, please use Redis 7.0 or above. {}", e));
        }
    }

    Ok(())
}

/// A string converted into a sha256 hash-sum. The string is unique
/// per the source string, due to the hash algorithm used.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
struct ObfuscatedString(String);
impl ObfuscatedString {
    fn new<S: Into<String>>(non_obfuscated: S) -> Self {
        Self(sha256::digest(non_obfuscated.into()))
    }
}
impl std::ops::Deref for ObfuscatedString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ObfuscatedString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: AsRef<str>> From<T> for ObfuscatedString {
    fn from(value: T) -> Self {
        Self::new(value.as_ref())
    }
}

trait ObfuscateString {
    fn obfuscate(&self) -> ObfuscatedString;
}

impl<T> ObfuscateString for T
where
    T: AsRef<str>,
{
    fn obfuscate(&self) -> ObfuscatedString {
        self.into()
    }
}

/// The meta information about the gears library instance at runtime.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GearsLibraryMetaData {
    name: String,
    engine: String,
    api_version: ApiVersion,
    code: String,
    config: Option<String>,
    user: RedisString,
}

/// The context of a single gears function.
struct GearsFunctionCtx {
    func: Box<dyn FunctionCtxInterface>,
    flags: FunctionFlags,
    is_async: bool,
    description: Option<String>,
}

impl GearsFunctionCtx {
    fn new(
        func: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
        is_async: bool,
        description: Option<String>,
    ) -> GearsFunctionCtx {
        GearsFunctionCtx {
            func,
            flags,
            is_async,
            description,
        }
    }
}

/// The gears library runtime context. It contains the live "instance"
/// of the global library state: all the functions registered and other
/// state information.
struct GearsLibraryCtx {
    meta_data: Arc<GearsLibraryMetaData>,
    functions: HashMap<String, GearsFunctionCtx>,
    remote_functions: HashMap<String, RemoteFunctionCtx>,
    stream_consumers:
        HashMap<String, Arc<RefCellWrapper<ConsumerData<GearsStreamRecord, GearsStreamConsumer>>>>,
    revert_stream_consumers: Vec<(String, GearsStreamConsumer, usize, bool, Option<String>)>,
    notifications_consumers: HashMap<String, Arc<RefCell<NotificationConsumer>>>,
    revert_notifications_consumers:
        Vec<(String, ConsumerKey, NotificationCallback, Option<String>)>,
    old_lib: Option<Arc<GearsLibrary>>,
}

struct GearsLoadLibraryCtx<'ctx, 'lib_ctx> {
    ctx: &'ctx Context,
    gears_lib_ctx: &'lib_ctx mut GearsLibraryCtx,
}

struct GearsLibrary {
    gears_lib_ctx: GearsLibraryCtx,
    lib_ctx: Box<dyn LibraryCtxInterface>,
    compile_lib_internals: Arc<CompiledLibraryInternals>,
}

impl<'ctx, 'lib_ctx> GearsLoadLibraryCtx<'ctx, 'lib_ctx> {
    fn register_function_internal(
        &mut self,
        name: &str,
        func_ctx: GearsFunctionCtx,
    ) -> Result<(), GearsApiError> {
        verify_name(name)
            .map_err(|e| GearsApiError::new(format!("Unallowed function name '{name}', {e}.")))?;

        if self.gears_lib_ctx.functions.contains_key(name) {
            return Err(GearsApiError::new(format!(
                "Function {} already exists",
                name
            )));
        }
        self.gears_lib_ctx
            .functions
            .insert(name.to_string(), func_ctx);
        Ok(())
    }
}

impl<'ctx, 'lib_ctx> LoadLibraryCtxInterface for GearsLoadLibraryCtx<'ctx, 'lib_ctx> {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
        description: Option<String>,
    ) -> Result<(), GearsApiError> {
        self.register_function_internal(
            name,
            GearsFunctionCtx::new(function_ctx, flags, false, description),
        )
    }

    fn register_async_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
        description: Option<String>,
    ) -> Result<(), GearsApiError> {
        self.register_function_internal(
            name,
            GearsFunctionCtx::new(function_ctx, flags, true, description),
        )
    }

    fn register_remote_task(
        &mut self,
        name: &str,
        remote_function_callback: RemoteFunctionCtx,
    ) -> Result<(), GearsApiError> {
        // TODO move to <https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.try_insert>
        // once stabilised.

        verify_name(name).map_err(|e| {
            GearsApiError::new(format!("Unallowed cluster function name '{name}', {e}."))
        })?;

        if self.gears_lib_ctx.remote_functions.contains_key(name) {
            return Err(GearsApiError::new(format!(
                "Remote function {} already exists",
                name
            )));
        }
        self.gears_lib_ctx
            .remote_functions
            .insert(name.to_string(), remote_function_callback);
        Ok(())
    }

    fn register_stream_consumer(
        &mut self,
        name: &str,
        prefix: &[u8],
        ctx: Box<dyn StreamCtxInterface>,
        window: usize,
        trim: bool,
        description: Option<String>,
    ) -> Result<(), GearsApiError> {
        verify_name(name).map_err(|e| {
            GearsApiError::new(format!("Unallowed stream trigger name '{name}', {e}."))
        })?;

        if self.gears_lib_ctx.stream_consumers.contains_key(name) {
            return Err(GearsApiError::new(
                "Stream registration already exists".to_string(),
            ));
        }

        let stream_registration = if let Some(old_consumer) = self
            .gears_lib_ctx
            .old_lib
            .as_ref()
            .and_then(|v| v.gears_lib_ctx.stream_consumers.get(name))
        {
            let mut o_c = old_consumer.ref_cell.borrow_mut();
            if o_c.prefix != prefix {
                return Err(GearsApiError::new(
                    format!("Can not upgrade an existing consumer with different prefix, consumer: '{}', old_prefix: {}, new_prefix: {}.",
                    name, std::str::from_utf8(&o_c.prefix).unwrap_or("[binary data]"), std::str::from_utf8(prefix).unwrap_or("[binary data]"))
                ));
            }
            let old_ctx = o_c.set_consumer(GearsStreamConsumer::new(
                &self.gears_lib_ctx.meta_data,
                FunctionFlags::empty(),
                ctx,
            ));
            let old_window = o_c.set_window(window);
            let old_trim = o_c.set_trim(trim);
            let old_description = o_c.set_description(description);
            self.gears_lib_ctx.revert_stream_consumers.push((
                name.to_string(),
                old_ctx,
                old_window,
                old_trim,
                old_description,
            ));
            Arc::clone(old_consumer)
        } else {
            let globals = get_globals_mut();
            let stream_ctx = &mut globals.stream_ctx;
            let lib_name = self.gears_lib_ctx.meta_data.name.clone();
            let consumer_name = name.to_string();
            let consumer = stream_ctx.add_consumer(
                prefix,
                GearsStreamConsumer::new(
                    &self.gears_lib_ctx.meta_data,
                    FunctionFlags::empty(),
                    ctx,
                ),
                window,
                trim,
                Some(Box::new(move |ctx, stream_name, ms, seq| {
                    ctx.replicate(
                        "_rg_internals.update_stream_last_read_id",
                        &[
                            lib_name.as_bytes(),
                            consumer_name.as_bytes(),
                            stream_name,
                            ms.to_string().as_bytes(),
                            seq.to_string().as_bytes(),
                        ],
                    );
                })),
                description,
            );
            if is_master(self.ctx) {
                // trigger a key scan
                scan_key_space_for_streams(self.ctx);
            }
            consumer
        };

        self.gears_lib_ctx
            .stream_consumers
            .insert(name.to_string(), stream_registration);
        Ok(())
    }

    fn register_key_space_notification_consumer(
        &mut self,
        name: &str,
        key: RegisteredKeys,
        keys_notifications_consumer_ctx: Box<dyn KeysNotificationsConsumerCtxInterface>,
        description: Option<String>,
    ) -> Result<(), GearsApiError> {
        verify_name(name).map_err(|e| {
            GearsApiError::new(format!("Unallowed key space trigger name '{name}', {e}."))
        })?;

        if self
            .gears_lib_ctx
            .notifications_consumers
            .contains_key(name)
        {
            return Err(GearsApiError::new(
                "Notification consumer already exists".to_string(),
            ));
        }

        let meta_data = Arc::clone(&self.gears_lib_ctx.meta_data);
        let permissions = AclPermissions::all();
        let fire_event_callback: NotificationCallback =
            Box::new(move |ctx, event, key, done_callback| {
                let key_redis_str = RedisString::create_from_slice(std::ptr::null_mut(), key);
                if let Err(e) =
                    ctx.acl_check_key_permission(&meta_data.user, &key_redis_str, &permissions)
                {
                    done_callback(Err(GearsApiError::new(format!(
                        "User '{}' has no permissions on key '{}', {}.",
                        meta_data.user,
                        std::str::from_utf8(key).unwrap_or("[binary data]"),
                        e
                    ))));
                    return;
                }
                let _notification_blocker = get_notification_blocker();
                keys_notifications_consumer_ctx.on_notification_fired(
                    event,
                    key,
                    &KeySpaceNotificationsCtx::new(
                        ctx,
                        meta_data.clone(),
                        FunctionFlags::NO_WRITES,
                    ),
                    done_callback,
                );
            });

        let consumer: Arc<RefCell<NotificationConsumer>> = if let Some(old_notification_consumer) =
            self.gears_lib_ctx
                .old_lib
                .as_ref()
                .and_then(|v| v.gears_lib_ctx.notifications_consumers.get(name))
        {
            let mut o_c = old_notification_consumer.borrow_mut();
            let old_consumer_callback = o_c.set_callback(fire_event_callback);
            let new_key = match key {
                RegisteredKeys::Key(s) => ConsumerKey::Key(s.to_vec()),
                RegisteredKeys::Prefix(s) => ConsumerKey::Prefix(s.to_vec()),
            };
            let old_key = o_c.set_key(new_key);
            let old_description = o_c.set_description(description);
            self.gears_lib_ctx.revert_notifications_consumers.push((
                name.to_string(),
                old_key,
                old_consumer_callback,
                old_description,
            ));
            Arc::clone(old_notification_consumer)
        } else {
            let globals = get_globals_mut();

            match key {
                RegisteredKeys::Key(k) => globals.notifications_ctx.add_consumer_on_key(
                    k,
                    fire_event_callback,
                    description,
                ),
                RegisteredKeys::Prefix(p) => globals.notifications_ctx.add_consumer_on_prefix(
                    p,
                    fire_event_callback,
                    description,
                ),
            }
        };

        self.gears_lib_ctx
            .notifications_consumers
            .insert(name.to_string(), consumer);
        Ok(())
    }
}

/// The policy configured on the current database
enum DbPolicy {
    /// Regular database
    Regular,
    /// A replica-of target
    PseudoSlave,
    /// A readonly replica-of target
    PseudoSlaveReadonly,
}

impl DbPolicy {
    pub(crate) fn is_regular(&self) -> bool {
        matches!(self, DbPolicy::Regular)
    }

    pub(crate) fn is_readonly(&self) -> bool {
        matches!(self, DbPolicy::PseudoSlaveReadonly)
    }

    pub(crate) fn is_pseudo_slave(&self) -> bool {
        matches!(self, DbPolicy::PseudoSlaveReadonly | DbPolicy::PseudoSlave)
    }
}

struct GlobalCtx {
    libraries: Mutex<HashMap<String, Arc<GearsLibrary>>>,
    backends: HashMap<String, Box<dyn BackendCtxInterfaceInitialised>>,
    uninitialised_backends: HashMap<String, Box<dyn BackendCtxInterfaceUninitialised>>,
    /// Holds the handler to the dyn library of all backends, we need to keep it so the handler will not be freed.
    _plugins: Vec<Library>,
    pool: Mutex<Option<ThreadPool>>,
    /// Thread pool which used to run management tasks that should not be
    /// starved by user tasks (which run on [`GlobalCtx::pool`]).
    management_pool: RedisGILGuard<Option<ThreadPool>>,
    stream_ctx: StreamReaderCtx<GearsStreamRecord, GearsStreamConsumer>,
    notifications_ctx: KeysNotificationsCtx,
    avoid_key_space_notifications: bool,
    allow_unsafe_redis_commands: bool,
    db_policy: DbPolicy,
    future_handlers: HashMap<String, Vec<Weak<RedisGILGuard<FutureHandlerContext>>>>,
    avoid_replication_traffic: bool,
}

static mut GLOBALS: Option<GlobalCtx> = None;

pub(crate) struct NotificationBlocker {
    old_val: bool,
}

pub(crate) fn get_notification_blocker() -> NotificationBlocker {
    let res = NotificationBlocker {
        old_val: get_globals_mut().avoid_key_space_notifications,
    };
    get_globals_mut().avoid_key_space_notifications = true;
    res
}

impl Drop for NotificationBlocker {
    fn drop(&mut self) {
        get_globals_mut().avoid_key_space_notifications = self.old_val;
    }
}

fn get_globals() -> &'static GlobalCtx {
    unsafe { GLOBALS.as_ref().unwrap() }
}

fn get_globals_mut() -> &'static mut GlobalCtx {
    unsafe { GLOBALS.as_mut().unwrap() }
}

/// Returns the global redis module context after the module has been
/// initialized.
fn get_backends_mut() -> &'static mut HashMap<String, Box<dyn BackendCtxInterfaceInitialised>> {
    &mut get_globals_mut().backends
}

/// Returns mutable reference to the uninitialised backends dictionary.
fn get_uninitialised_backends_mut(
) -> &'static mut HashMap<String, Box<dyn BackendCtxInterfaceUninitialised>> {
    &mut get_globals_mut().uninitialised_backends
}

fn get_libraries() -> MutexGuard<'static, HashMap<String, Arc<GearsLibrary>>> {
    get_globals().libraries.lock().unwrap()
}

pub(crate) fn get_thread_pool() -> MutexGuard<'static, Option<ThreadPool>> {
    get_globals().pool.lock().unwrap()
}

struct Sentinel;

impl Drop for Sentinel {
    fn drop(&mut self) {
        if std::thread::panicking() {
            unsafe {
                RedisModule__Assert.unwrap()(
                    "Crashed on panic on the main thread\0".as_ptr() as *const std::os::raw::c_char,
                    "\0".as_ptr() as *const std::os::raw::c_char,
                    0,
                );
            }
        }
    }
}

/// Executes the passed job object in a dedicated thread allocated
/// from the global module thread pool.
pub(crate) fn execute_on_pool<F: FnOnce() + Send + 'static>(job: F) {
    let mut pool = get_thread_pool();
    pool.get_or_insert_with(|| {
        ThreadPool::with_name(
            "RGExecutor".to_owned(),
            EXECUTION_THREADS.load(Ordering::Relaxed) as usize,
        )
    })
    .execute(move || {
        job();
    });
}

/// Calls a redis command and returns the value.
pub(crate) fn call_redis_command(
    ctx: &Context,
    user: &RedisString,
    command: &str,
    call_options: &CallOptions,
    args: &[&[u8]],
) -> CallResult<'static> {
    let _authenticate_scope = ctx
        .authenticate_user(user)
        .map_err(|e| ErrorReply::Message(e.to_string()))?;
    ctx.call_ext(command, call_options, args)
}

type FutureHandlerContextCallback = dyn FnOnce(&Context, CallResult<'static>);
type FutureHandlerContextDisposer = dyn FnOnce(&Context, bool);

/// a struct that holds information about not yet resolve future replies.
/// The struct allows to either abort the execution or invoke the on done callback.
struct FutureHandlerContext {
    callback: Option<Box<FutureHandlerContextCallback>>,
    disposer: Option<Box<FutureHandlerContextDisposer>>,
    command: Vec<Vec<u8>>,
}

impl FutureHandlerContext {
    /// Call the on done callback that was set to this future object.
    fn call(
        &mut self,
        ctx: &Context,
        reply: Result<redis_module::CallReply<'static>, ErrorReply<'static>>,
    ) {
        if let Some(callback) = self.callback.take() {
            callback(ctx, reply);
        }

        if let Some(disposer) = self.disposer.take() {
            disposer(ctx, false);
        }
    }

    /// Abort the command invocation (if possible) and send an error as a reply to the
    /// on done callback.
    fn abort(&mut self, ctx: &Context) {
        if let Some(callback) = self.callback.take() {
            callback(
                ctx,
                CallResult::Err(ErrorReply::Message("Command was aborted".to_owned())),
            );
        }
        if let Some(disposer) = self.disposer.take() {
            disposer(ctx, true);
        }
    }
}

/// Calls blocking redis command.
/// Returns [PromiseReply] which might be already resolved (in case we already got the reply)
/// or it might be resolved later when the command will be finished.
/// In case the [PromiseReply] was not yet resolved, the user is expected to give a callback
/// which will be called when the command will finish.
pub(crate) fn call_redis_command_async<'ctx>(
    ctx: &'ctx Context,
    lib: &str,
    user: &RedisString,
    command: &str,
    call_options: &BlockingCallOptions,
    args: &[&[u8]],
) -> PromiseReply<'static, 'ctx> {
    let _authenticate_scope = ctx
        .authenticate_user(user)
        .map_err(|e| ErrorReply::Message(e.to_string()));
    if let Err(e) = _authenticate_scope {
        return PromiseReply::Resolved(CallResult::Err(e));
    }

    match ctx.call_blocking(command, call_options, args) {
        PromiseCallReply::Resolved(res) => PromiseReply::Resolved(res),
        PromiseCallReply::Future(future) => {
            let lib = lib.to_owned();
            let mut command = vec![command.as_bytes().to_vec()];
            command.extend(args.iter().map(|v| v.to_vec()));
            PromiseReply::Future(Box::new(move |callback| {
                let future_handler_context = FutureHandlerContext {
                    callback: Some(callback),
                    disposer: None,
                    command,
                };
                let future_handler_context = Arc::new(RedisGILGuard::new(future_handler_context));
                let future_handler_context_unblocked = Arc::clone(&future_handler_context);
                let globals = get_globals_mut();

                // add the `future_handler_context` to future_abort_callbacks so we can abort it if needed.
                globals
                    .future_handlers
                    .entry(lib)
                    .or_insert(Vec::new())
                    .push(Arc::downgrade(&future_handler_context));

                // Set the unblock handler which will call the plugin callback and free the `future_handler_context`
                let future_handler = future.set_unblock_handler(move |ctx, reply| {
                    let mut future_handler_context_unblocked =
                        future_handler_context_unblocked.lock(ctx);
                    future_handler_context_unblocked.call(ctx, reply);
                });

                // Initialize the disposer methon which will abort the command and send an error as a reply to the plugin callback
                let mut future_handler_context = future_handler_context.lock(ctx);
                future_handler_context.disposer = Some(Box::new(move |ctx: &Context, abort| {
                    if abort {
                        future_handler.abort_and_dispose(ctx);
                    } else {
                        future_handler.dispose(ctx);
                    }
                }));
            }))
        }
    }
}

fn verify_v8_mem_usage_values() -> Result<(), RedisError> {
    let v8_max_memory = V8_MAX_MEMORY.load(Ordering::Relaxed);
    let v8_lib_initial_mem = V8_LIBRARY_INITIAL_MEMORY_USAGE.load(Ordering::Relaxed);
    let v8_lib_initial_mem_limit = V8_LIBRARY_INITIAL_MEMORY_LIMIT.load(Ordering::Relaxed);
    let v8_lib_mem_delta = V8_LIBRARY_MEMORY_USAGE_DELTA.load(Ordering::Relaxed);

    if v8_lib_initial_mem > v8_max_memory {
        return Err(RedisError::Str(
            "V8 library initial memory usage can not bypass the v8 max memory.",
        ));
    }

    if v8_lib_initial_mem_limit > v8_max_memory {
        return Err(RedisError::Str(
            "V8 library initial memory limit can not bypass the v8 max memory.",
        ));
    }

    if v8_lib_mem_delta > v8_max_memory {
        return Err(RedisError::Str(
            "V8 library memory delta can not bypass the v8 max memory.",
        ));
    }

    if v8_lib_initial_mem > v8_lib_initial_mem_limit {
        return Err(RedisError::Str(
            "V8 library initial initial memory usage can not bypass the initial memory limit.",
        ));
    }

    Ok(())
}

fn get_db_policy(ctx: &Context) -> DbPolicy {
    let call_options = CallOptionsBuilder::new()
        .resp(CallOptionResp::Resp3)
        .build();
    let res = ctx.call_ext(
        "config",
        &call_options,
        &[
            "get",
            PSEUDO_SLAVE_READONLY_CONFIG_NAME,
            PSEUDO_SLAVE_CONFIG_NAME,
        ],
    );
    if let Ok(res) = res {
        let res: RedisValue = (&res).into();
        if let RedisValue::Map(map) = res {
            let pseudo_slave_readonly = map
                .get(&RedisValueKey::String(
                    PSEUDO_SLAVE_READONLY_CONFIG_NAME.to_owned(),
                ))
                .map_or("no".to_owned(), |v| {
                    if let RedisValue::SimpleString(s) = v {
                        s.to_owned()
                    } else {
                        "no".to_owned()
                    }
                });
            if pseudo_slave_readonly == "yes" {
                return DbPolicy::PseudoSlaveReadonly;
            }

            let pseudo_slave = map
                .get(&RedisValueKey::String(PSEUDO_SLAVE_CONFIG_NAME.to_owned()))
                .map_or("no".to_owned(), |v| {
                    if let RedisValue::SimpleString(s) = v {
                        s.to_owned()
                    } else {
                        "no".to_owned()
                    }
                });
            if pseudo_slave == "yes" {
                return DbPolicy::PseudoSlave;
            }
        }
    }
    DbPolicy::Regular
}

fn load_v8_backend(
    ctx: &Context,
) -> Result<(String, Box<dyn BackendCtxInterfaceUninitialised>, Library), RedisError> {
    let v8_path = V8_PLUGIN_PATH.lock(ctx);

    let v8_path = std::env::var("modulesdatadir")
        .map(|val| {
            format!(
                "{}/redisgears_2/{}/deps/gears_v8/{}",
                val,
                VERSION_NUM.unwrap(),
                v8_path.as_str()
            )
        })
        .unwrap_or_else(|_| v8_path.to_string());

    let lib = unsafe { Library::new(&v8_path) }
        .map_err(|e| RedisError::String(format!("Failed loading '{}', {}", v8_path, e)))?;
    let func: Symbol<unsafe fn(&Context) -> *mut dyn BackendCtxInterfaceUninitialised> = unsafe {
        lib.get(b"initialize_plugin")
    }
    .map_err(|e| RedisError::String(format!("Failed getting initialize_plugin symbol, {e}")))?;
    let backend = unsafe { Box::from_raw(func(ctx)) };
    let name = backend.get_name();
    log::info!("Registered backend: {name}.");
    Ok((name.to_owned(), backend, lib))
}

fn initialize_v8_backend(
    _ctx: &Context,
    uninitialised_backend: Box<dyn BackendCtxInterfaceUninitialised>,
) -> Result<Box<dyn BackendCtxInterfaceInitialised>, RedisError> {
    let name = uninitialised_backend.get_name();
    let initialised_backend: Box<dyn BackendCtxInterfaceInitialised> =
        uninitialised_backend.initialize().map_err(|e| {
            RedisError::String(format!("Failed loading {} backend, {}.", name, e.get_msg()))
        })?;
    let version = initialised_backend.get_version();
    log::info!("Initialized backend: {name}, {version}.");
    Ok(initialised_backend)
}

pub(crate) fn get_backend(
    ctx: &Context,
    name: &str,
) -> Result<&'static mut Box<dyn BackendCtxInterfaceInitialised>, RedisError> {
    get_backends_mut()
        .get_mut(name)
        .ok_or(RedisError::Str("No such backend"))
        .or_else(|_| {
            let uninitialised_backend = get_uninitialised_backends_mut()
                .remove(name)
                .ok_or_else(|| RedisError::String(format!("Unknown backend {}", name)))?;
            let backend = initialize_v8_backend(ctx, uninitialised_backend)?;
            Ok(get_backends_mut().entry(name.to_owned()).or_insert(backend))
        })
}

fn js_init(ctx: &Context, _args: &[RedisString]) -> Status {
    mr_init(ctx, 1, None);

    if let Err(e) = redis_module::logging::setup() {
        ctx.log_notice(&format!("Failed to setup the standard logging: {e}"));
    }

    match redisai_rs::redisai_init(ctx) {
        Ok(_) => ctx.log_notice("RedisAI API was loaded successfully."),
        Err(_) => ctx.log_notice("Failed loading RedisAI API."),
    }

    log::info!(
        "RedisGears v{}, sha='{}', build_type='{}', built_for='{}-{}.{}'.",
        VERSION_STR.unwrap_or_default(),
        GIT_SHA.unwrap_or_default(),
        BUILD_TYPE.unwrap_or_default(),
        BUILD_OS.unwrap_or_default(),
        BUILD_OS_NICK.unwrap_or_default(),
        BUILD_OS_ARCH.unwrap_or_default()
    );

    if let Err(e) = check_redis_version_compatible(ctx) {
        log::error!("{e}");
        return Status::Err;
    }

    if let Err(e) = verify_v8_mem_usage_values() {
        log::error!("{e}");
        return Status::Err;
    }

    std::panic::set_hook(Box::new(|panic_info| {
        log::error!("Application panicked, {}", panic_info);
        let (file, line) = match panic_info.location() {
            Some(l) => (l.file(), l.line()),
            None => ("", 0),
        };
        let file = std::ffi::CString::new(file).unwrap();
        unsafe {
            RedisModule__Assert.unwrap()(
                "Crashed on panic\0".as_ptr() as *const std::os::raw::c_char,
                file.as_ptr(),
                line as i32,
            );
        }
    }));
    let (v8_backend_name, v8_backend, plugin_lib) = match load_v8_backend(ctx) {
        Ok(res) => res,
        Err(e) => {
            log::error!("{e}");
            return Status::Err;
        }
    };

    let v8_flags = V8_FLAGS.lock(ctx).to_owned();
    let backend_ctx = BackendCtx {
        allocator: &RedisAlloc,
        log_info: Box::new(|msg| log::info!("{msg}")),
        log_trace: Box::new(|msg| log::trace!("{msg}")),
        log_debug: Box::new(|msg| log::debug!("{msg}")),
        log_error: Box::new(|msg| log::error!("{msg}")),
        log_warning: Box::new(|msg| log::warn!("{msg}")),
        get_on_oom_policy: Box::new(|| match *FATAL_FAILURE_POLICY.lock().unwrap() {
            FatalFailurePolicyConfiguration::Abort => LibraryFatalFailurePolicy::Abort,
            FatalFailurePolicyConfiguration::Kill => LibraryFatalFailurePolicy::Kill,
        }),
        get_lock_timeout: Box::new(|| LOCK_REDIS_TIMEOUT.load(Ordering::Relaxed) as u128),
        get_rdb_lock_timeout: Box::new(|| {
            DB_LOADING_LOCK_REDIS_TIMEOUT.load(Ordering::Relaxed) as u128
        }),
        get_v8_maxmemory: Box::new(|| V8_MAX_MEMORY.load(Ordering::Relaxed) as usize),
        get_v8_library_initial_memory: Box::new(|| {
            V8_LIBRARY_INITIAL_MEMORY_USAGE.load(Ordering::Relaxed) as usize
        }),
        get_v8_library_initial_memory_limit: Box::new(|| {
            V8_LIBRARY_INITIAL_MEMORY_LIMIT.load(Ordering::Relaxed) as usize
        }),
        get_v8_library_memory_delta: Box::new(|| {
            V8_LIBRARY_MEMORY_USAGE_DELTA.load(Ordering::Relaxed) as usize
        }),
        get_v8_flags: Box::new(move || v8_flags.to_owned()),
    };

    let on_load_res = v8_backend.on_load(backend_ctx);
    if let Err(e) = on_load_res {
        log::error!("{e}");
        return Status::Err;
    }
    let global_ctx = GlobalCtx {
        libraries: Mutex::new(HashMap::new()),
        backends: HashMap::new(),
        uninitialised_backends: HashMap::from([(v8_backend_name, v8_backend)]),
        _plugins: vec![plugin_lib],
        pool: Mutex::new(None),
        management_pool: RedisGILGuard::new(None),
        stream_ctx: StreamReaderCtx::new(
            Box::new(|ctx, key, id, include_id| {
                // read data from the stream
                if !is_master(ctx) || ctx.avoid_replication_traffic() {
                    return Err(
                        "Can not read data on replica or the \"avoid replication traffic\" option is enabled"
                            .to_string(),
                    );
                }
                let stream_name = ctx.create_string(key);
                let key = ctx.open_key(&stream_name);
                let mut stream_iterator =
                    match key.get_stream_range_iterator(id, None, !include_id, false) {
                        Ok(s) => s,
                        Err(_) => return Err("Key does not exists on is not a stream".to_string()),
                    };

                Ok(stream_iterator
                    .next()
                    .map(|e| GearsStreamRecord { record: e }))
            }),
            Box::new(|ctx, key_name, id| {
                // trim the stream callback
                if !is_master(ctx) {
                    ctx.log_warning("Attempt to trim data on replica was denied.");
                    return;
                }
                let stream_name = RedisString::create(None, key_name);
                // We need to run inside a post execution job to make sure the trim
                // will happened last and will be replicated to the replica last.
                ctx.add_post_notification_job(move |ctx| {
                    let key = ctx.open_key_writable(&stream_name);
                    let res = key.trim_stream_by_id(id, false);
                    if let Err(e) = res {
                        ctx.log_debug(&format!(
                            "Error occured when trimming stream (stream was probably deleted): {}",
                            e
                        ))
                    } else {
                        redis_module::replicate(
                            ctx.ctx,
                            "xtrim",
                            &[
                                stream_name.as_slice(),
                                "MINID".as_bytes(),
                                format!("{}-{}", id.ms, id.seq).as_bytes(),
                            ],
                        );
                    }
                });
            }),
        ),
        notifications_ctx: KeysNotificationsCtx::new(),
        avoid_key_space_notifications: false,
        allow_unsafe_redis_commands: false,
        db_policy: get_db_policy(ctx),
        future_handlers: HashMap::new(),
        avoid_replication_traffic: false,
    };

    unsafe { GLOBALS = Some(global_ctx) };

    Status::Ok
}

fn build_uninitialised_backends_info(ctx: &InfoContext) -> RedisResult<()> {
    if get_uninitialised_backends_mut().is_empty() {
        return Ok(());
    }

    let mut section_builder = ctx.builder().add_section("UninitialisedBackends");

    section_builder = get_uninitialised_backends_mut()
        .keys()
        .try_fold(section_builder, |section_builder, name| {
            section_builder.field("backend_name", name.as_str())
        })?;

    let _ = section_builder.build_section()?.build_info()?;

    Ok(())
}

fn build_backend_module_info(ctx: &InfoContext, info: ModuleInfo) -> RedisResult<()> {
    let mut builder = ctx.builder();
    for section in &info.sections {
        let mut section_builder = builder.add_section(section.0);
        match section.1 {
            InfoSectionData::KeyValuePairs(map) => {
                for (key, value) in map {
                    section_builder = section_builder.field(key, value.as_str())?;
                }
            }
            InfoSectionData::Dictionaries(dictionaries) => {
                for dictionary in dictionaries {
                    let mut dictionary_builder = section_builder.add_dictionary(dictionary.0);
                    for (key, value) in dictionary.1 {
                        dictionary_builder = dictionary_builder.field(key, value.as_str())?;
                    }
                    section_builder = dictionary_builder.build_dictionary()?;
                }
            }
        }
        builder = section_builder.build_section()?;
    }

    let _ = builder.build_info()?;

    Ok(())
}

fn build_initialised_backends_info(ctx: &InfoContext) -> RedisResult<()> {
    get_backends_mut()
        .values_mut()
        .filter_map(|b| b.get_info())
        .try_for_each(|info| build_backend_module_info(ctx, info))
}

fn build_per_library_info(ctx: &InfoContext) -> RedisResult<()> {
    let libraries = get_globals().libraries.lock()?;
    if libraries.is_empty() {
        return Ok(());
    }

    let builder = ctx.builder();
    let section_builder = builder.add_section("PerLibraryInformation");
    let mut dictionaries = HashMap::new();
    for library in libraries.iter() {
        let mut library_info = HashMap::new();

        library_info.insert(
            "function_count(sync)".to_owned(),
            library
                .1
                .gears_lib_ctx
                .functions
                .iter()
                .filter(|f| !f.1.is_async)
                .count()
                .to_string(),
        );

        library_info.insert(
            "function_count(async)".to_owned(),
            library
                .1
                .gears_lib_ctx
                .functions
                .iter()
                .filter(|f| f.1.is_async)
                .count()
                .to_string(),
        );

        library_info.insert(
            "notification_consumers_count".to_owned(),
            library
                .1
                .gears_lib_ctx
                .notifications_consumers
                .len()
                .to_string(),
        );

        library_info.insert(
            "stream_consumers_count".to_owned(),
            library.1.gears_lib_ctx.stream_consumers.len().to_string(),
        );

        library_info.insert(
            "api_version".to_owned(),
            library.1.gears_lib_ctx.meta_data.api_version.to_string(),
        );

        library_info.insert(
            "pending_jobs_count".to_owned(),
            library.1.compile_lib_internals.pending_jobs().to_string(),
        );

        library_info.insert(
            "pending_async_calls_count".to_owned(),
            get_globals()
                .future_handlers
                .get(&library.1.gears_lib_ctx.meta_data.name)
                .iter()
                .count()
                .to_string(),
        );

        library_info.insert(
            "cluster_functions_count".to_owned(),
            library.1.gears_lib_ctx.remote_functions.len().to_string(),
        );

        if let Some(info) = library.1.lib_ctx.get_info() {
            if let Some(first_level) = info.sections.into_iter().next() {
                if let InfoSectionData::KeyValuePairs(key_value_pairs) = first_level.1 {
                    library_info.extend(key_value_pairs);
                }
            }
        }

        dictionaries.insert(library.0.obfuscate(), library_info);
    }

    let section_builder =
        dictionaries
            .into_iter()
            .try_fold(section_builder, |section_builder, d| {
                let mut dictionaries_builder = section_builder.add_dictionary(&d.0);

                for (k, v) in d.1 {
                    dictionaries_builder = dictionaries_builder.field(&k, v)?;
                }

                dictionaries_builder.build_dictionary()
            })?;

    let _ = section_builder.build_section()?.build_info()?;

    Ok(())
}

#[info_command_handler]
fn module_info(ctx: &InfoContext, _for_crash_report: bool) -> RedisResult<()> {
    build_uninitialised_backends_info(ctx)?;
    build_initialised_backends_info(ctx)?;
    build_per_library_info(ctx)?;

    Ok(())
}

/// Verifies that we haven't reached an Out Of Memory situation.
/// Returns `true` if the OOM isn't reached.
///
/// # Note
///
/// We can only reach the error if the function flags don't allow writes
/// and OOM and when the context returns an OOM error.
pub(crate) fn verify_oom(ctx: &Context, flags: FunctionFlags) -> bool {
    flags.contains(FunctionFlags::NO_WRITES)
        || flags.contains(FunctionFlags::ALLOW_OOM)
        || !ctx.get_flags().contains(ContextFlags::OOM)
}

/// Return true iff the current instance is a master which is not a pseudo slave (replica-of target).
pub(crate) fn is_master(ctx: &Context) -> bool {
    let globals = get_globals();
    ctx.get_flags().contains(ContextFlags::MASTER) && globals.db_policy.is_regular()
}

/// Returns `true` if the function with the specified flags is allowed
/// to run on replicas in case the current instance is a replica.
pub(crate) fn verify_ok_on_replica(ctx: &Context, flags: FunctionFlags) -> bool {
    // master which is not readonly and avoid replication traffic was not requested, ok to run || we can run function with no writes anyhow.
    (ctx.get_flags().contains(ContextFlags::MASTER)
        && !get_globals().db_policy.is_readonly()
        && !ctx.avoid_replication_traffic())
        || flags.contains(FunctionFlags::NO_WRITES)
}

fn function_call_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
    allow_block: bool,
) -> RedisResult {
    let mut lib_func_name = args.next_arg()?.try_as_str()?.split('.');

    let library_name = lib_func_name
        .next()
        .ok_or(RedisError::Str("Failed extracting library name"))?;
    let function_name = lib_func_name
        .next()
        .ok_or(RedisError::Str("Failed extracting function name"))?;

    let num_keys = args.next_arg()?.try_as_str()?.parse::<usize>()?;
    let libraries = get_libraries();

    let lib = libraries
        .get(library_name)
        .ok_or_else(|| RedisError::String(format!("Unknown library {}", library_name)))?;

    let function = lib
        .gears_lib_ctx
        .functions
        .get(function_name)
        .ok_or_else(|| RedisError::String(format!("Unknown function {}", function_name)))?;

    if !verify_ok_on_replica(ctx, function.flags) {
        return Err(RedisError::Str(
            "Err can not run a function that might perform writes on a replica or when the \"avoid replication traffic\" option is enabled",
        ));
    }

    if !verify_oom(ctx, function.flags) {
        return Err(RedisError::Str(
            "OOM can not run the function when out of memory",
        ));
    }

    let args = args.collect::<Vec<redis_module::RedisString>>();
    if args.len() < num_keys {
        return Err(RedisError::String(format!(
            "Not enough arguments was given, expected at least {} arguments, got {} arguments.",
            num_keys,
            args.len()
        )));
    }

    if function.is_async && !allow_block {
        // Blocking is not allowed but the function declated as async which means it might block, we will not invoke it.
        return Err(RedisError::Str("The function is declared as async and was called while blocking was not allowed; note that you cannot invoke async functions from within Lua or MULTI, and you must use TFCALLASYNC instead."));
    }

    {
        let _notification_blocker = get_notification_blocker();
        let res = function.func.call(&RunCtx {
            ctx,
            args,
            flags: function.flags,
            lib_meta_data: Arc::clone(&lib.gears_lib_ctx.meta_data),
            allow_block,
        });
        if matches!(res, FunctionCallResult::Hold) && !allow_block {
            // If we reach here, it means that the plugin violates the API, it blocked the client even though it is not allow to.
            log::warn!(
                "Plugin API violation, plugin blocked the client even though blocking is forbiden."
            );
            return Err(RedisError::Str(
                "Clien got blocked when blocking is not allow",
            ));
        }
        Ok(RedisValue::NoReply)
    }
}

fn function_debug_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let debug_command_enabled = *(ENABLE_DEBUG_COMMAND.lock(ctx));
    if !debug_command_enabled {
        return Err(RedisError::Str("Debug command are disabled"));
    }
    let backend_name = args.next_arg()?.try_as_str()?;
    match backend_name {
        "panic_on_thread_pool" => {
            execute_on_pool(|| panic!("debug panic"));
            return Ok(RedisValue::SimpleStringStatic("OK"));
        }
        "allow_unsafe_redis_commands" => {
            get_globals_mut().allow_unsafe_redis_commands = true;
            return Ok(RedisValue::SimpleStringStatic("OK"));
        }
        "dump_pending_async_calls" => {
            return Ok(RedisValue::OrderedMap(
                get_globals()
                    .future_handlers
                    .iter()
                    .map(|(lib, v)| {
                        (
                            RedisValueKey::String(lib.to_owned()),
                            RedisValue::Array(
                                v.iter()
                                    .map(|v| {
                                        v.upgrade().map_or(RedisValue::Null, |v| {
                                            let v = v.lock(ctx);
                                            let res: Vec<String> = v
                                                .command
                                                .iter()
                                                .map(|v| {
                                                    String::from_utf8_lossy(v.as_slice())
                                                        .into_owned()
                                                })
                                                .collect();
                                            RedisValue::BulkString(res.join(" "))
                                        })
                                    })
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            ))
        }
        "help" => {
            return Ok(RedisValue::Array(
                vec![
                    RedisValue::BulkString("allow_unsafe_redis_commands - enable the option to execute unsafe redis commands from within a function.".to_string()),
                    RedisValue::BulkString("panic_on_thread_pool - panic on thread pool to check panic reaction.".to_string()),
                    RedisValue::BulkString("help - print this message.".to_string()),
                    RedisValue::BulkString("<engine> [...] - engine specific debug command.".to_string()),
                ]
            ));
        }
        _ => (),
    }
    let backend = get_backend(ctx, backend_name).map_or(
        Err(RedisError::String(format!(
            "Backend '{}' does not exists or not yet loaded",
            backend_name
        ))),
        Ok,
    )?;
    let mut has_errors = false;
    let args = args
        .map(|v| {
            let res = v.try_as_str();
            if res.is_err() {
                has_errors = true;
            }
            res
        })
        .collect::<Vec<Result<&str, RedisError>>>();
    if has_errors {
        return Err(RedisError::Str("Failed converting arguments to string"));
    }
    let args = args.into_iter().map(|v| v.unwrap()).collect::<Vec<&str>>();
    backend
        .debug(args.as_slice())
        .map_err(|e| RedisError::String(e.get_msg().to_string()))
}

fn on_stream_touched(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if is_master(ctx) {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_touched(ctx, event, key);
    }
}

fn generic_notification(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if event == "del" {
        let event = event.to_owned();
        let key = key.to_vec();
        ctx.add_post_notification_job(move |_ctx| {
            let stream_ctx = &mut get_globals_mut().stream_ctx;
            stream_ctx.on_stream_deleted(&event, &key);
        });
    }
}

fn key_space_notification(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if !is_master(ctx) {
        // do not fire notifications on slave
        return;
    }

    let globals = get_globals();
    if globals.avoid_key_space_notifications {
        return;
    }

    globals.notifications_ctx.on_key_touched(ctx, event, key)
}

fn scan_key_space_for_streams(ctx: &Context) {
    let mut mgmt_pool = get_globals().management_pool.lock(ctx);
    mgmt_pool
        .get_or_insert_with(|| ThreadPool::with_name("RGMgmtExecutor".to_owned(), 1))
        .execute(|| {
            let cursor = KeysCursor::new();
            let thread_ctx = ThreadSafeContext::default();
            loop {
                let guard = thread_ctx.lock();
                let ctx = &guard;
                let scanned = cursor.scan(ctx, &|ctx, key_name, key| {
                    let key_type = match key {
                        Some(k) => k.key_type(),
                        None => ctx.open_key(&key_name).key_type(),
                    };
                    if key_type == Stream {
                        get_globals_mut().stream_ctx.on_stream_touched(
                            ctx,
                            "created",
                            key_name.as_slice(),
                        );
                    }
                });
                if !scanned {
                    break;
                }
            }
        })
}

#[role_changed_event_handler]
fn on_role_changed(ctx: &Context, _role_changed: ServerRole) {
    // we should use `is_master` here and not `_role_changed` because `is_master` will also
    // return false in case its a read only master (replicaof PseudoSlaveReadonly)
    if is_master(ctx) {
        ctx.log_notice("Role changed to primary, initializing key scan to search for streams.");
        scan_key_space_for_streams(ctx);
    } else {
        log::info!("Role changed to replica, abort all async commands invocation.");
        let globals = get_globals_mut();
        globals.future_handlers.drain().for_each(|(_, v)| {
            v.iter()
                .filter_map(|v| v.upgrade())
                .for_each(|v| v.lock(ctx).abort(ctx))
        })
    }
}

#[loading_event_handler]
fn on_loading_event(ctx: &Context, loading_sub_event: LoadingSubevent) {
    match loading_sub_event {
        LoadingSubevent::RdbStarted
        | LoadingSubevent::AofStarted
        | LoadingSubevent::ReplStarted => {
            // clean the entire functions data
            ctx.log_notice("Got a loading start event, clear the entire functions data.");
            let globals = get_globals_mut();
            globals.libraries.lock().unwrap().clear();
            globals.stream_ctx.clear();

            // During loading we do not want to get any key space notifications
            globals.avoid_key_space_notifications = true;
        }
        LoadingSubevent::Ended | LoadingSubevent::Failed => {
            // re-enable key space notifications
            ctx.log_notice("Loading finished, re-enable key space notificaitons.");
            let globals = get_globals_mut();
            globals.avoid_key_space_notifications = false;
        }
    }
}

#[module_changed_event_handler]
fn on_module_change(ctx: &Context, _: ModuleChangeSubevent) {
    ctx.log_notice("Got module load event, try to reload modules API.");
    match redisai_rs::redisai_init(ctx) {
        Ok(_) => ctx.log_notice("RedisAI API was loaded successfully."),
        Err(_) => ctx.log_notice("Failed loading RedisAI API."),
    }
}

#[flush_event_handler]
fn on_flush_event(ctx: &Context, flush_event: FlushSubevent) {
    if let FlushSubevent::Started = flush_event {
        ctx.log_notice("Got a flush started event");
        let globals = get_globals_mut();
        for lib in globals.libraries.lock().unwrap().values() {
            for consumer in lib.gears_lib_ctx.stream_consumers.values() {
                let mut c = consumer.ref_cell.borrow_mut();
                c.clear_streams_info();
            }
        }
        globals.stream_ctx.clear_tracked_streams();
    }
}

#[config_changed_event_handler]
fn on_config_change(ctx: &Context, values: &[&str]) {
    if values
        .iter()
        .any(|v| *v == PSEUDO_SLAVE_READONLY_CONFIG_NAME || *v == PSEUDO_SLAVE_CONFIG_NAME)
    {
        let globals = get_globals_mut();
        globals.db_policy = get_db_policy(ctx);
    }
}

/// Will be called by Redis to execute some repeated tasks.
/// Currently we will clean future handlers that has been finised.
#[cron_event_handler]
fn cron_event_handler(ctx: &Context, _hz: u64) {
    let globals = get_globals_mut();
    globals.future_handlers = globals
        .future_handlers
        .drain()
        .filter_map(|(k, mut v)| {
            let res: Vec<_> = v
                .drain(0..)
                .filter_map(|v| {
                    let _ = v.upgrade()?;
                    Some(v)
                })
                .collect();
            if res.is_empty() {
                return None;
            }
            Some((k, res))
        })
        .collect();

    if globals.avoid_replication_traffic && !ctx.avoid_replication_traffic() {
        // avoid replication traffic was turned off, lets reinitiate stream processing.
        if is_master(ctx) {
            ctx.log_notice("Avoid replication traffic was disabled, initializing key scan to search for streams.");
            scan_key_space_for_streams(ctx);
        }
    }
    globals.avoid_replication_traffic = ctx.avoid_replication_traffic();
}

pub(crate) fn verify_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Empty name is not allowed".to_owned());
    }
    name.chars().try_for_each(|c| {
        if c.is_ascii_alphanumeric() || c == '_' {
            return Ok(());
        }
        Err(format!("Unallowed char was given '{c}'"))
    })
}

pub(crate) fn get_msg_verbose(err: &GearsApiError) -> &str {
    if ERROR_VERBOSITY.load(Ordering::Relaxed) == 1 {
        return err.get_msg();
    }
    err.get_msg_verbose()
}

#[command(
    {
        name: "tfcall",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -3,
        key_spec: [
            {
                flags: [ReadWrite, Access, Update],
                begin_search: Index({ index : 2}),
                find_keys: Keynum({ key_num_idx : 0, first_key : 1, key_step : 1 }),
            }
        ],
    }
)]
fn function_call(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let args = args.into_iter().skip(1);
    function_call_command(ctx, args, false)
}

#[command(
    {
        name: "tfcallasync",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -3,
        key_spec: [
            {
                flags: [ReadWrite, Access, Update],
                begin_search: Index({ index : 2}),
                find_keys: Keynum({ key_num_idx : 0, first_key : 1, key_step : 1 }),
            }
        ],
    }
)]
fn function_call_async(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let args = args.into_iter().skip(1);
    function_call_command(ctx, args, true)
}

#[command(
    {
        name: "_rg_internals.function",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -3,
        key_spec: [],
    }
)]
fn function_command_on_replica(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "load" => function_load_command::function_load_on_replica(ctx, args),
        "del" => function_del_command::function_del_on_replica(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

#[command(
    {
        name: "tfunction",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -2,
        key_spec: [],
    }
)]
fn function_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "load" => function_load_command::function_load_command(ctx, args),
        "list" => function_list_command::function_list_command(ctx, args),
        "delete" => function_del_command::function_del_command(ctx, args),
        "debug" => function_debug_command(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

#[command(
    {
        name: "_rg_internals.update_stream_last_read_id",
        flags: [ReadOnly, DenyScript, NoMandatoryKeys],
        arity: 6,
        key_spec: [],
    }
)]
fn update_stream_last_read_id(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let library_name = args.next_arg()?.try_as_str()?;
    let stream_consumer = args.next_arg()?.try_as_str()?;
    let stream_arg = args.next_arg()?;
    let stream = stream_arg.as_slice();
    let ms = args.next_arg()?.try_as_str()?.parse::<u64>()?;
    let seq = args.next_arg()?.try_as_str()?.parse::<u64>()?;
    let libraries = get_libraries();
    let library = libraries
        .get(library_name)
        .ok_or_else(|| RedisError::String(format!("No such library '{}'", library_name)))?;
    let consumer = library
        .gears_lib_ctx
        .stream_consumers
        .get(stream_consumer)
        .ok_or_else(|| RedisError::String(format!("No such consumer '{}'", stream_consumer)))?;
    get_globals_mut()
        .stream_ctx
        .update_stream_for_consumer(stream, consumer, ms, seq);
    ctx.replicate_verbatim();
    Ok(RedisValue::SimpleStringStatic("OK"))
}

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        RedisAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

#[allow(missing_docs)]
mod gears_module {
    use super::*;
    use config::{
        GEARS_BOX_ADDRESS, REMOTE_TASK_DEFAULT_TIMEOUT, V8_LIBRARY_INITIAL_MEMORY_LIMIT,
        V8_LIBRARY_INITIAL_MEMORY_USAGE, V8_LIBRARY_MEMORY_USAGE_DELTA, V8_MAX_MEMORY,
    };
    use rdb::REDIS_GEARS_TYPE;
    use redis_module::configuration::ConfigurationFlags;

    redis_module::redis_module! {
        name: "redisgears_2",
        version: VERSION_NUM.unwrap().parse::<i32>().unwrap(),
        allocator: (get_allocator!(), get_allocator!()),
        data_types: [REDIS_GEARS_TYPE],
        init: js_init,
        commands: [],
        event_handlers: [
            [@STREAM: on_stream_touched],
            [@GENERIC: generic_notification],
            [@ALL @MISSED: key_space_notification],
        ]
        configurations:[
            i64: [
                ["error-verbosity", &*ERROR_VERBOSITY ,1, 1, 2, ConfigurationFlags::DEFAULT, None],
                ["execution-threads", &*EXECUTION_THREADS ,1, 1, 32, ConfigurationFlags::IMMUTABLE, None],
                ["remote-task-default-timeout", &*REMOTE_TASK_DEFAULT_TIMEOUT , 500, 1, i64::MAX, ConfigurationFlags::DEFAULT, None],
                ["lock-redis-timeout", &*LOCK_REDIS_TIMEOUT , 500, 100, 1000000000, ConfigurationFlags::DEFAULT, None],
                ["db-loading-lock-redis-timeout", &*DB_LOADING_LOCK_REDIS_TIMEOUT , 30000, 100, 1000000000, ConfigurationFlags::DEFAULT, None],

                [
                    "v8-maxmemory",
                    &*V8_MAX_MEMORY,
                    byte_unit::n_mb_bytes!(200) as i64,
                    byte_unit::n_mb_bytes!(50) as i64,
                    byte_unit::n_gb_bytes!(1) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-initial-memory-usage",
                    &*V8_LIBRARY_INITIAL_MEMORY_USAGE,
                    byte_unit::n_mb_bytes!(2) as i64,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(10) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-initial-memory-limit",
                    &*V8_LIBRARY_INITIAL_MEMORY_LIMIT,
                    byte_unit::n_mb_bytes!(3) as i64,
                    byte_unit::n_mb_bytes!(2) as i64,
                    byte_unit::n_mb_bytes!(20) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-memory-usage-delta",
                    &*V8_LIBRARY_MEMORY_USAGE_DELTA,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(10) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
            ],
            string: [
                ["gearsbox-address", &*GEARS_BOX_ADDRESS , "http://localhost:3000", ConfigurationFlags::DEFAULT, None],
                ["v8-plugin-path", &*V8_PLUGIN_PATH , "libredisgears_v8_plugin.so", ConfigurationFlags::IMMUTABLE, None],
                ["v8-flags", &*V8_FLAGS, "'--noexpose-wasm'", ConfigurationFlags::IMMUTABLE, None],
            ],
            bool: [
                ["enable-debug-command", &*ENABLE_DEBUG_COMMAND , false, ConfigurationFlags::IMMUTABLE, None],
            ],
            enum: [
                ["library-fatal-failure-policy", &*FATAL_FAILURE_POLICY , config::FatalFailurePolicyConfiguration::Abort, ConfigurationFlags::DEFAULT, None],
            ],
            module_args_as_configuration: true,
            module_config_get: "TCONFIG_GET",
            module_config_set: "TCONFIG_SET",
        ]
    }
}
