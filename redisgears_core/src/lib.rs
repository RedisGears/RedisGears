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

use redis_module::redisvalue::RedisValueKey;
use redis_module::{CallResult, ContextFlags, ErrorReply};
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtxInterfaceInitialised;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;
use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
use serde::{Deserialize, Serialize};

use config::{
    FatalFailurePolicyConfiguration, ENABLE_DEBUG_COMMAND, ERROR_VERBOSITY, EXECUTION_THREADS,
    FATAL_FAILURE_POLICY, LOCK_REDIS_TIMEOUT, V8_PLUGIN_PATH,
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
    flush_event_handler, loading_event_handler, module_changed_event_handler, redis_command,
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
use std::sync::{Arc, Mutex, MutexGuard};

use crate::stream_reader::{ConsumerData, StreamReaderCtx};
use std::iter::Skip;
use std::vec::IntoIter;

use crate::compiled_library_api::CompiledLibraryInternals;
use crate::gears_box::{gears_box_search, GearsBoxLibraryInfo};
use crate::keys_notifications::{KeysNotificationsCtx, NotificationCallback, NotificationConsumer};
use crate::keys_notifications_ctx::KeysNotificationsRunCtx;
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
mod gears_box;
mod keys_notifications;
mod keys_notifications_ctx;
mod rdb;
mod run_ctx;
mod stream_reader;
mod stream_run_ctx;

/// GIT commit hash used for this build.
pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
/// GIT branch used for this build.
pub const GIT_BRANCH: Option<&str> = std::option_env!("GIT_BRANCH");
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

fn check_redis_version_compatible(ctx: &Context) -> Result<(), String> {
    use redis_module::Version;

    const VERSION: Version = Version {
        major: 7,
        minor: 1,
        patch: 240,
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
}

impl GearsFunctionCtx {
    fn new(
        func: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
        is_async: bool,
    ) -> GearsFunctionCtx {
        GearsFunctionCtx {
            func: func,
            flags,
            is_async,
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
    revert_stream_consumers: Vec<(String, GearsStreamConsumer, usize, bool)>,
    notifications_consumers: HashMap<String, Arc<RefCell<NotificationConsumer>>>,
    revert_notifications_consumers: Vec<(String, ConsumerKey, NotificationCallback)>,
    old_lib: Option<Arc<GearsLibrary>>,
}

struct GearsLoadLibraryCtx<'ctx, 'lib_ctx> {
    ctx: &'ctx Context,
    gears_lib_ctx: &'lib_ctx mut GearsLibraryCtx,
}

struct GearsLibrary {
    gears_lib_ctx: GearsLibraryCtx,
    _lib_ctx: Box<dyn LibraryCtxInterface>,
    compile_lib_internals: Arc<CompiledLibraryInternals>,
    gears_box_lib: Option<GearsBoxLibraryInfo>,
}

impl<'ctx, 'lib_ctx> GearsLoadLibraryCtx<'ctx, 'lib_ctx> {
    fn register_function_internal(
        &mut self,
        name: &str,
        func_ctx: GearsFunctionCtx,
    ) -> Result<(), GearsApiError> {
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
    ) -> Result<(), GearsApiError> {
        self.register_function_internal(name, GearsFunctionCtx::new(function_ctx, flags, false))
    }

    fn register_async_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
    ) -> Result<(), GearsApiError> {
        self.register_function_internal(name, GearsFunctionCtx::new(function_ctx, flags, true))
    }

    fn register_remote_task(
        &mut self,
        name: &str,
        remote_function_callback: RemoteFunctionCtx,
    ) -> Result<(), GearsApiError> {
        // TODO move to <https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.try_insert>
        // once stabilised.
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
    ) -> Result<(), GearsApiError> {
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
            self.gears_lib_ctx.revert_stream_consumers.push((
                name.to_string(),
                old_ctx,
                old_window,
                old_trim,
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
            );
            if self.ctx.get_flags().contains(ContextFlags::MASTER) {
                // trigger a key scan
                scan_key_space_for_streams();
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
    ) -> Result<(), GearsApiError> {
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
                let val = keys_notifications_consumer_ctx.on_notification_fired(
                    event,
                    key,
                    &KeysNotificationsRunCtx::new(ctx, meta_data.clone(), FunctionFlags::empty()),
                );
                keys_notifications_consumer_ctx.post_command_notification(
                    val,
                    &KeysNotificationsRunCtx::new(ctx, meta_data.clone(), FunctionFlags::empty()),
                    done_callback,
                )
            });

        let consumer = if let Some(old_notification_consumer) = self
            .gears_lib_ctx
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
            self.gears_lib_ctx.revert_notifications_consumers.push((
                name.to_string(),
                old_key,
                old_consumer_callback,
            ));
            Arc::clone(old_notification_consumer)
        } else {
            let globals = get_globals_mut();

            match key {
                RegisteredKeys::Key(k) => globals
                    .notifications_ctx
                    .add_consumer_on_key(k, fire_event_callback),
                RegisteredKeys::Prefix(p) => globals
                    .notifications_ctx
                    .add_consumer_on_prefix(p, fire_event_callback),
            }
        };

        self.gears_lib_ctx
            .notifications_consumers
            .insert(name.to_string(), consumer);
        Ok(())
    }
}

struct GlobalCtx {
    libraries: Mutex<HashMap<String, Arc<GearsLibrary>>>,
    backends: HashMap<String, Box<dyn BackendCtxInterfaceInitialised>>,
    plugins: Vec<Library>,
    pool: Option<Mutex<ThreadPool>>,
    mgmt_pool: ThreadPool,
    stream_ctx: StreamReaderCtx<GearsStreamRecord, GearsStreamConsumer>,
    notifications_ctx: KeysNotificationsCtx,
    avoid_key_space_notifications: bool,
    allow_unsafe_redis_commands: bool,
}

static mut GLOBALS: Option<GlobalCtx> = None;

pub(crate) struct NotificationBlocker;

pub(crate) fn get_notification_blocker() -> NotificationBlocker {
    get_globals_mut().avoid_key_space_notifications = true;
    NotificationBlocker
}

impl Drop for NotificationBlocker {
    fn drop(&mut self) {
        get_globals_mut().avoid_key_space_notifications = false;
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

fn get_libraries() -> MutexGuard<'static, HashMap<String, Arc<GearsLibrary>>> {
    get_globals().libraries.lock().unwrap()
}

pub(crate) fn get_thread_pool() -> &'static Mutex<ThreadPool> {
    get_globals().pool.as_ref().unwrap()
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
    get_thread_pool().lock().unwrap().execute(move || {
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
        .autenticate_user(user)
        .map_err(|e| ErrorReply::Message(e.to_string()))?;
    ctx.call_ext(command, call_options, args)
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
        "RedisGears v{}, sha='{}', branch='{}', build_type='{}', built_for='{}-{}.{}'.",
        VERSION_STR.unwrap_or_default(),
        GIT_SHA.unwrap_or_default(),
        GIT_BRANCH.unwrap_or_default(),
        BUILD_TYPE.unwrap_or_default(),
        BUILD_OS.unwrap_or_default(),
        BUILD_OS_NICK.unwrap_or_default(),
        BUILD_OS_ARCH.unwrap_or_default()
    );
    if let Err(e) = check_redis_version_compatible(ctx) {
        log::warn!("{e}");
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
    let mgmt_pool = ThreadPool::new(1);
    let mut global_ctx = GlobalCtx {
        libraries: Mutex::new(HashMap::new()),
        backends: HashMap::new(),
        plugins: Vec::new(),
        pool: None,
        mgmt_pool,
        stream_ctx: StreamReaderCtx::new(
            Box::new(|ctx, key, id, include_id| {
                // read data from the stream
                if !ctx.get_flags().contains(ContextFlags::MASTER) {
                    return Err("Can not read data on replica".to_string());
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
                if !ctx.get_flags().contains(ContextFlags::MASTER) {
                    ctx.log_warning("Attempt to trim data on replica was denied.");
                    return;
                }
                let stream_name = ctx.create_string(key_name);
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
                            key_name,
                            "MINID".as_bytes(),
                            format!("{}-{}", id.ms, id.seq).as_bytes(),
                        ],
                    );
                }
            }),
        ),
        notifications_ctx: KeysNotificationsCtx::new(),
        avoid_key_space_notifications: false,
        allow_unsafe_redis_commands: false,
    };

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

    let lib = match unsafe { Library::new(&v8_path) } {
        Ok(l) => l,
        Err(e) => {
            ctx.log_warning(&format!("Failed loading '{}', {}", v8_path, e));
            return Status::Err;
        }
    };
    {
        let func: Symbol<unsafe fn(&Context) -> *mut dyn BackendCtxInterfaceUninitialised> =
            unsafe { lib.get(b"initialize_plugin") }.unwrap();
        let backend = unsafe { Box::from_raw(func(ctx)) };
        let name = backend.get_name();
        if global_ctx.backends.contains_key(name) {
            ctx.log_warning(&format!("Backend {} already exists", name));
            return Status::Err;
        }
        let initialised_backend = match backend.initialize(BackendCtx {
            allocator: &RedisAlloc,
            log: Box::new(|msg| log::info!("{msg}")),
            get_on_oom_policy: Box::new(|| match *FATAL_FAILURE_POLICY.lock().unwrap() {
                FatalFailurePolicyConfiguration::Abort => LibraryFatalFailurePolicy::Abort,
                FatalFailurePolicyConfiguration::Kill => LibraryFatalFailurePolicy::Kill,
            }),
            get_lock_timeout: Box::new(|| LOCK_REDIS_TIMEOUT.load(Ordering::Relaxed) as u128),
        }) {
            Ok(b) => b,
            Err(e) => {
                ctx.log_warning(&format!("Failed loading {} backend, {}", name, e.get_msg()));
                return Status::Err;
            }
        };
        let version = initialised_backend.get_version();
        ctx.log_notice(&format!("Registered backend: {name}, {version}."));
        global_ctx
            .backends
            .insert(name.to_string(), initialised_backend);
    }
    global_ctx.plugins.push(lib);

    unsafe { GLOBALS = Some(global_ctx) };

    let globals = get_globals_mut();
    globals.pool = Some(Mutex::new(ThreadPool::new(
        (*EXECUTION_THREADS.lock(ctx)) as usize,
    )));

    Status::Ok
}

const fn js_info(_ctx: &InfoContext, _for_crash_report: bool) {}

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

/// Returns `true` if the function with the specified flags is allowed
/// to run on replicas in case the current instance is a replica.
pub(crate) fn verify_ok_on_replica(ctx: &Context, flags: FunctionFlags) -> bool {
    // not replica, ok to run || we can run function with no writes on replica.
    ctx.get_flags().contains(ContextFlags::MASTER) || flags.contains(FunctionFlags::NO_WRITES)
}

fn function_call_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
    allow_block: bool,
) -> RedisResult {
    let library_name = args.next_arg()?.try_as_str()?;
    let function_name = args.next_arg()?.try_as_str()?;
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
            "Err can not run a function that might perform writes on a replica",
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
        return Err(RedisError::Str("Function is declated async and was called while blocking is not allowed, notice that you can not invoke async functions from within lua or multi, and you must use RG.FCALLASYNC."));
    }

    {
        let _notification_blocker = get_notification_blocker();
        let res = function.func.call(&RunCtx {
            ctx,
            args,
            flags: function.flags,
            lib_meta_data: Arc::clone(&lib.gears_lib_ctx.meta_data),
            allow_block: allow_block,
        });
        if let FunctionCallResult::Hold = res {
            if !allow_block {
                // If we reach here, it means that the plugin violates the API, it blocked the client even though it is not allow to.
                DETACHED_CONTEXT.log_warning("Plugin API violation, plugin blocked the client even though blocking is forbiden.");
                return Err(RedisError::Str(
                    "Clien got blocked when blocking is not allow",
                ));
            }
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
    let backend = get_backends_mut().get_mut(backend_name).map_or(
        Err(RedisError::String(format!(
            "Backend '{}' does not exists",
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

pub(crate) fn json_to_redis_value(val: serde_json::Value) -> RedisValue {
    match val {
        serde_json::Value::Bool(b) => RedisValue::Integer(if b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                RedisValue::Integer(n.as_i64().unwrap())
            } else {
                RedisValue::BulkString(n.as_f64().unwrap().to_string())
            }
        }
        serde_json::Value::String(s) => RedisValue::BulkString(s),
        serde_json::Value::Null => RedisValue::Null,
        serde_json::Value::Array(a) => {
            let mut res = Vec::new();
            for v in a {
                res.push(json_to_redis_value(v));
            }
            RedisValue::Array(res)
        }
        serde_json::Value::Object(o) => RedisValue::Map(
            o.into_iter()
                .map(|(k, v)| (RedisValueKey::String(k), json_to_redis_value(v)))
                .collect(),
        ),
    }
}

fn function_search_lib_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let search_token = args.next_arg()?.try_as_str()?;
    let search_result = gears_box_search(ctx, search_token)?;
    Ok(json_to_redis_value(search_result))
}

fn on_stream_touched(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if ctx.get_flags().contains(ContextFlags::MASTER) {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_touched(ctx, event, key);
    }
}

fn generic_notification(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if event == "del" {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_deleted(event, key);
    }
}

fn key_space_notification(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    let globals = get_globals();
    if globals.avoid_key_space_notifications {
        return;
    }
    globals.notifications_ctx.on_key_touched(ctx, event, key)
}

fn scan_key_space_for_streams() {
    get_globals().mgmt_pool.execute(|| {
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
fn on_role_changed(ctx: &Context, role_changed: ServerRole) {
    if let ServerRole::Primary = role_changed {
        ctx.log_notice("Role changed to primary, initializing key scan to search for streams.");
        scan_key_space_for_streams();
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
        }
        _ => {}
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

pub(crate) fn get_msg_verbose(err: &GearsApiError) -> &str {
    if ERROR_VERBOSITY.load(Ordering::Relaxed) == 1 {
        return err.get_msg();
    }
    err.get_msg_verbose()
}

#[redis_command(
    {
        name: "rg.fcall",
        flags: "may-replicate deny-script no-mandatory-keys",
        arity: -4,
        key_spec: [
            {
                flags: ["RW", "ACCESS", "UPDATE"],
                begin_search: Index(3),
                find_keys: Keynum((0, 1, 1)),
            }
        ],
    }
)]
fn function_call(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let args = args.into_iter().skip(1);
    function_call_command(ctx, args, false)
}

#[redis_command(
    {
        name: "rg.fcallasync",
        flags: "may-replicate deny-script no-mandatory-keys",
        arity: -4,
        key_spec: [
            {
                flags: ["RW", "ACCESS", "UPDATE"],
                begin_search: Index(3),
                find_keys: Keynum((0, 1, 1)),
            }
        ],
    }
)]
fn function_call_async(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let args = args.into_iter().skip(1);
    function_call_command(ctx, args, true)
}

#[redis_command(
    {
        name: "_rg_internals.function",
        flags: "may-replicate deny-script no-mandatory-keys",
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

#[redis_command(
    {
        name: "rg.function",
        flags: "may-replicate deny-script no-mandatory-keys",
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
        "del" => function_del_command::function_del_command(ctx, args),
        "debug" => function_debug_command(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

#[redis_command(
    {
        name: "rg.box",
        flags: "may-replicate deny-script no-mandatory-keys",
        arity: -3,
        key_spec: [],
    }
)]
fn gears_box_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "search" => function_search_lib_command(ctx, args),
        "install" => function_load_command::function_install_lib_command(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

#[redis_command(
    {
        name: "_rg_internals.update_stream_last_read_id",
        flags: "readonly deny-script no-mandatory-keys",
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
    let library = libraries.get(library_name);
    if library.is_none() {
        return Err(RedisError::String(format!(
            "No such library '{}'",
            library_name
        )));
    }
    let library = library.unwrap();
    let consumer = library.gears_lib_ctx.stream_consumers.get(stream_consumer);
    if consumer.is_none() {
        return Err(RedisError::String(format!(
            "No such consumer '{}'",
            stream_consumer
        )));
    }
    let consumer = consumer.unwrap();
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
    use config::{GEARS_BOX_ADDRESS, LIBRARY_MAX_MEMORY, REMOTE_TASK_DEFAULT_TIMEOUT};
    use rdb::REDIS_GEARS_TYPE;
    use redis_module::configuration::ConfigurationFlags;

    redis_module::redis_module! {
        name: "redisgears_2",
        version: VERSION_NUM.unwrap().parse::<i32>().unwrap(),
        allocator: (get_allocator!(), get_allocator!()),
        data_types: [REDIS_GEARS_TYPE],
        init: js_init,
        info: js_info,
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
                ["library-maxmemory", &*LIBRARY_MAX_MEMORY , 1024 * 1024 * 1024, 16 * 1024 * 1024, 2 * 1024 * 1024 * 1024, ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE, None],
                ["lock-redis-timeout", &*LOCK_REDIS_TIMEOUT , 500, 100, 1000000000, ConfigurationFlags::DEFAULT, None],
            ],
            string: [
                ["gearsbox-address", &*GEARS_BOX_ADDRESS , "http://localhost:3000", ConfigurationFlags::DEFAULT, None],
                ["v8-plugin-path", &*V8_PLUGIN_PATH , "libredisgears_v8_plugin.so", ConfigurationFlags::IMMUTABLE, None],
            ],
            bool: [
                ["enable-debug-command", &*ENABLE_DEBUG_COMMAND , false, ConfigurationFlags::IMMUTABLE, None],
            ],
            enum: [
                ["library-fatal-failure-policy", &*FATAL_FAILURE_POLICY , config::FatalFailurePolicyConfiguration::Abort, ConfigurationFlags::DEFAULT, None],
            ],
            module_args_as_configuration: true,
            module_config_get: "RG.CONFIG_GET",
            module_config_set: "RG.CONFIG_SET",
        ]
    }
}
