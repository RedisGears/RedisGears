#[macro_use]
extern crate serde_derive;

extern crate redis_module;

use redis_module::raw::{RedisModule_GetDetachedThreadSafeContext, RedisModule__Assert};
use threadpool::ThreadPool;

use redis_module::{
    context::keys_cursor::KeysCursor, context::server_events::FlushSubevent,
    context::server_events::LoadingSubevent, context::server_events::ServerEventData,
    context::server_events::ServerRole, context::AclPermissions, context::CallOptions,
    raw::KeyType::Stream, redis_command, redis_event_handler, redis_module, Context, InfoContext,
    NextArg, NotifyEvent, RedisError, RedisResult, RedisString, RedisValue, Status,
    ThreadSafeContext,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, backend_ctx::BackendCtxInterface, function_ctx::FunctionCtxInterface,
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    load_library_ctx::LibraryCtxInterface, load_library_ctx::LoadLibraryCtxInterface,
    load_library_ctx::RegisteredKeys, load_library_ctx::RemoteFunctionCtx,
    load_library_ctx::FUNCTION_FLAG_ALLOW_OOM, load_library_ctx::FUNCTION_FLAG_NO_WRITES,
    stream_ctx::StreamCtxInterface, CallResult, GearsApiError,
};

use redisgears_plugin_api::redisgears_plugin_api::RefCellWrapper;

use crate::run_ctx::RunCtx;

use libloading::{Library, Symbol};

use std::collections::{HashMap, HashSet};

use std::sync::{Arc, Mutex, MutexGuard};

use crate::stream_reader::{ConsumerData, StreamReaderCtx};
use std::iter::Skip;
use std::vec::IntoIter;

use crate::compiled_library_api::CompiledLibraryInternals;
use crate::gears_box::{gears_box_search, GearsBoxLibraryInfo};
use crate::keys_notifications::{KeysNotificationsCtx, NotificationCallback, NotificationConsumer};
use crate::keys_notifications_ctx::KeysNotificationsRunCtx;
use crate::stream_run_ctx::{GearsStreamConsumer, GearsStreamRecord};

use crate::config::Config;

use rdb::REDIS_GEARS_TYPE;

use std::cell::RefCell;

use crate::keys_notifications::ConsumerKey;

use mr::libmr::{mr_init, record::Record, remote_task::RemoteTask};

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

pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
pub const GIT_BRANCH: Option<&str> = std::option_env!("GIT_BRANCH");
pub const VERSION_STR: Option<&str> = std::option_env!("VERSION_STR");
pub const VERSION_NUM: Option<&str> = std::option_env!("VERSION_NUM");
pub const BUILD_OS: Option<&str> = std::option_env!("BUILD_OS");
pub const BUILD_OS_TYPE: Option<&str> = std::option_env!("BUILD_OS_TYPE");
pub const BUILD_OS_VERSION: Option<&str> = std::option_env!("BUILD_OS_VERSION");
pub const BUILD_OS_ARCH: Option<&str> = std::option_env!("BUILD_OS_ARCH");
pub const BUILD_TYPE: Option<&str> = std::option_env!("BUILD_TYPE");

pub struct GearsLibraryMataData {
    name: String,
    engine: String,
    code: String,
    config: Option<String>,
    user: String,
}

struct GearsFunctionCtx {
    func: Box<dyn FunctionCtxInterface>,
    flags: u8,
}

impl GearsFunctionCtx {
    fn new(func: Box<dyn FunctionCtxInterface>, flags: u8) -> GearsFunctionCtx {
        GearsFunctionCtx { func, flags }
    }
}

struct GearsLibraryCtx {
    meta_data: Arc<GearsLibraryMataData>,
    functions: HashMap<String, GearsFunctionCtx>,
    remote_functions: HashMap<String, RemoteFunctionCtx>,
    stream_consumers:
        HashMap<String, Arc<RefCellWrapper<ConsumerData<GearsStreamRecord, GearsStreamConsumer>>>>,
    revert_stream_consumers: Vec<(String, GearsStreamConsumer, usize, bool)>,
    notifications_consumers: HashMap<String, Arc<RefCell<NotificationConsumer>>>,
    revert_notifications_consumers: Vec<(String, ConsumerKey, NotificationCallback)>,
    old_lib: Option<Arc<GearsLibrary>>,
}

struct GearsLibrary {
    gears_lib_ctx: GearsLibraryCtx,
    _lib_ctx: Box<dyn LibraryCtxInterface>,
    compile_lib_internals: Arc<CompiledLibraryInternals>,
    gears_box_lib: Option<GearsBoxLibraryInfo>,
}

fn redis_value_to_call_reply(r: RedisValue) -> CallResult {
    match r {
        RedisValue::SimpleString(s) => CallResult::SimpleStr(s),
        RedisValue::SimpleStringStatic(s) => CallResult::SimpleStr(s.to_string()),
        RedisValue::BulkString(s) => CallResult::BulkStr(s),
        RedisValue::BulkRedisString(s) => {
            let slice = s.as_slice().to_vec();
            CallResult::StringBuffer(slice)
        }
        RedisValue::StringBuffer(s) => CallResult::StringBuffer(s),
        RedisValue::Integer(i) => CallResult::Long(i),
        RedisValue::Float(f) => CallResult::Double(f),
        RedisValue::Array(a) => {
            let res = a
                .into_iter()
                .map(redis_value_to_call_reply)
                .collect::<Vec<CallResult>>();
            CallResult::Array(res)
        }
        RedisValue::Map(m) => {
            let mut map = HashMap::new();
            for (k, v) in m {
                map.insert(k, redis_value_to_call_reply(v));
            }
            CallResult::Map(map)
        }
        RedisValue::Set(s) => {
            let mut set = HashSet::new();
            for v in s {
                set.insert(v);
            }
            CallResult::Set(set)
        }
        RedisValue::Bool(b) => CallResult::Bool(b),
        RedisValue::Double(d) => CallResult::Double(d),
        RedisValue::BigNumber(s) => CallResult::BigNumber(s),
        RedisValue::VerbatimString((t, s)) => CallResult::VerbatimString((t, s)),

        RedisValue::Null => CallResult::Null,
        _ => panic!("not yet supported"),
    }
}

impl LoadLibraryCtxInterface for GearsLibraryCtx {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: u8,
    ) -> Result<(), GearsApiError> {
        if self.functions.contains_key(name) {
            return Err(GearsApiError::Msg(format!(
                "Function {} already exists",
                name
            )));
        }
        let func_ctx = GearsFunctionCtx::new(function_ctx, flags);
        self.functions.insert(name.to_string(), func_ctx);
        Ok(())
    }

    fn register_remote_task(
        &mut self,
        name: &str,
        remote_function_callback: RemoteFunctionCtx,
    ) -> Result<(), GearsApiError> {
        if self.remote_functions.contains_key(name) {
            return Err(GearsApiError::Msg(format!(
                "Remote function {} already exists",
                name
            )));
        }
        self.remote_functions
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
        if self.stream_consumers.contains_key(name) {
            return Err(GearsApiError::Msg(
                "Stream registration already exists".to_string(),
            ));
        }

        let stream_registration = if let Some(old_consumer) = self
            .old_lib
            .as_ref()
            .and_then(|v| v.gears_lib_ctx.stream_consumers.get(name))
        {
            let mut o_c = old_consumer.ref_cell.borrow_mut();
            if o_c.prefix != prefix {
                return Err(GearsApiError::Msg(
                    format!("Can not upgrade an existing consumer with different prefix, consumer: '{}', old_prefix: {}, new_prefix: {}.",
                    name, std::str::from_utf8(&o_c.prefix).unwrap_or("[binary data]"), std::str::from_utf8(prefix).unwrap_or("[binary data]"))
                ));
            }
            let old_ctx = o_c.set_consumer(GearsStreamConsumer::new(&self.meta_data, 0, ctx));
            let old_window = o_c.set_window(window);
            let old_trim = o_c.set_trim(trim);
            self.revert_stream_consumers
                .push((name.to_string(), old_ctx, old_window, old_trim));
            Arc::clone(old_consumer)
        } else {
            let globals = get_globals_mut();
            let stream_ctx = &mut globals.stream_ctx;
            let lib_name = self.meta_data.name.clone();
            let consumer_name = name.to_string();
            let consumer = stream_ctx.add_consumer(
                prefix,
                GearsStreamConsumer::new(&self.meta_data, 0, ctx),
                window,
                trim,
                Some(Box::new(move |stream_name, ms, seq| {
                    redis_module::replicate_slices(
                        get_ctx().ctx,
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
            if get_ctx().is_primary() {
                // trigger a key scan
                scan_key_space_for_streams();
            }
            consumer
        };

        self.stream_consumers
            .insert(name.to_string(), stream_registration);
        Ok(())
    }

    fn register_key_space_notification_consumer(
        &mut self,
        name: &str,
        key: RegisteredKeys,
        keys_notifications_consumer_ctx: Box<dyn KeysNotificationsConsumerCtxInterface>,
    ) -> Result<(), GearsApiError> {
        if self.notifications_consumers.contains_key(name) {
            return Err(GearsApiError::Msg(
                "Notification consumer already exists".to_string(),
            ));
        }

        let meta_data = Arc::clone(&self.meta_data);
        let mut permissions = AclPermissions::new();
        permissions.add_full_permission();
        let fire_event_callback: NotificationCallback =
            Box::new(move |event, key, done_callback| {
                let key_redis_str = RedisString::create_from_slice(std::ptr::null_mut(), key);
                if let Err(e) = get_ctx().acl_check_key_permission(
                    &meta_data.user,
                    &key_redis_str,
                    &permissions,
                ) {
                    done_callback(Err(format!(
                        "User '{}' has no permissions on key '{}', {}.",
                        meta_data.user,
                        std::str::from_utf8(key).unwrap_or("[binary data]"),
                        e
                    )));
                    return;
                }
                let _notification_blocker = get_notification_blocker();
                let val = keys_notifications_consumer_ctx.on_notification_fired(
                    event,
                    key,
                    Box::new(KeysNotificationsRunCtx::new(&meta_data, 0)),
                );
                keys_notifications_consumer_ctx.post_command_notification(
                    val,
                    Box::new(KeysNotificationsRunCtx::new(&meta_data, 0)),
                    done_callback,
                )
            });

        let consumer = if let Some(old_notification_consumer) = self
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
            self.revert_notifications_consumers.push((
                name.to_string(),
                old_key,
                old_consumer_callback,
            ));
            Arc::clone(old_notification_consumer)
        } else {
            let globlas = get_globals_mut();

            match key {
                RegisteredKeys::Key(k) => globlas
                    .notifications_ctx
                    .add_consumer_on_key(k, fire_event_callback),
                RegisteredKeys::Prefix(p) => globlas
                    .notifications_ctx
                    .add_consumer_on_prefix(p, fire_event_callback),
            }
        };

        self.notifications_consumers
            .insert(name.to_string(), consumer);
        Ok(())
    }
}

struct GlobalCtx {
    libraries: Mutex<HashMap<String, Arc<GearsLibrary>>>,
    backends: HashMap<String, Box<dyn BackendCtxInterface>>,
    redis_ctx: Context,
    authenticated_redis_ctx: Context,
    plugins: Vec<Library>,
    pool: Option<Mutex<ThreadPool>>,
    mgmt_pool: ThreadPool,
    stream_ctx: StreamReaderCtx<GearsStreamRecord, GearsStreamConsumer>,
    notifications_ctx: KeysNotificationsCtx,
    config: Config,
    avoid_key_space_notifications: bool,
    allow_unsafe_redis_commands: bool,
}

static mut GLOBALS: Option<GlobalCtx> = None;

pub(crate) struct NotificationBlocker;

pub(crate) fn get_notification_blocker() -> NotificationBlocker {
    get_globals_mut().avoid_key_space_notifications = true;
    NotificationBlocker
}

impl<'a> Drop for NotificationBlocker {
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

pub fn get_ctx() -> &'static Context {
    &get_globals().redis_ctx
}

fn get_backends_mut() -> &'static mut HashMap<String, Box<dyn BackendCtxInterface>> {
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

pub(crate) fn execute_on_pool<F: FnOnce() + Send + 'static>(job: F) {
    get_thread_pool().lock().unwrap().execute(move || {
        job();
    });
}

pub(crate) fn call_redis_command(
    user: Option<&String>,
    command: &str,
    call_options: &CallOptions,
    args: &[&[u8]],
) -> CallResult {
    let ctx = match user {
        Some(u) => {
            let ctx = &get_globals().authenticated_redis_ctx;
            if ctx.autenticate_user(u) == Status::Err {
                return CallResult::Error("Failed authenticating client".to_string());
            }
            ctx
        }
        None => get_ctx(),
    };
    let res = ctx.call_ext(command, call_options, args);
    match res {
        Ok(r) => redis_value_to_call_reply(r),
        Err(e) => match e {
            RedisError::Str(s) => CallResult::Error(s.to_string()),
            RedisError::String(s) => CallResult::Error(s),
            RedisError::WrongArity => CallResult::Error("Wrong arity".to_string()),
            RedisError::WrongType => CallResult::Error("Wrong type".to_string()),
        },
    }
}

fn js_post_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    let mut args = args.iter().skip(1); // skip the plugin
    while let Some(config_key) = args.next() {
        let key = match config_key.try_as_str() {
            Ok(s) => s,
            Err(e) => {
                ctx.log_warning(&format!("Can not convert config key to str, {}.", e));
                return Status::Err;
            }
        };
        let config_val = match args.next() {
            Some(s) => s,
            None => {
                ctx.log_warning(&format!("Config name '{}' has not value", key));
                return Status::Err;
            }
        };
        let val = match config_val.try_as_str() {
            Ok(s) => s,
            Err(e) => {
                ctx.log_warning(&format!(
                    "Can not convert config value for key '{}' to str, {}.",
                    key, e
                ));
                return Status::Err;
            }
        };
        if let Err(e) = get_globals_mut().config.initial_set(key, val) {
            ctx.log_warning(&format!(
                "Failed setting configuration '{}' with value '{}', {}.",
                key, val, e
            ));
            return Status::Err;
        }
    }
    let globals = get_globals_mut();
    globals.pool = Some(Mutex::new(ThreadPool::new(
        globals.config.execution_threads.size,
    )));
    Status::Ok
}

fn js_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    mr_init(ctx, 1);
    background_run_ctx::GearsRemoteFunctionInputsRecord::register();
    background_run_ctx::GearsRemoteFunctionOutputRecord::register();
    background_run_ctx::GearsRemoteTask::register();

    function_load_command::GearsFunctionLoadInputRecord::register();
    function_load_command::GearsFunctionLoadOutputRecord::register();
    function_load_command::GearsFunctionLoadRemoteTask::register();

    function_del_command::GearsFunctionDelInputRecord::register();
    function_del_command::GearsFunctionDelOutputRecord::register();
    function_del_command::GearsFunctionDelRemoteTask::register();

    match redisai_rs::redisai_init(ctx) {
        Ok(_) => ctx.log_notice("RedisAI API was loaded successfully."),
        Err(_) => ctx.log_notice("Failed loading RedisAI API."),
    }

    ctx.log_notice(&format!(
        "RedisGears v{}, sha='{}', branch='{}', build_type='{}', built_on='{}-{}.{}-{}'.",
        VERSION_STR.unwrap_or_default(),
        GIT_SHA.unwrap_or_default(),
        GIT_BRANCH.unwrap_or_default(),
        BUILD_TYPE.unwrap_or_default(),
        BUILD_OS.unwrap_or_default(),
        BUILD_OS_TYPE.unwrap_or_default(),
        BUILD_OS_VERSION.unwrap_or_default(),
        BUILD_OS_ARCH.unwrap_or_default()
    ));
    match ctx.get_redis_version() {
        Ok(v) => {
            if v.major < 7 || (v.major == 7 && v.minor == 0 && v.patch < 3) {
                ctx.log_warning("Redis version must be 7.0.3 or greater");
                return Status::Err;
            }
        }
        Err(e) => {
            ctx.log_warning(&format!("Failed getting Redis version, version is probably to old, please use Redis 7.0 or above. {}", e));
            return Status::Err;
        }
    }
    std::panic::set_hook(Box::new(|panic_info| {
        get_ctx().log_warning(&format!("Application paniced, {}", panic_info));
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
    unsafe {
        let inner_ctx = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let inner_autenticated_ctx = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let mut global_ctx = GlobalCtx {
            libraries: Mutex::new(HashMap::new()),
            redis_ctx: Context::new(inner_ctx),
            authenticated_redis_ctx: Context::new(inner_autenticated_ctx),
            backends: HashMap::new(),
            plugins: Vec::new(),
            pool: None,
            mgmt_pool,
            stream_ctx: StreamReaderCtx::new(
                Box::new(|key, id, include_id| {
                    // read data from the stream
                    let ctx = get_ctx();
                    if !ctx.is_primary() {
                        return Err("Can not read data on replica".to_string());
                    }
                    let stream_name = ctx.create_string_from_slice(key);
                    let key = ctx.open_key(&stream_name);
                    let mut stream_iterator =
                        match key.get_stream_range_iterator(id, None, !include_id) {
                            Ok(s) => s,
                            Err(_) => {
                                return Err("Key does not exists on is not a stream".to_string())
                            }
                        };

                    Ok(stream_iterator
                        .next()
                        .map(|e| GearsStreamRecord { record: e }))
                }),
                Box::new(|key_name, id| {
                    // trim the stream callback
                    let ctx = get_ctx();
                    if !ctx.is_primary() {
                        ctx.log_warning("Attempt to trim data on replica was denied.");
                        return;
                    }
                    let stream_name = ctx.create_string_from_slice(key_name);
                    let key = ctx.open_key_writable(&stream_name);
                    let res = key.trim_stream_by_id(id, false);
                    if let Err(e) = res {
                        ctx.log_debug(&format!(
                            "Error occured when trimming stream (stream was probably deleted): {}",
                            e
                        ))
                    } else {
                        redis_module::replicate_slices(
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
            config: Config::new(),
            avoid_key_space_notifications: false,
            allow_unsafe_redis_commands: false,
        };

        let v8_path = match args.iter().next() {
            Some(a) => a,
            None => {
                ctx.log_warning("Path to libredisgears_v8_plugin.so must be specified");
                return Status::Err;
            }
        }
        .try_as_str();
        let v8_path = match v8_path {
            Ok(a) => a,
            Err(_) => {
                ctx.log_warning("Path to libredisgears_v8_plugin.so must be specified");
                return Status::Err;
            }
        };

        let v8_path = match std::env::var("modulesdatadir") {
            Ok(val) => format!("{}/redisgears_2/{}/deps/gears_v8/{}", val, VERSION_NUM.unwrap(), v8_path),
            Err(_) => v8_path.to_string(),
        };

        let lib = match Library::new(&v8_path) {
            Ok(l) => l,
            Err(e) => {
                ctx.log_warning(&format!("Failed loading '{}', {}", v8_path, e));
                return Status::Err;
            }
        };
        {
            let func: Symbol<unsafe fn() -> *mut dyn BackendCtxInterface> =
                lib.get(b"initialize_plugin").unwrap();
            let backend = Box::from_raw(func());
            let name = backend.get_name();
            ctx.log_notice(&format!("registering backend: {}", name));
            if global_ctx.backends.contains_key(name) {
                ctx.log_warning(&format!("Backend {} already exists", name));
                return Status::Err;
            }
            if let Err(e) = backend.initialize(BackendCtx {
                allocator: &redis_module::ALLOC,
                log: Box::new(|msg| get_ctx().log_notice(msg)),
                get_on_oom_policy: Box::new(|| {
                    get_globals()
                        .config
                        .libraray_fatal_failure_policy
                        .policy
                        .clone()
                }),
                get_lock_timeout: Box::new(|| get_globals().config.lock_regis_timeout.size),
            }) {
                ctx.log_warning(&format!("Failed loading {} backend, {}", name, e.get_msg()));
                return Status::Err;
            }
            global_ctx.backends.insert(name.to_string(), backend);
        }
        global_ctx.plugins.push(lib);

        GLOBALS = Some(global_ctx);
    }
    Status::Ok
}

const fn js_info(_ctx: &InfoContext, _for_crash_report: bool) {}

pub(crate) fn verify_oom(flags: u8) -> bool {
    if (flags & FUNCTION_FLAG_NO_WRITES) == 0 {
        // function can potentially write data, make sure we are not OOM.
        if (flags & FUNCTION_FLAG_ALLOW_OOM) == 0 && get_ctx().is_oom() {
            return false;
        }
    }
    true
}

pub(crate) fn verify_ok_on_replica(flags: u8) -> bool {
    let ctx = get_ctx();
    if ctx.is_primary() {
        // not replica, ok to run.
        return true;
    }
    if (flags & FUNCTION_FLAG_NO_WRITES) != 0 {
        // we can run functions with no-writes flag on replica
        return true;
    }
    false
}

fn function_call_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let library_name = args.next_arg()?.try_as_str()?;
    let function_name = args.next_arg()?.try_as_str()?;
    let num_keys = args.next_arg()?.try_as_str()?.parse::<usize>()?;
    let libraries = get_libraries();

    let lib = libraries.get(library_name);
    if lib.is_none() {
        return Err(RedisError::String(format!(
            "Unknown library {}",
            library_name
        )));
    }

    let lib = lib.unwrap();
    let function = lib.gears_lib_ctx.functions.get(function_name);
    if function.is_none() {
        return Err(RedisError::String(format!(
            "Unknown function {}",
            function_name
        )));
    }

    let function = function.unwrap();

    if !verify_ok_on_replica(function.flags) {
        return Err(RedisError::Str(
            "Err can not run a function that might perform writes on a replica",
        ));
    }

    if !verify_oom(function.flags) {
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
    let args_iter = args.iter();

    {
        let _notification_blocker = get_notification_blocker();
        function.func.call(&mut RunCtx {
            ctx,
            iter: args_iter,
            flags: function.flags,
            lib_meta_data: Arc::clone(&lib.gears_lib_ctx.meta_data),
        });
    }

    Ok(RedisValue::NoReply)
}

fn function_call_result_to_redis_result(res: CallResult) -> RedisValue {
    match res {
        CallResult::Long(l) => RedisValue::Integer(l),
        CallResult::BulkStr(s) => RedisValue::BulkString(s),
        CallResult::SimpleStr(s) => RedisValue::SimpleString(s),
        CallResult::Null => RedisValue::Null,
        CallResult::Double(d) => RedisValue::Float(d),
        CallResult::Error(s) => RedisValue::SimpleString(s),
        CallResult::Array(arr) => RedisValue::Array(
            arr.into_iter()
                .map(function_call_result_to_redis_result)
                .collect::<Vec<RedisValue>>(),
        ),
        _ => panic!("not yet supported"),
    }
}

fn function_debug_command(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    if !get_globals().config.enable_debug_command.enabled {
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
    let res = backend.debug(args.as_slice());
    match res {
        Ok(res) => Ok(function_call_result_to_redis_result(res)),
        Err(e) => match e {
            GearsApiError::Msg(msg) => Err(RedisError::String(msg)),
        },
    }
}

pub(crate) fn to_redis_value(val: serde_json::Value) -> RedisValue {
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
                res.push(to_redis_value(v));
            }
            RedisValue::Array(res)
        }
        serde_json::Value::Object(o) => {
            let mut res = Vec::new();
            for (k, v) in o.into_iter() {
                res.push(RedisValue::BulkString(k));
                res.push(to_redis_value(v));
            }
            RedisValue::Array(res)
        }
    }
}

fn function_search_lib_command(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let search_token = args.next_arg()?.try_as_str()?;
    let search_result = gears_box_search(search_token)?;
    Ok(to_redis_value(search_result))
}

fn function_call(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let args = args.into_iter().skip(1);
    function_call_command(ctx, args)
}

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

fn config_command(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "get" => {
            let config_name = args.next_arg()?.try_as_str()?;
            Ok(RedisValue::BulkString(
                get_globals().config.get(config_name)?,
            ))
        }
        "set" => {
            let config_name = args.next_arg()?.try_as_str()?;
            let config_val = args.next_arg()?.try_as_str()?;
            get_globals_mut().config.set(config_name, config_val)?;
            Ok(RedisValue::SimpleStringStatic("OK"))
        }
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

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

fn on_stream_touched(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if get_ctx().is_primary() {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_touched(event, key);
    }
}

fn generic_notification(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if event == "del" {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_deleted(event, key);
    }
}

fn key_space_notification(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    let globals = get_globals();
    if globals.avoid_key_space_notifications {
        return;
    }
    globals.notifications_ctx.on_key_touched(event, key)
}

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

fn scan_key_space_for_streams() {
    get_globals().mgmt_pool.execute(|| {
        let cursor = KeysCursor::new();
        let ctx = get_ctx();
        let thread_ctx = ThreadSafeContext::new();
        let mut _gaurd = Some(thread_ctx.lock());
        while cursor.scan(ctx, &|ctx, key_name, key| {
            let key_type = match key {
                Some(k) => k.key_type(),
                None => ctx.open_key(&key_name).key_type(),
            };
            if key_type == Stream {
                get_globals_mut()
                    .stream_ctx
                    .on_stream_touched("created", key_name.as_slice());
            }
        }) {
            _gaurd = None; // will release the lock
            _gaurd = Some(thread_ctx.lock());
        }
    })
}

fn on_role_changed(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::RoleChangedEvent(role_changed) => {
            if let ServerRole::Primary = role_changed.role {
                ctx.log_notice(
                    "Role changed to primary, initializing key scan to search for streams.",
                );
                scan_key_space_for_streams();
            }
        }
        _ => panic!("got unexpected sub event"),
    }
}

fn on_loading_event(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::LoadingEvent(loading_sub_event) => {
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
        _ => panic!("got unexpected sub event"),
    }
}

fn on_module_change(ctx: &Context, _event_data: ServerEventData) {
    ctx.log_notice("Got module load event, try to reload modules API.");
    match redisai_rs::redisai_init(ctx) {
        Ok(_) => ctx.log_notice("RedisAI API was loaded successfully."),
        Err(_) => ctx.log_notice("Failed loading RedisAI API."),
    }
}

fn on_flush_event(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::FlushEvent(loading_sub_event) => {
            if let FlushSubevent::Started = loading_sub_event {
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
        _ => panic!("got unexpected sub event"),
    }
}

redis_module! {
    name: "redisgears_2",
    version: VERSION_NUM.unwrap().parse::<i32>().unwrap(),
    data_types: [REDIS_GEARS_TYPE],
    init: js_init,
    post_init: js_post_init,
    info: js_info,
    commands: [
        ["rg.function", function_command, "may-replicate deny-script", 0,0,0],
        ["_rg.function", function_command_on_replica, "may-replicate deny-script", 0,0,0],
        ["rg.fcall", function_call, "may-replicate deny-script", 4,4,1],
        ["rg.fcall_no_keys", function_call, "may-replicate deny-script", 0,0,0],
        ["rg.box", gears_box_command, "may-replicate deny-script", 0,0,0],
        ["rg.config", config_command, "readonly deny-script", 0,0,0],
        ["_rg_internals.update_stream_last_read_id", update_stream_last_read_id, "readonly", 0,0,0],
    ],
    event_handlers: [
        [@STREAM: on_stream_touched],
        [@GENERIC: generic_notification],
        [@ALL @MISSED: key_space_notification],
    ],
    server_events: [
        [@RuleChanged: on_role_changed],
        [@Loading: on_loading_event],
        [@Flush: on_flush_event],
        [@ModuleChange: on_module_change],
    ],
    string_configurations: [
        &get_globals().config.gears_box_address,
    ],
    numeric_configurations: [
        &get_globals().config.execution_threads,
        &get_globals().config.library_maxmemory,
        &get_globals().config.lock_regis_timeout,
        &get_globals().config.remote_task_default_timeout,
    ],
    enum_configurations: [
        &get_globals().config.libraray_fatal_failure_policy,
        &get_globals().config.enable_debug_command,
    ]
}
