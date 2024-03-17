/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{
    Context, NextArg, RedisError, RedisResult, RedisString, RedisValue, ThreadSafeContext,
};
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtxInterfaceInitialised;
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::DebuggerBackend;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use redisgears_plugin_api::redisgears_plugin_api::{GearsApiError, GearsApiResult};

use crate::compiled_library_api::{CompiledLibraryAPI, CompiledLibraryInternals};
use crate::config::V8_DEBUG_SERVER_ADDRESS;
use crate::get_globals;
use crate::GILBackendStorage;
use crate::{verify_name, Deserialize, Serialize};

use crate::{get_libraries, GearsLibrary, GearsLibraryCtx, GearsLibraryMetaData};

use mr::libmr::{
    record::Record as LibMRRecord, remote_task::run_on_all_shards, remote_task::RemoteTask,
    RustMRError,
};

use std::iter::Skip;
use std::vec::IntoIter;

use std::collections::HashMap;
use std::sync::Arc;

use crate::get_msg_verbose;

use mr_derive::BaseObject;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FunctionLoadArgs {
    upgrade: bool,
    config: Option<String>,
    code: String,
    user: Option<RedisString>,
    debug: bool,
}

/// The data of a compiled library.
pub(crate) struct CompiledLibraryData {
    meta_data: GearsLibraryMetaData,
    library_ctx_interface: Box<dyn LibraryCtxInterface>,
    compiled_library_internals: Arc<CompiledLibraryInternals>,
}

/// The compilation arguments for a script.
#[derive(Debug, Clone)]
pub(crate) struct CompilationArguments {
    user: RedisString,
    code: String,
    config: Option<String>,
}

impl TryFrom<FunctionLoadArgs> for CompilationArguments {
    type Error = &'static str;

    fn try_from(args: FunctionLoadArgs) -> Result<Self, Self::Error> {
        Ok(Self::new(
            args.user.ok_or("User should've been set.")?,
            args.code,
            args.config,
        ))
    }
}

impl CompilationArguments {
    /// Creates a new
    pub(crate) fn new<T: Into<RedisString>>(user: T, code: String, config: Option<String>) -> Self {
        Self {
            user: user.into(),
            code,
            config,
        }
    }

    pub(crate) fn compile(
        &self,
        context: &Context,
        debug: bool,
    ) -> GearsApiResult<CompiledLibraryData> {
        compile_library(context, self, debug)
    }

    /// Returns the library metadata.
    pub(crate) fn get_metadata(&self) -> Result<GearsLibraryMetaData, RedisError> {
        library_extract_metadata(&self.code, self.config.clone(), self.user.clone())
    }

    /// Returns a mutable reference to the backend for the library.
    pub(crate) fn get_backend_mut(
        &self,
        context: &Context,
    ) -> Result<&mut Box<dyn BackendCtxInterfaceInitialised>, RedisError> {
        context.get_backend(self.get_metadata()?.engine.as_str())
    }
}

fn start_debug_server_for_library(
    context: &Context,
    address: &str,
    compilation_arguments: &CompilationArguments,
) -> Result<Box<dyn DebuggerBackend>, GearsApiError> {
    compilation_arguments
        .get_backend_mut(context)?
        .start_debug_server(address)
}

fn library_extract_metadata(
    code: &str,
    config: Option<String>,
    user: RedisString,
) -> Result<GearsLibraryMetaData, RedisError> {
    let prologue = redisgears_plugin_api::redisgears_plugin_api::prologue::parse_prologue(code)
        .map_err(|e| RedisError::String(GearsApiError::from(e).get_msg().to_owned()))?;

    verify_name(prologue.library_name).map_err(|e| {
        RedisError::String(format!(
            "Unallowed library name '{}', {e}.",
            prologue.library_name
        ))
    })?;

    Ok(GearsLibraryMetaData {
        engine: prologue.engine.to_owned(),
        api_version: prologue.api_version,
        name: prologue.library_name.to_owned(),
        code: code.to_string(),
        config,
        user,
    })
}

pub(crate) fn function_load_revert(
    mut gears_library: GearsLibraryCtx,
    libraries: &mut HashMap<String, Arc<GearsLibrary>>,
) {
    if let Some(old_lib) = gears_library.old_lib.take() {
        for (name, old_ctx, old_window, old_trim, description) in
            gears_library.revert_stream_consumers
        {
            let stream_data = gears_library.stream_consumers.get(&name).unwrap();
            let mut s_d = stream_data.ref_cell.borrow_mut();
            s_d.set_consumer(old_ctx);
            s_d.set_window(old_window);
            s_d.set_trim(old_trim);
            s_d.set_description(description);
        }

        for (name, key, callback, description) in gears_library.revert_notifications_consumers {
            let notification_consumer = gears_library.notifications_consumers.get(&name).unwrap();
            let mut s_d = notification_consumer.borrow_mut();
            s_d.set_key(key);
            let _ = s_d.set_callback(callback);
            s_d.set_description(description);
        }

        libraries.insert(gears_library.meta_data.name.clone(), old_lib);
    }
}

fn compile_library(
    context: &Context,
    compilation_arguments: &CompilationArguments,
    debug: bool,
) -> GearsApiResult<CompiledLibraryData> {
    let globals = get_globals();
    let meta_data = compilation_arguments
        .get_metadata()
        .map_err(GearsApiError::from)?;
    let backend = compilation_arguments.get_backend_mut(context)?;
    let compile_lib_ctx = CompiledLibraryAPI::new(globals.redis_version.is_enterprise);
    let compile_lib_internals = compile_lib_ctx.take_internals();
    let library = backend.compile_library(
        debug,
        &meta_data.name,
        &compilation_arguments.code,
        meta_data.api_version,
        meta_data.config.as_ref(),
        Box::new(compile_lib_ctx),
    )?;

    Ok(CompiledLibraryData {
        meta_data,
        library_ctx_interface: library,
        compiled_library_internals: compile_lib_internals,
    })
}

/// Evaluates the compiled function code and if everything is okay,
/// stores the compiled and evaluated script within the global storage.
pub(crate) fn function_evaluate_and_store(
    context: &Context,
    compiled_function_info: CompiledLibraryInfo,
    upgrade: bool,
    is_loading_rdb: bool,
) -> Result<Arc<GearsLibrary>, String> {
    let meta_data = compiled_function_info.meta_data;
    let lib_ctx = compiled_function_info.library_context;
    let compile_lib_internals = compiled_function_info.internals;

    let mut libraries = get_libraries();
    let old_lib = libraries.remove(&meta_data.name);
    if !upgrade {
        if let Some(old_lib) = old_lib {
            let err = Err(format!("Library {} already exists.", &meta_data.name));
            libraries.insert(meta_data.name, old_lib);
            return err;
        }
    }
    let mut gears_library_ctx = GearsLibraryCtx::new(Arc::new(meta_data), old_lib);
    let res = lib_ctx.load_library(&gears_library_ctx.get_loader(context), is_loading_rdb);
    if let Err(err) = res {
        function_load_revert(gears_library_ctx, &mut libraries);

        return Err(format!(
            "Failed loading library: {}.",
            get_msg_verbose(&err)
        ));
    }
    if gears_library_ctx.functions.is_empty()
        && gears_library_ctx.stream_consumers.is_empty()
        && gears_library_ctx.notifications_consumers.is_empty()
    {
        function_load_revert(gears_library_ctx, &mut libraries);
        return Err("Neither function nor other registrations were found.".to_owned());
    }
    gears_library_ctx.old_lib = None;
    let gears_library = Arc::new(GearsLibrary {
        gears_lib_ctx: gears_library_ctx,
        lib_ctx,
        compile_lib_internals,
    });
    libraries.insert(
        gears_library.gears_lib_ctx.meta_data.name.clone(),
        gears_library.clone(),
    );
    Ok(gears_library)
}

/// Contains the information of a compiled library.
pub(crate) struct CompiledLibraryInfo {
    pub(crate) meta_data: GearsLibraryMetaData,
    pub(crate) library_context: Box<dyn LibraryCtxInterface>,
    pub(crate) internals: Arc<CompiledLibraryInternals>,
}

/// Compiles the script code but does not evaluate it.
pub(crate) fn function_compile(
    context: &Context,
    compilation_arguments: CompilationArguments,
    debug: bool,
) -> Result<CompiledLibraryInfo, String> {
    let compiled_library_data = compilation_arguments
        .compile(context, debug)
        .map_err(|e| format!("Failed to compile library: {}", e.get_msg()))?;
    let (meta_data, library_context, internals) = (
        compiled_library_data.meta_data,
        compiled_library_data.library_ctx_interface,
        compiled_library_data.compiled_library_internals,
    );

    Ok(CompiledLibraryInfo {
        meta_data,
        library_context,
        internals,
    })
}

pub(crate) fn function_load_internal(
    context: &Context,
    compilation_arguments: CompilationArguments,
    debug: bool,
    upgrade: bool,
    is_loading_rdb: bool,
) -> Result<Arc<GearsLibrary>, String> {
    let compiled_function_info = function_compile(context, compilation_arguments, debug)?;
    function_evaluate_and_store(context, compiled_function_info, upgrade, is_loading_rdb)
}

fn get_args_values(
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> Result<FunctionLoadArgs, RedisError> {
    let mut replace = false;
    let mut debug = false;
    let mut config = None;
    let mut user = None;
    let last_arg = loop {
        let arg = args.next_arg();
        if arg.is_err() {
            break Err(RedisError::Str("Could not find library payload"));
        }
        let arg = arg.unwrap();
        let arg_str = match arg.try_as_str() {
            Ok(arg) => arg,
            Err(_) => break Ok(arg),
        };
        let arg_str = arg_str.to_lowercase();
        match arg_str.as_ref() {
            "replace" => replace = true,
            "debug" => debug = true,
            "user" => {
                let arg = args
                    .next_arg()
                    .map_err(|_e| RedisError::Str("configuration value was not given"))?;
                user = Some(arg);
            }
            "config" => {
                let arg = args
                    .next_arg()
                    .map_err(|_e| RedisError::Str("configuration value was not given"))?
                    .try_as_str()
                    .map_err(|_e| {
                        RedisError::Str("given configuration value is not a valid string")
                    })?;
                let v = serde_json::from_str::<serde_json::Value>(arg).map_err(|e| {
                    RedisError::String(format!(
                        "configuration must be a valid json, '{}', {}.",
                        arg, e
                    ))
                })?;
                match v {
                    serde_json::Value::Object(_) => (),
                    _ => return Err(RedisError::Str("configuration must be a valid json object")),
                }
                config = Some(arg.to_string());
            }
            _ => break Ok(arg),
        }
    }?;

    let code = match last_arg.try_as_str() {
        Ok(s) => s,
        Err(_) => return Err(RedisError::Str("lib code must a valid string")),
    }
    .to_string();
    Ok(FunctionLoadArgs {
        upgrade: replace,
        config,
        code,
        debug,
        user,
    })
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionLoadInputRecord {
    args: FunctionLoadArgs,
}

impl LibMRRecord for GearsFunctionLoadInputRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Null
    }

    fn hash_slot(&self) -> usize {
        1 // not relevant here
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionLoadOutputRecord;

impl LibMRRecord for GearsFunctionLoadOutputRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Null
    }

    fn hash_slot(&self) -> usize {
        1 // not relevant here
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionLoadRemoteTask;

impl RemoteTask for GearsFunctionLoadRemoteTask {
    type InRecord = GearsFunctionLoadInputRecord;
    type OutRecord = GearsFunctionLoadOutputRecord;

    fn task(
        self,
        r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>) + Send>,
    ) {
        let res = {
            let ctx_guard = ThreadSafeContext::new().lock();
            let user = r.args.user.unwrap();
            let compilation_arguments = CompilationArguments::new(
                user.safe_clone(&ctx_guard),
                r.args.code.clone(),
                r.args.config.clone(),
            );
            let res = function_load_internal(
                &ctx_guard,
                compilation_arguments,
                false,
                r.args.upgrade,
                false,
            );
            if res.is_ok() {
                let mut replicate_args = Vec::new();
                replicate_args.push("load".as_bytes());
                if r.args.upgrade {
                    replicate_args.push("REPLACE".as_bytes());
                }
                if let Some(conf) = &r.args.config {
                    replicate_args.push("CONFIG".as_bytes());
                    replicate_args.push(conf.as_bytes());
                }
                replicate_args.push("USER".as_bytes());
                replicate_args.push(user.as_slice());
                replicate_args.push(r.args.code.as_bytes());
                ctx_guard.replicate("_rg_internals.function", replicate_args.as_slice());
            }
            res
        };
        on_done(res.map(|_v| GearsFunctionLoadOutputRecord));
    }
}

/// Loads the function only on this shard, starts a debugger server,
/// waits for the connection and starts the debugging session.
///
/// It makes sure that the function is only going to be loaded onto this
/// shard by simply not propagating the event to all the other shards.
fn function_load_with_args_with_debugger(
    context: &Context,
    args: FunctionLoadArgs,
) -> RedisResult<()> {
    if mr::libmr::is_cluster_in_cluster_mode() {
        return Err(RedisError::Str(
            "Debugging when in cluster mode isn't supported.",
        ));
    }

    let address = { &V8_DEBUG_SERVER_ADDRESS.lock(context) };

    if address.is_empty() {
        return Err(RedisError::Str(
            "The debug server address was not specified in the configuration.",
        ));
    }

    if crate::get_globals_mut().debugger_server.is_some() {
        return Err(RedisError::Str(
            "Only one debugging session can be established at a time.",
        ));
    }

    let compilation_arguments = CompilationArguments::try_from(args).map_err(|e| {
        RedisError::String(format!("Couldn't build the compilation arguments: {e}"))
    })?;

    let (connection_hints_message, debugger_backend) =
        start_debug_server_for_library(context, address, &compilation_arguments)
            .map(|debugger_server| {
                let connection_hints_message = RedisValue::VerbatimString((
                    "txt"
                        .try_into()
                        .expect("Couldn't create a VerbatimStringFormat"),
                    debugger_server
                        .get_connection_hints()
                        .unwrap_or_else(|| "The debugger server has started.".to_string())
                        .as_bytes()
                        .to_vec(),
                ));
                (connection_hints_message, debugger_server)
            })
            .map_err(|e| RedisError::String(e.to_string()))?;

    context.reply(Ok(connection_hints_message));

    let _ = crate::get_globals_mut()
        .debugger_server
        .insert(crate::debugging::Server::prepare(
            compilation_arguments,
            debugger_backend,
        ));

    Ok(())
}

/// Loads the provided function on the current shard and other shards.
pub(crate) fn function_load_with_args_without_debugger(
    context: &Context,
    args: FunctionLoadArgs,
) -> RedisResult<()> {
    let blocked_client = context.block_client();
    run_on_all_shards(
        GearsFunctionLoadRemoteTask,
        GearsFunctionLoadInputRecord { args },
        |_results: Vec<GearsFunctionLoadOutputRecord>, mut errors| {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            if errors.is_empty() {
                thread_ctx.reply(Ok(RedisValue::SimpleStringStatic("OK")));
            } else {
                thread_ctx.reply(Err(RedisError::String(errors.pop().unwrap())));
            }
        },
        10000,
    );

    Ok(())
}

pub(crate) fn function_load_with_args(
    context: &Context,
    args: FunctionLoadArgs,
) -> RedisResult<()> {
    if args.debug {
        function_load_with_args_with_debugger(context, args)
    } else {
        function_load_with_args_without_debugger(context, args)
    }
}

pub(crate) fn function_load_command(
    ctx: &Context,
    args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut args = get_args_values(args)?;
    if args.user.is_some() {
        return Err(RedisError::Str("Unknown argument user"));
    }
    args.user = Some(ctx.get_current_user());

    if let Err(e) = function_load_with_args(ctx, args) {
        ctx.reply(Err(e));
    }

    Ok(RedisValue::NoReply)
}

pub(crate) fn function_load_on_replica(
    ctx: &Context,
    args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let args = get_args_values(args)?;
    let user = args
        .user
        .ok_or(RedisError::Str("User was not provided by primary"))?;
    let compilation_arguments = CompilationArguments::new(user, args.code, args.config);
    // On replica, we always obey the primary and perform an upgrade.
    match function_load_internal(ctx, compilation_arguments, false, true, true) {
        Ok(_) => Ok(RedisValue::SimpleStringStatic("OK")),
        Err(e) => Err(RedisError::String(e)),
    }
}
