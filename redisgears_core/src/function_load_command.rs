/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{
    Context, ContextFlags, NextArg, RedisError, RedisResult, RedisString, RedisValue,
    ThreadSafeContext,
};
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

use crate::compiled_library_api::CompiledLibraryAPI;
use crate::{get_backend, get_globals, verify_name, Deserialize, Serialize};

use crate::{
    get_libraries, GearsLibrary, GearsLibraryCtx, GearsLibraryMetaData, GearsLoadLibraryCtx,
};

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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct FunctionLoadArgs {
    upgrade: bool,
    config: Option<String>,
    code: String,
    user: Option<RedisString>,
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

pub(crate) fn function_load_internal(
    ctx: &Context,
    user: RedisString,
    code: &str,
    config: Option<String>,
    upgrade: bool,
    is_loading_rdb: bool,
) -> Result<(), String> {
    let meta_data = library_extract_metadata(code, config, user).map_err(|e| e.to_string())?;
    let backend_name = meta_data.engine.as_str();
    let backend = get_backend(ctx, backend_name).map_err(|e| e.to_string())?;
    let compile_lib_ctx = CompiledLibraryAPI::new();
    let compile_lib_internals = compile_lib_ctx.take_internals();
    let lib_ctx = backend.compile_library(
        &meta_data.name,
        code,
        meta_data.api_version,
        meta_data.config.as_ref(),
        Box::new(compile_lib_ctx),
    );
    let lib_ctx = lib_ctx.map_err(|e| format!("Failed library compilation: {}", e.get_msg()))?;
    let mut libraries = get_libraries();
    let old_lib = libraries.remove(&meta_data.name);
    if !upgrade {
        if let Some(old_lib) = old_lib {
            let err = Err(format!("Library {} already exists.", &meta_data.name));
            libraries.insert(meta_data.name, old_lib);
            return err;
        }
    }
    let mut gears_library = GearsLibraryCtx {
        meta_data: Arc::new(meta_data),
        functions: HashMap::new(),
        remote_functions: HashMap::new(),
        stream_consumers: HashMap::new(),
        notifications_consumers: HashMap::new(),
        revert_stream_consumers: Vec::new(),
        revert_notifications_consumers: Vec::new(),
        old_lib,
    };
    let res = lib_ctx.load_library(
        &GearsLoadLibraryCtx {
            ctx,
            gears_lib_ctx: &mut gears_library,
        },
        is_loading_rdb,
    );
    if let Err(err) = res {
        function_load_revert(gears_library, &mut libraries);

        return Err(format!(
            "Failed loading library: {}.",
            get_msg_verbose(&err)
        ));
    }
    if gears_library.functions.is_empty()
        && gears_library.stream_consumers.is_empty()
        && gears_library.notifications_consumers.is_empty()
    {
        function_load_revert(gears_library, &mut libraries);
        return Err("Neither function nor other registrations were found.".to_owned());
    }
    gears_library.old_lib = None;
    libraries.insert(
        gears_library.meta_data.name.to_string(),
        Arc::new(GearsLibrary {
            gears_lib_ctx: gears_library,
            lib_ctx,
            compile_lib_internals,
        }),
    );
    Ok(())
}

fn get_args_values(
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> Result<FunctionLoadArgs, RedisError> {
    let mut replace = false;
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
            let res = function_load_internal(
                &ctx_guard,
                user.safe_clone(&ctx_guard),
                &r.args.code,
                r.args.config.clone(),
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

pub(crate) fn function_load_with_args(ctx: &Context, args: FunctionLoadArgs) {
    let blocked_client = ctx.block_client();
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
    function_load_with_args(ctx, args);
    Ok(RedisValue::NoReply)
}

/// Verify that it is OK to perform an internal command.
/// Internal command should only run if it came from replication stream
/// or from AOF loading. In other words, the command did not came directly from a user.
fn verify_internal_command(ctx: &Context) -> Result<(), RedisError> {
    let flags = ctx.get_flags();
    let globals = get_globals();
    if !flags.contains(ContextFlags::LOADING)
        && !(flags.contains(ContextFlags::REPLICATED) || globals.db_policy.is_pseudo_slave())
    {
        // Internal commands should either be loaded from AOF ([`ContextFlags::LOADING`])
        // or sent over the replication stream([`ContextFlags::REPLICATED`]).
        // Another special option is if the instance is pseudo slave (replica of) which is treated as if the command was replicated from primary.
        // If none of those cases holds we will return an error.
        return Err(RedisError::Str(
            "Internal command should only be sent from primary or loaded from AOF",
        ));
    }
    Ok(())
}

pub(crate) fn function_load_on_replica(
    ctx: &Context,
    args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    verify_internal_command(ctx)?;

    let args = get_args_values(args)?;
    if args.user.is_none() {
        return Err(RedisError::Str("User was not provided by primary"));
    }
    match function_load_internal(
        ctx,
        args.user.unwrap(),
        &args.code,
        args.config,
        args.upgrade,
        true,
    ) {
        Ok(_) => Ok(RedisValue::SimpleStringStatic("OK")),
        Err(e) => Err(RedisError::String(e)),
    }
}
