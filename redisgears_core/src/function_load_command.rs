/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{
    Context, NextArg, RedisError, RedisResult, RedisString, RedisValue, ThreadSafeContext,
};
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;

use crate::compiled_library_api::CompiledLibraryAPI;
use crate::gears_box::GearsBoxLibraryInfo;
use crate::{get_functions_mut, Deserialize, GearsFunctionData, Serialize};

use crate::{
    get_backends_mut, get_libraries, GearsLibrary, GearsLibraryCtx, GearsLibraryMetaData,
    GearsLoadLibraryCtx,
};

use mr::libmr::{
    record::Record as LibMRRecord, remote_task::run_on_all_shards, remote_task::RemoteTask,
    RustMRError,
};

use std::iter::Skip;
use std::vec::IntoIter;

use std::collections::HashMap;
use std::sync::Arc;

use crate::gears_box::{do_http_get_text, gears_box_get_library};

use crate::get_msg_verbose;

use mr_derive::BaseObject;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct FunctionLoadArgs {
    upgrade: bool,
    config: Option<String>,
    code: String,
    gears_box: Option<GearsBoxLibraryInfo>,
    user: Option<RedisString>,
}

fn library_extract_metadata(
    code: &str,
    config: Option<String>,
    user: RedisString,
) -> Result<GearsLibraryMetaData, RedisError> {
    let prologue = redisgears_plugin_api::redisgears_plugin_api::prologue::parse_prologue(code)
        .map_err(|e| RedisError::String(GearsApiError::from(e).get_msg().to_owned()))?;

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
    functions: &mut HashMap<String, GearsFunctionData>,
) {
    if let Some(old_lib) = gears_library.old_lib.take() {
        for (name, old_ctx, old_window, old_trim) in gears_library.revert_stream_consumers {
            let stream_data = gears_library.stream_consumers.get(&name).unwrap();
            let mut s_d = stream_data.ref_cell.borrow_mut();
            s_d.set_consumer(old_ctx);
            s_d.set_window(old_window);
            s_d.set_trim(old_trim);
        }

        for (name, key, callback) in gears_library.revert_notifications_consumers {
            let notification_consumer = gears_library.notifications_consumers.get(&name).unwrap();
            let mut s_d = notification_consumer.borrow_mut();
            s_d.set_key(key);
            let _ = s_d.set_callback(callback);
        }

        old_lib
            .gears_lib_ctx
            .functions
            .iter()
            .for_each(|(name, func)| {
                functions.insert(
                    name.to_owned(),
                    GearsFunctionData::new(func, &old_lib.gears_lib_ctx.meta_data),
                );
            });

        libraries.insert(gears_library.meta_data.name.clone(), old_lib);
    }
}

pub(crate) fn function_load_internal(
    ctx: &Context,
    user: RedisString,
    code: &str,
    config: Option<String>,
    upgrade: bool,
    gears_box_lib: Option<GearsBoxLibraryInfo>,
) -> Result<(), String> {
    let meta_data = library_extract_metadata(code, config, user).map_err(|e| e.to_string())?;
    let backend_name = meta_data.engine.as_str();
    let backend = get_backends_mut().get_mut(backend_name);
    if backend.is_none() {
        return Err(format!("Unknown backend {}", backend_name));
    }
    let backend = backend.unwrap();
    let compile_lib_ctx = CompiledLibraryAPI::new();
    let compile_lib_internals = compile_lib_ctx.take_internals();
    let lib_ctx = backend.compile_library(
        &meta_data.name,
        code,
        meta_data.api_version,
        meta_data.config.as_ref(),
        Box::new(compile_lib_ctx),
    );
    let lib_ctx = match lib_ctx {
        Err(e) => return Err(format!("Failed library compilation: {}", e.get_msg())),
        Ok(lib_ctx) => lib_ctx,
    };
    let mut libraries = get_libraries();
    let old_lib = libraries.remove(&meta_data.name);
    if !upgrade {
        if let Some(old_lib) = old_lib {
            let err = Err(format!("Library {} already exists.", &meta_data.name));
            libraries.insert(meta_data.name, old_lib);
            return err;
        }
    }

    let functions = get_functions_mut();
    old_lib.as_ref().map(|v| {
        v.gears_lib_ctx.functions.iter().for_each(|(name, _)| {
            functions.remove(name.as_str());
        })
    });

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
    let res = lib_ctx.load_library(&GearsLoadLibraryCtx {
        ctx,
        gears_lib_ctx: &mut gears_library,
    });
    let functions = get_functions_mut();
    if let Err(err) = res {
        let ret = Err(format!(
            "Failed loading library: {}.",
            get_msg_verbose(&err)
        ));
        function_load_revert(gears_library, &mut libraries, functions);
        return ret;
    }
    if gears_library.functions.is_empty()
        && gears_library.stream_consumers.is_empty()
        && gears_library.notifications_consumers.is_empty()
    {
        function_load_revert(gears_library, &mut libraries, functions);
        return Err("No function nor registrations was registered".to_string());
    }
    gears_library.old_lib = None;
    gears_library.functions.iter().for_each(|(name, func)| {
        functions.insert(
            name.to_owned(),
            GearsFunctionData::new(func, &gears_library.meta_data),
        );
    });
    libraries.insert(
        gears_library.meta_data.name.to_string(),
        Arc::new(GearsLibrary {
            gears_lib_ctx: gears_library,
            _lib_ctx: lib_ctx,
            compile_lib_internals,
            gears_box_lib,
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
        gears_box: None,
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
                None,
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

pub(crate) fn function_load_on_replica(
    ctx: &Context,
    args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
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
        None,
    ) {
        Ok(_) => Ok(RedisValue::SimpleStringStatic("OK")),
        Err(e) => Err(RedisError::String(e)),
    }
}

pub(crate) fn function_install_lib_command(
    ctx: &Context,
    args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut args = get_args_values(args)?;
    if args.user.is_some() {
        return Err(RedisError::Str("Unknown argument user"));
    }
    let gear_box_lib = gears_box_get_library(ctx, &args.code)?;
    let function_code = do_http_get_text(&gear_box_lib.installed_version_info.url)?;

    let calculated_sha = sha256::digest(function_code.to_string());
    if calculated_sha != gear_box_lib.installed_version_info.sha256 {
        return Err(RedisError::Str(
            "File validation failure, calculated sha256sum does not match the expected value.",
        ));
    }

    args.user = Some(ctx.get_current_user());
    args.code = function_code;
    function_load_with_args(ctx, args);
    Ok(RedisValue::NoReply)
}
