/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{CallReply, CallResult, ErrorReply};
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;
use redisgears_plugin_api::redisgears_plugin_api::prologue::{self, ApiVersion};
use redisgears_plugin_api::redisgears_plugin_api::run_function_ctx::PromiseReply;
use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LoadLibraryCtxInterface, load_library_ctx::RegisteredKeys,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::RemoteFunctionData, GearsApiError, RefCellWrapper,
};

use v8_rs::v8::v8_array::V8LocalArray;
use v8_rs::v8::{
    isolate_scope::V8IsolateScope, v8_array_buffer::V8LocalArrayBuffer,
    v8_context_scope::V8ContextScope, v8_native_function_template::V8LocalNativeFunctionArgsIter,
    v8_object::V8LocalObject, v8_utf8::V8LocalUtf8, v8_value::V8LocalValue, v8_version,
};

use v8_derive::{new_native_function, NativeFunctionArgument};

use crate::v8_redisai::{get_redisai_api, get_redisai_client};

use crate::v8_backend::log_warning;
use crate::v8_function_ctx::V8Function;
use crate::v8_notifications_ctx::V8NotificationsCtx;
use crate::v8_script_ctx::{GilStatus, V8ScriptCtx};
use crate::v8_stream_ctx::V8StreamCtx;
use crate::{
    get_exception_msg, get_exception_v8_value, get_function_flags_from_strings,
    get_function_flags_globals,
};

use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

const REGISTER_NOTIFICATIONS_CONSUMER: &str = "registerKeySpaceTrigger";
const FUNCTION_FLAGS_GLOBAL_NAME: &str = "functionFlags";
const V8_VERSION_GLOBAL_NAME: &str = "v8Version";
const REGISTER_STREAM_TRIGGER_GLOBAL_NAME: &str = "registerStreamTrigger";
const REGISTER_FUNCTION_GLOBAL_NAME: &str = "registerFunction";
const REGISTER_ASYNC_FUNCTION_GLOBAL_NAME: &str = "registerAsyncFunction";
const REGISTER_CLUSTER_FUNCTION_GLOBAL_NAME: &str = "registerClusterFunction";
const LOG_GLOBAL_NAME: &str = "log";
const REDISAI_GLOBAL_NAME: &str = "redisai";
const REDIS_GLOBAL_NAME: &str = "redis";
const BLOCK_GLOBAL_NAME: &str = "block";
const RUN_ON_KEY_GLOBAL_NAME: &str = "runOnKey";
const RUN_ON_SHARDS_GLOBAL_NAME: &str = "runOnShards";
const CALL_GLOBAL_NAME: &str = "call";
const CALL_RAW_GLOBAL_NAME: &str = "callRaw";
const CALL_ASYNC_GLOBAL_NAME: &str = "callAsync";
const CALL_ASYNC_RAW_GLOBAL_NAME: &str = "callAsyncRaw";
const IS_BLOCK_ALLOW_GLOBAL_NAME: &str = "isBlockAllowed";
const EXECUTE_ASYNC_GLOBAL_NAME: &str = "executeAsync";

pub(crate) fn call_result_to_js_object<'isolate_scope, 'isolate>(
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope,
    res: CallResult,
    decode_responses: bool,
) -> Result<V8LocalValue<'isolate_scope, 'isolate>, String> {
    let res = res.map_err(|err| {
        err.to_utf8_string()
            .unwrap_or("Failed converting error to utf8".into())
    })?;
    Ok(match res {
        CallReply::String(s) => {
            if decode_responses {
                let s = s
                    .to_string()
                    .ok_or("Could not decode value as string".to_string())?;
                isolate_scope.new_string(&s).to_value()
            } else {
                isolate_scope.new_array_buffer(s.as_bytes()).to_value()
            }
        }
        CallReply::I64(l) => isolate_scope.new_long(l.to_i64()),
        CallReply::Double(d) => isolate_scope.new_double(d.to_double()),
        CallReply::Bool(b) => isolate_scope.new_bool(b.to_bool()),
        CallReply::Null(_b) => isolate_scope.new_null(),
        CallReply::Unknown => isolate_scope.new_null(),
        CallReply::VerbatimString(s) => {
            let (format, data) = s
                .as_parts()
                .ok_or("Could not decode format as string".to_string())?;
            let val = if decode_responses {
                isolate_scope
                    .new_string(std::str::from_utf8(data).map_err(|e| e.to_string())?)
                    .to_string_object()
                    .to_value()
            } else {
                isolate_scope.new_array_buffer(data).to_value()
            };
            let obj = val.as_object();
            obj.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("verbatim").to_value(),
            );
            obj.set(
                ctx_scope,
                &isolate_scope.new_string("__format").to_value(),
                &isolate_scope.new_string(format).to_value(),
            );
            obj.to_value()
        }
        CallReply::BigNumber(b) => {
            let s = b
                .to_string()
                .ok_or("Could not decode big number as string".to_string())?;
            let s = isolate_scope.new_string(&s).to_string_object();
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("big_number").to_value(),
            );
            s.to_value()
        }
        CallReply::Array(a) => {
            let res: Vec<V8LocalValue> = a.iter().fold(Ok::<_, String>(Vec::new()), |agg, v| {
                let mut agg = agg?;
                agg.push(call_result_to_js_object(
                    isolate_scope,
                    ctx_scope,
                    v,
                    decode_responses,
                )?);
                Ok(agg)
            })?;
            isolate_scope
                .new_array(&res.iter().collect::<Vec<&V8LocalValue>>())
                .to_value()
        }
        CallReply::Set(s) => s
            .iter()
            .fold(Ok::<_, String>(isolate_scope.new_set()), |agg, v| {
                let agg = agg?;
                agg.add(
                    ctx_scope,
                    &call_result_to_js_object(isolate_scope, ctx_scope, v, decode_responses)?,
                );
                Ok(agg)
            })?
            .to_value(),
        CallReply::Map(m) => m
            .iter()
            .fold(Ok(isolate_scope.new_object()), |agg, (k, v)| {
                let key = k.map_err(|e| {
                    e.to_utf8_string()
                        .unwrap_or("Failed converting error to utf8".to_string())
                })?;
                match key {
                    CallReply::String(k) => {
                        let key = k
                            .to_string()
                            .ok_or("Binary map key is not supported".to_string())?;
                        let agg = agg?;
                        agg.set(
                            ctx_scope,
                            &isolate_scope.new_string(&key).to_value(),
                            &call_result_to_js_object(
                                isolate_scope,
                                ctx_scope,
                                v,
                                decode_responses,
                            )?,
                        );
                        Ok(agg)
                    }
                    CallReply::I64(i) => {
                        let agg = agg?;
                        agg.set(
                            ctx_scope,
                            &isolate_scope.new_long(i.to_i64()),
                            &call_result_to_js_object(
                                isolate_scope,
                                ctx_scope,
                                v,
                                decode_responses,
                            )?,
                        );
                        Ok(agg)
                    }
                    _ => Err("Given object can not be a object key".to_string()),
                }
            })?
            .to_value(),
    })
}

pub(crate) struct RedisClient {
    pub(crate) client: Option<NonNull<dyn RedisClientCtxInterface>>,
    allow_block: Option<bool>,
}

impl RedisClient {
    pub(crate) fn new() -> Self {
        Self {
            client: None,
            allow_block: Some(true),
        }
    }

    pub(crate) fn with_client(client: &dyn RedisClientCtxInterface) -> Self {
        let mut c = Self::new();
        c.set_client(client);
        c
    }

    pub(crate) fn make_invalid(&mut self) {
        self.client = None;
        self.allow_block = None;
    }

    pub(crate) fn get(&self) -> Option<&dyn RedisClientCtxInterface> {
        self.client.map(|c| unsafe { &*c.as_ptr() })
    }

    pub(crate) fn set_client(&mut self, c: &dyn RedisClientCtxInterface) {
        self.client = NonNull::new(
            c as *const dyn RedisClientCtxInterface as *mut dyn RedisClientCtxInterface,
        );
    }

    pub(crate) fn set_allow_block(&mut self, allow_block: bool) {
        self.allow_block = Some(allow_block);
    }
}

fn js_value_to_remote_function_data(
    ctx_scope: &V8ContextScope,
    val: V8LocalValue,
) -> Option<RemoteFunctionData> {
    if val.is_array_buffer() {
        let array_buff = val.as_array_buffer();
        let data = array_buff.data();
        Some(RemoteFunctionData::Binary(data.to_vec()))
    } else {
        let arg_str = ctx_scope.json_stringify(&val);

        // if None return None
        arg_str.as_ref()?;

        let arg_str_utf8 = arg_str.unwrap().to_value().to_utf8().unwrap();
        Some(RemoteFunctionData::String(
            arg_str_utf8.as_str().to_string(),
        ))
    }
}

pub(crate) fn get_backgrounnd_client<'isolate_scope, 'isolate>(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope<'isolate_scope, 'isolate>,
    redis_background_client: Arc<Box<dyn BackgroundRunFunctionCtxInterface>>,
) -> V8LocalObject<'isolate_scope, 'isolate> {
    let bg_client = isolate_scope.new_object();

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_ref = Arc::downgrade(script_ctx);
    bg_client.set_native_function(
        ctx_scope,
        BLOCK_GLOBAL_NAME,
        new_native_function!(move |isolate_scope, ctx_scope, f: V8LocalValue| {
            if !f.is_function() {
                return Err("Argument to 'block' must be a function".into());
            }

            let is_already_blocked = ctx_scope.get_private_data::<bool, _>(0);
            if is_already_blocked.is_some() && *is_already_blocked.unwrap() {
                return Err("Main thread is already blocked".into());
            }

            let redis_client = {
                let _unlocker = isolate_scope.new_unlocker();
                match redis_background_client_ref.lock() {
                    Ok(l) => l,
                    Err(err) => {
                        return Err(format!("Can not lock Redis, {}", err.get_msg()));
                    }
                }
            };
            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    return Err("Function were unregistered".into());
                }
            };

            let r_client = Arc::new(RefCell::new(RedisClient::with_client(
                redis_client.as_ref(),
            )));
            let c = get_redis_client(&script_ctx_ref, isolate_scope, ctx_scope, &r_client);

            let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

            Ok(script_ctx_ref.call(&f, ctx_scope, Some(&[&c.to_value()]), GilStatus::Locked))
        }),
    );

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_weak_ref = Arc::downgrade(script_ctx);
    bg_client.set_native_function(ctx_scope, RUN_ON_KEY_GLOBAL_NAME, new_native_function!(move |
        _isolate,
        ctx_scope,
        key: V8RedisCallArgs,
        remote_function_name: V8LocalUtf8,
        args: Vec<V8LocalValue>,
    | {
        let args_vec:Vec<RemoteFunctionData> = args.into_iter().map(|v| js_value_to_remote_function_data(ctx_scope, v).ok_or("Failed serializing arguments")).collect::<Result<_,_>>()?;

        let _ = script_ctx_weak_ref.upgrade().ok_or("Function were unregistered")?;

        let resolver = ctx_scope.new_resolver();
        let promise = resolver.get_promise();
        let mut resolver = resolver.to_value().persist();
        let script_ctx_weak_ref = Weak::clone(&script_ctx_weak_ref);
        redis_background_client_ref.run_on_key(key.as_bytes(), remote_function_name.as_str(), args_vec, Box::new(move |result|{
            let script_ctx = match script_ctx_weak_ref.upgrade() {
                Some(s) => s,
                None => {
                    resolver.forget();
                    log_warning("Library was delete while not all the remote jobs were done");
                    return;
                }
            };

            script_ctx.compiled_library_api.run_on_background(Box::new(move||{
                let script_ctx = match script_ctx_weak_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        resolver.forget();
                        log_warning("Library was delete while not all the remote jobs were done");
                        return;
                    }
                };

                let isolate_scope = script_ctx.isolate.enter();
                let ctx_scope = script_ctx.ctx.enter(&isolate_scope);

                let resolver = resolver.take_local(&isolate_scope).as_resolver();
                match result {
                    Ok(r) => {
                        let v = match &r {
                            RemoteFunctionData::Binary(b) => isolate_scope.new_array_buffer(b).to_value(),
                            RemoteFunctionData::String(s) => {
                                let v8_str = isolate_scope.new_string(s);
                                let v8_obj = ctx_scope.new_object_from_json(&v8_str);
                                if v8_obj.is_none() {
                                    script_ctx.reject(&resolver, &ctx_scope, &isolate_scope.new_string("Failed deserializing remote function result").to_value());
                                    return;
                                }
                                v8_obj.unwrap()
                            }
                        };
                        script_ctx.resolve(&resolver, &ctx_scope, &v);
                    },
                    Err(e) => {
                        script_ctx.reject(&resolver, &ctx_scope, &isolate_scope.new_string(e.get_msg()).to_value());
                    }
                }
            }));
        }));
        Ok::<_, &'static str>(Some(promise.to_value()))
    }));

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_weak_ref = Arc::downgrade(script_ctx);
    bg_client.set_native_function(ctx_scope, RUN_ON_SHARDS_GLOBAL_NAME, new_native_function!(move |
        _isolate,
        ctx_scope,
        remote_function_name: V8LocalUtf8,
        args: Vec<V8LocalValue>,
    | {
        let args_vec:Vec<RemoteFunctionData> = args.into_iter().map(|v| js_value_to_remote_function_data(ctx_scope, v).ok_or("Failed serializing arguments")).collect::<Result<_,_>>()?;

        let _ = match script_ctx_weak_ref.upgrade() {
            Some(s) => s,
            None => {
                return Err("Function were unregistered");
            }
        };

        let resolver = ctx_scope.new_resolver();
        let promise = resolver.get_promise();
        let mut resolver = resolver.to_value().persist();
        let script_ctx_weak_ref = Weak::clone(&script_ctx_weak_ref);
        redis_background_client_ref.run_on_all_shards(remote_function_name.as_str(), args_vec, Box::new(move |results, mut errors|{
            let script_ctx = match script_ctx_weak_ref.upgrade() {
                Some(s) => s,
                None => {
                    resolver.forget();
                    log_warning("Library was delete while not all the remote jobs were done");
                    return;
                }
            };

            script_ctx.compiled_library_api.run_on_background(Box::new(move||{
                let script_ctx = match script_ctx_weak_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        resolver.forget();
                        log_warning("Library was delete while not all the remote jobs were done");
                        return;
                    }
                };

                let isolate_scope = script_ctx.isolate.enter();
                let ctx_scope = script_ctx.ctx.enter(&isolate_scope);

                let resolver = resolver.take_local(&isolate_scope).as_resolver();
                let results: Vec<V8LocalValue> = results.into_iter().map(|v| {
                    match v {
                        RemoteFunctionData::Binary(b) => isolate_scope.new_array_buffer(&b).to_value(),
                        RemoteFunctionData::String(s) => {
                            let v8_str = isolate_scope.new_string(&s);
                            let v8_obj = ctx_scope.new_object_from_json(&v8_str);
                            if v8_obj.is_none() {
                                errors.push(GearsApiError::new(format!("Failed deserializing remote function result '{}'", s)));
                            }
                            v8_obj.unwrap()
                        }
                    }
                }).collect();
                let errors: Vec<V8LocalValue> = errors.into_iter().map(|e| isolate_scope.new_string(e.get_msg()).to_value()).collect();
                let results_array = isolate_scope.new_array(&results.iter().collect::<Vec<&V8LocalValue>>()).to_value();
                let errors_array = isolate_scope.new_array(&errors.iter().collect::<Vec<&V8LocalValue>>()).to_value();

                script_ctx.resolve(&resolver, &ctx_scope, &isolate_scope.new_array(&[&results_array, &errors_array]).to_value());
            }));
        }));
        Ok(Some(promise.to_value()))
    }));

    bg_client
}

enum V8RedisCallArgs<'isolate_scope, 'isolate> {
    Utf8(V8LocalUtf8<'isolate_scope, 'isolate>),
    ArrBuff(V8LocalArrayBuffer<'isolate_scope, 'isolate>),
}

impl<'isolate_scope, 'isolate> V8RedisCallArgs<'isolate_scope, 'isolate> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            V8RedisCallArgs::Utf8(val) => val.as_str().as_bytes(),
            V8RedisCallArgs::ArrBuff(val) => val.data(),
        }
    }
}

impl<'isolate_scope, 'isolate> TryFrom<V8LocalValue<'isolate_scope, 'isolate>>
    for V8RedisCallArgs<'isolate_scope, 'isolate>
{
    type Error = &'static str;

    fn try_from(val: V8LocalValue<'isolate_scope, 'isolate>) -> Result<Self, Self::Error> {
        if val.is_string() || val.is_string_object() {
            match val.to_utf8() {
                Some(val) => Ok(V8RedisCallArgs::Utf8(val)),
                None => Err("Can not convert value into bytes buffer"),
            }
        } else if val.is_array_buffer() {
            Ok(V8RedisCallArgs::ArrBuff(val.as_array_buffer()))
        } else {
            Err("Can not convert value into bytes buffer")
        }
    }
}

impl<'isolate_scope, 'isolate, 'ctx_scope, 'a>
    TryFrom<&mut V8LocalNativeFunctionArgsIter<'isolate_scope, 'isolate, 'ctx_scope, 'a>>
    for V8RedisCallArgs<'isolate_scope, 'isolate>
{
    type Error = &'static str;

    fn try_from(
        val: &mut V8LocalNativeFunctionArgsIter<'isolate_scope, 'isolate, 'ctx_scope, 'a>,
    ) -> Result<Self, Self::Error> {
        val.next().ok_or("Wrong number of arguments.")?.try_into()
    }
}

#[derive(Debug, Copy, Clone)]
enum BackgroundExecution {
    /// Allow the command to go to the background (if it wants to)
    /// and return a future object that will be fulfill when the execution
    /// finishes.
    Allow,
    /// Deny the command to go to the background at any cost, even if
    /// it will need to fallback to some default behavior or return an error.
    Deny,
}
impl BackgroundExecution {
    fn allow(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

fn add_call_function(
    ctx_scope: &V8ContextScope,
    redis_client: &Arc<RefCell<RedisClient>>,
    script_ctx: &Arc<V8ScriptCtx>,
    client: &V8LocalObject,
    function_name: &str,
    decode_response: bool,
    background_execution: BackgroundExecution,
) {
    let redis_client_ref = Arc::clone(redis_client);
    let script_ctx_weak = Arc::downgrade(script_ctx);
    client.set_native_function(
        ctx_scope,
        function_name,
        new_native_function!(
            move |isolate_scope,
                  ctx_scope,
                  command_utf8: V8LocalUtf8,
                  commands_args: Vec<V8RedisCallArgs>| {
                let is_already_blocked = ctx_scope.get_private_data::<bool, _>(0);
                if is_already_blocked.is_none() || !*is_already_blocked.unwrap() {
                    return Err("Main thread is not locked".to_string());
                }

                let borrow_client = redis_client_ref.borrow();
                let c = borrow_client
                    .get()
                    .ok_or_else(|| "Used on invalid client".to_owned())?;

                if background_execution.allow() {
                    let script_ctx_ref = script_ctx_weak.upgrade().ok_or_else(|| "Library was already deleted".to_owned())?;
                    let res = c.call_async(
                        command_utf8.as_str(),
                        &commands_args
                            .iter()
                            .map(|v| v.as_bytes())
                            .collect::<Vec<&[u8]>>(),
                    );
                    let resolver = ctx_scope.new_resolver();
                    let promise = resolver.get_promise();
                    let mut persisted_resolver = resolver.to_value().persist();
                    let script_ctx_weak_resolve_result = script_ctx_weak.clone();
                    let mut resolve_result = move |res: Result<CallReply<'static>, ErrorReply<'static>>| {
                        let script_ctx_ref = match script_ctx_weak_resolve_result.upgrade() {
                            Some(s) => s,
                            None => {
                                log_warning("library was deleted while not all async job were finished");
                                return;
                            }
                        };
                        let isolate_scope = script_ctx_ref.isolate.enter();
                        let ctx_scope = script_ctx_ref.ctx.enter(&isolate_scope);

                        let resolver = persisted_resolver.take_local(&isolate_scope).as_resolver();
                        let res = call_result_to_js_object(
                            &isolate_scope,
                            &ctx_scope,
                            res,
                            decode_response,
                        );
                        match res {
                            Ok(res) => script_ctx_ref.resolve(&resolver, &ctx_scope, &res),
                            Err(e) => script_ctx_ref.reject(&resolver, &ctx_scope, &isolate_scope.new_string(&e).to_value())
                        }

                    };
                    match res {
                        PromiseReply::Resolved(res) => {
                            script_ctx_ref
                                .compiled_library_api
                                .run_on_background(Box::new(move || {
                                    resolve_result(res);
                                }));
                        }
                        PromiseReply::Future(set_on_done) => {
                            let script_ctx_weak = script_ctx_weak.clone();
                            set_on_done(Box::new(move |_ctx, reply| {
                                let script_ctx_ref = match script_ctx_weak.upgrade() {
                                    Some(s) => s,
                                    None => {
                                        log_warning("library was deleted while not all async job were finished");
                                        return;
                                    }
                                };
                                script_ctx_ref.compiled_library_api.run_on_background(Box::new(move || {
                                    resolve_result(reply);
                                }));

                            }));
                        }
                    };
                    Ok(Some(promise.to_value()))
                } else {
                    let res = c.call(
                        command_utf8.as_str(),
                        &commands_args
                            .iter()
                            .map(|v| v.as_bytes())
                            .collect::<Vec<&[u8]>>(),
                    );

                    Ok(Some(call_result_to_js_object(
                        isolate_scope,
                        ctx_scope,
                        res,
                        decode_response,
                    )?))
                }
            }
        ),
    );
}

pub(crate) fn get_redis_client<'isolate_scope, 'isolate>(
    script_ctx: &Arc<V8ScriptCtx>,
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope,
    redis_client: &Arc<RefCell<RedisClient>>,
) -> V8LocalObject<'isolate_scope, 'isolate> {
    let client = isolate_scope.new_object();

    add_call_function(
        ctx_scope,
        redis_client,
        script_ctx,
        &client,
        CALL_GLOBAL_NAME,
        true,
        BackgroundExecution::Deny,
    );
    add_call_function(
        ctx_scope,
        redis_client,
        script_ctx,
        &client,
        CALL_RAW_GLOBAL_NAME,
        false,
        BackgroundExecution::Deny,
    );
    add_call_function(
        ctx_scope,
        redis_client,
        script_ctx,
        &client,
        CALL_ASYNC_GLOBAL_NAME,
        true,
        BackgroundExecution::Allow,
    );
    add_call_function(
        ctx_scope,
        redis_client,
        script_ctx,
        &client,
        CALL_ASYNC_RAW_GLOBAL_NAME,
        false,
        BackgroundExecution::Allow,
    );

    let redis_client_ref = Arc::clone(redis_client);
    client.set_native_function(
        ctx_scope,
        IS_BLOCK_ALLOW_GLOBAL_NAME,
        new_native_function!(move |isolate_scope, _ctx_scope| {
            let res = match redis_client_ref.borrow().allow_block.as_ref() {
                Some(c) => *c,
                None => {
                    return Err("Used on invalid client");
                }
            };

            Ok(Some(isolate_scope.new_bool(res)))
        }),
    );

    let redisai_client = get_redisai_client(script_ctx, isolate_scope, ctx_scope, redis_client);
    client.set(
        ctx_scope,
        &isolate_scope.new_string(REDISAI_GLOBAL_NAME).to_value(),
        &redisai_client,
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    client.set_native_function(
        ctx_scope,
        EXECUTE_ASYNC_GLOBAL_NAME,
        new_native_function!(move |_isolate, ctx_scope, f: V8LocalValue| {
            let bg_redis_client = match redis_client_ref.borrow().get() {
                Some(c) => c.get_background_redis_client(),
                None => {
                    return Err(format!(
                        "Called '{EXECUTE_ASYNC_GLOBAL_NAME}' out of context"
                    ));
                }
            };

            if !f.is_async_function() {
                return Err(format!(
                    "First argument to '{EXECUTE_ASYNC_GLOBAL_NAME}' must be an async function"
                ));
            }

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    return Err("Use of invalid function context".to_owned());
                }
            };
            let mut f = f.persist();
            let new_script_ctx_ref = Arc::clone(&script_ctx_ref);
            let resolver = ctx_scope.new_resolver();
            let promise = resolver.get_promise();
            let mut resolver = resolver.to_value().persist();
            script_ctx_ref
                .compiled_library_api
                .run_on_background(Box::new(move || {
                    let isolate_scope = new_script_ctx_ref.isolate.enter();
                    let ctx_scope = new_script_ctx_ref.ctx.enter(&isolate_scope);
                    let trycatch = isolate_scope.new_try_catch();

                    let background_client = get_backgrounnd_client(
                        &new_script_ctx_ref,
                        &isolate_scope,
                        &ctx_scope,
                        Arc::new(bg_redis_client),
                    );
                    let res = new_script_ctx_ref.call(
                        &f.take_local(&isolate_scope),
                        &ctx_scope,
                        Some(&[&background_client.to_value()]),
                        GilStatus::Unlocked,
                    );

                    let resolver = resolver.take_local(&isolate_scope).as_resolver();
                    match res {
                        Some(r) => {
                            new_script_ctx_ref.resolve(&resolver, &ctx_scope, &r);
                        }
                        None => {
                            let error_utf8 = get_exception_v8_value(
                                &new_script_ctx_ref.isolate,
                                &isolate_scope,
                                trycatch,
                            );
                            new_script_ctx_ref.reject(&resolver, &ctx_scope, &error_utf8);
                        }
                    }
                }));
            Ok(Some(promise.to_value()))
        }),
    );
    client
}

/// A type defining an API version implementation.
pub(crate) type ApiVersionImplementation = fn(
    api_version: ApiVersionSupported,
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError>;

/// Defines a supported API version.
/// An object of type [`ApiVersionSupported`] is impossible to create if
/// the version isn't supported.
///
/// # Example
///
/// The only way to create an object of this type is to use the
/// [`std::convert::TryFrom`] with an object of [`ApiVersion`]:
///
/// ```rust,no_run,ignore
/// use redisgears_plugin_api::redisgears_plugin_api::prologue::ApiVersion;
///
/// let api_version = ApiVersion(1, 0);
/// let api_version_supported: ApiVersionSupported = api_version.try_into().unwrap();
/// ```
#[derive(Copy, Clone)]
pub struct ApiVersionSupported {
    version: ApiVersion,
    implementation: ApiVersionImplementation,
    is_deprecated: bool,
}
impl PartialOrd for ApiVersionSupported {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.version.partial_cmp(&other.version)
    }
}
impl Ord for ApiVersionSupported {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.version.cmp(&other.version)
    }
}
impl PartialEq for ApiVersionSupported {
    fn eq(&self, other: &Self) -> bool {
        self.version.eq(&other.version)
    }
}
impl Eq for ApiVersionSupported {}
impl std::fmt::Debug for ApiVersionSupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiVersionSupported")
            .field("version", &self.version)
            .field(
                "implementation",
                &(self.implementation as *const std::ffi::c_void),
            )
            .field("is_deprecated", &self.is_deprecated)
            .finish()
    }
}

impl ApiVersionSupported {
    /// A list of all currently supported and deprecated versions.
    const SUPPORTED: [ApiVersionSupported; 2] = [
        Self::new(ApiVersion(1, 0), initialize_globals_1_0, false),
        Self::new(ApiVersion(1, 1), initialize_globals_1_1, false),
    ];

    const fn new(
        version: ApiVersion,
        implementation: ApiVersionImplementation,
        is_deprecated: bool,
    ) -> Self {
        Self {
            version,
            implementation,
            is_deprecated,
        }
    }

    /// Returns the version stored.
    pub fn get_version(&self) -> ApiVersion {
        self.version
    }

    /// Returns a pointer to the API implementation of this version.
    pub(crate) fn get_implementation(&self) -> &ApiVersionImplementation {
        &self.implementation
    }

    /// Returns the minimum supported version.
    ///
    /// # Panics
    ///
    /// Panics if there are no supported versions available.
    pub fn minimum_supported() -> Self {
        Self::SUPPORTED
            .iter()
            .min()
            .cloned()
            .expect("No supported versions found.")
    }

    /// Returns the maximum supported version.
    ///
    /// # Panics
    ///
    /// Panics if there are no supported versions available.
    #[allow(dead_code)]
    pub fn maximum_supported() -> Self {
        Self::SUPPORTED
            .iter()
            .max()
            .cloned()
            .expect("No supported versions found.")
    }

    /// Returns all the version supported.
    pub const fn all_supported() -> &'static [ApiVersionSupported] {
        &Self::SUPPORTED
    }

    /// Returns all the version deprecated.
    #[allow(dead_code)]
    pub fn all_deprecated() -> Vec<ApiVersion> {
        Self::SUPPORTED
            .iter()
            .filter(|v| v.is_deprecated)
            .map(|v| v.version)
            .collect()
    }

    /// Returns `true` if the version is supported.
    #[allow(dead_code)]
    pub fn is_supported(version: ApiVersion) -> bool {
        Self::SUPPORTED.iter().any(|v| v.version == version)
    }

    /// Returns `true` if the version is supported but deprecated.
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }

    /// Converts the current version into the latest compatible,
    /// following the semantic versioning scheme.
    ///
    /// # Example
    ///
    /// If there are versions supported: 1.0, 1.1, 1.2 and 1.3, then
    /// for the any of those versions, the latest compatible one is the
    /// version 1.3, so with the same major number (1) but the maximum
    /// minor number (3).
    ///
    /// ```rust,no_run,ignore
    /// use redisgears_v8_plugin::v8_native_functions::ApiVersionSupported;
    ///
    /// let api_version = ApiVersionSupported::default();
    /// assert_eq!(
    ///    api_version.into_latest_compatible(),
    ///    ApiVersionSupported::maximum_supported()
    /// );
    /// ```
    pub fn into_latest_compatible(self) -> ApiVersionSupported {
        Self::SUPPORTED
            .iter()
            .filter(|v| v.get_version().get_major() == self.get_version().get_major())
            .max()
            .cloned()
            .unwrap_or(self)
    }

    /// Validates the code against using this API version.
    pub(crate) fn validate_code(&self, _code: &str) -> Vec<GearsApiError> {
        let mut messages = Vec::new();

        if self.is_deprecated() {
            messages.push(GearsApiError::new(format!(
                "The code uses a deprecated version of the API: {self}"
            )));
        }

        // Potentially check if uses deprecated symbols here using a JS parser.

        messages
    }
}

impl Default for ApiVersionSupported {
    fn default() -> Self {
        Self::minimum_supported()
    }
}

impl TryFrom<ApiVersion> for ApiVersionSupported {
    type Error = prologue::Error;

    fn try_from(value: ApiVersion) -> Result<Self, Self::Error> {
        Self::SUPPORTED
            .iter()
            .find(|v| v.version == value)
            .cloned()
            .ok_or_else(|| Self::Error::UnsupportedApiVersion {
                requested: value,
                supported: Self::all_supported().iter().map(|v| v.version).collect(),
            })
    }
}

impl std::fmt::Display for ApiVersionSupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.version.fmt(f)
    }
}

/// Initialises the global `redis` object by populating it with methods
/// for the provided supported API version.
pub(crate) fn initialize_globals_for_version(
    api_version: ApiVersionSupported,
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError> {
    let redis = isolate_scope.new_object();
    api_version.get_implementation()(
        api_version,
        &redis,
        script_ctx,
        globals,
        isolate_scope,
        ctx_scope,
        config,
    )
}

/// Creates a global `redis` object with methods for the API of version "1.1".
/// The `1.1` API is an extension to the `1.0` which provides the
/// `redis.api_version()` method to obtain the current version of the API used.
pub(crate) fn initialize_globals_1_1(
    api_version: ApiVersionSupported,
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError> {
    initialize_globals_1_0(
        api_version,
        redis,
        script_ctx,
        globals,
        isolate_scope,
        ctx_scope,
        config,
    )?;

    redis.set_native_function(
        ctx_scope,
        "apiVersion",
        new_native_function!(move |isolate_scope, _curr_ctx_scope| {
            let v_v8_str = isolate_scope.new_string(&api_version.to_string());
            Ok::<Option<V8LocalValue>, String>(Some(v_v8_str.to_value()))
        }),
    );

    Ok(())
}

#[derive(NativeFunctionArgument)]
struct NativeFunctionOptionalArgs<'isolate_scope, 'isolate> {
    description: Option<String>,
    flags: Option<V8LocalArray<'isolate_scope, 'isolate>>,
}

fn add_register_function_api(
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
    is_async: bool,
) {
    let name = if !is_async {
        REGISTER_FUNCTION_GLOBAL_NAME
    } else {
        REGISTER_ASYNC_FUNCTION_GLOBAL_NAME
    };
    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set_native_function(
        ctx_scope,
        name,
        new_native_function!(
            move |isolate_scope,
                  curr_ctx_scope,
                  function_name_utf8: V8LocalUtf8,
                  function_callback: V8LocalValue,
                  optional_args: Option<NativeFunctionOptionalArgs>| {
                if !function_callback.is_function() {
                    return Err(format!(
                        "Second argument to '{REGISTER_FUNCTION_GLOBAL_NAME}' must be a function"
                    ));
                }
                if !is_async && function_callback.is_async_function() {
                    return Err(format!(
                        "'{REGISTER_FUNCTION_GLOBAL_NAME}' can not be used with async function, use '{REGISTER_ASYNC_FUNCTION_GLOBAL_NAME}' instead."
                    ));
                }

                let persisted_function = function_callback.persist();

                let function_flags =
                    optional_args
                        .as_ref()
                        .map_or(Ok(FunctionFlags::empty()), |v| {
                            v.flags.as_ref().map_or(Ok(FunctionFlags::empty()), |v| {
                                get_function_flags_from_strings(curr_ctx_scope, v)
                                    .map_err(|e| format!("Failed parsing function flags, {}", e))
                            })
                        })?;

                let description = optional_args.and_then(|v| v.description);

                let load_ctx =
                    curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface, _>(0);
                if load_ctx.is_none() {
                    return Err("Called 'register_function' out of context".into());
                }

                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        return Err("Use of uninitialized script context".into());
                    }
                };

                let load_ctx = load_ctx.unwrap();
                let c = Arc::new(RefCell::new(RedisClient::new()));
                let redis_client =
                    get_redis_client(&script_ctx_ref, isolate_scope, curr_ctx_scope, &c);

                let f = V8Function::new(
                    &script_ctx_ref,
                    persisted_function,
                    redis_client.to_value().persist(),
                    &c,
                    function_callback.is_async_function(),
                    !function_flags.contains(FunctionFlags::RAW_ARGUMENTS),
                );

                let res = if is_async {
                    load_ctx.register_async_function(
                        function_name_utf8.as_str(),
                        Box::new(f),
                        function_flags,
                        description,
                    )
                } else {
                    load_ctx.register_function(
                        function_name_utf8.as_str(),
                        Box::new(f),
                        function_flags,
                        description,
                    )
                };
                if let Err(err) = res {
                    return Err(err.get_msg().into());
                }
                Ok(None)
            }
        ),
    );
}

#[allow(non_snake_case)]
#[derive(NativeFunctionArgument)]
struct StreamTriggerOptionalArgs {
    description: Option<String>,
    window: Option<i64>,
    isStreamTrimmed: Option<bool>,
}

fn add_stream_trigger_api(
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
) {
    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set_native_function(ctx_scope, REGISTER_STREAM_TRIGGER_GLOBAL_NAME, new_native_function!(move|
        _isolate_scope,
        curr_ctx_scope,
        registration_name_utf8: V8LocalUtf8,
        prefix: V8LocalValue,
        function_callback: V8LocalValue,
        optional_args: Option<StreamTriggerOptionalArgs>,
    | {
        if !function_callback.is_function() {
            return Err(format!("The fifth argument to '{REGISTER_STREAM_TRIGGER_GLOBAL_NAME}' must be a function"));
        }
        let persisted_function = function_callback.persist();

        let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface, _>(0).ok_or_else(|| "Called 'registerFunction' out of context".to_string())?;

        let script_ctx_ref = script_ctx_ref.upgrade().ok_or_else(|| "Use of uninitialized script context".to_string())?;

        let window = optional_args.as_ref().map_or(1, |v| v.window.as_ref().map_or(1, |v| *v));
        if window < 1 {
            return Err("window argument must be a positive number".into());
        }
        let trim = optional_args.as_ref().map_or(false, |v| v.isStreamTrimmed.as_ref().map_or(false, |v| *v));
        let description = optional_args.and_then(|v| v.description);

        let v8_stream_ctx = V8StreamCtx::new(persisted_function, &script_ctx_ref, function_callback.is_async_function());
        let res = if prefix.is_string() {
            let prefix = prefix.to_utf8().unwrap();
            load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix.as_str().as_bytes(), Box::new(v8_stream_ctx), window as usize, trim, description)
        } else if prefix.is_array_buffer() {
            let prefix = prefix.as_array_buffer();
            load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix.data(), Box::new(v8_stream_ctx), window as usize, trim, description)
        } else {
            return Err(format!("Second argument to '{REGISTER_STREAM_TRIGGER_GLOBAL_NAME}' must be a String or ArrayBuffer representing the prefix"));
        };
        if let Err(err) = res {
            return Err(err.get_msg().to_string());
        }
        Ok(None)
    }));
}

#[allow(non_snake_case)]
#[derive(NativeFunctionArgument)]
struct NoficationConsumerOptionalArgs<'isolate_scope, 'isolate> {
    onTriggerFired: Option<V8LocalValue<'isolate_scope, 'isolate>>,
    description: Option<String>,
}

fn add_register_notification_consumer_api(
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
) {
    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set_native_function(ctx_scope, REGISTER_NOTIFICATIONS_CONSUMER, new_native_function!(move|
        _isolate_scope,
        curr_ctx_scope,
        registration_name_utf8: V8LocalUtf8,
        prefix: V8LocalValue,
        function_callback: V8LocalValue,
        optional_args: Option<NoficationConsumerOptionalArgs>,
    | {
        if !function_callback.is_function() {
            return Err(format!("Third argument to '{REGISTER_NOTIFICATIONS_CONSUMER}' must be a function"));
        }
        let persisted_function = function_callback.persist();

        let on_trigger_fired = optional_args.as_ref().map_or(Result::<_, String>::Ok(None), |v| {
            v.onTriggerFired.as_ref().map_or(Ok(None), |v|{
                if !v.is_function() || v.is_async_function() {
                    return Err(format!("'onTriggerFired' argument to '{REGISTER_NOTIFICATIONS_CONSUMER}' must be a function"));
                }
                Ok(Some(v.persist()))
            })
        })?;

        let description = optional_args.and_then(|v| v.description);

        let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface, _>(0).ok_or_else(|| format!("Called '{REGISTER_NOTIFICATIONS_CONSUMER}' out of context"))?;

        let script_ctx_ref = script_ctx_ref.upgrade().ok_or_else(|| "Use of uninitialized script context".to_owned())?;

        let v8_notification_ctx = V8NotificationsCtx::new(persisted_function, on_trigger_fired, &script_ctx_ref, function_callback.is_async_function());

        let res = if prefix.is_string() {
            let prefix = prefix.to_utf8().unwrap();
            load_ctx.register_key_space_notification_consumer(registration_name_utf8.as_str(), RegisteredKeys::Prefix(prefix.as_str().as_bytes()), Box::new(v8_notification_ctx), description)
        } else if prefix.is_array_buffer() {
            let prefix = prefix.as_array_buffer();
            load_ctx.register_key_space_notification_consumer(registration_name_utf8.as_str(), RegisteredKeys::Prefix(prefix.data()), Box::new(v8_notification_ctx, ), description)
        } else {
            return Err(format!("Second argument to '{REGISTER_NOTIFICATIONS_CONSUMER}' must be a string or ArrayBuffer representing the prefix"));
        };
        if let Err(err) = res {
            return Err(err.get_msg().to_string());
        }
        Ok(None)
    }));
}

/// Creates a global `redis` object with methods for the API of version "1.0".
pub(crate) fn initialize_globals_1_0(
    _api_version: ApiVersionSupported,
    redis: &V8LocalObject,
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError> {
    match config {
        Some(c) => {
            let string = isolate_scope.new_string(c);
            let trycatch = isolate_scope.new_try_catch();
            let config_json = ctx_scope.new_object_from_json(&string);
            if config_json.is_none() {
                return Err(get_exception_msg(&script_ctx.isolate, trycatch, ctx_scope));
            }
            redis.set(
                ctx_scope,
                &isolate_scope.new_string("config").to_value(),
                &config_json.unwrap(),
            )
        }
        None => {
            // setting empty config
            redis.set(
                ctx_scope,
                &isolate_scope.new_string("config").to_value(),
                &isolate_scope.new_object().to_value(),
            )
        }
    }

    add_stream_trigger_api(redis, script_ctx, ctx_scope);

    add_register_notification_consumer_api(redis, script_ctx, ctx_scope);

    // add 'register_function'
    add_register_function_api(redis, script_ctx, ctx_scope, false);
    // add 'register_async_function'
    add_register_function_api(redis, script_ctx, ctx_scope, true);

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set_native_function(ctx_scope, REGISTER_CLUSTER_FUNCTION_GLOBAL_NAME, new_native_function!(move|
        _isolate_scope,
        curr_ctx_scope,
        function_name_utf8: V8LocalUtf8,
        function_callback: V8LocalValue,
    | {
        if !function_callback.is_function() {
            return Err(format!("Second argument to '{REGISTER_CLUSTER_FUNCTION_GLOBAL_NAME}' must be a function"));
        }

        if !function_callback.is_async_function() {
            return Err("Remote function must be async".into());
        }

        let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface, _>(0);
        if load_ctx.is_none() {
            return Err(format!("Called '{REGISTER_CLUSTER_FUNCTION_GLOBAL_NAME}' out of context"));
        }

        let mut persisted_function = function_callback.persist();
        persisted_function.forget();
        let persisted_function = Arc::new(persisted_function);

        let load_ctx = load_ctx.unwrap();
        let new_script_ctx_ref = Weak::clone(&script_ctx_ref);
        let res = load_ctx.register_remote_task(function_name_utf8.as_str(), Box::new(move |inputs, background_ctx, on_done|{
            let script_ctx = match new_script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    on_done(Err(GearsApiError::new("Use of uninitialized script context".to_string())));
                    return;
                }
            };

            let new_script_ctx_ref = Weak::clone(&new_script_ctx_ref);
            let weak_function = Arc::downgrade(&persisted_function);
            script_ctx.compiled_library_api.run_on_background(Box::new(move || {
                let script_ctx = match new_script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        on_done(Err(GearsApiError::new("Use of uninitialized script context".to_string())));
                        return;
                    }
                };
                let persisted_function = match weak_function.upgrade() {
                    Some(s) => s,
                    None => {
                        on_done(Err(GearsApiError::new("Use of uninitialized function context".to_string())));
                        return;
                    }
                };
                let isolate_scope = script_ctx.isolate.enter();
                let ctx_scope = script_ctx.ctx.enter(&isolate_scope);
                let trycatch = isolate_scope.new_try_catch();

                let mut args = Vec::new();
                args.push(get_backgrounnd_client(&script_ctx, &isolate_scope, &ctx_scope, Arc::new(background_ctx)).to_value());
                for input in inputs {
                    args.push(match input {
                        RemoteFunctionData::Binary(b) => isolate_scope.new_array_buffer(&b).to_value(),
                        RemoteFunctionData::String(s) => {
                            let v8_str = isolate_scope.new_string(&s);
                            let v8_obj = ctx_scope.new_object_from_json(&v8_str);
                            if v8_obj.is_none() {
                                on_done(Err(GearsApiError::new("Failed deserializing remote function argument".to_string())));
                                return;
                            }
                            v8_obj.unwrap()
                        }
                    });
                }
                let args_refs = args.iter().collect::<Vec<&V8LocalValue>>();

                let res = script_ctx.call(&persisted_function.as_local(&isolate_scope), &ctx_scope, Some(&args_refs), GilStatus::Unlocked);

                match res {
                    Some(r) => {
                        if r.is_promise() {
                            script_ctx.handle_promise(&isolate_scope, &ctx_scope, &r.as_promise(), move |res| {
                                match res {
                                    Ok(v) => {
                                        let trycatch = v.isolate_scope.new_try_catch();
                                        let r = js_value_to_remote_function_data(v.ctx_scope, v.res);
                                        if let Some(v) = r {
                                            on_done(Ok(v));
                                        } else {
                                            let error_utf8 = trycatch.get_exception().to_utf8().unwrap();
                                            on_done(Err(GearsApiError::new(format!("Failed serializing result, {}.", error_utf8.as_str()))));
                                        }
                                    }
                                    Err(e) => on_done(Err(e)),
                                }
                            });
                        } else {
                            let r = js_value_to_remote_function_data(&ctx_scope, r);
                            if let Some(v) = r {
                                on_done(Ok(v));
                            } else {
                                on_done(Err(GearsApiError::new("Failed serializing result".to_string())));
                            }
                        }
                    }
                    None => {
                        let error_msg = get_exception_msg(&script_ctx.isolate, trycatch, &ctx_scope);
                        on_done(Err(error_msg));
                    }
                };
            }));
        }));

        if let Err(err) = res {
            return Err(err.get_msg().to_string());
        }
        Ok(None)
    }));

    redis.set_native_function(
        ctx_scope,
        V8_VERSION_GLOBAL_NAME,
        new_native_function!(move |isolate_scope, _curr_ctx_scope| {
            let v = v8_version();
            let v_v8_str = isolate_scope.new_string(v);
            Ok::<Option<V8LocalValue>, String>(Some(v_v8_str.to_value()))
        }),
    );

    // add function flags
    let function_flags = get_function_flags_globals(ctx_scope, isolate_scope);
    redis.set(
        ctx_scope,
        &isolate_scope
            .new_string(FUNCTION_FLAGS_GLOBAL_NAME)
            .to_value(),
        &function_flags,
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set_native_function(
        ctx_scope,
        LOG_GLOBAL_NAME,
        new_native_function!(move |_isolate, _curr_ctx_scope, msg: V8LocalUtf8| {
            match script_ctx_ref.upgrade() {
                Some(s) => s.compiled_library_api.log_info(msg.as_str()),
                None => crate::v8_backend::log_info(msg.as_str()), /* do not abort logs */
            }
            Ok::<Option<V8LocalValue>, String>(None)
        }),
    );

    let redis_ai = get_redisai_api(script_ctx, isolate_scope, ctx_scope);
    redis.set(
        ctx_scope,
        &isolate_scope.new_string(REDISAI_GLOBAL_NAME).to_value(),
        &redis_ai,
    );

    globals.set(
        ctx_scope,
        &isolate_scope.new_string(REDIS_GLOBAL_NAME).to_value(),
        &redis.to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    globals.set_native_function(
        ctx_scope,
        "Promise",
        new_native_function!(
            move |_isolate_scope, curr_ctx_scope, function: V8LocalValue| {
                if !function.is_function() || function.is_async_function() {
                    return Err("Bad argument to 'Promise' function");
                }

                let script_ctx_ref = script_ctx_ref
                    .upgrade()
                    .ok_or("Use of uninitialized script context")?;

                let script_ctx_ref_resolve = Arc::downgrade(&script_ctx_ref);
                let script_ctx_ref_reject = Arc::downgrade(&script_ctx_ref);
                let resolver = curr_ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver_resolve = Arc::new(RefCellWrapper {
                    ref_cell: RefCell::new(Some(resolver.to_value().persist())),
                });
                let resolver_reject = Arc::clone(&resolver_resolve);

                let resolve = curr_ctx_scope.new_native_function(new_native_function!(
                    move |_isolate, _curr_ctx_scope, arg: V8LocalValue| {
                        let script_ctx_ref_resolve = match script_ctx_ref_resolve.upgrade() {
                            Some(s) => s,
                            None => {
                                if let Some(mut v) = resolver_resolve.ref_cell.borrow_mut().take() {
                                    v.forget();
                                }
                                return Err("Library was deleted");
                            }
                        };

                        let mut res = arg.persist();
                        let new_script_ctx_ref_resolve = Arc::downgrade(&script_ctx_ref_resolve);
                        let resolver_resolve = Arc::clone(&resolver_resolve);
                        script_ctx_ref_resolve
                            .compiled_library_api
                            .run_on_background(Box::new(move || {
                                let new_script_ctx_ref_resolve =
                                    match new_script_ctx_ref_resolve.upgrade() {
                                        Some(s) => s,
                                        None => {
                                            if let Some(mut v) =
                                                resolver_resolve.ref_cell.borrow_mut().take()
                                            {
                                                v.forget();
                                            }
                                            res.forget();
                                            log_warning(
                                            "Library was delete while not all the jobs were done",
                                        );
                                            return;
                                        }
                                    };
                                let isolate_scope = new_script_ctx_ref_resolve.isolate.enter();
                                let ctx_scope =
                                    new_script_ctx_ref_resolve.ctx.enter(&isolate_scope);
                                let _trycatch = isolate_scope.new_try_catch();
                                let res = res.take_local(&isolate_scope);
                                if let Some(mut v) = resolver_resolve.ref_cell.borrow_mut().take() {
                                    let resolver = v.take_local(&isolate_scope).as_resolver();
                                    new_script_ctx_ref_resolve.resolve(&resolver, &ctx_scope, &res);
                                };
                            }));
                        Ok(None)
                    }
                ));

                let reject = curr_ctx_scope.new_native_function(new_native_function!(
                    move |_isolate_scope, _curr_ctx_scope, arg: V8LocalValue| {
                        let script_ctx_ref_reject = match script_ctx_ref_reject.upgrade() {
                            Some(s) => s,
                            None => {
                                if let Some(mut v) = resolver_reject.ref_cell.borrow_mut().take() {
                                    v.forget();
                                }
                                return Err("Library was deleted");
                            }
                        };

                        let mut res = arg.persist();
                        let new_script_ctx_ref_reject = Arc::downgrade(&script_ctx_ref_reject);
                        let resolver_reject = Arc::clone(&resolver_reject);
                        script_ctx_ref_reject
                            .compiled_library_api
                            .run_on_background(Box::new(move || {
                                let new_script_ctx_ref_reject =
                                    match new_script_ctx_ref_reject.upgrade() {
                                        Some(s) => s,
                                        None => {
                                            res.forget();
                                            if let Some(mut v) =
                                                resolver_reject.ref_cell.borrow_mut().take()
                                            {
                                                v.forget();
                                            }
                                            log_warning(
                                            "Library was delete while not all the jobs were done",
                                        );
                                            return;
                                        }
                                    };
                                let isolate_scope = new_script_ctx_ref_reject.isolate.enter();
                                let ctx_scope = new_script_ctx_ref_reject.ctx.enter(&isolate_scope);
                                let _trycatch = isolate_scope.new_try_catch();
                                let res = res.take_local(&isolate_scope);
                                if let Some(mut v) = resolver_reject.ref_cell.borrow_mut().take() {
                                    let resolver = v.take_local(&isolate_scope).as_resolver();
                                    new_script_ctx_ref_reject.reject(&resolver, &ctx_scope, &res);
                                };
                            }));
                        Ok(None)
                    }
                ));

                let _ = function.call(
                    curr_ctx_scope,
                    Some(&[&resolve.to_value(), &reject.to_value()]),
                );
                Ok(Some(promise.to_value()))
            }
        ),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latest_supported_version_available() {
        let api_version = ApiVersionSupported::default();
        assert_eq!(
            api_version.into_latest_compatible(),
            ApiVersionSupported::maximum_supported()
        );
    }
}
