use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LoadLibraryCtxInterface, load_library_ctx::RegisteredKeys,
    load_library_ctx::FUNCTION_FLAG_RAW_ARGUMENTS,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::RemoteFunctionData, CallResult, GearsApiError, RefCellWrapper,
};

use v8_rs::v8::{
    isolate_scope::V8IsolateScope, v8_array_buffer::V8LocalArrayBuffer,
    v8_context_scope::V8ContextScope, v8_object::V8LocalObject, v8_promise::V8PromiseState,
    v8_utf8::V8LocalUtf8, v8_value::V8LocalValue, v8_version,
};

use crate::v8_redisai::{get_redisai_api, get_redisai_client};

use crate::v8_backend::log;
use crate::v8_function_ctx::V8Function;
use crate::v8_notifications_ctx::V8NotificationsCtx;
use crate::v8_script_ctx::V8ScriptCtx;
use crate::v8_stream_ctx::V8StreamCtx;
use crate::{get_exception_msg, get_exception_v8_value, get_function_flags};

use std::cell::RefCell;
use std::sync::{Arc, Weak};

pub(crate) fn call_result_to_js_object<'isolate_scope, 'isolate>(
    isolate_scope: &'isolate_scope V8IsolateScope<'isolate>,
    ctx_scope: &V8ContextScope,
    res: CallResult,
    decode_responses: bool,
) -> Option<V8LocalValue<'isolate_scope, 'isolate>> {
    match res {
        CallResult::SimpleStr(s) => {
            let s = isolate_scope.new_string(&s).to_string_object();
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("status").to_value(),
            );
            Some(s.to_value())
        }
        CallResult::BulkStr(s) => {
            let s = isolate_scope.new_string(&s).to_string_object();
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("bulk_string").to_value(),
            );
            Some(s.to_value())
        }
        CallResult::Error(e) => {
            isolate_scope.raise_exception_str(&e);
            None
        }
        CallResult::Long(l) => Some(isolate_scope.new_long(l)),
        CallResult::Double(d) => Some(isolate_scope.new_double(d)),
        CallResult::Array(a) => {
            let mut has_error = false;
            let vals = a
                .into_iter()
                .map(|v| {
                    let res =
                        call_result_to_js_object(isolate_scope, ctx_scope, v, decode_responses);
                    if res.is_none() {
                        has_error = true;
                    }
                    res
                })
                .collect::<Vec<Option<V8LocalValue>>>();
            if has_error {
                return None;
            }

            let array = isolate_scope.new_array(
                &vals
                    .iter()
                    .map(|v| v.as_ref().unwrap())
                    .collect::<Vec<&V8LocalValue>>(),
            );
            Some(array.to_value())
        }
        CallResult::Map(m) => {
            let obj = isolate_scope.new_object();
            for (k, v) in m {
                let k_js_string = if decode_responses {
                    let s = match String::from_utf8(k) {
                        Ok(s) => s,
                        Err(_) => {
                            isolate_scope.raise_exception_str("Could not decode value as string");
                            return None;
                        }
                    };
                    isolate_scope.new_string(&s).to_value()
                } else {
                    isolate_scope.raise_exception_str("Binary map key is not supported");
                    return None;
                };
                let v_js = call_result_to_js_object(isolate_scope, ctx_scope, v, decode_responses);
                if v_js.is_none() {
                    return None;
                }
                let v_js = v_js.unwrap();
                obj.set(ctx_scope, &k_js_string, &v_js);
            }
            Some(obj.to_value())
        }
        CallResult::Set(s) => {
            let set = isolate_scope.new_set();
            for v in s {
                let v_js_string = if decode_responses {
                    let s = match String::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            isolate_scope.raise_exception_str("Could not decode value as string");
                            return None;
                        }
                    };
                    isolate_scope.new_string(&s).to_value()
                } else {
                    isolate_scope.raise_exception_str("Binary set element is not supported");
                    return None;
                };
                set.add(ctx_scope, &v_js_string);
            }
            Some(set.to_value())
        }
        CallResult::Bool(b) => Some(isolate_scope.new_bool(b)),
        CallResult::BigNumber(s) => {
            let s = isolate_scope.new_string(&s).to_string_object();
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("big_number").to_value(),
            );
            Some(s.to_value())
        }
        CallResult::VerbatimString((ext, s)) => {
            let s = isolate_scope.new_string(&s).to_string_object();
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__reply_type").to_value(),
                &isolate_scope.new_string("verbatim").to_value(),
            );
            s.set(
                ctx_scope,
                &isolate_scope.new_string("__ext").to_value(),
                &isolate_scope.new_string(&ext).to_value(),
            );
            Some(s.to_value())
        }
        CallResult::Null => Some(isolate_scope.new_null()),
        CallResult::StringBuffer(s) => {
            if decode_responses {
                let s = match String::from_utf8(s) {
                    Ok(s) => s,
                    Err(_) => {
                        isolate_scope.raise_exception_str("Could not decode value as string");
                        return None;
                    }
                };
                let s = isolate_scope.new_string(&s).to_string_object();
                s.set(
                    ctx_scope,
                    &isolate_scope.new_string("__reply_type").to_value(),
                    &isolate_scope.new_string("bulk_string").to_value(),
                );
                Some(s.to_value())
            } else {
                Some(isolate_scope.new_array_buffer(&s).to_value())
            }
        }
    }
}

pub(crate) struct RedisClient {
    pub(crate) client: Option<Box<dyn RedisClientCtxInterface>>,
    allow_block: Option<bool>,
}

impl RedisClient {
    pub(crate) fn new() -> RedisClient {
        RedisClient {
            client: None,
            allow_block: Some(true),
        }
    }

    pub(crate) fn make_invalid(&mut self) {
        self.client = None;
        self.allow_block = None;
    }

    pub(crate) fn set_client(&mut self, c: Box<dyn RedisClientCtxInterface>) {
        self.client = Some(c);
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
        Some(RemoteFunctionData::Binary(
            data.into_iter().map(|v| *v).collect(),
        ))
    } else {
        let arg_str = ctx_scope.json_stringify(&val);
        if arg_str.is_none() {
            return None;
        }
        let arg_str_utf8 = arg_str.unwrap().to_value().to_utf8().unwrap();
        Some(RemoteFunctionData::String(
            arg_str_utf8.as_str().to_string(),
        ))
    }
}

enum BinaryDataHelper<'isolate_scope, 'isolate> {
    V8Uft(V8LocalUtf8<'isolate_scope, 'isolate>),
    V8Binary(V8LocalArrayBuffer<'isolate_scope, 'isolate>),
}

impl<'isolate_scope, 'isolate> BinaryDataHelper<'isolate_scope, 'isolate> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            BinaryDataHelper::V8Uft(val) => val.as_str().as_bytes(),
            BinaryDataHelper::V8Binary(val) => val.data(),
        }
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
    bg_client.set(
        ctx_scope,
        &isolate_scope.new_string("block").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, ctx_scope| {
                if args.len() < 1 {
                    isolate_scope.raise_exception_str(
                        "Wrong number of arguments to 'isolate_scopelock' function",
                    );
                    return None;
                }
                let f = args.get(0);
                if !f.is_function() {
                    isolate_scope
                        .raise_exception_str("Argument to 'isolate_scopelock' must be a function");
                    return None;
                }

                let is_already_blocked = ctx_scope.get_private_data::<bool>(0);
                if !is_already_blocked.is_none() && *is_already_blocked.unwrap() {
                    isolate_scope.raise_exception_str("Main thread is already blocked");
                    return None;
                }

                let redis_client = {
                    let _unlocker = isolate_scope.new_unlocker();
                    match redis_background_client_ref.lock() {
                        Ok(l) => l,
                        Err(err) => {
                            isolate_scope.raise_exception_str(&format!(
                                "Can not lock Redis, {}",
                                err.get_msg()
                            ));
                            return None;
                        }
                    }
                };
                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate_scope.raise_exception_str("Function were unregistered");
                        return None;
                    }
                };

                let r_client = Arc::new(RefCell::new(RedisClient::new()));
                r_client.borrow_mut().set_client(redis_client);
                let c = get_redis_client(&script_ctx_ref, isolate_scope, ctx_scope, &r_client);

                ctx_scope.set_private_data(0, Some(&true)); // indicate we are blocked

                script_ctx_ref.after_lock_gil();
                let res = f.call(ctx_scope, Some(&[&c.to_value()]));
                script_ctx_ref.before_release_gil();

                ctx_scope.set_private_data::<bool>(0, None);

                r_client.borrow_mut().make_invalid();
                res
            })
            .to_value(),
    );

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_weak_ref = Arc::downgrade(script_ctx);
    bg_client.set(
        ctx_scope,
        &isolate_scope.new_string("run_on_key").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() < 2 {
                    isolate.raise_exception_str("Wrong number of arguments to 'isolate_scopelock' function");
                    return None;
                }

                let key = args.get(0);
                let key = if key.is_string() || key.is_string_object() {
                    BinaryDataHelper::V8Uft(key.to_utf8().unwrap())
                } else if key.is_array_buffer() {
                    BinaryDataHelper::V8Binary(key.as_array_buffer())
                } else {
                    isolate.raise_exception_str("First argument to 'run_on_key' must be a string represnting a key name");
                    return None;
                };

                let remote_function_name = args.get(1);
                if !remote_function_name.is_string() && !remote_function_name.is_string_object() {
                    isolate.raise_exception_str("Second argument to 'run_on_key' must be a string represnting a remote function name");
                    return None;
                }
                let remote_function_name = remote_function_name.to_utf8().unwrap();

                let mut args_vec = Vec::new();
                for i in 2 .. args.len() {
                    let arg = args.get(i);
                    let arg = js_value_to_remote_function_data(ctx_scope, arg);
                    match  arg {
                        Some(arg) => args_vec.push(arg),
                        None => return None,
                    };
                }

                let _ = match script_ctx_weak_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate.raise_exception_str("Function were unregistered");
                        return None;
                    }
                };

                let resolver = ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let mut resolver = resolver.to_value().persist();
                let script_ctx_weak_ref = Weak::clone(&script_ctx_weak_ref);
                redis_background_client_ref.run_on_key(key.as_bytes(), remote_function_name.as_str(), args_vec, Box::new(move |result|{
                    let script_ctx = match script_ctx_weak_ref.upgrade() {
                        Some(s) => s,
                        None => {
                            resolver.forget();
                            log("Library was delete while not all the remote jobs were done");
                            return;
                        }
                    };

                    script_ctx.compiled_library_api.run_on_background(Box::new(move||{
                        let script_ctx = match script_ctx_weak_ref.upgrade() {
                            Some(s) => s,
                            None => {
                                resolver.forget();
                                log("Library was delete while not all the remote jobs were done");
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
                                            resolver.reject(&ctx_scope, &isolate_scope.new_string("Failed deserializing remote function result").to_value());
                                            return;
                                        }
                                        v8_obj.unwrap()
                                    }
                                };
                                resolver.resolve(&ctx_scope, &v)
                            },
                            Err(e) => {
                                match e {
                                    GearsApiError::Msg(msg) => resolver.reject(&ctx_scope, &isolate_scope.new_string(&msg).to_value())
                                }
                            }
                        }
                    }));
                }));
                Some(promise.to_value())
            })
            .to_value(),
    );

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_weak_ref = Arc::downgrade(script_ctx);
    bg_client.set(
        ctx_scope,
        &isolate_scope.new_string("run_on_all_shards").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() < 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'isolate_scopelock' function");
                    return None;
                }

                let remote_function_name = args.get(0);
                if !remote_function_name.is_string() && !remote_function_name.is_string_object() {
                    isolate.raise_exception_str("Second argument to 'run_on_all_shards' must be a string represnting a remote function name");
                    return None;
                }
                let remote_function_name = remote_function_name.to_utf8().unwrap();

                let mut args_vec = Vec::new();
                for i in 1 .. args.len() {
                    let arg = args.get(i);
                    let arg = js_value_to_remote_function_data(ctx_scope, arg);
                    match  arg {
                        Some(arg) => args_vec.push(arg),
                        None => return None,
                    };
                }

                let _ = match script_ctx_weak_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate.raise_exception_str("Function were unregistered");
                        return None;
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
                            log("Library was delete while not all the remote jobs were done");
                            return;
                        }
                    };

                    script_ctx.compiled_library_api.run_on_background(Box::new(move||{
                        let script_ctx = match script_ctx_weak_ref.upgrade() {
                            Some(s) => s,
                            None => {
                                resolver.forget();
                                log("Library was delete while not all the remote jobs were done");
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
                                        errors.push(GearsApiError::Msg(format!("Failed deserializing remote function result '{}'", s)));
                                    }
                                    v8_obj.unwrap()
                                }
                            }
                        }).collect();
                        let errors: Vec<V8LocalValue> = errors.into_iter().map(|e| isolate_scope.new_string(e.get_msg()).to_value()).collect();
                        let results_array = isolate_scope.new_array(&results.iter().collect::<Vec<&V8LocalValue>>()).to_value();
                        let errors_array = isolate_scope.new_array(&errors.iter().collect::<Vec<&V8LocalValue>>()).to_value();

                        resolver.resolve(&ctx_scope, &isolate_scope.new_array(&[&results_array, &errors_array]).to_value());
                    }));
                }));
                Some(promise.to_value())
            })
            .to_value(),
    );

    bg_client
}

fn add_call_function(
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    redis_client: &Arc<RefCell<RedisClient>>,
    client: &V8LocalObject,
    function_name: &str,
    decode_response: bool,
) {
    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &isolate_scope.new_string(function_name).to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, ctx_scope| {
                if args.len() < 1 {
                    isolate_scope.raise_exception_str("Wrong number of arguments to 'call' function");
                    return None;
                }

                let is_already_blocked = ctx_scope.get_private_data::<bool>(0);
                if is_already_blocked.is_none() || !*is_already_blocked.unwrap() {
                    isolate_scope.raise_exception_str("Main thread is not locked");
                    return None;
                }

                let command = args.get(0);
                if !command.is_string() {
                    isolate_scope.raise_exception_str("First argument to 'command' must be a string");
                    return None;
                }

                let command_utf8 = command.to_utf8().unwrap();

                let mut commands_args = Vec::new();
                for i in 1..args.len() {
                    let arg = args.get(i);
                    let arg = if arg.is_string() {
                        arg.to_utf8().unwrap().as_str().as_bytes().iter().map(|v| *v).collect::<Vec<u8>>()
                    } else if arg.is_array_buffer(){
                        arg.as_array_buffer().data().clone().iter().map(|v| *v).collect::<Vec<u8>>()
                    } else {
                        isolate_scope.raise_exception_str("Bad argument was given to 'call', argument must be either String or ArrayBuffer");
                        return None;
                    };
                    commands_args.push(arg);
                }

                let res = match redis_client_ref.borrow().client.as_ref() {
                    Some(c) => c.call(command_utf8.as_str(), &commands_args.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>()),
                    None => {
                        isolate_scope.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                call_result_to_js_object(isolate_scope, ctx_scope, res, decode_response)
            })
            .to_value(),
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
        isolate_scope,
        ctx_scope,
        redis_client,
        &client,
        "call",
        true,
    );
    add_call_function(
        isolate_scope,
        ctx_scope,
        redis_client,
        &client,
        "call_raw",
        false,
    );

    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &isolate_scope.new_string("allow_block").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, _ctx_scope| {
                if args.len() != 0 {
                    isolate_scope.raise_exception_str(
                        "Wrong number of arguments to 'isolatellow_block' function",
                    );
                    return None;
                }

                let res = match redis_client_ref.borrow().allow_block.as_ref() {
                    Some(c) => *c,
                    None => {
                        isolate_scope.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                Some(isolate_scope.new_bool(res))
            })
            .to_value(),
    );

    let redisai_client = get_redisai_client(script_ctx, isolate_scope, ctx_scope, redis_client);
    client.set(ctx_scope, &isolate_scope.new_string("redisai").to_value(), &redisai_client);

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &isolate_scope.new_string("run_on_background").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str(
                        "Wrong number of arguments to 'run_on_background' function",
                    );
                    return None;
                }

                let bg_redis_client = match redis_client_ref.borrow().client.as_ref() {
                    Some(c) => c.get_background_redis_client(),
                    None => {
                        isolate.raise_exception_str("Called 'run_on_background' out of context");
                        return None;
                    }
                };

                let f = args.get(0);
                if !f.is_async_function() {
                    isolate.raise_exception_str(
                        "First argument to 'run_on_background' must be an async function",
                    );
                    return None;
                }

                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate.raise_exception_str("Use of invalid function context");
                        return None;
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
                        let res = f
                            .take_local(&isolate_scope)
                            .call(&ctx_scope, Some(&[&background_client.to_value()]));

                        let resolver = resolver.take_local(&isolate_scope).as_resolver();
                        match res {
                            Some(r) => resolver.resolve(&ctx_scope, &r),
                            None => {
                                let error_utf8 = get_exception_v8_value(
                                    &new_script_ctx_ref.isolate,
                                    &isolate_scope,
                                    trycatch,
                                );
                                resolver.reject(&ctx_scope, &error_utf8);
                            }
                        }
                    }));
                Some(promise.to_value())
            })
            .to_value(),
    );
    client
}

pub(crate) fn initialize_globals(
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    config: Option<&String>,
) -> Result<(), GearsApiError> {
    let redis = isolate_scope.new_object();

    match config {
        Some(c) => {
            let string = isolate_scope.new_string(c);
            let trycatch = isolate_scope.new_try_catch();
            let config_json = ctx_scope.new_object_from_json(&string);
            if config_json.is_none() {
                let error_msg = get_exception_msg(&script_ctx.isolate, trycatch);
                return Err(GearsApiError::Msg(format!(
                    "Failed parsing configuration as json, {}",
                    error_msg
                )));
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

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &isolate_scope.new_string("register_stream_consumer").to_value(), 
        &ctx_scope.new_native_function(move|args, isolate_scope, curr_ctx_scope| {
            if args.len() != 5 {
                isolate_scope.raise_exception_str("Wrong number of arguments to 'register_stream_consumer' function");
                return None;
            }

            let consumer_name = args.get(0);
            if !consumer_name.is_string() {
                isolate_scope.raise_exception_str("First argument to 'register_stream_consumer' must be a string representing the function name");
                return None;
            }
            let registration_name_utf8 = consumer_name.to_utf8().unwrap();

            let window = args.get(2);
            if !window.is_long() {
                isolate_scope.raise_exception_str("Third argument to 'register_stream_consumer' must be a long representing the window size");
                return None;
            }
            let window = window.get_long();

            let trim = args.get(3);
            if !trim.is_boolean() {
                isolate_scope.raise_exception_str("Dourth argument to 'register_stream_consumer' must be a boolean representing the trim option");
                return None;
            }
            let trim = trim.get_boolean();

            let function_callback = args.get(4);
            if !function_callback.is_function() {
                isolate_scope.raise_exception_str("Fith argument to 'register_stream_consumer' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist();

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate_scope.raise_exception_str("Called 'register_function' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate_scope.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };
            let v8_stream_ctx = V8StreamCtx::new(persisted_function, &script_ctx_ref, if function_callback.is_async_function() {true} else {false});
            let prefix = args.get(1);
            let res = if prefix.is_string() {
                let prefix = prefix.to_utf8().unwrap();
                load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix.as_str().as_bytes(), Box::new(v8_stream_ctx), window as usize, trim)
            } else if prefix.is_array_buffer() {
                let prefix = prefix.as_array_buffer();
                load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix.data(), Box::new(v8_stream_ctx), window as usize, trim)
            } else {
                isolate_scope.raise_exception_str("Second argument to 'register_stream_consumer' must be a String or ArrayBuffer representing the prefix");
                return None;
            };
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate_scope.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &isolate_scope.new_string("register_notifications_consumer").to_value(), 
        &ctx_scope.new_native_function(move|args, isolate_scope, curr_ctx_scope| {
            if args.len() != 3 {
                isolate_scope.raise_exception_str("Wrong number of arguments to 'register_notifications_consumer' function");
                return None;
            }

            let consumer_name = args.get(0);
            if !consumer_name.is_string() {
                isolate_scope.raise_exception_str("First argument to 'register_notifications_consumer' must be a string representing the consumer name");
                return None;
            }
            let registration_name_utf8 = consumer_name.to_utf8().unwrap();

            let function_callback = args.get(2);
            if !function_callback.is_function() {
                isolate_scope.raise_exception_str("Third argument to 'register_notifications_consumer' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist();

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate_scope.raise_exception_str("Called 'register_notifications_consumer' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate_scope.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };
            let v8_notification_ctx = V8NotificationsCtx::new(persisted_function, &script_ctx_ref, if function_callback.is_async_function() {true} else {false});

            let prefix = args.get(1);
            let res = if prefix.is_string() {
                let prefix = prefix.to_utf8().unwrap();
                load_ctx.register_key_space_notification_consumer(registration_name_utf8.as_str(), RegisteredKeys::Prefix(prefix.as_str().as_bytes()), Box::new(v8_notification_ctx))
            } else if prefix.is_array_buffer() {
                let prefix = prefix.as_array_buffer();
                load_ctx.register_key_space_notification_consumer(registration_name_utf8.as_str(), RegisteredKeys::Prefix(prefix.data()), Box::new(v8_notification_ctx))
            } else {
                isolate_scope.raise_exception_str("Second argument to 'register_notifications_consumer' must be a string or ArrayBuffer representing the prefix");
                return None;
            };
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate_scope.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &isolate_scope.new_string("register_function").to_value(),
        &ctx_scope.new_native_function(move|args, isolate_scope, curr_ctx_scope| {
            if args.len() < 2 {
                isolate_scope.raise_exception_str("Wrong number of arguments to 'register_function' function");
                return None;
            }

            let function_name = args.get(0);
            if !function_name.is_string() {
                isolate_scope.raise_exception_str("First argument to 'register_function' must be a string representing the function name");
                return None;
            }
            let function_name_utf8 = function_name.to_utf8().unwrap();

            let function_callback = args.get(1);
            if !function_callback.is_function() {
                isolate_scope.raise_exception_str("Second argument to 'register_function' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist();

            let function_flags = if args.len() == 3 {
                let function_flags = args.get(2);
                if !function_flags.is_array() {
                    isolate_scope.raise_exception_str("Second argument to 'register_function' must be an array of functions flags");
                    return None;
                }
                let function_flags = function_flags.as_array();
                match get_function_flags(curr_ctx_scope, &function_flags) {
                    Ok(flags) => flags,
                    Err(e) => {
                        isolate_scope.raise_exception_str(&format!("Failed parsing function flags, {}", e));
                        return None;
                    }
                }
            } else {
                0
            };

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate_scope.raise_exception_str("Called 'register_function' out of context");
                return None;
            }

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate_scope.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };

            let load_ctx = load_ctx.unwrap();
            let c = Arc::new(RefCell::new(RedisClient::new()));
            let redis_client = get_redis_client(&script_ctx_ref, isolate_scope, curr_ctx_scope, &c);

            let f = V8Function::new(
                &script_ctx_ref,
                persisted_function,
                redis_client.to_value().persist(),
                &c,
                function_callback.is_async_function(),
                function_flags & FUNCTION_FLAG_RAW_ARGUMENTS == 0,
            );

            let res = load_ctx.register_function(function_name_utf8.as_str(), Box::new(f), function_flags);
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate_scope.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &isolate_scope.new_string("register_remote_function").to_value(),
        &ctx_scope.new_native_function(move|args, isolate_scope, curr_ctx_scope| {
            if args.len() < 2 {
                isolate_scope.raise_exception_str("Wrong number of arguments to 'register_remote_function' function");
                return None;
            }

            let function_name = args.get(0);
            if !function_name.is_string() {
                isolate_scope.raise_exception_str("First argument to 'register_remote_function' must be a string representing the function name");
                return None;
            }
            let function_name_utf8 = function_name.to_utf8().unwrap();

            let function_callback = args.get(1);
            if !function_callback.is_function() {
                isolate_scope.raise_exception_str("Second argument to 'register_remote_function' must be a function");
                return None;
            }

            if !function_callback.is_async_function() {
                isolate_scope.raise_exception_str("Remote function must be async");
                return None;
            }

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate_scope.raise_exception_str("Called 'register_remote_function' out of context");
                return None;
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
                        on_done(Err(GearsApiError::Msg("Use of uninitialize script context".to_string())));
                        return;
                    }
                };

                let new_script_ctx_ref = Weak::clone(&new_script_ctx_ref);
                let weak_function = Arc::downgrade(&persisted_function);
                script_ctx.compiled_library_api.run_on_background(Box::new(move || {
                    let script_ctx = match new_script_ctx_ref.upgrade() {
                        Some(s) => s,
                        None => {
                            on_done(Err(GearsApiError::Msg("Use of uninitialize script context".to_string())));
                            return;
                        }
                    };
                    let persisted_function = match weak_function.upgrade() {
                        Some(s) => s,
                        None => {
                            on_done(Err(GearsApiError::Msg("Use of uninitialize function context".to_string())));
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
                                    on_done(Err(GearsApiError::Msg("Failed deserializing remote function argument".to_string())));
                                    return;
                                }
                                v8_obj.unwrap()
                            }
                        });
                    }
                    let args_refs = args.iter().collect::<Vec<&V8LocalValue>>();

                    script_ctx.before_run();
                    let res = persisted_function
                        .as_local(&isolate_scope)
                        .call(
                            &ctx_scope,
                            Some(&args_refs),
                        );
                    script_ctx.after_run();
                    match res {
                        Some(r) => {
                            if r.is_promise() {
                                let res = r.as_promise();
                                if res.state() == V8PromiseState::Fulfilled
                                    || res.state() == V8PromiseState::Rejected
                                {
                                    let r = res.get_result();
                                    if res.state() == V8PromiseState::Fulfilled {
                                        let r = js_value_to_remote_function_data(&ctx_scope, r);
                                        if r.is_none() {
                                            let error_utf8 = trycatch.get_exception().to_utf8().unwrap();
                                            on_done(Err(GearsApiError::Msg(format!("Failed serializing result, {}.", error_utf8.as_str()))));
                                        } else {
                                            on_done(Ok(r.unwrap()));
                                        }
                                    } else {
                                        let r = r.to_utf8().unwrap();
                                        on_done(Err(GearsApiError::Msg(r.as_str().to_string())));
                                    }
                                } else {
                                    // Notice, we are allowed to do this trick because we are protected by the isolate GIL
                                    let done_resolve = Arc::new(RefCellWrapper{ref_cell: RefCell::new(Some(on_done))});
                                    let done_reject = Arc::clone(&done_resolve);
                                    let resolve =
                                        ctx_scope.new_native_function(move |args, isolate_scope, ctx_scope| {
                                            {
                                                if done_resolve.ref_cell.borrow().is_none() {
                                                    return None;
                                                }
                                            }
                                            let on_done = done_resolve.ref_cell.borrow_mut().take().unwrap();
                                            let trycatch = isolate_scope.new_try_catch();
                                            let r = js_value_to_remote_function_data(ctx_scope, args.get(0));
                                            if r.is_none() {
                                                let error_utf8 = trycatch.get_exception().to_utf8().unwrap();
                                                on_done(Err(GearsApiError::Msg(format!("Failed serializing result, {}.", error_utf8.as_str()))));
                                            } else {
                                                on_done(Ok(r.unwrap()));
                                            }
                                            None
                                        });
                                    let reject =
                                        ctx_scope.new_native_function(move |args, _isolate_scope, _ctx_scope| {
                                            {
                                                if done_reject.ref_cell.borrow().is_none() {
                                                    return None;
                                                }
                                            }
                                            let on_done = done_reject.ref_cell.borrow_mut().take().unwrap();

                                            let r = args.get(0);
                                            let utf8_str = r.to_utf8().unwrap();
                                            on_done(Err(GearsApiError::Msg(utf8_str.as_str().to_string())));
                                            None
                                        });
                                    res.then(&ctx_scope, &resolve, &reject);
                                }
                            } else {
                                let r = js_value_to_remote_function_data(&ctx_scope, r);
                                if r.is_none() {
                                    on_done(Err(GearsApiError::Msg("Failed serializing result".to_string())));
                                } else {
                                    on_done(Ok(r.unwrap()));
                                }
                            }
                        }
                        None => {
                            let error_msg = get_exception_msg(&script_ctx.isolate, trycatch);
                            on_done(Err(GearsApiError::Msg(error_msg)));
                        }
                    };
                }));
            }));

            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate_scope.raise_exception_str(&s),
                }
            }
            None
    }).to_value());

    redis.set(
        ctx_scope,
        &isolate_scope.new_string("v8_version").to_value(),
        &ctx_scope
            .new_native_function(|_args, isolate, _curr_ctx_scope| {
                let v = v8_version();
                let v_v8_str = isolate.new_string(v);
                Some(v_v8_str.to_value())
            })
            .to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(
        ctx_scope,
        &isolate_scope.new_string("log").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, _curr_ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'log' function");
                    return None;
                }

                let msg = args.get(0);
                if !msg.is_string() && !msg.is_string_object() {
                    isolate.raise_exception_str("First argument to 'log' must be a string message");
                    return None;
                }

                let msg_utf8 = msg.to_utf8().unwrap();
                match script_ctx_ref.upgrade() {
                    Some(s) => s.compiled_library_api.log(msg_utf8.as_str()),
                    None => crate::v8_backend::log(msg_utf8.as_str()), /* do not abort logs */
                }
                None
            })
            .to_value(),
    );

    let redis_ai = get_redisai_api(script_ctx, isolate_scope, ctx_scope);
    redis.set(ctx_scope, &isolate_scope.new_string("redisai").to_value(), &redis_ai);

    globals.set(
        ctx_scope,
        &isolate_scope.new_string("redis").to_value(),
        &redis.to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    globals.set(
        ctx_scope,
        &isolate_scope.new_string("Promise").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate_scope, curr_ctx_scope| {
                if args.len() != 1 {
                    isolate_scope
                        .raise_exception_str("Wrong number of arguments to 'Promise' function");
                    return None;
                }

                let function = args.get(0);
                if !function.is_function() || function.is_async_function() {
                    isolate_scope.raise_exception_str("Bad argument to 'Promise' function");
                    return None;
                }

                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate_scope.raise_exception_str("Use of uninitialize script context");
                        return None;
                    }
                };

                let script_ctx_ref_resolve = Arc::downgrade(&script_ctx_ref);
                let script_ctx_ref_reject = Arc::downgrade(&script_ctx_ref);
                let resolver = curr_ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver_resolve = Arc::new(RefCellWrapper {
                    ref_cell: RefCell::new(resolver.to_value().persist()),
                });
                let resolver_reject = Arc::clone(&resolver_resolve);

                let resolve =
                    curr_ctx_scope.new_native_function(move |args, isolate, _curr_ctx_scope| {
                        if args.len() != 1 {
                            isolate.raise_exception_str(
                                "Wrong number of arguments to 'resolve' function",
                            );
                            return None;
                        }

                        let script_ctx_ref_resolve = match script_ctx_ref_resolve.upgrade() {
                            Some(s) => s,
                            None => {
                                resolver_resolve.ref_cell.borrow_mut().forget();
                                isolate.raise_exception_str("Library was deleted");
                                return None;
                            }
                        };

                        let mut res = args.get(0).persist();
                        let new_script_ctx_ref_resolve = Arc::downgrade(&script_ctx_ref_resolve);
                        let resolver_resolve = Arc::clone(&resolver_resolve);
                        script_ctx_ref_resolve
                            .compiled_library_api
                            .run_on_background(Box::new(move || {
                                let new_script_ctx_ref_resolve = match new_script_ctx_ref_resolve
                                    .upgrade()
                                {
                                    Some(s) => s,
                                    None => {
                                        resolver_resolve.ref_cell.borrow_mut().forget();
                                        res.forget();
                                        log("Library was delete while not all the jobs were done");
                                        return;
                                    }
                                };
                                let isolate_scope = new_script_ctx_ref_resolve.isolate.enter();
                                let ctx_scope =
                                    new_script_ctx_ref_resolve.ctx.enter(&isolate_scope);
                                let _trycatch = isolate_scope.new_try_catch();
                                let res = res.take_local(&isolate_scope);
                                let resolver = resolver_resolve
                                    .ref_cell
                                    .borrow_mut()
                                    .take_local(&isolate_scope)
                                    .as_resolver();
                                resolver.resolve(&ctx_scope, &res);
                            }));
                        None
                    });

                let reject = curr_ctx_scope.new_native_function(
                    move |args, isolate_scope, _curr_ctx_scope| {
                        if args.len() != 1 {
                            isolate_scope.raise_exception_str(
                                "Wrong number of arguments to 'resolve' function",
                            );
                            return None;
                        }

                        let script_ctx_ref_reject = match script_ctx_ref_reject.upgrade() {
                            Some(s) => s,
                            None => {
                                resolver_reject.ref_cell.borrow_mut().forget();
                                isolate_scope.raise_exception_str("Library was deleted");
                                return None;
                            }
                        };

                        let mut res = args.get(0).persist();
                        let new_script_ctx_ref_reject = Arc::downgrade(&script_ctx_ref_reject);
                        let resolver_reject = Arc::clone(&resolver_reject);
                        script_ctx_ref_reject
                            .compiled_library_api
                            .run_on_background(Box::new(move || {
                                let new_script_ctx_ref_reject = match new_script_ctx_ref_reject
                                    .upgrade()
                                {
                                    Some(s) => s,
                                    None => {
                                        res.forget();
                                        resolver_reject.ref_cell.borrow_mut().forget();
                                        log("Library was delete while not all the jobs were done");
                                        return;
                                    }
                                };
                                let isolate_scope = new_script_ctx_ref_reject.isolate.enter();
                                let ctx_scope = new_script_ctx_ref_reject.ctx.enter(&isolate_scope);
                                let _trycatch = isolate_scope.new_try_catch();
                                let res = res.take_local(&isolate_scope);
                                let resolver = resolver_reject
                                    .ref_cell
                                    .borrow_mut()
                                    .take_local(&isolate_scope)
                                    .as_resolver();
                                resolver.reject(&ctx_scope, &res);
                            }));
                        None
                    },
                );

                let _ = function.call(
                    curr_ctx_scope,
                    Some(&[&resolve.to_value(), &reject.to_value()]),
                );
                Some(promise.to_value())
            })
            .to_value(),
    );

    Ok(())
}
