/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::redisvalue::RedisValueKey;
use redis_module::{RedisError, RedisResult, RedisValue};
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;
use redisgears_plugin_api::redisgears_plugin_api::{
    function_ctx::FunctionCtxInterface, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::ReplyCtxInterface, run_function_ctx::RunFunctionCtxInterface,
    FunctionCallResult,
};

use v8_rs::v8::{
    isolate_scope::V8IsolateScope, v8_context_scope::V8ContextScope, v8_promise::V8PromiseState,
    v8_value::V8LocalValue, v8_value::V8PersistValue,
};

use crate::v8_native_functions::{get_backgrounnd_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;
use crate::{get_error_from_object, get_exception_msg};

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use std::str;

use v8_derive::new_native_function;

struct BackgroundClientHolder {
    c: Option<Box<dyn ReplyCtxInterface>>,
}

impl BackgroundClientHolder {
    fn unblock(&mut self) {
        self.c = None;
    }
}

pub struct V8InternalFunction {
    persisted_client: V8PersistValue,
    persisted_function: V8PersistValue,
    script_ctx: Arc<V8ScriptCtx>,
}

fn v8_value_to_redis_value_key(val: V8LocalValue) -> Result<RedisValueKey, RedisError> {
    Ok(if val.is_long() {
        RedisValueKey::Integer(val.get_long())
    } else if val.is_string() || val.is_string_object() {
        RedisValueKey::String(
            val.to_utf8()
                .ok_or(RedisError::Str("Failed converting value into String"))?
                .as_str()
                .to_string(),
        )
    } else if val.is_array_buffer() {
        let val = val.as_array_buffer();
        RedisValueKey::BulkString(val.data().to_vec())
    } else if val.is_boolean() {
        RedisValueKey::Bool(val.get_boolean())
    } else {
        return Err(RedisError::Str("Give value is not a key"));
    })
}

fn v8_value_to_call_result(
    nesting_level: usize,
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    val: V8LocalValue,
) -> RedisResult {
    if nesting_level > 100 {
        return Err(RedisError::Str("nesting level reached"));
    }
    Ok(if val.is_long() {
        RedisValue::Integer(val.get_long())
    } else if val.is_number() {
        RedisValue::Float(val.get_number())
    } else if val.is_string() {
        RedisValue::BulkString(val.to_utf8().unwrap().as_str().to_string())
    } else if val.is_string_object() {
        // check the type of the reply
        let obj_reply = val.as_object();
        let reply_type = obj_reply.get(
            ctx_scope,
            &isolate_scope.new_string("__reply_type").to_value(),
        );
        if let Some(t) = reply_type {
            if let Some(reply_type_v8_str) = t.to_utf8() {
                if reply_type_v8_str.as_str() == "status" {
                    return Ok(RedisValue::SimpleString(
                        val.to_utf8().unwrap().as_str().to_string(),
                    ));
                }
            }
        }
        RedisValue::BulkString(val.to_utf8().unwrap().as_str().to_string())
    } else if val.is_array_buffer() {
        let val = val.as_array_buffer();
        RedisValue::StringBuffer(val.data().to_vec())
    } else if val.is_null() {
        RedisValue::Null
    } else if val.is_array() {
        let arr = val.as_array();
        let mut res = Vec::new();
        for i in 0..arr.len() {
            let val = arr.get(ctx_scope, i);
            res.push(v8_value_to_call_result(
                nesting_level + 1,
                isolate_scope,
                ctx_scope,
                val,
            )?);
        }
        RedisValue::Array(res)
    } else if val.is_object() {
        let res = val.as_object();
        let keys = res.get_property_names(ctx_scope);
        let mut result = HashMap::new();
        for i in 0..keys.len() {
            let key = keys.get(ctx_scope, i);
            let obj = res.get(ctx_scope, &key).unwrap();
            result.insert(
                v8_value_to_redis_value_key(key)?,
                v8_value_to_call_result(nesting_level + 1, isolate_scope, ctx_scope, obj)?,
            );
        }
        RedisValue::Map(result)
    } else {
        RedisValue::BulkString(val.to_utf8().unwrap().as_str().to_string())
    })
}

fn send_reply(
    isolate_scope: &V8IsolateScope,
    ctx_scope: &V8ContextScope,
    client: &dyn ReplyCtxInterface,
    val: V8LocalValue,
) {
    let reply = v8_value_to_call_result(0, isolate_scope, ctx_scope, val);
    client.send_reply(reply);
}

impl V8InternalFunction {
    fn call_async(
        &self,
        command_args: Vec<Vec<u8>>,
        bg_client: Box<dyn ReplyCtxInterface>,
        redis_background_client: Box<dyn BackgroundRunFunctionCtxInterface>,
        decode_args: bool,
    ) -> FunctionCallResult {
        let isolate_scope = self.script_ctx.isolate.enter();
        let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
        let trycatch = isolate_scope.new_try_catch();

        let res = {
            let r_client = get_backgrounnd_client(
                &self.script_ctx,
                &isolate_scope,
                &ctx_scope,
                Arc::new(redis_background_client),
            );
            let args = {
                let mut args = Vec::new();
                args.push(r_client.to_value());
                for arg in command_args.iter() {
                    let arg = if decode_args {
                        let arg = match str::from_utf8(arg) {
                            Ok(s) => s,
                            Err(_) => {
                                bg_client.reply_with_error(GearsApiError::new(
                                    "Can not convert argument to string",
                                ));
                                return FunctionCallResult::Done;
                            }
                        };
                        isolate_scope.new_string(arg).to_value()
                    } else {
                        isolate_scope.new_array_buffer(arg).to_value()
                    };
                    args.push(arg);
                }
                Some(args)
            };

            let args_ref = args
                .as_ref()
                .map(|v| v.iter().collect::<Vec<&V8LocalValue>>());

            self.script_ctx.before_run();
            let res = self
                .persisted_function
                .as_local(&isolate_scope)
                .call(&ctx_scope, args_ref.as_deref());
            self.script_ctx.after_run();
            res
        };

        match res {
            Some(r) => {
                if r.is_promise() {
                    let res = r.as_promise();
                    if res.state() == V8PromiseState::Fulfilled
                        || res.state() == V8PromiseState::Rejected
                    {
                        let r = res.get_result();
                        if res.state() == V8PromiseState::Fulfilled {
                            send_reply(&isolate_scope, &ctx_scope, bg_client.as_ref(), r);
                        } else {
                            bg_client.reply_with_error(get_error_from_object(&r, &ctx_scope));
                        }
                    } else {
                        let bg_execution_ctx = BackgroundClientHolder { c: Some(bg_client) };
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve = ctx_scope.new_native_function(new_native_function!(
                            move |isolate, context, rep: V8LocalValue| {
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                send_reply(
                                    isolate,
                                    context,
                                    execution_ctx.c.as_ref().unwrap().as_ref(),
                                    rep,
                                );
                                execution_ctx.unblock();
                                Ok::<_, String>(None)
                            }
                        ));
                        let reject = ctx_scope.new_native_function(new_native_function!(
                            move |_isolate_scope, ctx_scope, reply: V8LocalValue| {
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                // see if we can extract trace
                                execution_ctx
                                    .c
                                    .as_ref()
                                    .unwrap()
                                    .reply_with_error(get_error_from_object(&reply, ctx_scope));
                                execution_ctx.unblock();
                                Ok::<_, String>(None)
                            }
                        ));
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(&isolate_scope, &ctx_scope, bg_client.as_ref(), r);
                }
            }
            None => {
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope);
                bg_client.reply_with_error(error_msg);
            }
        }
        FunctionCallResult::Done
    }

    fn call_sync(
        &self,
        run_ctx: &dyn RunFunctionCtxInterface,
        decode_arguments: bool,
    ) -> FunctionCallResult {
        let isolate_scope = self.script_ctx.isolate.enter();
        let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
        let trycatch = isolate_scope.new_try_catch();

        let res = {
            let args = {
                let mut args = Vec::new();
                args.push(self.persisted_client.as_local(&isolate_scope));
                for arg in run_ctx.get_args_iter() {
                    let arg = if decode_arguments {
                        let arg = match str::from_utf8(arg) {
                            Ok(s) => s,
                            Err(_) => {
                                run_ctx.reply_with_error(GearsApiError::new(
                                    "Can not convert argument to string",
                                ));
                                return FunctionCallResult::Done;
                            }
                        };
                        isolate_scope.new_string(arg).to_value()
                    } else {
                        isolate_scope.new_array_buffer(arg).to_value()
                    };
                    args.push(arg);
                }
                Some(args)
            };

            let args_ref = args
                .as_ref()
                .map(|v| v.iter().collect::<Vec<&V8LocalValue>>());

            let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

            self.script_ctx.before_run();
            self.script_ctx.after_lock_gil();
            let res = self
                .persisted_function
                .as_local(&isolate_scope)
                .call(&ctx_scope, args_ref.as_deref());
            self.script_ctx.before_release_gil();
            self.script_ctx.after_run();

            res
        };

        match res {
            Some(r) => {
                if r.is_promise() {
                    let res = r.as_promise();
                    if res.state() == V8PromiseState::Fulfilled
                        || res.state() == V8PromiseState::Rejected
                    {
                        let r = res.get_result();
                        if res.state() == V8PromiseState::Fulfilled {
                            send_reply(&isolate_scope, &ctx_scope, run_ctx.as_client(), r);
                        } else {
                            run_ctx.reply_with_error(get_error_from_object(&r, &ctx_scope));
                        }
                    } else {
                        let bc = match run_ctx.get_background_client() {
                            Ok(bc) => bc,
                            Err(e) => {
                                run_ctx.reply_with_error(GearsApiError::new(format!(
                                    "Can not block client for background execution, {}.",
                                    e.get_msg()
                                )));
                                return FunctionCallResult::Done;
                            }
                        };
                        let bg_execution_ctx = BackgroundClientHolder { c: Some(bc) };
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve = ctx_scope.new_native_function(new_native_function!(
                            move |isolate_scope, ctx_scope, reply: V8LocalValue| {
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                let client = execution_ctx.c.as_ref().unwrap();
                                send_reply(isolate_scope, ctx_scope, client.as_ref(), reply);
                                execution_ctx.unblock();
                                Ok::<_, String>(None)
                            }
                        ));
                        let reject = ctx_scope.new_native_function(new_native_function!(
                            move |_isolate_scope, ctx_scope, reply: V8LocalValue| {
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                execution_ctx
                                    .c
                                    .as_ref()
                                    .unwrap()
                                    .reply_with_error(get_error_from_object(&reply, ctx_scope));
                                execution_ctx.unblock();
                                Ok::<_, String>(None)
                            }
                        ));
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(&isolate_scope, &ctx_scope, run_ctx.as_client(), r);
                }
            }
            None => {
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope);
                run_ctx.reply_with_error(error_msg);
            }
        }
        FunctionCallResult::Done
    }
}

pub struct V8Function {
    inner_function: Arc<V8InternalFunction>,
    client: Arc<RefCell<RedisClient>>,
    is_async: bool,
    decode_arguments: bool,
}

impl V8Function {
    pub(crate) fn new(
        script_ctx: &Arc<V8ScriptCtx>,
        mut persisted_function: V8PersistValue,
        mut persisted_client: V8PersistValue,
        client: &Arc<RefCell<RedisClient>>,
        is_async: bool,
        decode_arguments: bool,
    ) -> Self {
        persisted_function.forget();
        persisted_client.forget();
        Self {
            inner_function: Arc::new(V8InternalFunction {
                script_ctx: Arc::clone(script_ctx),
                persisted_function,
                persisted_client,
            }),
            client: Arc::clone(client),
            is_async,
            decode_arguments,
        }
    }
}

impl FunctionCtxInterface for V8Function {
    fn call(&self, run_ctx: &dyn RunFunctionCtxInterface) -> FunctionCallResult {
        if self.is_async {
            let bg_client = match run_ctx.get_background_client() {
                Ok(bc) => bc,
                Err(e) => {
                    run_ctx.send_reply(Err(RedisError::String(format!(
                        "Can not block client for background execution, {}.",
                        e.get_msg()
                    ))));
                    return FunctionCallResult::Done;
                }
            };
            let inner_function = Arc::clone(&self.inner_function);
            // if we are going to the background we must consume all the arguments
            let args = run_ctx
                .get_args_iter()
                .map(|v| v.to_vec())
                .collect::<Vec<_>>();
            let bg_redis_client = run_ctx.get_redis_client().get_background_redis_client();
            let decode_arguments = self.decode_arguments;
            self.inner_function
                .script_ctx
                .compiled_library_api
                .run_on_background(Box::new(move || {
                    inner_function.call_async(args, bg_client, bg_redis_client, decode_arguments);
                }));
            FunctionCallResult::Done
        } else {
            let redis_client = run_ctx.get_redis_client();
            {
                let mut c = self.client.borrow_mut();
                c.set_allow_block(run_ctx.allow_block());
                c.set_client(redis_client.as_ref());
            }
            self.inner_function
                .call_sync(run_ctx, self.decode_arguments);
            self.client.borrow_mut().make_invalid();
            FunctionCallResult::Done
        }
    }
}
