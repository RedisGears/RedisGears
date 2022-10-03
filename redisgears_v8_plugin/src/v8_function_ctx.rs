use redisgears_plugin_api::redisgears_plugin_api::{
    function_ctx::FunctionCtxInterface, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::ReplyCtxInterface, run_function_ctx::RunFunctionCtxInterface,
    FunctionCallResult,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context_scope::V8ContextScope, v8_promise::V8PromiseState,
    v8_value::V8LocalValue, v8_value::V8PersistValue,
};

use crate::get_exception_msg;
use crate::v8_native_functions::{get_backgrounnd_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;

use std::cell::RefCell;
use std::sync::Arc;

use std::str;

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

fn send_reply(
    nesting_level: usize,
    isolate: &V8Isolate,
    ctx_scope: &V8ContextScope,
    client: &dyn ReplyCtxInterface,
    val: V8LocalValue,
) {
    if nesting_level > 100 {
        client.reply_with_simple_string("nesting level reached");
        return;
    }
    if val.is_long() {
        client.reply_with_long(val.get_long());
    } else if val.is_number() {
        client.reply_with_double(val.get_number());
    } else if val.is_string() {
        client.reply_with_bulk_string(val.to_utf8(isolate).unwrap().as_str());
    } else if val.is_string_object() {
        // check the type of the reply
        let obj_reply = val.as_object();
        let reply_type = obj_reply.get(ctx_scope, &isolate.new_string("__reply_type").to_value());
        if reply_type.is_some() {
            let reply_type = reply_type.unwrap();
            if reply_type.is_string() {
                let reply_type_v8_str = reply_type.to_utf8(isolate).unwrap();
                if reply_type_v8_str.as_str() == "status" {
                    client.reply_with_simple_string(val.to_utf8(isolate).unwrap().as_str());
                    return;
                }
            }
        }
        client.reply_with_bulk_string(val.to_utf8(isolate).unwrap().as_str());
    } else if val.is_array_buffer() {
        let val = val.as_array_buffer();
        client.reply_with_slice(val.data());
    } else if val.is_null() {
        client.reply_with_null();
    } else if val.is_array() {
        let arr = val.as_array();
        client.reply_with_array(arr.len());
        for i in 0..arr.len() {
            let val = arr.get(ctx_scope, i);
            send_reply(nesting_level + 1, isolate, ctx_scope, client, val);
        }
    } else if val.is_object() {
        let res = val.as_object();
        let keys = res.get_property_names(ctx_scope);
        client.reply_with_array(keys.len() * 2);
        for i in 0..keys.len() {
            let key = keys.get(ctx_scope, i);
            let obj = res.get(ctx_scope, &key).unwrap();
            send_reply(nesting_level + 1, isolate, ctx_scope, client, key);
            send_reply(nesting_level + 1, isolate, ctx_scope, client, obj);
        }
    } else {
        client.reply_with_bulk_string(val.to_utf8(isolate).unwrap().as_str());
    }
}

impl V8InternalFunction {
    fn call_async(
        &self,
        command_args: Vec<Vec<u8>>,
        bg_client: Box<dyn ReplyCtxInterface>,
        redis_background_client: Box<dyn BackgroundRunFunctionCtxInterface>,
        decode_args: bool,
    ) -> FunctionCallResult {
        let _isolate_scope = self.script_ctx.isolate.enter();
        let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
        let ctx_scope = self.script_ctx.ctx.enter();
        let trycatch = self.script_ctx.isolate.new_try_catch();

        let res = {
            let r_client = get_backgrounnd_client(
                &self.script_ctx,
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
                                bg_client.reply_with_error("Can not convert argument to string");
                                return FunctionCallResult::Done;
                            }
                        };
                        self.script_ctx.isolate.new_string(arg).to_value()
                    } else {
                        self.script_ctx.isolate.new_array_buffer(arg).to_value()
                    };
                    args.push(arg);
                }
                Some(args)
            };

            let args_ref = args.as_ref().map_or(None, |v| {
                let s = v.iter().map(|v| v).collect::<Vec<&V8LocalValue>>();
                Some(s)
            });

            self.script_ctx.before_run();
            let res = self
                .persisted_function
                .as_local(&self.script_ctx.isolate)
                .call(
                    &ctx_scope,
                    args_ref.as_ref().map_or(None, |v| Some(v.as_slice())),
                );
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
                            send_reply(
                                0,
                                &self.script_ctx.isolate,
                                &ctx_scope,
                                bg_client.as_ref(),
                                r,
                            );
                        } else {
                            let r = r.to_utf8(&self.script_ctx.isolate).unwrap();
                            bg_client.reply_with_error(r.as_str());
                        }
                    } else {
                        let bg_execution_ctx = BackgroundClientHolder { c: Some(bg_client) };
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve =
                            ctx_scope.new_native_function(move |args, isolate, context| {
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                send_reply(
                                    0,
                                    isolate,
                                    context,
                                    execution_ctx.c.as_ref().unwrap().as_ref(),
                                    args.get(0),
                                );
                                execution_ctx.unblock();
                                None
                            });
                        let reject =
                            ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                execution_ctx
                                    .c
                                    .as_ref()
                                    .unwrap()
                                    .reply_with_error(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(
                        0,
                        &self.script_ctx.isolate,
                        &ctx_scope,
                        bg_client.as_ref(),
                        r,
                    );
                }
            }
            None => {
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
                bg_client.reply_with_error(&error_msg);
            }
        }
        FunctionCallResult::Done
    }

    fn call_sync(
        &self,
        run_ctx: &mut dyn RunFunctionCtxInterface,
        decode_arguments: bool,
    ) -> FunctionCallResult {
        let _isolate_scope = self.script_ctx.isolate.enter();
        let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
        let ctx_scope = self.script_ctx.ctx.enter();
        let trycatch = self.script_ctx.isolate.new_try_catch();

        let res = {
            let args = {
                let mut args = Vec::new();
                args.push(self.persisted_client.as_local(&self.script_ctx.isolate));
                while let Some(a) = run_ctx.next_arg() {
                    let arg = if decode_arguments {
                        let arg = match str::from_utf8(a) {
                            Ok(s) => s,
                            Err(_) => {
                                run_ctx.reply_with_error("Can not convert argument to string");
                                return FunctionCallResult::Done;
                            }
                        };
                        self.script_ctx.isolate.new_string(arg).to_value()
                    } else {
                        self.script_ctx.isolate.new_array_buffer(a).to_value()
                    };
                    args.push(arg);
                }
                Some(args)
            };

            let args_ref = args.as_ref().map_or(None, |v| {
                let s = v.iter().map(|v| v).collect::<Vec<&V8LocalValue>>();
                Some(s)
            });

            ctx_scope.set_private_data(0, Some(&true)); // indicate we are blocked

            self.script_ctx.before_run();
            self.script_ctx.after_lock_gil();
            let res = self
                .persisted_function
                .as_local(&self.script_ctx.isolate)
                .call(
                    &ctx_scope,
                    args_ref.as_ref().map_or(None, |v| Some(v.as_slice())),
                );
            self.script_ctx.before_release_gil();
            self.script_ctx.after_run();

            ctx_scope.set_private_data::<bool>(0, None); // indicate we are not blocked
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
                            send_reply(
                                0,
                                &self.script_ctx.isolate,
                                &ctx_scope,
                                run_ctx.as_client(),
                                r,
                            );
                        } else {
                            let r = r.to_utf8(&self.script_ctx.isolate).unwrap();
                            run_ctx.reply_with_error(r.as_str());
                        }
                    } else {
                        let bc = match run_ctx.get_background_client() {
                            Ok(bc) => bc,
                            Err(e) => {
                                run_ctx.reply_with_error(&format!(
                                    "Can not block client for background execution, {}.",
                                    e.get_msg()
                                ));
                                return FunctionCallResult::Done;
                            }
                        };
                        let bg_execution_ctx = BackgroundClientHolder { c: Some(bc) };
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve =
                            ctx_scope.new_native_function(move |args, isolate, _context| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                execution_ctx
                                    .c
                                    .as_ref()
                                    .unwrap()
                                    .reply_with_bulk_string(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        let reject =
                            ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                execution_ctx
                                    .c
                                    .as_ref()
                                    .unwrap()
                                    .reply_with_error(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(
                        0,
                        &self.script_ctx.isolate,
                        &ctx_scope,
                        run_ctx.as_client(),
                        r,
                    );
                }
            }
            None => {
                let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
                run_ctx.reply_with_error(&error_msg);
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
    ) -> V8Function {
        persisted_function.forget();
        persisted_client.forget();
        V8Function {
            inner_function: Arc::new(V8InternalFunction {
                script_ctx: Arc::clone(script_ctx),
                persisted_function: persisted_function,
                persisted_client: persisted_client,
            }),
            client: Arc::clone(client),
            is_async: is_async,
            decode_arguments: decode_arguments,
        }
    }
}

impl FunctionCtxInterface for V8Function {
    fn call(&self, run_ctx: &mut dyn RunFunctionCtxInterface) -> FunctionCallResult {
        if self.is_async {
            let bg_client = match run_ctx.get_background_client() {
                Ok(bc) => bc,
                Err(e) => {
                    run_ctx.reply_with_error(&format!(
                        "Can not block client for background execution, {}.",
                        e.get_msg()
                    ));
                    return FunctionCallResult::Done;
                }
            };
            let inner_function = Arc::clone(&self.inner_function);
            // if we are going to the background we must consume all the arguments
            let mut args = Vec::new();
            while let Some(a) = run_ctx.next_arg() {
                args.push(a.into_iter().map(|v| *v).collect::<Vec<u8>>());
            }
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
            self.client.borrow_mut().set_client(redis_client);
            self.client
                .borrow_mut()
                .set_allow_block(run_ctx.allow_block());
            self.inner_function
                .call_sync(run_ctx, self.decode_arguments);
            self.client.borrow_mut().make_invalid();
            FunctionCallResult::Done
        }
    }
}
