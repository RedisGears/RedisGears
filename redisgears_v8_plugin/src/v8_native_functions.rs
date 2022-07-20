use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LoadLibraryCtxInterface, load_library_ctx::RegisteredKeys,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    CallResult, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context_scope::V8ContextScope, v8_object::V8LocalObject,
    v8_value::V8LocalValue, v8_version,
};

use crate::get_function_flags;
use crate::v8_backend::log;
use crate::v8_function_ctx::V8Function;
use crate::v8_notifications_ctx::V8NotificationsCtx;
use crate::v8_script_ctx::V8ScriptCtx;
use crate::v8_stream_ctx::V8StreamCtx;

use std::cell::RefCell;
use std::str;
use std::sync::Arc;

pub(crate) fn call_result_to_js_object(
    isolate: &V8Isolate,
    ctx_scope: &V8ContextScope,
    res: CallResult,
) -> Option<V8LocalValue> {
    match res {
        CallResult::SimpleStr(s) => Some(isolate.new_string(&s).to_value()),
        CallResult::BulkStr(s) => Some(isolate.new_string(&s).to_value()),
        CallResult::Error(e) => {
            isolate.raise_exception_str(&e);
            None
        }
        CallResult::Long(l) => Some(isolate.new_long(l)),
        CallResult::Double(d) => Some(isolate.new_double(d)),
        CallResult::Array(a) => {
            let mut has_error = false;
            let vals = a
                .into_iter()
                .map(|v| {
                    let res = call_result_to_js_object(isolate, ctx_scope, v);
                    if res.is_none() {
                        has_error = true;
                    }
                    res
                })
                .collect::<Vec<Option<V8LocalValue>>>();
            if has_error {
                return None;
            }

            let array = isolate.new_array(
                &vals
                    .iter()
                    .map(|v| v.as_ref().unwrap())
                    .collect::<Vec<&V8LocalValue>>(),
            );
            Some(array.to_value())
        }
        CallResult::Null => None,
    }
}

pub(crate) struct RedisClient {
    client: Option<Box<dyn RedisClientCtxInterface>>,
}

impl RedisClient {
    pub(crate) fn new() -> RedisClient {
        RedisClient { client: None }
    }

    pub(crate) fn make_invalid(&mut self) {
        self.client = None;
    }

    pub(crate) fn set_client(&mut self, c: Box<dyn RedisClientCtxInterface>) {
        self.client = Some(c);
    }
}

pub(crate) fn get_backgrounnd_client(
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
    redis_background_client: Box<dyn BackgroundRunFunctionCtxInterface>,
) -> V8LocalObject {
    let bg_client = script_ctx.isolate.new_object();
    let redis_background_client = Arc::new(redis_background_client);
    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_ref = Arc::downgrade(script_ctx);
    bg_client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("block").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() < 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'block' function");
                    return None;
                }
                let f = args.get(0);
                if !f.is_function() {
                    isolate.raise_exception_str("Argument to 'block' must be a function");
                    return None;
                }

                let is_already_blocked = ctx_scope.get_private_data::<bool>(0);
                if !is_already_blocked.is_none() && *is_already_blocked.unwrap() {
                    isolate.raise_exception_str("Main thread is already blocked");
                    return None;
                }

                let redis_client = {
                    let _unlocker = isolate.new_unlocker();
                    match redis_background_client_ref.lock() {
                        Ok(l) => l,
                        Err(err) => {
                            isolate.raise_exception_str(&format!(
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
                        isolate.raise_exception_str("Function were unregistered");
                        return None;
                    }
                };

                let r_client = Arc::new(RefCell::new(RedisClient::new()));
                r_client.borrow_mut().set_client(redis_client);
                let c = get_redis_client(&script_ctx_ref, ctx_scope, &r_client);

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
    bg_client
}

pub(crate) fn get_redis_client(
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
    redis_client: &Arc<RefCell<RedisClient>>,
) -> V8LocalObject {
    let client = script_ctx.isolate.new_object();

    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("call").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() < 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'call' function");
                    return None;
                }

                let is_already_blocked = ctx_scope.get_private_data::<bool>(0);
                if is_already_blocked.is_none() || !*is_already_blocked.unwrap() {
                    isolate.raise_exception_str("Main thread is not locked");
                    return None;
                }

                let command = args.get(0);
                if !command.is_string() {
                    isolate.raise_exception_str("First argument to 'command' must be a string");
                    return None;
                }

                let command_utf8 = command.to_utf8(isolate).unwrap();

                let mut commands_args_str = Vec::new();
                for i in 1..args.len() {
                    commands_args_str.push(args.get(i).to_utf8(isolate).unwrap());
                }

                let command_args_rust_str = commands_args_str
                    .iter()
                    .map(|v| v.as_str())
                    .collect::<Vec<&str>>();

                let res = match redis_client_ref.borrow().client.as_ref() {
                    Some(c) => c.call(command_utf8.as_str(), &command_args_rust_str),
                    None => {
                        isolate.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                call_result_to_js_object(isolate, ctx_scope, res)
            })
            .to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &script_ctx
            .isolate
            .new_string("run_on_background")
            .to_value(),
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
                let f = f.persist(isolate);

                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate.raise_exception_str("Use of invalid function context");
                        return None;
                    }
                };
                let new_script_ctx_ref = Arc::clone(&script_ctx_ref);
                let resolver = ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver = resolver.to_value().persist(isolate);
                script_ctx_ref
                    .compiled_library_api
                    .run_on_background(Box::new(move || {
                        let _isolate_scope = new_script_ctx_ref.isolate.enter();
                        let _handlers_scope = new_script_ctx_ref.isolate.new_handlers_scope();
                        let ctx_scope = new_script_ctx_ref.ctx.enter();
                        let trycatch = new_script_ctx_ref.isolate.new_try_catch();

                        let background_client = get_backgrounnd_client(
                            &new_script_ctx_ref,
                            &ctx_scope,
                            bg_redis_client,
                        );
                        let res = f
                            .as_local(&new_script_ctx_ref.isolate)
                            .call(&ctx_scope, Some(&[&background_client.to_value()]));

                        let resolver = resolver.as_local(&new_script_ctx_ref.isolate).as_resolver();
                        match res {
                            Some(r) => resolver.resolve(&ctx_scope, &r),
                            None => {
                                let error_utf8 = trycatch.get_exception();
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
    ctx_scope: &V8ContextScope,
) {
    let redis = script_ctx.isolate.new_object();

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &script_ctx.isolate.new_string("register_stream_consumer").to_value(), 
        &ctx_scope.new_native_function(move|args, isolate, curr_ctx_scope| {
            if args.len() != 5 {
                isolate.raise_exception_str("Wrong number of arguments to 'register_stream_consumer' function");
                return None;
            }

            let consumer_name = args.get(0);
            if !consumer_name.is_string() {
                isolate.raise_exception_str("First argument to 'register_stream_consumer' must be a string representing the function name");
                return None;
            }
            let registration_name_utf8 = consumer_name.to_utf8(isolate).unwrap();

            let prefix = args.get(1);
            if !prefix.is_string() {
                isolate.raise_exception_str("Second argument to 'register_stream_consumer' must be a string representing the prefix");
                return None;
            }
            let prefix_utf8 = prefix.to_utf8(isolate).unwrap();

            let window = args.get(2);
            if !window.is_long() {
                isolate.raise_exception_str("Third argument to 'register_stream_consumer' must be a long representing the window size");
                return None;
            }
            let window = window.get_long();

            let trim = args.get(3);
            if !trim.is_boolean() {
                isolate.raise_exception_str("Dourth argument to 'register_stream_consumer' must be a boolean representing the trim option");
                return None;
            }
            let trim = trim.get_boolean();

            let function_callback = args.get(4);
            if !function_callback.is_function() {
                isolate.raise_exception_str("Fith argument to 'register_stream_consumer' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist(isolate);

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate.raise_exception_str("Called 'register_function' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };
            let v8_stream_ctx = V8StreamCtx::new(persisted_function, &script_ctx_ref, if function_callback.is_async_function() {true} else {false});
            let res = load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix_utf8.as_str(), Box::new(v8_stream_ctx), window as usize, trim);
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &script_ctx.isolate.new_string("register_notifications_consumer").to_value(), 
        &ctx_scope.new_native_function(move|args, isolate, curr_ctx_scope| {
            if args.len() != 3 {
                isolate.raise_exception_str("Wrong number of arguments to 'register_notifications_consumer' function");
                return None;
            }

            let consumer_name = args.get(0);
            if !consumer_name.is_string() {
                isolate.raise_exception_str("First argument to 'register_notifications_consumer' must be a string representing the consumer name");
                return None;
            }
            let registration_name_utf8 = consumer_name.to_utf8(isolate).unwrap();

            let prefix = args.get(1);
            if !prefix.is_string() {
                isolate.raise_exception_str("Second argument to 'register_notifications_consumer' must be a string representing the prefix");
                return None;
            }
            let prefix_utf8 = prefix.to_utf8(isolate).unwrap();

            let function_callback = args.get(2);
            if !function_callback.is_function() {
                isolate.raise_exception_str("Third argument to 'register_notifications_consumer' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist(isolate);

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate.raise_exception_str("Called 'register_notifications_consumer' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };
            let v8_notification_ctx = V8NotificationsCtx::new(persisted_function, &script_ctx_ref, if function_callback.is_async_function() {true} else {false});
            let res = load_ctx.register_key_space_notification_consumer(registration_name_utf8.as_str(), RegisteredKeys::Prefix(prefix_utf8.as_str()), Box::new(v8_notification_ctx));
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::downgrade(script_ctx);
    redis.set(ctx_scope,
        &script_ctx.isolate.new_string("register_function").to_value(),
        &ctx_scope.new_native_function(move|args, isolate, curr_ctx_scope| {
            if args.len() < 2 {
                isolate.raise_exception_str("Wrong number of arguments to 'register_function' function");
                return None;
            }

            let function_name = args.get(0);
            if !function_name.is_string() {
                isolate.raise_exception_str("First argument to 'register_function' must be a string representing the function name");
                return None;
            }
            let function_name_utf8 = function_name.to_utf8(isolate).unwrap();

            let function_callback = args.get(1);
            if !function_callback.is_function() {
                isolate.raise_exception_str("Second argument to 'register_function' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist(isolate);

            let function_flags = if args.len() == 3 {
                let function_flags = args.get(2);
                if !function_flags.is_array() {
                    isolate.raise_exception_str("Second argument to 'register_function' must be an array of functions flags");
                    return None;
                }
                let function_flags = function_flags.as_array();
                match get_function_flags(isolate, curr_ctx_scope, &function_flags) {
                    Ok(flags) => flags,
                    Err(e) => {
                        isolate.raise_exception_str(&format!("Failed parsing function flags, {}", e));
                        return None;
                    }
                }
            } else {
                0
            };

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate.raise_exception_str("Called 'register_function' out of context");
                return None;
            }

            let script_ctx_ref = match script_ctx_ref.upgrade() {
                Some(s) => s,
                None => {
                    isolate.raise_exception_str("Use of uninitialize script context");
                    return None;
                }
            };

            let load_ctx = load_ctx.unwrap();
            let c = Arc::new(RefCell::new(RedisClient::new()));
            let redis_client = get_redis_client(&script_ctx_ref, curr_ctx_scope, &c);

            let f = V8Function::new(
                &script_ctx_ref,
                persisted_function,
                redis_client.to_value().persist(isolate),
                &c,
                function_callback.is_async_function(),
            );

            let res = load_ctx.register_function(function_name_utf8.as_str(), Box::new(f), function_flags);
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    redis.set(
        ctx_scope,
        &script_ctx.isolate.new_string("v8_version").to_value(),
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
        &script_ctx.isolate.new_string("log").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, _curr_ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'log' function");
                    return None;
                }

                let msg = args.get(0);
                if !msg.is_string() {
                    isolate.raise_exception_str("First argument to 'log' must be a string message");
                    return None;
                }

                let msg_utf8 = msg.to_utf8(isolate).unwrap();
                match script_ctx_ref.upgrade() {
                    Some(s) => s.compiled_library_api.log(msg_utf8.as_str()),
                    None => crate::v8_backend::log(msg_utf8.as_str()), /* do not abort logs */
                }
                None
            })
            .to_value(),
    );

    globals.set(
        ctx_scope,
        &script_ctx.isolate.new_string("redis").to_value(),
        &redis.to_value(),
    );

    let script_ctx_ref = Arc::downgrade(script_ctx);
    globals.set(
        ctx_scope,
        &script_ctx.isolate.new_string("Promise").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, curr_ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'Promise' function");
                    return None;
                }

                let function = args.get(0);
                if !function.is_function() || function.is_async_function() {
                    isolate.raise_exception_str("Bad argument to 'Promise' function");
                    return None;
                }

                let script_ctx_ref = match script_ctx_ref.upgrade() {
                    Some(s) => s,
                    None => {
                        isolate.raise_exception_str("Use of uninitialize script context");
                        return None;
                    }
                };

                let script_ctx_ref_resolve = Arc::downgrade(&script_ctx_ref);
                let script_ctx_ref_reject = Arc::downgrade(&script_ctx_ref);
                let resolver = curr_ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver_resolve = Arc::new(resolver.to_value().persist(isolate));
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
                                isolate.raise_exception_str("Library was deleted");
                                return None;
                            }
                        };

                        let res = args.get(0).persist(isolate);
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
                                        log("Library was delete while not all the jobs were done");
                                        return;
                                    }
                                };
                                let _isolate_scope = new_script_ctx_ref_resolve.isolate.enter();
                                let _isolate_scope =
                                    new_script_ctx_ref_resolve.isolate.new_handlers_scope();
                                let ctx_scope = new_script_ctx_ref_resolve.ctx.enter();
                                let _trycatch = new_script_ctx_ref_resolve.isolate.new_try_catch();
                                let res = res.as_local(&new_script_ctx_ref_resolve.isolate);
                                let resolver = resolver_resolve
                                    .as_local(&new_script_ctx_ref_resolve.isolate)
                                    .as_resolver();
                                resolver.resolve(&ctx_scope, &res);
                            }));
                        None
                    });

                let reject =
                    curr_ctx_scope.new_native_function(move |args, isolate, _curr_ctx_scope| {
                        if args.len() != 1 {
                            isolate.raise_exception_str(
                                "Wrong number of arguments to 'resolve' function",
                            );
                            return None;
                        }

                        let script_ctx_ref_reject = match script_ctx_ref_reject.upgrade() {
                            Some(s) => s,
                            None => {
                                isolate.raise_exception_str("Library was deleted");
                                return None;
                            }
                        };

                        let res = args.get(0).persist(isolate);
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
                                        log("Library was delete while not all the jobs were done");
                                        return;
                                    }
                                };
                                let _isolate_scope = new_script_ctx_ref_reject.isolate.enter();
                                let _isolate_scope =
                                    new_script_ctx_ref_reject.isolate.new_handlers_scope();
                                let ctx_scope = new_script_ctx_ref_reject.ctx.enter();
                                let _trycatch = new_script_ctx_ref_reject.isolate.new_try_catch();
                                let res = res.as_local(&new_script_ctx_ref_reject.isolate);
                                let resolver = resolver_reject
                                    .as_local(&new_script_ctx_ref_reject.isolate)
                                    .as_resolver();
                                resolver.reject(&ctx_scope, &res);
                            }));
                        None
                    });

                let _ = function.call(
                    curr_ctx_scope,
                    Some(&[&resolve.to_value(), &reject.to_value()]),
                );
                Some(promise.to_value())
            })
            .to_value(),
    );
}
