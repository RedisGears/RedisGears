/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;
use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    keys_notifications_consumer_ctx::NotificationFiredDataInterface,
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,
};

use v8_rs::v8::v8_value::V8LocalValue;
use v8_rs::v8::{v8_promise::V8PromiseState, v8_value::V8PersistValue};

use crate::v8_native_functions::{get_backgrounnd_client, get_redis_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;
use crate::{get_error_from_object, get_exception_msg, v8_backend::bypass_memory_limit};

use std::any::Any;
use std::cell::RefCell;
use std::sync::Arc;

use v8_derive::new_native_function;

struct V8NotificationCtxData {
    event: String,
    key: Vec<u8>,
}

impl NotificationFiredDataInterface for V8NotificationCtxData {}

struct V8AckCallbackInternal {
    ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    locker: Box<dyn BackgroundRunFunctionCtxInterface>,
}

struct V8AckCallback {
    internal: Option<V8AckCallbackInternal>,
}

struct V8NotificationsCtxInternal {
    persisted_function: V8PersistValue,
    script_ctx: Arc<V8ScriptCtx>,
}

impl V8NotificationsCtxInternal {
    fn run_sync(
        &self,
        notification_ctx: &dyn NotificationRunCtxInterface,
        data: &V8NotificationCtxData,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        let res = {
            let isolate_scope = self.script_ctx.isolate.enter();
            let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
            let try_catch = isolate_scope.new_try_catch();

            let notification_data = isolate_scope.new_object();
            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("event").to_value(),
                &isolate_scope.new_string(&data.event).to_value(),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key").to_value(),
                &std::str::from_utf8(&data.key).map_or(isolate_scope.new_null(), |v| {
                    isolate_scope.new_string(v).to_value()
                }),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key_raw").to_value(),
                &isolate_scope.new_array_buffer(&data.key).to_value(),
            );

            let c = notification_ctx.get_redis_client();
            let redis_client = Arc::new(RefCell::new(RedisClient::with_client(c.as_ref())));
            let r_client =
                get_redis_client(&self.script_ctx, &isolate_scope, &ctx_scope, &redis_client);

            let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

            self.script_ctx.before_run();
            self.script_ctx.after_lock_gil();
            let res = self.persisted_function.as_local(&isolate_scope).call(
                &ctx_scope,
                Some(&[&r_client.to_value(), &notification_data.to_value()]),
            );
            self.script_ctx.before_release_gil();
            self.script_ctx.after_run();

            redis_client.borrow_mut().make_invalid();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let res = res.get_result();
                            Some(Err(get_error_from_object(&res, &ctx_scope)))
                        } else if res.state() == V8PromiseState::Fulfilled {
                            Some(Ok(()))
                        } else {
                            let ack_callback_resolve = Arc::new(RefCell::new(V8AckCallback {
                                internal: Some(V8AckCallbackInternal {
                                    ack_callback,
                                    locker: notification_ctx.get_background_redis_client(),
                                }),
                            }));
                            let ack_callback_reject = Arc::clone(&ack_callback_resolve);
                            let resolve = ctx_scope.new_native_function(new_native_function!(
                                move |isolate, _context| {
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_resolve.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Ok(()));
                                    }
                                    Ok::<_, String>(None)
                                }
                            ));
                            let reject = ctx_scope.new_native_function(new_native_function!(
                                move |isolate, ctx_scope, res: V8LocalValue| {
                                    let res = get_error_from_object(&res, ctx_scope);
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_reject.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Err(res));
                                    }
                                    Ok::<_, String>(None)
                                }
                            ));
                            res.then(&ctx_scope, &resolve, &reject);
                            return;
                        }
                    } else {
                        Some(Ok(()))
                    }
                }
                None => {
                    let error_msg =
                        get_exception_msg(&self.script_ctx.isolate, try_catch, &ctx_scope);
                    Some(Err(error_msg))
                }
            }
        };

        if let Some(res) = res {
            ack_callback(res);
        }
    }

    fn run_async(
        &self,
        background_client: Box<dyn BackgroundRunFunctionCtxInterface>,
        locker: Box<dyn BackgroundRunFunctionCtxInterface>,
        data: &V8NotificationCtxData,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        let res = {
            let isolate_scope = self.script_ctx.isolate.enter();
            let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
            let trycatch = isolate_scope.new_try_catch();

            let notification_data = isolate_scope.new_object();
            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("event").to_value(),
                &isolate_scope.new_string(&data.event).to_value(),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key").to_value(),
                &std::str::from_utf8(&data.key).map_or(isolate_scope.new_null(), |v| {
                    isolate_scope.new_string(v).to_value()
                }),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key_raw").to_value(),
                &isolate_scope.new_array_buffer(&data.key).to_value(),
            );

            let r_client = get_backgrounnd_client(
                &self.script_ctx,
                &isolate_scope,
                &ctx_scope,
                Arc::new(background_client),
            );

            self.script_ctx.before_run();
            let res = self.persisted_function.as_local(&isolate_scope).call(
                &ctx_scope,
                Some(&[&r_client.to_value(), &notification_data.to_value()]),
            );
            self.script_ctx.after_run();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let res = res.get_result();
                            Some(Err(get_error_from_object(&res, &ctx_scope)))
                        } else if res.state() == V8PromiseState::Fulfilled {
                            Some(Ok(()))
                        } else {
                            let ack_callback_resolve = Arc::new(RefCell::new(V8AckCallback {
                                internal: Some(V8AckCallbackInternal {
                                    ack_callback,
                                    locker,
                                }),
                            }));
                            let ack_callback_reject = Arc::clone(&ack_callback_resolve);
                            let resolve = ctx_scope.new_native_function(new_native_function!(
                                move |isolate, _context| {
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_resolve.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Ok(()));
                                    }
                                    Ok::<_, String>(None)
                                }
                            ));
                            let reject = ctx_scope.new_native_function(new_native_function!(
                                move |isolate, ctx_scope, res: V8LocalValue| {
                                    let res = get_error_from_object(&res, ctx_scope);
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_reject.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Err(res));
                                    }
                                    Ok::<_, String>(None)
                                }
                            ));
                            res.then(&ctx_scope, &resolve, &reject);
                            return;
                        }
                    } else {
                        Some(Ok(()))
                    }
                }
                None => {
                    let error_msg =
                        get_exception_msg(&self.script_ctx.isolate, trycatch, &ctx_scope);
                    Some(Err(error_msg))
                }
            }
        };

        if let Some(res) = res {
            let _locker = locker.lock();
            ack_callback(res);
        }
    }
}

pub(crate) struct V8NotificationsCtx {
    internal: Arc<V8NotificationsCtxInternal>,
    is_async: bool,
}

impl V8NotificationsCtx {
    pub(crate) fn new(
        mut persisted_function: V8PersistValue,
        script_ctx: &Arc<V8ScriptCtx>,
        is_async: bool,
    ) -> Self {
        persisted_function.forget();
        Self {
            internal: Arc::new(V8NotificationsCtxInternal {
                persisted_function,
                script_ctx: Arc::clone(script_ctx),
            }),
            is_async,
        }
    }
}

impl KeysNotificationsConsumerCtxInterface for V8NotificationsCtx {
    fn on_notification_fired(
        &self,
        event: &str,
        key: &[u8],
        _notification_ctx: &dyn NotificationRunCtxInterface,
    ) -> Option<Box<dyn Any>> {
        Some(Box::new(V8NotificationCtxData {
            event: event.to_string(),
            key: key.to_vec(),
        }))
    }

    fn post_command_notification(
        &self,
        notificaion_data: Option<Box<dyn Any>>,
        notification_ctx: &dyn NotificationRunCtxInterface,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        if bypass_memory_limit() {
            ack_callback(Err(GearsApiError::new(
                "JS engine reached OOM state and can not run any more code",
            )));
            return;
        }

        let notificaion_data = notificaion_data
            .unwrap()
            .downcast::<V8NotificationCtxData>()
            .unwrap();
        if self.is_async {
            let redis_background_client = notification_ctx.get_background_redis_client();
            let locker = notification_ctx.get_background_redis_client();
            let internal = Arc::clone(&self.internal);
            self.internal
                .script_ctx
                .compiled_library_api
                .run_on_background(Box::new(move || {
                    internal.run_async(
                        redis_background_client,
                        locker,
                        &notificaion_data,
                        ack_callback,
                    );
                }));
        } else {
            self.internal
                .run_sync(notification_ctx, &notificaion_data, ack_callback);
        }
    }
}
