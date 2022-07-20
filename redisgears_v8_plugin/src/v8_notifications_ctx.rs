use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    keys_notifications_consumer_ctx::NotificationFiredDataInterface,
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,
};

use v8_rs::v8::{v8_promise::V8PromiseState, v8_value::V8PersistValue};

use crate::get_exception_msg;
use crate::v8_native_functions::{get_backgrounnd_client, get_redis_client, RedisClient};
use crate::v8_script_ctx::V8ScriptCtx;

use std::any::Any;
use std::cell::RefCell;
use std::sync::Arc;

struct V8NotificationCtxData {
    event: String,
    key: String,
}

impl NotificationFiredDataInterface for V8NotificationCtxData {}

struct V8AckCallbackInternal {
    ack_callback: Box<dyn FnOnce(Result<(), String>) + Send + Sync>,
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
        notification_ctx: Box<dyn NotificationRunCtxInterface>,
        data: Box<V8NotificationCtxData>,
        ack_callback: Box<dyn FnOnce(Result<(), String>) + Send + Sync>,
    ) {
        let res = {
            let _isolate_scope = self.script_ctx.isolate.enter();
            let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
            let ctx_scope = self.script_ctx.ctx.enter();
            let trycatch = self.script_ctx.isolate.new_try_catch();

            let notification_data = self.script_ctx.isolate.new_object();
            notification_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("event").to_value(),
                &self.script_ctx.isolate.new_string(&data.event).to_value(),
            );

            notification_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("key").to_value(),
                &self.script_ctx.isolate.new_string(&data.key).to_value(),
            );

            let c = notification_ctx.get_redis_client();
            let mut redis_client = RedisClient::new();
            redis_client.set_client(c);
            let redis_client = Arc::new(RefCell::new(redis_client));
            let r_client = get_redis_client(&self.script_ctx, &ctx_scope, &redis_client);

            ctx_scope.set_private_data(0, Some(&true)); // indicate we are blocked

            self.script_ctx.before_run();
            self.script_ctx.after_lock_gil();
            let res = self
                .persisted_function
                .as_local(&self.script_ctx.isolate)
                .call(
                    &ctx_scope,
                    Some(&[&r_client.to_value(), &notification_data.to_value()]),
                );
            self.script_ctx.before_release_gil();
            self.script_ctx.after_run();

            ctx_scope.set_private_data::<bool>(0, None); // indicate we are not blocked

            redis_client.borrow_mut().make_invalid();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let error_utf8 =
                                res.get_result().to_utf8(&self.script_ctx.isolate).unwrap();
                            Some(Err(error_utf8.as_str().to_string()))
                        } else if res.state() == V8PromiseState::Fulfilled {
                            Some(Ok(()))
                        } else {
                            let ack_callback_resolve = Arc::new(RefCell::new(V8AckCallback {
                                internal: Some(V8AckCallbackInternal {
                                    ack_callback: ack_callback,
                                    locker: notification_ctx.get_background_redis_client(),
                                }),
                            }));
                            let ack_callback_reject = Arc::clone(&ack_callback_resolve);
                            let resolve =
                                ctx_scope.new_native_function(move |_args, isolate, _context| {
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_resolve.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Ok(()));
                                    }
                                    None
                                });
                            let reject =
                                ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                    let res = args.get(0);
                                    let res = res.to_utf8(isolate).unwrap();
                                    let res = res.as_str().to_string();
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_reject.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Err(res));
                                    }
                                    None
                                });
                            res.then(&ctx_scope, &resolve, &reject);
                            return;
                        }
                    } else {
                        Some(Ok(()))
                    }
                }
                None => {
                    let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
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
        data: Box<V8NotificationCtxData>,
        ack_callback: Box<dyn FnOnce(Result<(), String>) + Send + Sync>,
    ) {
        let res = {
            let _isolate_scope = self.script_ctx.isolate.enter();
            let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
            let ctx_scope = self.script_ctx.ctx.enter();
            let trycatch = self.script_ctx.isolate.new_try_catch();

            let notification_data = self.script_ctx.isolate.new_object();
            notification_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("event").to_value(),
                &self.script_ctx.isolate.new_string(&data.event).to_value(),
            );

            notification_data.set(
                &ctx_scope,
                &self.script_ctx.isolate.new_string("key").to_value(),
                &self.script_ctx.isolate.new_string(&data.key).to_value(),
            );

            let r_client = get_backgrounnd_client(&self.script_ctx, &ctx_scope, background_client);

            self.script_ctx.before_run();
            let res = self
                .persisted_function
                .as_local(&self.script_ctx.isolate)
                .call(
                    &ctx_scope,
                    Some(&[&r_client.to_value(), &notification_data.to_value()]),
                );
            self.script_ctx.after_run();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let res = res.as_promise();
                        if res.state() == V8PromiseState::Rejected {
                            let error_utf8 =
                                res.get_result().to_utf8(&self.script_ctx.isolate).unwrap();
                            Some(Err(error_utf8.as_str().to_string()))
                        } else if res.state() == V8PromiseState::Fulfilled {
                            Some(Ok(()))
                        } else {
                            let ack_callback_resolve = Arc::new(RefCell::new(V8AckCallback {
                                internal: Some(V8AckCallbackInternal {
                                    ack_callback: ack_callback,
                                    locker: locker,
                                }),
                            }));
                            let ack_callback_reject = Arc::clone(&ack_callback_resolve);
                            let resolve =
                                ctx_scope.new_native_function(move |_args, isolate, _context| {
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_resolve.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Ok(()));
                                    }
                                    None
                                });
                            let reject =
                                ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                    let res = args.get(0);
                                    let res = res.to_utf8(isolate).unwrap();
                                    let res = res.as_str().to_string();
                                    let _unlocker = isolate.new_unlocker();
                                    if let Some(ack) =
                                        ack_callback_reject.borrow_mut().internal.take()
                                    {
                                        let _locker = ack.locker.lock();
                                        (ack.ack_callback)(Err(res));
                                    }
                                    None
                                });
                            res.then(&ctx_scope, &resolve, &reject);
                            return;
                        }
                    } else {
                        Some(Ok(()))
                    }
                }
                None => {
                    let error_msg = get_exception_msg(&self.script_ctx.isolate, trycatch);
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
        persisted_function: V8PersistValue,
        script_ctx: &Arc<V8ScriptCtx>,
        is_async: bool,
    ) -> V8NotificationsCtx {
        V8NotificationsCtx {
            internal: Arc::new(V8NotificationsCtxInternal {
                persisted_function: persisted_function,
                script_ctx: Arc::clone(script_ctx),
            }),
            is_async: is_async,
        }
    }
}

impl KeysNotificationsConsumerCtxInterface for V8NotificationsCtx {
    fn on_notification_fired(
        &self,
        event: &str,
        key: &str,
        _notification_ctx: Box<dyn NotificationRunCtxInterface>,
    ) -> Option<Box<dyn Any>> {
        Some(Box::new(V8NotificationCtxData {
            event: event.to_string(),
            key: key.to_string(),
        }))
    }

    fn post_command_notification(
        &self,
        notificaion_data: Option<Box<dyn Any>>,
        notification_ctx: Box<dyn NotificationRunCtxInterface>,
        ack_callback: Box<dyn FnOnce(Result<(), String>) + Send + Sync>,
    ) {
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
                        notificaion_data,
                        ack_callback,
                    );
                }));
        } else {
            self.internal
                .run_sync(notification_ctx, notificaion_data, ack_callback);
        }
    }
}
