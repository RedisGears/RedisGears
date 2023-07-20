/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::keys_notifications_consumer_ctx::NotificationCtxInterface;
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;
use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,
};

use v8_rs::v8::v8_value::V8PersistValue;

use crate::v8_native_functions::{get_backgrounnd_client, get_redis_client, RedisClient};
use crate::v8_script_ctx::{GilStatus, V8ScriptCtx};
use crate::{get_exception_msg, v8_backend::bypass_memory_limit};

use std::cell::RefCell;
use std::sync::Arc;

struct V8NotificationsCtxInternal {
    persisted_function: V8PersistValue,
    on_trigger_fired: Option<V8PersistValue>,
    script_ctx: Arc<V8ScriptCtx>,
}

impl V8NotificationsCtxInternal {
    fn run_sync(
        &self,
        notification_ctx: &dyn NotificationRunCtxInterface,
        mut data: V8PersistValue,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        let res = {
            let isolate_scope = self.script_ctx.isolate.enter();
            let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
            let try_catch = isolate_scope.new_try_catch();

            let notification_data = data.take_local(&isolate_scope);

            let c = notification_ctx.get_redis_client();
            let redis_client = Arc::new(RefCell::new(RedisClient::with_client(c.as_ref())));
            let r_client =
                get_redis_client(&self.script_ctx, &isolate_scope, &ctx_scope, &redis_client);

            let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

            let res = self.script_ctx.call(
                &self.persisted_function.as_local(&isolate_scope),
                &ctx_scope,
                Some(&[&r_client.to_value(), &notification_data]),
                GilStatus::Locked,
            );

            redis_client.borrow_mut().make_invalid();

            match res {
                Some(res) => {
                    if res.is_promise() {
                        let promise = res.as_promise();
                        if self.script_ctx.is_reject_or_fulfilled(&promise) {
                            self.script_ctx.promise_rejected_or_fulfilled(
                                &isolate_scope,
                                &ctx_scope,
                                &promise,
                                move |res| {
                                    ack_callback(res.map(|_| ()));
                                },
                            );
                        } else {
                            // in case the promise was not yet resolved, it will be resolved when the Redis
                            // GIL is not lock, so we must lock it before calling the ack callback.
                            let locker: Box<dyn BackgroundRunFunctionCtxInterface> =
                                notification_ctx.get_background_redis_client();
                            self.script_ctx.promise_rejected_or_fulfilled_async(
                                &ctx_scope,
                                &promise,
                                move |res| {
                                    let _locker = locker.lock();
                                    ack_callback(res.map(|_| ()));
                                },
                            );
                        };
                        return;
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
        mut data: V8PersistValue,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        let res = {
            let isolate_scope = self.script_ctx.isolate.enter();
            let ctx_scope = self.script_ctx.ctx.enter(&isolate_scope);
            let trycatch = isolate_scope.new_try_catch();

            let notification_data = data.take_local(&isolate_scope);

            let r_client = get_backgrounnd_client(
                &self.script_ctx,
                &isolate_scope,
                &ctx_scope,
                Arc::new(background_client),
            );

            let res = self.script_ctx.call(
                &self.persisted_function.as_local(&isolate_scope),
                &ctx_scope,
                Some(&[&r_client.to_value(), &notification_data]),
                GilStatus::Unlocked,
            );

            match res {
                Some(res) => {
                    if res.is_promise() {
                        self.script_ctx.handle_promise(
                            &isolate_scope,
                            &ctx_scope,
                            &res.as_promise(),
                            move |res| {
                                let _unlocker =
                                    res.as_ref().map(|v| v.isolate_scope.new_unlocker());
                                let _locker = locker.lock();
                                ack_callback(res.map(|_| ()));
                            },
                        );
                        return;
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
        on_trigger_fired: Option<V8PersistValue>,
        script_ctx: &Arc<V8ScriptCtx>,
        is_async: bool,
    ) -> Self {
        persisted_function.forget();
        let on_trigger_fired = on_trigger_fired.map(|mut v| {
            v.forget();
            v
        });
        Self {
            internal: Arc::new(V8NotificationsCtxInternal {
                persisted_function,
                on_trigger_fired,
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
        notification_ctx: &dyn NotificationCtxInterface,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    ) {
        if bypass_memory_limit() {
            ack_callback(Err(GearsApiError::new(
                "JS engine reached OOM state and can not run any more code",
            )));
            return;
        }

        let data = {
            let isolate_scope = self.internal.script_ctx.isolate.enter();
            let ctx_scope = self.internal.script_ctx.ctx.enter(&isolate_scope);
            let try_catch = isolate_scope.new_try_catch();

            let notification_data = isolate_scope.new_object();
            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("event").to_value(),
                &isolate_scope.new_string(event).to_value(),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key").to_value(),
                &std::str::from_utf8(key).map_or(isolate_scope.new_null(), |v| {
                    isolate_scope.new_string(v).to_value()
                }),
            );

            notification_data.set(
                &ctx_scope,
                &isolate_scope.new_string("key_raw").to_value(),
                &isolate_scope.new_array_buffer(key).to_value(),
            );
            let val = notification_data.to_value();

            // possibly enhance the data with more information
            let res = self.internal.on_trigger_fired.as_ref().map_or(Ok(()), |v| {
                let on_trigger_fired = v.as_local(&isolate_scope);

                let c = notification_ctx.get_redis_client();
                let redis_client = Arc::new(RefCell::new(RedisClient::with_client(c.as_ref())));
                let r_client = get_redis_client(
                    &self.internal.script_ctx,
                    &isolate_scope,
                    &ctx_scope,
                    &redis_client,
                );

                let _block_guard = ctx_scope.set_private_data(0, &true); // indicate we are blocked

                let res = self.internal.script_ctx.call(
                    &on_trigger_fired,
                    &ctx_scope,
                    Some(&[&r_client.to_value(), &val]),
                    GilStatus::Locked,
                );

                redis_client.borrow_mut().make_invalid();

                res.map_or_else(
                    || {
                        Err(get_exception_msg(
                            &self.internal.script_ctx.isolate,
                            try_catch,
                            &ctx_scope,
                        ))
                    },
                    |_| Ok(()),
                )
            });

            if let Err(res) = res {
                ack_callback(Err(res));
                return;
            }

            val.persist()
        };
        let internal = Arc::clone(&self.internal);
        let is_async = self.is_async;
        notification_ctx.add_post_notification_job(Box::new(move |notification_run_ctx| {
            if is_async {
                let redis_background_client = notification_run_ctx.get_background_redis_client();
                let locker = notification_run_ctx.get_background_redis_client();
                let new_internal = Arc::clone(&internal);
                internal
                    .script_ctx
                    .compiled_library_api
                    .run_on_background(Box::new(move || {
                        new_internal.run_async(redis_background_client, locker, data, ack_callback);
                    }));
            } else {
                internal.run_sync(notification_run_ctx, data, ack_callback);
            }
        }));
    }
}
