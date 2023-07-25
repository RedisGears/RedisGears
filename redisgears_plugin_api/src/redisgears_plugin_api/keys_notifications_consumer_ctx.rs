/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RedisClientCtxInterface;

use super::GearsApiError;

pub trait NotificationRunCtxInterface {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface + '_>;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
}

type NotificationPostJobFn = dyn FnOnce(&dyn NotificationRunCtxInterface);

pub trait NotificationPostJobCtxInterface {
    fn add_post_notification_job(&self, job: Box<NotificationPostJobFn>);
}

pub trait NotificationCtxInterface:
    NotificationPostJobCtxInterface + NotificationRunCtxInterface
{
}

impl<T> NotificationCtxInterface for T where
    T: NotificationPostJobCtxInterface + NotificationRunCtxInterface
{
}

pub trait KeysNotificationsConsumerCtxInterface {
    fn on_notification_fired(
        &self,
        event: &str,
        key: &[u8],
        notification_ctx: &dyn NotificationCtxInterface,
        ack_callback: Box<dyn FnOnce(Result<(), GearsApiError>) + Send + Sync>,
    );
}
