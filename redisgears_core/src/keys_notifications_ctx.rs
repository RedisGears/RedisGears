use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_ctx::BackgroundRunCtx;
use crate::run_ctx::{RedisClient, RedisClientCallOptions};

pub(crate) struct KeysNotificationsRunCtx {
    user: String,
    flags: u8,
}

impl KeysNotificationsRunCtx {
    pub(crate) fn new(user: &str, flags: u8) -> KeysNotificationsRunCtx {
        KeysNotificationsRunCtx {
            user: user.to_string(),
            flags: flags,
        }
    }
}

impl NotificationRunCtxInterface for KeysNotificationsRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient::new(Some(self.user.clone()), self.flags))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            Some(self.user.clone()),
            RedisClientCallOptions::new(self.flags),
        ))
    }
}
