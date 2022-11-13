use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_ctx::BackgroundRunCtx;
use crate::run_ctx::{RedisClient, RedisClientCallOptions};
use crate::GearsLibraryMataData;

use std::sync::Arc;

pub(crate) struct KeysNotificationsRunCtx {
    lib_meta_data: Arc<GearsLibraryMataData>,
    flags: u8,
}

impl KeysNotificationsRunCtx {
    pub(crate) fn new(meta_data: &Arc<GearsLibraryMataData>, flags: u8) -> KeysNotificationsRunCtx {
        KeysNotificationsRunCtx {
            lib_meta_data: Arc::clone(meta_data),
            flags,
        }
    }
}

impl NotificationRunCtxInterface for KeysNotificationsRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient::new(&self.lib_meta_data, None, self.flags))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            None,
            &self.lib_meta_data,
            RedisClientCallOptions::new(self.flags),
        ))
    }
}
