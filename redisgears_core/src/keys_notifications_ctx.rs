/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;
use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_ctx::BackgroundRunCtx;
use crate::run_ctx::{RedisClient, RedisClientCallOptions};
use crate::GearsLibraryMetaData;

use std::sync::Arc;

pub(crate) struct KeysNotificationsRunCtx<'ctx> {
    ctx: &'ctx Context,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    flags: FunctionFlags,
}

impl<'ctx> KeysNotificationsRunCtx<'ctx> {
    pub(crate) fn new(
        ctx: &'ctx Context,
        lib_meta_data: Arc<GearsLibraryMetaData>,
        flags: FunctionFlags,
    ) -> KeysNotificationsRunCtx {
        KeysNotificationsRunCtx {
            ctx,
            lib_meta_data,
            flags,
        }
    }
}

impl<'ctx> NotificationRunCtxInterface for KeysNotificationsRunCtx<'ctx> {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface + '_> {
        Box::new(RedisClient::new(
            self.ctx,
            self.lib_meta_data.clone(),
            self.lib_meta_data.user.safe_clone(self.ctx),
            self.flags,
        ))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.lib_meta_data.user.safe_clone(self.ctx),
            &self.lib_meta_data,
            RedisClientCallOptions::new(self.flags),
        ))
    }
}
