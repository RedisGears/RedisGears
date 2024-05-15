/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::keys_notifications_consumer_ctx::NotificationPostJobCtxInterface;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;
use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use super::background_run_ctx::BackgroundRunCtx;
use super::run_ctx::{RedisClient, RedisClientCallOptions};
use super::{get_notification_blocker, GearsLibraryMetaData};

use std::sync::Arc;

pub(crate) struct KeySpaceNotificationsCtx<'ctx> {
    ctx: &'ctx Context,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    flags: FunctionFlags,
}

impl<'ctx> KeySpaceNotificationsCtx<'ctx> {
    pub(crate) fn new(
        ctx: &'ctx Context,
        lib_meta_data: Arc<GearsLibraryMetaData>,
        flags: FunctionFlags,
    ) -> KeySpaceNotificationsCtx {
        KeySpaceNotificationsCtx {
            ctx,
            lib_meta_data,
            flags,
        }
    }
}

impl<'ctx> NotificationRunCtxInterface for KeySpaceNotificationsCtx<'ctx> {
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

impl<'ctx> NotificationPostJobCtxInterface for KeySpaceNotificationsCtx<'ctx> {
    fn add_post_notification_job(&self, job: Box<dyn FnOnce(&dyn NotificationRunCtxInterface)>) {
        let lib_meta_data = Arc::clone(&self.lib_meta_data);
        self.ctx.add_post_notification_job(move |ctx| {
            let post_notification_ctx =
                KeySpaceNotificationsCtx::new(ctx, lib_meta_data, FunctionFlags::empty());
            let _notification_blocker = get_notification_blocker();
            job(&post_notification_ctx);
        });
    }
}
