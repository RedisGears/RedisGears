/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::CallResult;
use redis_module::DetachedContextGuard;
use redis_module::RedisString;

use redisgears_plugin_api::redisgears_plugin_api::run_function_ctx::PromiseReply;
use redisgears_plugin_api::redisgears_plugin_api::{
    redisai_interface::AIModelInterface, redisai_interface::AIScriptInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    GearsApiError,
};

use crate::call_redis_command_async;
use crate::run_ctx::RedisClientCallOptions;
use crate::{
    background_run_ctx::BackgroundRunCtx, call_redis_command, get_notification_blocker,
    GearsLibraryMetaData, NotificationBlocker,
};

use std::sync::Arc;

use redisai_rs::redisai::redisai_model::RedisAIModel;
use redisai_rs::redisai::redisai_script::RedisAIScript;

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) detached_ctx_guard: DetachedContextGuard,
    call_options: RedisClientCallOptions,
    user: RedisString,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    _notification_blocker: NotificationBlocker,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl BackgroundRunScopeGuardCtx {
    pub(crate) fn new(
        ctx_guard: DetachedContextGuard,
        user: RedisString,
        lib_meta_data: &Arc<GearsLibraryMetaData>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunScopeGuardCtx {
        BackgroundRunScopeGuardCtx {
            detached_ctx_guard: ctx_guard,
            call_options,
            user,
            lib_meta_data: Arc::clone(lib_meta_data),
            _notification_blocker: get_notification_blocker(),
        }
    }
}

impl RedisClientCtxInterface for BackgroundRunScopeGuardCtx {
    fn call(&self, command: &str, args: &[&[u8]]) -> CallResult {
        call_redis_command(
            &self.detached_ctx_guard,
            &self.user,
            command,
            &self.call_options.call_options,
            args,
        )
    }

    fn call_async(&self, command: &str, args: &[&[u8]]) -> PromiseReply<'static, '_> {
        call_redis_command_async(
            &self.detached_ctx_guard,
            &self.lib_meta_data.name,
            &self.user,
            command,
            &self.call_options.blocking_call_options,
            args,
        )
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.user.clone(),
            &self.lib_meta_data,
            self.call_options.clone(),
        ))
    }

    fn open_ai_model(&self, name: &str) -> Result<Box<dyn AIModelInterface>, GearsApiError> {
        let ctx = &self.detached_ctx_guard;
        let _authenticate_scope = ctx
            .authenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        RedisAIModel::open_from_key(ctx, name)
            .map(|v| Box::new(v) as Box<dyn AIModelInterface>)
            .map_err(GearsApiError::new)
    }

    fn open_ai_script(&self, name: &str) -> Result<Box<dyn AIScriptInterface>, GearsApiError> {
        let ctx = &self.detached_ctx_guard;
        let _authenticate_scope = ctx
            .authenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        RedisAIScript::open_from_key(ctx, name)
            .map(|v| Box::new(v) as Box<dyn AIScriptInterface>)
            .map_err(GearsApiError::new)
    }
}
