/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use lazy_static::__Deref;
use redis_module::CallResult;
use redis_module::ContextGuard;
use redis_module::RedisString;

use redisgears_plugin_api::redisgears_plugin_api::{
    redisai_interface::AIModelInterface, redisai_interface::AIScriptInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    GearsApiError,
};

use crate::run_ctx::RedisClientCallOptions;
use crate::{
    background_run_ctx::BackgroundRunCtx, call_redis_command, get_notification_blocker,
    GearsLibraryMetaData, NotificationBlocker,
};

use std::sync::Arc;

use redisai_rs::redisai::redisai_model::RedisAIModel;
use redisai_rs::redisai::redisai_script::RedisAIScript;

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) ctx_guard: ContextGuard,
    call_options: RedisClientCallOptions,
    user: RedisString,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    _notification_blocker: NotificationBlocker,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl BackgroundRunScopeGuardCtx {
    pub(crate) fn new(
        ctx_guard: ContextGuard,
        user: RedisString,
        lib_meta_data: &Arc<GearsLibraryMetaData>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunScopeGuardCtx {
        BackgroundRunScopeGuardCtx {
            ctx_guard,
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
            self.ctx_guard.deref(),
            &self.user,
            command,
            &self.call_options.call_options,
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
        let ctx = self.ctx_guard.deref();
        let _authenticate_scope = ctx
            .autenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        let res = RedisAIModel::open_from_key(ctx, name);
        match res {
            Ok(res) => Ok(Box::new(res)),
            Err(e) => Err(GearsApiError::new(e)),
        }
    }

    fn open_ai_script(&self, name: &str) -> Result<Box<dyn AIScriptInterface>, GearsApiError> {
        let ctx = self.ctx_guard.deref();
        let _authenticate_scope = ctx
            .autenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        let res = RedisAIScript::open_from_key(ctx, name);
        match res {
            Ok(res) => Ok(Box::new(res)),
            Err(e) => Err(GearsApiError::new(e)),
        }
    }
}
