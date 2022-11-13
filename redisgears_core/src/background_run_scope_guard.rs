use redis_module::context::thread_safe::ContextGuard;

use redis_module::Status;

use redisgears_plugin_api::redisgears_plugin_api::{
    redisai_interface::AIModelInterface, redisai_interface::AIScriptInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    CallResult, GearsApiError,
};

use crate::run_ctx::RedisClientCallOptions;
use crate::{
    background_run_ctx::BackgroundRunCtx, call_redis_command, get_notification_blocker,
    GearsLibraryMataData, NotificationBlocker,
};

use std::sync::Arc;

use redisai_rs::redisai::redisai_model::RedisAIModel;
use redisai_rs::redisai::redisai_script::RedisAIScript;

use crate::{get_ctx, get_globals};

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) _ctx_guard: ContextGuard,
    call_options: RedisClientCallOptions,
    user: Option<String>,
    lib_meta_data: Arc<GearsLibraryMataData>,
    _notification_blocker: NotificationBlocker,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl BackgroundRunScopeGuardCtx {
    pub(crate) fn new(
        ctx_guard: ContextGuard,
        user: Option<String>,
        lib_meta_data: &Arc<GearsLibraryMataData>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunScopeGuardCtx {
        BackgroundRunScopeGuardCtx {
            _ctx_guard: ctx_guard,
            call_options,
            user,
            lib_meta_data: Arc::clone(lib_meta_data),
            _notification_blocker: get_notification_blocker(),
        }
    }
}

impl RedisClientCtxInterface for BackgroundRunScopeGuardCtx {
    fn call(&self, command: &str, args: &[&[u8]]) -> CallResult {
        let user = match self.user.as_ref() {
            Some(u) => Some(u),
            None => Some(&self.lib_meta_data.user),
        };
        call_redis_command(user, command, &self.call_options.call_options, args)
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.user.clone(),
            &self.lib_meta_data,
            self.call_options.clone(),
        ))
    }

    fn open_ai_model(&self, name: &str) -> Result<Box<dyn AIModelInterface>, GearsApiError> {
        let ctx = match &self.user {
            Some(u) => {
                let ctx = &get_globals().authenticated_redis_ctx;
                if ctx.autenticate_user(u) == Status::Err {
                    return Err(GearsApiError::Msg(format!(
                        "Failed authenticate user {}",
                        u
                    )));
                }
                ctx
            }
            None => get_ctx(),
        };
        let res = RedisAIModel::open_from_key(ctx, name);
        match res {
            Ok(res) => Ok(Box::new(res)),
            Err(e) => Err(GearsApiError::Msg(e)),
        }
    }

    fn open_ai_script(&self, name: &str) -> Result<Box<dyn AIScriptInterface>, GearsApiError> {
        let ctx = match &self.user {
            Some(u) => {
                let ctx = &get_globals().authenticated_redis_ctx;
                if ctx.autenticate_user(u) == Status::Err {
                    return Err(GearsApiError::Msg(format!(
                        "Failed authenticate user {}",
                        u
                    )));
                }
                ctx
            }
            None => get_ctx(),
        };
        let res = RedisAIScript::open_from_key(ctx, name);
        match res {
            Ok(res) => Ok(Box::new(res)),
            Err(e) => Err(GearsApiError::Msg(e)),
        }
    }
}
