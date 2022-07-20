use redis_module::context::thread_safe::ContextGuard;

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    CallResult,
};

use crate::run_ctx::RedisClientCallOptions;
use crate::{
    background_run_ctx::BackgroundRunCtx, call_redis_command, get_notification_blocker,
    NotificationBlocker,
};

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) _ctx_guard: ContextGuard,
    call_options: RedisClientCallOptions,
    user: Option<String>,
    _notification_blocker: NotificationBlocker,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl BackgroundRunScopeGuardCtx {
    pub(crate) fn new(
        ctx_guard: ContextGuard,
        user: Option<String>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunScopeGuardCtx {
        BackgroundRunScopeGuardCtx {
            _ctx_guard: ctx_guard,
            call_options: call_options,
            user: user,
            _notification_blocker: get_notification_blocker(),
        }
    }
}

impl RedisClientCtxInterface for BackgroundRunScopeGuardCtx {
    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        call_redis_command(
            self.user.as_ref(),
            command,
            &self.call_options.call_options,
            args,
        )
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.user.clone(),
            self.call_options.clone(),
        ))
    }

    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface {
        self
    }
}
