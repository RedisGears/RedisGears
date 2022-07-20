use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    GearsApiError,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;
use crate::run_ctx::RedisClientCallOptions;
use crate::{verify_ok_on_replica, verify_oom};

use redis_module::ThreadSafeContext;

pub(crate) struct BackgroundRunCtx {
    call_options: RedisClientCallOptions,
    user: Option<String>,
}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunCtx {
    pub(crate) fn new(
        user: Option<String>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunCtx {
        BackgroundRunCtx {
            user: user,
            call_options: call_options,
        }
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Result<Box<dyn RedisClientCtxInterface>, GearsApiError> {
        let ctx_guard = ThreadSafeContext::new().lock();
        if !verify_ok_on_replica(self.call_options.flags) {
            return Err(GearsApiError::Msg(
                "Can not lock redis for write on replica".to_string(),
            ));
        }
        if !verify_oom(self.call_options.flags) {
            return Err(GearsApiError::Msg(
                "OOM Can not lock redis for write".to_string(),
            ));
        }
        Ok(Box::new(BackgroundRunScopeGuardCtx::new(
            ctx_guard,
            self.user.clone(),
            self.call_options.clone(),
        )))
    }
}
