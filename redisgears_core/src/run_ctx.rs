use redis_module::{
    context::{CallOptions, CallOptionsBuilder},
    Context, ThreadSafeContext,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::FUNCTION_FLAG_NO_WRITES, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisClientCtxInterface, run_function_ctx::ReplyCtxInterface,
    run_function_ctx::RunFunctionCtxInterface, CallResult, GearsApiError,
};

use crate::{call_redis_command, get_globals, GearsLibraryMataData};

use std::slice::Iter;

use crate::background_run_ctx::BackgroundRunCtx;

use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct RedisClientCallOptions {
    pub(crate) call_options: CallOptions,
    pub(crate) flags: u8,
}

impl RedisClientCallOptions {
    pub(crate) fn new(flags: u8) -> RedisClientCallOptions {
        let call_options = CallOptionsBuilder::new()
            .replicate()
            .verify_acl()
            .errors_as_replies()
            .resp_3();
        let call_options = if !get_globals().allow_unsafe_redis_commands {
            call_options.script_mode()
        } else {
            call_options
        };
        let call_options = if flags & FUNCTION_FLAG_NO_WRITES != 0 {
            call_options.no_writes()
        } else {
            call_options
        };

        RedisClientCallOptions {
            call_options: call_options.constract(),
            flags: flags,
        }
    }
}

pub(crate) struct RedisClient {
    call_options: RedisClientCallOptions,
    lib_meta_data: Arc<GearsLibraryMataData>,
    user: Option<String>,
}

unsafe impl Sync for RedisClient {}
unsafe impl Send for RedisClient {}

impl RedisClient {
    pub(crate) fn new(lib_meta_data: &Arc<GearsLibraryMataData>, user: Option<String>, flags: u8) -> RedisClient {
        RedisClient {
            call_options: RedisClientCallOptions::new(flags),
            lib_meta_data: Arc::clone(lib_meta_data),
            user: user,
        }
    }
}

impl RedisClientCtxInterface for RedisClient {
    fn call(&self, command: &str, args: &[&[u8]]) -> CallResult {
        let user = match self.user.as_ref() {
            Some(u) => Some(u),
            None => Some(&self.lib_meta_data.user),
        };
        call_redis_command(
            user,
            command,
            &self.call_options.call_options,
            args,
        )
    }

    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface {
        self
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.user.clone(),
            &self.lib_meta_data,
            self.call_options.clone(),
        ))
    }
}

pub(crate) struct RunCtx<'a> {
    pub(crate) ctx: &'a Context,
    pub(crate) iter: Iter<'a, redis_module::RedisString>,
    pub(crate) flags: u8,
    pub(crate) lib_meta_data: Arc<GearsLibraryMataData>
}

impl<'a> ReplyCtxInterface for RunCtx<'a> {
    fn reply_with_simple_string(&self, val: &str) {
        self.ctx.reply_simple_string(val);
    }

    fn reply_with_error(&self, val: &str) {
        self.ctx.reply_error_string(val);
    }

    fn reply_with_long(&self, val: i64) {
        self.ctx.reply_long(val);
    }

    fn reply_with_double(&self, val: f64) {
        self.ctx.reply_double(val);
    }

    fn reply_with_bulk_string(&self, val: &str) {
        self.ctx.reply_bulk_string(val);
    }

    fn reply_with_array(&self, size: usize) {
        self.ctx.reply_array(size);
    }

    fn reply_with_slice(&self, val: &[u8]) {
        self.ctx.reply_bulk_slice(val);
    }

    fn reply_with_null(&self) {
        self.ctx.reply_null();
    }

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}

unsafe impl<'a> Sync for RunCtx<'a> {}
unsafe impl<'a> Send for RunCtx<'a> {}

impl<'a> RunFunctionCtxInterface for RunCtx<'a> {
    fn next_arg(&mut self) -> Option<&[u8]> {
        Some(self.iter.next()?.as_slice())
    }

    fn get_background_client(&self) -> Result<Box<dyn ReplyCtxInterface>, GearsApiError> {
        if !self.allow_block() {
            return Err(GearsApiError::Msg(
                "Blocking is not allow inside multi/exec, Lua, or within another module (RM_Call)"
                    .to_string(),
            ));
        }
        let blocked_client = self.ctx.block_client();
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let ctx = thread_ctx.get_ctx();
        Ok(Box::new(BackgroundClientCtx {
            _thread_ctx: thread_ctx,
            ctx: ctx,
        }))
    }

    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        let user = match self.ctx.get_current_user() {
            Ok(u) => Some(u),
            Err(_) => None,
        };
        Box::new(RedisClient::new(&self.lib_meta_data, user, self.flags))
    }

    fn allow_block(&self) -> bool {
        self.ctx.allow_block()
    }
}

pub(crate) struct BackgroundClientCtx {
    _thread_ctx: ThreadSafeContext<redis_module::BlockedClient>,
    ctx: Context,
}

unsafe impl Sync for BackgroundClientCtx {}
unsafe impl Send for BackgroundClientCtx {}

impl ReplyCtxInterface for BackgroundClientCtx {
    fn reply_with_simple_string(&self, val: &str) {
        self.ctx.reply_simple_string(val);
    }

    fn reply_with_error(&self, val: &str) {
        self.ctx.reply_error_string(val);
    }

    fn reply_with_long(&self, val: i64) {
        self.ctx.reply_long(val);
    }

    fn reply_with_double(&self, val: f64) {
        self.ctx.reply_double(val);
    }

    fn reply_with_bulk_string(&self, val: &str) {
        self.ctx.reply_bulk_string(val);
    }

    fn reply_with_array(&self, size: usize) {
        self.ctx.reply_array(size);
    }

    fn reply_with_slice(&self, val: &[u8]) {
        self.ctx.reply_bulk_slice(val);
    }

    fn reply_with_null(&self) {
        self.ctx.reply_null();
    }

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}
