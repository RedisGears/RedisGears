/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{
    CallResult, Context, ContextFlags, RedisError, RedisResult, RedisString, ThreadSafeContext,
    {BlockingCallOptions, CallOptionResp, CallOptions, CallOptionsBuilder},
};

use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::FunctionFlags,
    redisai_interface::AIModelInterface,
    redisai_interface::AIScriptInterface,
    run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::ReplyCtxInterface,
    run_function_ctx::RunFunctionCtxInterface,
    run_function_ctx::{BackgroundRunFunctionCtxInterface, PromiseReply},
    GearsApiError,
};

use crate::{
    call_redis_command, call_redis_command_async, get_globals, get_msg_verbose,
    GearsLibraryMetaData,
};

use crate::background_run_ctx::BackgroundRunCtx;

use std::sync::Arc;

use redisai_rs::redisai::redisai_model::RedisAIModel;
use redisai_rs::redisai::redisai_script::RedisAIScript;

#[derive(Clone)]
pub(crate) struct RedisClientCallOptions {
    pub(crate) call_options: CallOptions,
    pub(crate) blocking_call_options: BlockingCallOptions,
    pub(crate) flags: FunctionFlags,
}

impl RedisClientCallOptions {
    fn get_builder(flags: FunctionFlags) -> CallOptionsBuilder {
        let call_options = CallOptionsBuilder::new()
            .replicate()
            .verify_acl()
            .errors_as_replies()
            .resp(CallOptionResp::Resp3);
        let call_options = if !get_globals().allow_unsafe_redis_commands {
            call_options.script_mode()
        } else {
            call_options
        };
        if flags.contains(FunctionFlags::NO_WRITES) {
            call_options.no_writes()
        } else {
            call_options
        }
    }
    pub(crate) fn new(flags: FunctionFlags) -> RedisClientCallOptions {
        RedisClientCallOptions {
            call_options: Self::get_builder(flags).build(),
            blocking_call_options: Self::get_builder(flags).build_blocking(),
            flags,
        }
    }
}

pub(crate) struct RedisClient<'ctx> {
    ctx: &'ctx Context,
    call_options: RedisClientCallOptions,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    user: RedisString,
}

unsafe impl<'ctx> Sync for RedisClient<'ctx> {}
unsafe impl<'ctx> Send for RedisClient<'ctx> {}

impl<'ctx> RedisClient<'ctx> {
    pub(crate) fn new(
        ctx: &'ctx Context,
        lib_meta_data: Arc<GearsLibraryMetaData>,
        user: RedisString,
        flags: FunctionFlags,
    ) -> RedisClient {
        RedisClient {
            ctx,
            call_options: RedisClientCallOptions::new(flags),
            lib_meta_data,
            user,
        }
    }
}

impl<'ctx> RedisClientCtxInterface for RedisClient<'ctx> {
    fn call(&self, command: &str, args: &[&[u8]]) -> CallResult {
        call_redis_command(
            self.ctx,
            &self.user,
            command,
            &self.call_options.call_options,
            args,
        )
    }

    fn call_async(&self, command: &str, args: &[&[u8]]) -> PromiseReply<'static, '_> {
        call_redis_command_async(
            self.ctx,
            &self.lib_meta_data.name,
            &self.user,
            command,
            &self.call_options.blocking_call_options,
            args,
        )
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(
            self.user.safe_clone(self.ctx),
            &self.lib_meta_data,
            self.call_options.clone(),
        ))
    }

    fn open_ai_model(&self, name: &str) -> Result<Box<dyn AIModelInterface>, GearsApiError> {
        let _authenticate_scope = self
            .ctx
            .authenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        RedisAIModel::open_from_key(self.ctx, name)
            .map(|v| Box::new(v) as Box<dyn AIModelInterface>)
            .map_err(GearsApiError::new)
    }

    fn open_ai_script(&self, name: &str) -> Result<Box<dyn AIScriptInterface>, GearsApiError> {
        let _authenticate_scope = self
            .ctx
            .authenticate_user(&self.user)
            .map_err(|e| GearsApiError::new(e.to_string()))?;
        RedisAIScript::open_from_key(self.ctx, name)
            .map(|v| Box::new(v) as Box<dyn AIScriptInterface>)
            .map_err(GearsApiError::new)
    }
}

pub(crate) struct RunCtx<'a> {
    pub(crate) ctx: &'a Context,
    pub(crate) args: Vec<redis_module::RedisString>,
    pub(crate) flags: FunctionFlags,
    pub(crate) lib_meta_data: Arc<GearsLibraryMetaData>,
    pub(crate) allow_block: bool,
}

impl<'a> ReplyCtxInterface for RunCtx<'a> {
    fn send_reply(&self, reply: RedisResult) {
        self.ctx.reply(reply);
    }

    fn reply_with_error(&self, val: GearsApiError) {
        self.ctx.reply_error_string(get_msg_verbose(&val));
    }

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}

unsafe impl<'a> Sync for RunCtx<'a> {}
unsafe impl<'a> Send for RunCtx<'a> {}

impl<'a> RunFunctionCtxInterface for RunCtx<'a> {
    fn get_args_iter(&self) -> Box<dyn Iterator<Item = &[u8]> + '_> {
        Box::new(self.args.iter().map(|v| v.as_slice()))
    }

    fn get_background_client(&self) -> Result<Box<dyn ReplyCtxInterface>, GearsApiError> {
        if !self.allow_block() {
            return Err(GearsApiError::new(
                "Blocking is not allow inside multi/exec, Lua, or within another module (RM_Call)"
                    .to_string(),
            ));
        }
        let blocked_client = self.ctx.block_client();
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        Ok(Box::new(BackgroundClientCtx { thread_ctx }))
    }

    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface + '_> {
        Box::new(RedisClient::new(
            self.ctx,
            self.lib_meta_data.clone(),
            self.ctx.get_current_user(),
            self.flags,
        ))
    }

    fn allow_block(&self) -> bool {
        self.allow_block && !self.ctx.get_flags().contains(ContextFlags::DENY_BLOCKING)
    }
}

pub(crate) struct BackgroundClientCtx {
    thread_ctx: ThreadSafeContext<redis_module::BlockedClient>,
}

unsafe impl Sync for BackgroundClientCtx {}
unsafe impl Send for BackgroundClientCtx {}

impl ReplyCtxInterface for BackgroundClientCtx {
    fn send_reply(&self, reply: RedisResult) {
        self.thread_ctx.reply(reply);
    }

    fn reply_with_error(&self, val: GearsApiError) {
        self.thread_ctx
            .reply(Err(RedisError::String(get_msg_verbose(&val).into())));
    }

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}
