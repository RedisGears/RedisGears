/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisgears_plugin_api::function_ctx::FunctionCtxInterface;
use crate::redisgears_plugin_api::keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RemoteFunctionData;
use crate::redisgears_plugin_api::stream_ctx::StreamCtxInterface;
use crate::redisgears_plugin_api::GearsApiError;

pub trait LibraryCtxInterface {
    fn load_library(
        &self,
        load_library_ctx: &dyn LoadLibraryCtxInterface,
    ) -> Result<(), GearsApiError>;
}

pub enum RegisteredKeys<'a> {
    Key(&'a [u8]),
    Prefix(&'a [u8]),
}

bitflags::bitflags! {
    /// The flags a function might have related to the way it is
    /// executed.
    #[derive(Default)]
    pub struct FunctionFlags: u8 {
        /// The function is not performing writes to the database.
        const NO_WRITES = 0x01;
        /// TODO
        const ALLOW_OOM = 0x02;
        /// TODO
        const RAW_ARGUMENTS = 0x04;
    }
}

pub type RemoteFunctionCtx = Box<
    dyn Fn(
        Vec<RemoteFunctionData>,
        Box<dyn BackgroundRunFunctionCtxInterface>,
        Box<dyn FnOnce(Result<RemoteFunctionData, GearsApiError>) + Send>,
    ),
>;

pub trait LoadLibraryCtxInterface {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
    ) -> Result<(), GearsApiError>;
    fn register_remote_task(
        &mut self,
        name: &str,
        remote_function_ctx: RemoteFunctionCtx,
    ) -> Result<(), GearsApiError>;
    fn register_stream_consumer(
        &mut self,
        name: &str,
        prefix: &[u8],
        stream_ctx: Box<dyn StreamCtxInterface>,
        window: usize,
        trim: bool,
    ) -> Result<(), GearsApiError>;
    fn register_key_space_notification_consumer(
        &mut self,
        name: &str,
        key: RegisteredKeys,
        keys_notifications_consumer_ctx: Box<dyn KeysNotificationsConsumerCtxInterface>,
    ) -> Result<(), GearsApiError>;
}
