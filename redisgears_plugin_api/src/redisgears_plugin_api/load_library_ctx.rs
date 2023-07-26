/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::collections::HashMap;

use crate::redisgears_plugin_api::function_ctx::FunctionCtxInterface;
use crate::redisgears_plugin_api::keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RemoteFunctionData;
use crate::redisgears_plugin_api::stream_ctx::StreamCtxInterface;
use crate::redisgears_plugin_api::GearsApiError;

pub const FUNCTION_FLAG_NO_WRITES_GLOBAL_NAME: &str = "NO_WRITES";
pub const FUNCTION_FLAG_NO_WRITES_GLOBAL_VALUE: &str = "no-writes";

pub const FUNCTION_FLAG_ALLOW_OOM_GLOBAL_NAME: &str = "ALLOW_OOM";
pub const FUNCTION_FLAG_ALLOW_OOM_GLOBAL_VALUE: &str = "allow-oom";

pub const FUNCTION_FLAG_RAW_ARGUMENTS_GLOBAL_NAME: &str = "RAW_ARGUMENTS";
pub const FUNCTION_FLAG_RAW_ARGUMENTS_GLOBAL_VALUE: &str = "raw-arguments";

/// The type of information we can get from a backend that is useful to
/// a user.
#[derive(Debug, Clone)]
pub enum InfoSectionData {
    /// Simple key-value pairs.
    KeyValuePairs(HashMap<String, String>),
    /// Dictionaries have a name, and then under the name they have
    /// key-value pairs.
    Dictionaries(HashMap<String, HashMap<String, String>>),
}

/// The backend information split into sections.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ModuleInfo {
    /// A section always has a name and a set of values.
    pub sections: HashMap<String, InfoSectionData>,
}

pub trait LibraryCtxInterface {
    fn load_library(
        &self,
        load_library_ctx: &dyn LoadLibraryCtxInterface,
        is_being_loaded_from_rdb: bool,
    ) -> Result<(), GearsApiError>;

    fn get_info(&self) -> Option<ModuleInfo>;
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
        description: Option<String>,
    ) -> Result<(), GearsApiError>;
    fn register_async_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: FunctionFlags,
        description: Option<String>,
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
        description: Option<String>,
    ) -> Result<(), GearsApiError>;
    fn register_key_space_notification_consumer(
        &mut self,
        name: &str,
        key: RegisteredKeys,
        keys_notifications_consumer_ctx: Box<dyn KeysNotificationsConsumerCtxInterface>,
        description: Option<String>,
    ) -> Result<(), GearsApiError>;
}
