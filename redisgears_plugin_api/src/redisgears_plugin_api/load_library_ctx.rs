use crate::redisgears_plugin_api::function_ctx::FunctionCtxInterface;
use crate::redisgears_plugin_api::keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface;
use crate::redisgears_plugin_api::stream_ctx::StreamCtxInterface;
use crate::redisgears_plugin_api::GearsApiError;

pub trait LibraryCtxInterface {
    fn load_library(
        &self,
        load_library_ctx: &mut dyn LoadLibraryCtxInterface,
    ) -> Result<(), GearsApiError>;
}

pub enum RegisteredKeys<'a> {
    Key(&'a [u8]),
    Prefix(&'a [u8]),
}

pub const FUNCTION_FLAG_NO_WRITES: u8 = 0x01;
pub const FUNCTION_FLAG_ALLOW_OOM: u8 = 0x02;
pub const FUNCTION_FLAG_RAW_ARGUMENTS: u8 = 0x04;

pub trait LoadLibraryCtxInterface {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
        flags: u8,
    ) -> Result<(), GearsApiError>;
    fn register_remote_task(
        &mut self,
        name: &str,
        remote_function_ctx: Box<dyn Fn(Vec<u8>, Box<dyn FnOnce(Result<Vec<u8>, GearsApiError>) + Send>)>,
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
