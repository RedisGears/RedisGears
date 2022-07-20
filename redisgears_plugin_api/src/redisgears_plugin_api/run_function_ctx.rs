use crate::redisgears_plugin_api::CallResult;
use crate::redisgears_plugin_api::GearsApiError;

pub trait RedisClientCtxInterface: Send + Sync {
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface;
}

pub trait ReplyCtxInterface: Send + Sync {
    fn reply_with_simple_string(&self, val: &str);
    fn reply_with_error(&self, val: &str);
    fn reply_with_long(&self, val: i64);
    fn reply_with_double(&self, val: f64);
    fn reply_with_bulk_string(&self, val: &str);
    fn reply_with_array(&self, size: usize);
    fn as_client(&self) -> &dyn ReplyCtxInterface;
}

pub trait BackgroundRunFunctionCtxInterface: Send + Sync {
    fn lock<'a>(&'a self) -> Result<Box<dyn RedisClientCtxInterface>, GearsApiError>;
}

pub trait RunFunctionCtxInterface: ReplyCtxInterface {
    fn next_arg<'a>(&'a mut self) -> Option<&'a [u8]>;
    fn get_background_client(&self) -> Box<dyn ReplyCtxInterface>;
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
}
