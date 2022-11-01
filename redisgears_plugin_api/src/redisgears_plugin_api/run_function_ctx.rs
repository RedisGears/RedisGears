use crate::redisgears_plugin_api::CallResult;
use crate::redisgears_plugin_api::GearsApiError;

pub trait RedisClientCtxInterface: Send + Sync {
    fn call(&self, command: &str, args: &[&[u8]]) -> CallResult;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface;
}

pub trait ReplyCtxInterface: Send + Sync {
    fn reply_with_simple_string(&self, val: &str);
    fn reply_with_error(&self, val: &str);
    fn reply_with_long(&self, val: i64);
    fn reply_with_double(&self, val: f64);
    fn reply_with_bulk_string(&self, val: &str);
    fn reply_with_slice(&self, val: &[u8]);
    fn reply_with_array(&self, size: usize);
    fn reply_with_null(&self);
    fn as_client(&self) -> &dyn ReplyCtxInterface;
}

#[derive(Clone, Serialize, Deserialize)]
pub enum RemoteFunctionData {
    Binary(Vec<u8>),
    String(String),
}

pub trait BackgroundRunFunctionCtxInterface: Send + Sync {
    fn lock(&self) -> Result<Box<dyn RedisClientCtxInterface>, GearsApiError>;
    fn run_on_key(
        &self,
        key: &[u8],
        job_name: &str,
        inputs: Vec<RemoteFunctionData>,
        on_done: Box<dyn FnOnce(Result<RemoteFunctionData, GearsApiError>)>,
    );
    fn run_on_all_shards(
        &self,
        job_name: &str,
        inputs: Vec<RemoteFunctionData>,
        on_done: Box<dyn FnOnce(Vec<RemoteFunctionData>, Vec<GearsApiError>)>,
    );
}

pub trait RunFunctionCtxInterface: ReplyCtxInterface {
    fn next_arg(&mut self) -> Option<&[u8]>;
    fn get_background_client(&self) -> Result<Box<dyn ReplyCtxInterface>, GearsApiError>;
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
    fn allow_block(&self) -> bool;
}
