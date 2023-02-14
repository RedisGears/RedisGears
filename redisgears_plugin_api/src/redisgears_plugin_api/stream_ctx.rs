/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RedisClientCtxInterface;

use super::GearsApiError;

pub trait StreamProcessCtxInterface {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
}

pub trait StreamRecordInterface {
    fn get_id(&self) -> (u64, u64);
    fn fields<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a>;
}

pub enum StreamRecordAck {
    Ack,
    Nack(GearsApiError),
}

pub trait StreamCtxInterface {
    fn process_record(
        &self,
        stream_name: &[u8],
        record: Box<dyn StreamRecordInterface + Send>,
        run_ctx: &dyn StreamProcessCtxInterface,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) -> Option<StreamRecordAck>;
}
