/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{Context, RedisError, RedisResult, RedisValue, ThreadSafeContext};

use std::iter::Skip;
use std::vec::IntoIter;

use crate::{get_ctx, get_libraries};

use mr_derive::BaseObject;

use mr::libmr::{
    record::Record as LibMRRecord, remote_task::run_on_all_shards,
    remote_task::RemoteTask, RustMRError,
};

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionDelInputRecord {
    lib_name: String,
}

impl LibMRRecord for GearsFunctionDelInputRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Null
    }

    fn hash_slot(&self) -> usize {
        1 // not relevant here
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionDelOutputRecord;

impl LibMRRecord for GearsFunctionDelOutputRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Null
    }

    fn hash_slot(&self) -> usize {
        1 // not relevant here
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsFunctionDelRemoteTask;

impl RemoteTask for GearsFunctionDelRemoteTask {
    type InRecord = GearsFunctionDelInputRecord;
    type OutRecord = GearsFunctionDelOutputRecord;

    fn task(
        self,
        r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>) + Send>,
    ) {
        let _ctx_guard = ThreadSafeContext::new().lock();
        let mut libraries = get_libraries();
        let res = match libraries.remove(&r.lib_name) {
            Some(_) => {
                redis_module::replicate_slices(
                    get_ctx().ctx,
                    "_rg.function",
                    &vec!["del".as_bytes(), r.lib_name.as_bytes()],
                );
                Ok(GearsFunctionDelOutputRecord)
            }
            None => Err("library does not exists".to_string()),
        };
        on_done(res);
    }
}

pub(crate) fn function_del_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let lib_name = args
        .next()
        .map_or(Err(RedisError::Str("function name was not given")), |s| {
            s.try_as_str()
        })?
        .to_string();
    let blocked_client = ctx.block_client();
    run_on_all_shards(
        GearsFunctionDelRemoteTask,
        GearsFunctionDelInputRecord { lib_name },
        |_results: Vec<GearsFunctionDelOutputRecord>, mut errors| {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            if errors.is_empty() {
                thread_ctx.reply(Ok(RedisValue::SimpleStringStatic("OK")));
            } else {
                thread_ctx.reply(Err(RedisError::String(errors.pop().unwrap())));
            }
        },
        10000,
    );
    Ok(RedisValue::NoReply)
}

pub(crate) fn function_del_on_replica(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let lib_name = args
        .next()
        .map_or(Err(RedisError::Str("function name was not given")), |s| {
            s.try_as_str()
        })?;
    let mut libraries = get_libraries();
    match libraries.remove(lib_name) {
        Some(_) => Ok(RedisValue::SimpleStringStatic("OK")),
        None => Err(RedisError::Str("library does not exists")),
    }
}
