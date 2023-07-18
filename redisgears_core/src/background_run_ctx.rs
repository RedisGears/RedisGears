/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;
use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::RemoteFunctionData, GearsApiError,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;
use crate::run_ctx::RedisClientCallOptions;
use crate::{
    get_libraries, verify_ok_on_replica, verify_oom, Deserialize, GearsLibraryMetaData, Serialize,
};

use redis_module::{RedisString, RedisValue};

use std::sync::atomic::Ordering;
use std::sync::Arc;

use mr::libmr::{calc_slot, record::Record, remote_task::RemoteTask, RustMRError};

use mr_derive::BaseObject;

use crate::config::REMOTE_TASK_DEFAULT_TIMEOUT;

pub(crate) struct BackgroundRunCtx {
    call_options: RedisClientCallOptions,
    lib_meta_data: Arc<GearsLibraryMetaData>,
    user: RedisString,
}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunCtx {
    pub(crate) fn new(
        user: RedisString,
        lib_meta_data: &Arc<GearsLibraryMetaData>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunCtx {
        BackgroundRunCtx {
            user,
            lib_meta_data: Arc::clone(lib_meta_data),
            call_options,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsRemoteFunctionInputsRecord {
    inputs: Vec<RemoteFunctionData>,
}

impl Record for GearsRemoteFunctionInputsRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::Array(
            self.inputs
                .iter()
                .map(|v| {
                    let buff = match v {
                        RemoteFunctionData::Binary(b) => b,
                        RemoteFunctionData::String(s) => s.as_bytes(),
                    };
                    RedisValue::StringBuffer(buff.to_vec())
                })
                .collect(),
        )
    }

    fn hash_slot(&self) -> usize {
        1 // not relevant here
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsRemoteFunctionOutputRecord {
    output: RemoteFunctionData,
}

impl Record for GearsRemoteFunctionOutputRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        let buff = match &self.output {
            RemoteFunctionData::Binary(b) => b,
            RemoteFunctionData::String(s) => s.as_bytes(),
        };
        RedisValue::StringBuffer(buff.to_vec())
    }

    fn hash_slot(&self) -> usize {
        let buff = match &self.output {
            RemoteFunctionData::Binary(b) => b,
            RemoteFunctionData::String(s) => s.as_bytes(),
        };
        calc_slot(buff)
    }
}

#[derive(Clone, Serialize, Deserialize, BaseObject)]
pub(crate) struct GearsRemoteTask {
    lib_name: String,
    job_name: String,
    user: RedisString,
}

impl RemoteTask for GearsRemoteTask {
    type InRecord = GearsRemoteFunctionInputsRecord;
    type OutRecord = GearsRemoteFunctionOutputRecord;

    fn task(
        self,
        r: Self::InRecord,
        on_done: Box<dyn FnOnce(Result<Self::OutRecord, RustMRError>) + Send>,
    ) {
        let library = {
            let libraries = get_libraries();
            let library = libraries.get(&self.lib_name);
            if library.is_none() {
                on_done(Err(format!(
                    "Library {} does not exists on remote shard",
                    self.lib_name
                )));
                return;
            }
            Arc::clone(library.unwrap()) // make sure the library will not be free while in use
        };
        let remote_function = library.gears_lib_ctx.remote_functions.get(&self.job_name);
        if remote_function.is_none() {
            on_done(Err(format!(
                "Remote function {} does not exists on library {}",
                self.job_name, self.lib_name
            )));
            return;
        }
        let remote_function = remote_function.unwrap();
        remote_function(
            r.inputs,
            Box::new(BackgroundRunCtx::new(
                self.user,
                &library.gears_lib_ctx.meta_data,
                RedisClientCallOptions::new(FunctionFlags::NO_WRITES),
            )),
            Box::new(move |result| {
                let res = match result {
                    Ok(r) => Ok(GearsRemoteFunctionOutputRecord { output: r }),
                    Err(e) => Err(e.get_msg().to_string()),
                };
                on_done(res);
            }),
        );
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock(&self) -> Result<Box<dyn RedisClientCtxInterface>, GearsApiError> {
        let detached_ctx_guard = redis_module::MODULE_CONTEXT.lock();
        if !verify_ok_on_replica(&detached_ctx_guard, self.call_options.flags) {
            return Err(GearsApiError::new(
                "Can not lock redis for write on replica or when avoid replication traffic is requested".to_string(),
            ));
        }
        if !verify_oom(&detached_ctx_guard, self.call_options.flags) {
            return Err(GearsApiError::new(
                "OOM Can not lock redis for write".to_string(),
            ));
        }
        let user = self.user.safe_clone(&detached_ctx_guard);
        Ok(Box::new(BackgroundRunScopeGuardCtx::new(
            detached_ctx_guard,
            user,
            &self.lib_meta_data,
            self.call_options.clone(),
        )))
    }

    fn run_on_key(
        &self,
        key: &[u8],
        job_name: &str,
        inputs: Vec<RemoteFunctionData>,
        on_done: Box<dyn FnOnce(Result<RemoteFunctionData, GearsApiError>)>,
    ) {
        let task = GearsRemoteTask {
            lib_name: self.lib_meta_data.name.clone(),
            job_name: job_name.to_string(),
            user: self.user.clone(),
        };
        let input_record = GearsRemoteFunctionInputsRecord { inputs };
        mr::libmr::remote_task::run_on_key(
            key,
            task,
            input_record,
            move |result: Result<GearsRemoteFunctionOutputRecord, RustMRError>| {
                let res = match result {
                    Ok(r) => Ok(r.output),
                    Err(e) => Err(GearsApiError::new(e)),
                };
                on_done(res);
            },
            REMOTE_TASK_DEFAULT_TIMEOUT.load(Ordering::Relaxed) as usize,
        );
    }

    fn run_on_all_shards(
        &self,
        job_name: &str,
        inputs: Vec<RemoteFunctionData>,
        on_done: Box<dyn FnOnce(Vec<RemoteFunctionData>, Vec<GearsApiError>)>,
    ) {
        let task = GearsRemoteTask {
            lib_name: self.lib_meta_data.name.clone(),
            job_name: job_name.to_string(),
            user: self.user.clone(),
        };
        let input_record = GearsRemoteFunctionInputsRecord { inputs };
        mr::libmr::remote_task::run_on_all_shards(
            task,
            input_record,
            move |results: Vec<GearsRemoteFunctionOutputRecord>, errors| {
                let errors: Vec<GearsApiError> =
                    errors.into_iter().map(GearsApiError::new).collect();
                let results: Vec<RemoteFunctionData> =
                    results.into_iter().map(|r| r.output).collect();
                on_done(results, errors);
            },
            REMOTE_TASK_DEFAULT_TIMEOUT.load(Ordering::Relaxed) as usize,
        )
    }
}
