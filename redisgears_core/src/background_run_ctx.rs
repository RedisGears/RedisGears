/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::FUNCTION_FLAG_NO_WRITES, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisClientCtxInterface, run_function_ctx::RemoteFunctionData, GearsApiError,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;
use crate::run_ctx::RedisClientCallOptions;
use crate::{get_globals, get_libraries, verify_ok_on_replica, verify_oom, GearsLibraryMataData};

use redis_module::{RedisValue, ThreadSafeContext};

use std::sync::Arc;

use mr::libmr::{
    base_object::BaseObject, calc_slot, record::Record, remote_task::RemoteTask, RustMRError,
};

pub(crate) struct BackgroundRunCtx {
    call_options: RedisClientCallOptions,
    lib_meta_data: Arc<GearsLibraryMataData>,
    user: Option<String>,
}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunCtx {
    pub(crate) fn new(
        user: Option<String>,
        lib_meta_data: &Arc<GearsLibraryMataData>,
        call_options: RedisClientCallOptions,
    ) -> BackgroundRunCtx {
        BackgroundRunCtx {
            user,
            lib_meta_data: Arc::clone(lib_meta_data),
            call_options,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

impl BaseObject for GearsRemoteFunctionInputsRecord {
    fn get_name() -> &'static str {
        "GearsRemoteFunctionInputsRecord\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

impl BaseObject for GearsRemoteFunctionOutputRecord {
    fn get_name() -> &'static str {
        "GearsRemoteFunctionOutputRecord\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GearsRemoteTask {
    lib_name: String,
    job_name: String,
    user: Option<String>,
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
                RedisClientCallOptions::new(FUNCTION_FLAG_NO_WRITES),
            )),
            Box::new(move |result| {
                let res = match result {
                    Ok(r) => Ok(GearsRemoteFunctionOutputRecord { output: r }),
                    Err(e) => match e {
                        GearsApiError::Msg(msg) => Err(msg),
                    },
                };
                on_done(res);
            }),
        );
    }
}

impl BaseObject for GearsRemoteTask {
    fn get_name() -> &'static str {
        "GearsRemoteTask\0"
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Result<Box<dyn RedisClientCtxInterface>, GearsApiError> {
        let ctx_guard = ThreadSafeContext::new().lock();
        if !verify_ok_on_replica(self.call_options.flags) {
            return Err(GearsApiError::Msg(
                "Can not lock redis for write on replica".to_string(),
            ));
        }
        if !verify_oom(self.call_options.flags) {
            return Err(GearsApiError::Msg(
                "OOM Can not lock redis for write".to_string(),
            ));
        }
        Ok(Box::new(BackgroundRunScopeGuardCtx::new(
            ctx_guard,
            self.user.clone(),
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
                    Err(e) => Err(GearsApiError::Msg(e)),
                };
                on_done(res);
            },
            get_globals().config.remote_task_default_timeout.timeout,
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
                    errors.into_iter().map(GearsApiError::Msg).collect();
                let results: Vec<RemoteFunctionData> =
                    results.into_iter().map(|r| r.output).collect();
                on_done(results, errors);
            },
            get_globals().config.remote_task_default_timeout.timeout,
        )
    }
}
