use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    GearsApiError,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;
use crate::run_ctx::RedisClientCallOptions;
use crate::{get_libraries, verify_ok_on_replica, verify_oom, GearsLibraryMataData};

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
            user: user,
            lib_meta_data: Arc::clone(lib_meta_data),
            call_options: call_options,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GearsBufferRecord {
    pub buff: Vec<u8>,
}

impl Record for GearsBufferRecord {
    fn to_redis_value(&mut self) -> RedisValue {
        RedisValue::StringBuffer(self.buff.clone())
    }

    fn hash_slot(&self) -> usize {
        calc_slot(&self.buff)
    }
}

impl BaseObject for GearsBufferRecord {
    fn get_name() -> &'static str {
        "GearsBufferRecord\0"
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct GearsRemoteTask {
    lib_name: String,
    job_name: String,
}

impl RemoteTask for GearsRemoteTask {
    type InRecord = GearsBufferRecord;
    type OutRecord = GearsBufferRecord;

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
            r.buff,
            Box::new(move |result| {
                let res = match result {
                    Ok(r) => Ok(GearsBufferRecord { buff: r }),
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
        input: &[u8],
        on_done: Box<dyn FnOnce(Result<Vec<u8>, GearsApiError>)>,
    ) {
        let task = GearsRemoteTask {
            lib_name: self.lib_meta_data.name.clone(),
            job_name: job_name.to_string(),
        };
        let input_record = GearsBufferRecord {
            buff: input.into_iter().map(|v| *v).collect(),
        };
        mr::libmr::remote_task::run_on_key(
            key,
            task,
            input_record,
            move |result: Result<GearsBufferRecord, RustMRError>| {
                let res = match result {
                    Ok(r) => Ok(r.buff),
                    Err(e) => Err(GearsApiError::Msg(e)),
                };
                on_done(res);
            },
        );
    }
}
