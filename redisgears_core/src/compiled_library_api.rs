/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::execute_on_pool;
use redisai_rs::redisai::redisai_tensor::RedisAITensor;
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::CompiledLibraryInterface;
use redisgears_plugin_api::redisgears_plugin_api::redisai_interface::AITensorInterface;
use redisgears_plugin_api::redisgears_plugin_api::GearsApiError;
use std::collections::LinkedList;
use std::sync::{Arc, Mutex};

pub(crate) struct CompiledLibraryInternals {
    mutex: Mutex<LinkedList<Box<dyn FnOnce() + Send>>>,
}

impl CompiledLibraryInternals {
    fn new() -> CompiledLibraryInternals {
        CompiledLibraryInternals {
            mutex: Mutex::new(LinkedList::new()),
        }
    }

    fn run_next_job(internals: &Arc<CompiledLibraryInternals>) {
        let (job, jobs_left) = {
            let mut queue = internals.mutex.lock().unwrap();
            let job = queue.pop_back();
            match job {
                Some(j) => (j, queue.len()),
                None => return,
            }
        };
        job();
        if jobs_left > 0 {
            let internals_ref = Arc::clone(internals);
            execute_on_pool(move || {
                Self::run_next_job(&internals_ref);
            });
        }
    }

    fn add_job(internals: &Arc<CompiledLibraryInternals>, job: Box<dyn FnOnce() + Send>) {
        let pending_jobs = {
            let mut queue = internals.mutex.lock().unwrap();
            let pending_jobs = queue.len();
            queue.push_front(job);
            pending_jobs
        };
        if pending_jobs == 0 {
            let internals_ref = Arc::clone(internals);
            execute_on_pool(move || {
                Self::run_next_job(&internals_ref);
            });
        }
    }

    pub(crate) fn pending_jobs(&self) -> usize {
        let queue = self.mutex.lock().unwrap();
        queue.len()
    }
}

pub(crate) struct CompiledLibraryAPI {
    internals: Arc<CompiledLibraryInternals>,
}

impl CompiledLibraryAPI {
    pub(crate) fn new() -> CompiledLibraryAPI {
        CompiledLibraryAPI {
            internals: Arc::new(CompiledLibraryInternals::new()),
        }
    }

    fn add_job(&self, job: Box<dyn FnOnce() + Send>) {
        CompiledLibraryInternals::add_job(&self.internals, job);
    }

    pub(crate) fn take_internals(&self) -> Arc<CompiledLibraryInternals> {
        Arc::clone(&self.internals)
    }
}

impl CompiledLibraryInterface for CompiledLibraryAPI {
    fn log_debug(&self, msg: &str) {
        log::debug!("{msg}");
    }

    fn log_info(&self, msg: &str) {
        log::info!("{msg}");
    }

    fn log_trace(&self, msg: &str) {
        log::trace!("{msg}");
    }

    fn log_warning(&self, msg: &str) {
        log::warn!("{msg}");
    }

    fn log_error(&self, msg: &str) {
        log::error!("{msg}");
    }

    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>) {
        self.add_job(job);
    }

    fn redisai_create_tensor(
        &self,
        data_type: &str,
        dims: &[i64],
        data: &[u8],
    ) -> Result<Box<dyn AITensorInterface>, GearsApiError> {
        let mut tensor = RedisAITensor::create(data_type, dims).map_err(GearsApiError::new)?;
        Ok(tensor
            .set_data(data)
            .map(|_| Box::new(tensor))
            .map_err(GearsApiError::new)?)
    }
}
