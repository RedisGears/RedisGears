use crate::{execute_on_pool, get_ctx, get_globals};
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::CompiledLibraryInterface;
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
        let pending_jons = {
            let mut queue = internals.mutex.lock().unwrap();
            let pending_jons = queue.len();
            queue.push_front(job);
            pending_jons
        };
        if pending_jons == 0 {
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
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>) {
        self.add_job(job);
    }

    fn get_maxmemory(&self) -> usize {
        get_globals().config.library_maxmemory.size as usize
    }
}
