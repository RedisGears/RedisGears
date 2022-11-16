/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use crate::redisgears_plugin_api::redisai_interface::AITensorInterface;
use crate::redisgears_plugin_api::CallResult;
use crate::redisgears_plugin_api::GearsApiError;
use std::alloc::GlobalAlloc;

pub trait CompiledLibraryInterface {
    fn log(&self, msg: &str);
    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>);
    fn get_maxmemory(&self) -> usize;
    fn redisai_create_tensor(
        &self,
        data_type: &str,
        dims: &[i64],
        data: &[u8],
    ) -> Result<Box<dyn AITensorInterface>, GearsApiError>;
}

#[derive(Clone)]
pub enum LibraryFatalFailurePolicy {
    Abort = 0,
    Kill = 1,
}

pub struct BackendCtx {
    pub allocator: &'static dyn GlobalAlloc,
    pub log: Box<dyn Fn(&str) + 'static>,
    pub get_on_oom_policy: Box<dyn Fn() -> LibraryFatalFailurePolicy + 'static>,
    pub get_lock_timeout: Box<dyn Fn() -> u128 + 'static>,
}

pub trait BackendCtxInterface {
    fn get_name(&self) -> &'static str;
    fn initialize(&self, backend_ctx: BackendCtx) -> Result<(), GearsApiError>;
    fn compile_library(
        &mut self,
        code: &str,
        config: Option<&String>,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError>;
    fn debug(&mut self, args: &[&str]) -> Result<CallResult, GearsApiError>;
}
