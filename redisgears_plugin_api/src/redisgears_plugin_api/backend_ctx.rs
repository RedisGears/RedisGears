/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::alloc::GlobalAlloc;
use std::collections::HashMap;

use redis_module::RedisValue;

use crate::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use crate::redisgears_plugin_api::redisai_interface::AITensorInterface;
use crate::redisgears_plugin_api::GearsApiError;

use super::prologue::ApiVersion;

/// The type of information we can get from a backend that is useful to
/// a user.
#[derive(Debug, Clone)]
pub enum InfoSectionData {
    /// Simple key-value pairs.
    KeyValuePairs(HashMap<String, String>),
    /// Dictionaries have a name, and then under the name they have
    /// key-value pairs.
    Dictionaries(HashMap<String, HashMap<String, String>>),
}

/// The backend information split into sections.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct BackendInfo {
    /// A section always has a name and a set of values.
    pub sections: HashMap<String, InfoSectionData>,
}

pub trait CompiledLibraryInterface {
    fn log_debug(&self, msg: &str);
    fn log_info(&self, msg: &str);
    fn log_trace(&self, msg: &str);
    fn log_warning(&self, msg: &str);
    fn log_error(&self, msg: &str);
    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>);
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
    pub log_info: Box<dyn Fn(&str) + 'static>,
    pub log_debug: Box<dyn Fn(&str) + 'static>,
    pub log_trace: Box<dyn Fn(&str) + 'static>,
    pub log_warning: Box<dyn Fn(&str) + 'static>,
    pub log_error: Box<dyn Fn(&str) + 'static>,
    pub get_on_oom_policy: Box<dyn Fn() -> LibraryFatalFailurePolicy + 'static>,
    pub get_lock_timeout: Box<dyn Fn() -> u128 + 'static>,
    pub get_v8_maxmemory: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_initial_memory: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_initial_memory_limit: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_memory_delta: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_flags: Box<dyn Fn() -> String + 'static>,
}

/// The trait which is only implemented for a successfully initialised
/// backend.
pub trait BackendCtxInterfaceInitialised {
    fn get_version(&self) -> String;
    fn compile_library(
        &mut self,
        module_name: &str,
        code: &str,
        api_version: ApiVersion,
        config: Option<&String>,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError>;
    fn debug(&mut self, args: &[&str]) -> Result<RedisValue, GearsApiError>;
    fn get_info(&mut self) -> Option<BackendInfo>;
}

pub trait BackendCtxInterfaceUninitialised {
    /// Returns the name of the backend.
    fn get_name(&self) -> &'static str;

    /// This callback will be called on loading phase to allow
    /// the backend to perform minimal operation that must be done
    /// on loading phase. The backend should only perform the minimal
    /// needed operation and should avoid any resources allocation
    /// like thread or network.
    fn on_load(&self, backend_ctx: BackendCtx) -> Result<(), GearsApiError>;

    /// Initialises the backend with the information passed and returns
    /// a successfully initialised instance.
    fn initialize(
        self: Box<Self>,
    ) -> Result<Box<dyn BackendCtxInterfaceInitialised>, GearsApiError>;
}
