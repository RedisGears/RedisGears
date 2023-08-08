/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::alloc::GlobalAlloc;
use std::any::Any;
use std::sync::Arc;

use redis_module::RedisValue;

use crate::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use crate::redisgears_plugin_api::redisai_interface::AITensorInterface;
use crate::redisgears_plugin_api::GearsApiError;

use super::load_library_ctx::ModuleInfo;
use super::prologue::ApiVersion;
use super::GearsApiResult;

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

/// A payload a [DebuggerBackend] can receive, work with and store.
/// This type of payload is useful for various backend implementations,
/// so that those can downcast to their known type, so that that they
/// can work with it.
pub type DebuggerBackendPayload = Arc<dyn Any + Send + Sync>;
/// A type for the script deletion callback. Should be invoked when
/// the debugger no longer needs the script.
pub type DebuggerDeleteCallback = Box<dyn FnOnce() -> GearsApiResult>;

/// The trait which the plugins can implement to support the debugging.
/// The debugging processed is assumed to follow the client-server
/// architecture, supporting just one client at a time.
pub trait DebuggerBackend {
    /// Accepts one single connection of a client. After a successful
    /// accepting of a connection, the debugger backend may proceed
    /// by having the [DebuggerBackend::start] method called.
    fn accept_connection(&mut self) -> GearsApiResult;

    /// Starts the debugging session. A connection is expected to have
    /// been established prior to calling this method.
    fn start_session(&mut self, payload: DebuggerBackendPayload) -> GearsApiResult;

    /// Processes the events until there are no more events to process.
    /// It is expected to have this method called repeatedly, until the
    /// session is stopped.
    ///
    /// # Note
    ///
    /// The connection must have been established prior to calling this
    /// method, as well as a debugging session must have been started.
    fn process_events(&mut self) -> GearsApiResult;

    /// Returns a human-readable string, explaining how to connect to
    /// the server.
    fn get_connection_hints(&self) -> Option<String>;

    /// Returns [`true`] if it has previously accepted a client.
    /// See [`DebuggerBackend::accept_connection`] for more information.
    fn accepted_connection(&self) -> bool;
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
    pub get_rdb_lock_timeout: Box<dyn Fn() -> u128 + 'static>,
    pub get_v8_maxmemory: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_initial_memory: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_initial_memory_limit: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_library_memory_delta: Box<dyn Fn() -> usize + 'static>,
    pub get_v8_flags: Box<dyn Fn() -> String + 'static>,
}

/// The trait which is only implemented for a successfully initialised
/// backend.
pub trait BackendCtxInterfaceInitialised {
    /// Returns the name of the backend.
    fn get_name(&self) -> &'static str;
    fn get_version(&self) -> String;
    fn compile_library(
        &mut self,
        debug: bool,
        module_name: &str,
        code: &str,
        api_version: ApiVersion,
        config: Option<&String>,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError>;
    fn debug(&mut self, args: &[&str]) -> Result<RedisValue, GearsApiError>;
    fn get_info(&mut self) -> Option<ModuleInfo>;

    /// Starts a server for remote debugging. The server listens to
    /// connections but doesn't start a debugging session yet. See
    /// [DebuggerBackend] for more information about debugging.
    fn start_debug_server(
        &self,
        _address: &str,
    ) -> Result<Box<dyn DebuggerBackend + Send>, GearsApiError> {
        Err(GearsApiError::new(format!(
            "Debugging isn't implemented for this backend: {}, {}",
            self.get_name(),
            self.get_version()
        )))
    }
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
        logger: &'static dyn log::Log,
    ) -> Result<Box<dyn BackendCtxInterfaceInitialised>, GearsApiError>;
}
