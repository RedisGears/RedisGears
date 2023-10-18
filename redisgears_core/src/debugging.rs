use std::sync::Arc;

use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::DebuggerBackend, GearsApiError, GearsApiResult,
};

use crate::{
    function_load_command::{function_compile, function_evaluate_and_store, CompilationArguments},
    GearsLibrary,
};

/// The connection state of the debugger server [`Server`].
#[derive(Debug, Copy, Clone)]
pub(crate) enum ConnectionState {
    /// The connecting hasn't been established yet, and the server is
    /// still listening for those.
    Waiting,
    /// The connection has been successfully established.
    Connected,
    /// The connection was dropped for whatever reason, the server will
    /// no longer accept any connections.
    Dropped,
}

/// A debugger server is a thread that executes a user library with
/// a remote debugger attached.
pub(crate) enum Server {
    /// A state when the [`DebuggerBackend`] has been started and is
    /// listening for a connection, but hasn't accepted any yet, so
    /// there is no debugging session as of this moment.
    AwaitingConnection {
        compilation_arguments: CompilationArguments,
        // Made an [`Option`] to be able to change the state via a
        // mutable reference to "this".
        backend: Option<Box<dyn DebuggerBackend>>,
        //// The time the server started listening for connections.
        start_time: std::time::Instant,
    },
    /// A state when the [`DebuggerServer`] has established (accepted)
    /// a connection with a remote debugger and compiled+evaluated the
    /// script, and the debugging process is taking place.
    InProgress {
        /// The library being debugged.
        library: Arc<GearsLibrary>,
        /// The debugger backend (server).
        backend: Box<dyn DebuggerBackend>,
    },
}

impl Server {
    /// How long to wait for the connection to happen.
    const WAIT_FOR_CONNECTION_TIME: std::time::Duration = std::time::Duration::from_secs(60);

    /// Prepares a debugger server for a session.
    pub fn prepare(
        compilation_arguments: CompilationArguments,
        backend: Box<dyn DebuggerBackend>,
    ) -> Self {
        Self::AwaitingConnection {
            compilation_arguments,
            backend: Some(backend),
            start_time: std::time::Instant::now(),
        }
    }

    /// Returns the server connection state on success.
    fn ensure_connection_is_accepted(&mut self) -> GearsApiResult<ConnectionState> {
        match self {
            Self::AwaitingConnection {
                backend,
                start_time,
                ..
            } => {
                if start_time.elapsed() >= Self::WAIT_FOR_CONNECTION_TIME {
                    return Ok(ConnectionState::Dropped);
                }

                match backend
                    .as_mut()
                    .ok_or_else(|| {
                        GearsApiError::new("The debugger backend wasn't set correctly.")
                    })?
                    .accept_connection()
                {
                    Ok(_) => Ok(ConnectionState::Connected),
                    Err(e) => {
                        // TODO: use error codes of `GearsApiError`.
                        if e.get_msg().contains("No connection attempted.") {
                            Ok(ConnectionState::Waiting)
                        } else {
                            Ok(ConnectionState::Dropped)
                        }
                    }
                }
            }
            _ => Ok(ConnectionState::Connected),
        }
    }

    fn ensure_script_is_compiled(&mut self, context: &Context) -> GearsApiResult {
        match self {
            Self::AwaitingConnection {
                compilation_arguments,
                backend,
                start_time: _,
            } => {
                let library = if let Some(backend) = backend {
                    if backend.has_accepted_connection() {
                        let compiled_function_info =
                            function_compile(context, compilation_arguments.clone(), true)
                                .map_err(GearsApiError::new)?;

                        let debug_payload =
                            compiled_function_info.library_context.get_debug_payload()?;

                        backend.start_session(debug_payload)?;
                        function_evaluate_and_store(context, compiled_function_info, true, false)
                            .map_err(|e| {
                                backend.stop_session();
                                GearsApiError::new(e)
                            })?
                    } else {
                        return Err(GearsApiError::new("The debugger will not compile a script until there is a client connection accepted."));
                    }
                } else {
                    return Err(GearsApiError::new("The debugger backend wasn't set."));
                };

                let backend = backend
                    .take()
                    .ok_or_else(|| GearsApiError::new("The debugger backend disappeared."))?;

                std::mem::swap(self, &mut Self::InProgress { library, backend });

                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn make_progress(&mut self) -> GearsApiResult<bool> {
        match self {
            Self::InProgress { backend, .. } => backend.process_events(),
            _ => Err(GearsApiError::new("Invalid state of debugger.")),
        }
    }

    /// Processes the queued incoming and outcoming messages, or
    /// advances the state of the server (if it was prepared, it will
    /// attempt to accept a connection).
    ///
    /// Returns [`Ok(true)`] if the server has stopped and there will be
    /// no further progress made, so the object can be freed.
    pub fn process_events(&mut self, context: &Context) -> GearsApiResult<bool> {
        match self.ensure_connection_is_accepted()? {
            ConnectionState::Waiting => return Ok(false),
            ConnectionState::Dropped => return Ok(true),
            ConnectionState::Connected => {}
        }
        self.ensure_script_is_compiled(context)?;
        self.make_progress()
    }

    /// Stops the server and the session.
    pub fn stop(&mut self) {
        match self {
            Self::AwaitingConnection { backend, .. } => {
                if let Some(backend) = backend {
                    backend.stop_session();
                }
            }
            Self::InProgress { backend, .. } => {
                backend.stop_session();
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AwaitingConnection {
                compilation_arguments,
                backend,
                start_time,
            } => f
                .debug_struct("AwaitingConnection")
                .field("compilation_arguments", &compilation_arguments)
                .field("backend", &format!("{:p}", backend))
                .field("start_time", &start_time)
                .finish(),
            Self::InProgress { library, backend } => f
                .debug_struct("InProgress")
                .field("library", &library)
                .field("backend", &format!("{:p}", backend))
                .finish(),
        }
    }
}
