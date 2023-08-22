use std::sync::Arc;

use redis_module::Context;
use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::DebuggerBackend, GearsApiError, GearsApiResult,
};

use crate::{
    function_load_command::{function_compile, function_evaluate_and_store, CompilationArguments},
    GearsLibrary,
};

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
    /// Prepares a debugger server for a session.
    pub fn prepare(
        compilation_arguments: CompilationArguments,
        backend: Box<dyn DebuggerBackend>,
    ) -> Self {
        Self::AwaitingConnection {
            compilation_arguments,
            backend: Some(backend),
        }
    }

    fn ensure_connection_is_accepted(&mut self) -> GearsApiResult {
        match self {
            Self::AwaitingConnection { backend, .. } => backend
                .as_mut()
                .ok_or_else(|| GearsApiError::new("The debugger backend wasn't set correctly."))?
                .accept_connection(),
            _ => Ok(()),
        }
    }

    fn ensure_script_is_compiled(&mut self, context: &Context) -> GearsApiResult {
        match self {
            Self::AwaitingConnection {
                compilation_arguments,
                backend,
            } => {
                let library = if let Some(backend) = backend {
                    if backend.has_accepted_connection() {
                        let compiled_function_info =
                            function_compile(compilation_arguments.clone(), true)
                                .map_err(GearsApiError::new)?;

                        let debug_payload =
                            compiled_function_info.library_context.get_debug_payload()?;

                        backend.start_session(debug_payload)?;
                        function_evaluate_and_store(context, compiled_function_info, false, false)
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

    /// Process the queued incoming and outcoming messages, or advances
    /// the state of the server (if it was prepared, it will attempt
    /// to accept a connection).
    pub fn process_events(&mut self, context: &Context) -> GearsApiResult<bool> {
        self.ensure_connection_is_accepted()?;
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

    /// Returns [`true`] if the server is prepared.
    /// See [`Self::AwaitingConnection`] for more information.
    fn is_prepared(&self) -> bool {
        matches!(self, Self::AwaitingConnection { .. })
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
            } => f
                .debug_struct("AwaitingConnection")
                .field("compilation_arguments", &compilation_arguments)
                .field("backend", &format!("{:p}", backend))
                .finish(),
            Self::InProgress { library, backend } => f
                .debug_struct("InProgress")
                .field("library", &library)
                .field("backend", &format!("{:p}", backend))
                .finish(),
        }
    }
}
