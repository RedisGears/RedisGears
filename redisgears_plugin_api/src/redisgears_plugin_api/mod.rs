/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::cell::RefCell;

use redis_module::RedisError;

pub mod backend_ctx;
pub mod function_ctx;
pub mod keys_notifications_consumer_ctx;
pub mod load_library_ctx;
pub mod prologue;
pub mod redisai_interface;
pub mod run_function_ctx;
pub mod stream_ctx;

/// A [Result] type for the Gears API.
pub type GearsApiResult<R = ()> = Result<R, GearsApiError>;

#[derive(Debug, Clone)]
pub struct GearsApiError {
    msg: String,
    verbose_msg: Option<String>,
}

impl GearsApiError {
    pub fn new_verbose<T: Into<String>, F: Into<String>>(
        msg: T,
        verbose_msg: Option<F>,
    ) -> GearsApiError {
        GearsApiError {
            msg: msg.into(),
            verbose_msg: verbose_msg.map(|v| v.into()),
        }
    }

    pub fn new<T: Into<String>>(msg: T) -> GearsApiError {
        GearsApiError {
            msg: msg.into(),
            verbose_msg: None,
        }
    }

    pub fn get_msg(&self) -> &str {
        &self.msg
    }

    pub fn get_msg_verbose(&self) -> &str {
        self.verbose_msg.as_ref().unwrap_or(&self.msg)
    }
}

impl From<GearsApiError> for RedisError {
    fn from(value: GearsApiError) -> Self {
        RedisError::String(value.msg)
    }
}

impl From<RedisError> for GearsApiError {
    fn from(value: RedisError) -> Self {
        Self::new(value.to_string())
    }
}

impl std::fmt::Display for GearsApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

pub enum FunctionCallResult {
    Done,
    Hold,
}

pub struct RefCellWrapper<T> {
    pub ref_cell: RefCell<T>,
}

impl<T> std::fmt::Debug for RefCellWrapper<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("RefCellWrapper<{}>", std::any::type_name::<T>()))
            .field("ref_cell", &self.ref_cell)
            .finish()
    }
}

unsafe impl<T> Sync for RefCellWrapper<T> {}
unsafe impl<T> Send for RefCellWrapper<T> {}
