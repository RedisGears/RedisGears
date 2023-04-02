/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::cell::RefCell;

pub mod backend_ctx;
pub mod function_ctx;
pub mod keys_notifications_consumer_ctx;
pub mod load_library_ctx;
pub mod redisai_interface;
pub mod run_function_ctx;
pub mod stream_ctx;

#[derive(Clone)]
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

pub enum FunctionCallResult {
    Done,
    Hold,
}

pub struct RefCellWrapper<T> {
    pub ref_cell: RefCell<T>,
}

unsafe impl<T> Sync for RefCellWrapper<T> {}
unsafe impl<T> Send for RefCellWrapper<T> {}
