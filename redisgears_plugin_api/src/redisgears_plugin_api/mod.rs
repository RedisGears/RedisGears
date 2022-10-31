use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

pub mod backend_ctx;
pub mod function_ctx;
pub mod keys_notifications_consumer_ctx;
pub mod load_library_ctx;
pub mod run_function_ctx;
pub mod stream_ctx;

pub enum GearsApiError {
    Msg(String),
}

impl GearsApiError {
    pub fn get_msg(&self) -> &str {
        match self {
            GearsApiError::Msg(s) => s,
        }
    }
}

pub enum FunctionCallResult {
    Done,
    Hold,
}

pub enum CallResult {
    Error(String),
    SimpleStr(String),
    BulkStr(String),
    StringBuffer(Vec<u8>),
    Long(i64),
    Double(f64),
    Array(Vec<CallResult>),
    Map(HashMap<Vec<u8>, CallResult>),
    Set(HashSet<Vec<u8>>),
    Bool(bool),
    BigNumber(String),
    VerbatimString((String, String)),
    Null,
}

pub struct RefCellWrapper<T> {
    pub ref_cell: RefCell<T>,
}

unsafe impl<T> Sync for RefCellWrapper<T> {}
unsafe impl<T> Send for RefCellWrapper<T> {}
