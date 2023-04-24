/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use std::sync::atomic::AtomicI64;
use std::sync::Mutex;

use lazy_static::lazy_static;
use redis_module::enum_configuration;
use redis_module::RedisGILGuard;

enum_configuration! {
    pub(crate) enum FatalFailurePolicyConfiguration {
        Abort = 1,
        Kill = 2,
    }
}

lazy_static! {
    pub(crate) static ref ERROR_VERBOSITY: AtomicI64 = AtomicI64::default();
    pub(crate) static ref EXECUTION_THREADS: RedisGILGuard<i64> = RedisGILGuard::default();
    pub(crate) static ref REMOTE_TASK_DEFAULT_TIMEOUT: AtomicI64 = AtomicI64::default();
    pub(crate) static ref LIBRARY_MAX_MEMORY: AtomicI64 = AtomicI64::default();
    pub(crate) static ref LOCK_REDIS_TIMEOUT: AtomicI64 = AtomicI64::default();
    pub(crate) static ref GEARS_BOX_ADDRESS: RedisGILGuard<String> = RedisGILGuard::default();
    pub(crate) static ref FATAL_FAILURE_POLICY: Mutex<FatalFailurePolicyConfiguration> =
        Mutex::new(FatalFailurePolicyConfiguration::Abort);
    pub(crate) static ref ENABLE_DEBUG_COMMAND: RedisGILGuard<bool> = RedisGILGuard::default();
    pub(crate) static ref V8_PLUGIN_PATH: RedisGILGuard<String> = RedisGILGuard::default();
}
