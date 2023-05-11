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
    /// Configuration value indicates how verbose the error messages will be give
    /// to the user. Value 1 means simple one line error message. Value of 2
    /// means a full stack trace.
    pub(crate) static ref ERROR_VERBOSITY: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the number of execution threads for
    /// background tasks.
    pub(crate) static ref EXECUTION_THREADS: RedisGILGuard<i64> = RedisGILGuard::default();

    /// Configuration value indicates the timeout for remote tasks that runs on a remote shard.
    pub(crate) static ref REMOTE_TASK_DEFAULT_TIMEOUT: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the timeout for locking Redis.
    pub(crate) static ref LOCK_REDIS_TIMEOUT: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the gears box url.
    pub(crate) static ref GEARS_BOX_ADDRESS: RedisGILGuard<String> = RedisGILGuard::default();

    /// Configuration value indicates policy for fatal errors such as timeout or OOM.
    pub(crate) static ref FATAL_FAILURE_POLICY: Mutex<FatalFailurePolicyConfiguration> =
        Mutex::new(FatalFailurePolicyConfiguration::Abort);

    /// Configuration value indicates if it is allowed to run debug commands.
    pub(crate) static ref ENABLE_DEBUG_COMMAND: RedisGILGuard<bool> = RedisGILGuard::default();

    // V8 specific configuration

    /// Configuration value indicates the path to the V8 plugin.
    pub(crate) static ref V8_PLUGIN_PATH: RedisGILGuard<String> = RedisGILGuard::default();

    /// Configuration value indicates the maximum memory usage for the V8 engine.
    pub(crate) static ref V8_MAX_MEMORY: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the initial memory usage for a V8 library.
    pub(crate) static ref V8_LIBRARY_INITIAL_MEMORY_USAGE: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the initial memory limit for a V8 library.
    pub(crate) static ref V8_LIBRARY_INITIAL_MEMORY_LIMIT: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the delta by which we allow the library to consume more
    /// memory. For example, assumming the initial library memory limit is 2M, and this value
    /// is also 1M, when the library reaches 2M memory usage we will allow it to consume
    /// 1M more memory, but if the total memory usage used by all the libraries will not bypass
    /// the `V8_MAX_MEMORY` configuration.
    ///
    /// This basically means that we might bypass the `V8_MAX_MEMORY` configuration by at most
    /// `V8_LIBRARY_MEMORY_USAGE_DELTA`.
    pub(crate) static ref V8_LIBRARY_MEMORY_USAGE_DELTA: AtomicI64 = AtomicI64::default();
}
