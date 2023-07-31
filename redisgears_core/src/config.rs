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

macro_rules! generate_lock_timeout_struct {
    ($vis:vis $name:ident) => {
        /// A transparent structure to have a lock timeout value with custom
        /// setters and getters.
        #[derive(Debug, Default)]
        #[repr(transparent)]
        $vis struct $name(AtomicI64);
        impl std::ops::Deref for $name {
            type Target = AtomicI64;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

generate_lock_timeout_struct!(pub RdbLockTimeout);
generate_lock_timeout_struct!(pub LoadLockTimeout);

impl redis_module::ConfigurationValue<i64> for RdbLockTimeout {
    fn get(&self, _: &redis_module::configuration::ConfigurationContext) -> i64 {
        self.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set(
        &self,
        _: &redis_module::configuration::ConfigurationContext,
        val: i64,
    ) -> Result<(), redis_module::RedisError> {
        if val < LOCK_REDIS_TIMEOUT.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(redis_module::RedisError::Str(
                "The db-loading-lock-redis-timeout value can't be less than lock-redis-timeout value.",
            ));
        }
        self.store(val, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl redis_module::ConfigurationValue<i64> for LoadLockTimeout {
    fn get(&self, _: &redis_module::configuration::ConfigurationContext) -> i64 {
        self.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set(
        &self,
        _: &redis_module::configuration::ConfigurationContext,
        val: i64,
    ) -> Result<(), redis_module::RedisError> {
        self.store(val, std::sync::atomic::Ordering::SeqCst);
        // db-loading-lock-redis-timeout shouldn't be less than the
        // lock-redis-timeout value, as it wouldn't make sense then.
        // Hence we are updating it here too as well.
        let current_db_loading_timeout =
            DB_LOADING_LOCK_REDIS_TIMEOUT.load(std::sync::atomic::Ordering::Relaxed);
        DB_LOADING_LOCK_REDIS_TIMEOUT.store(
            std::cmp::max(val, current_db_loading_timeout),
            std::sync::atomic::Ordering::SeqCst,
        );
        Ok(())
    }
}

lazy_static! {
    /// Configuration value indicates how verbose the error messages will be give
    /// to the user. Value 1 means simple one line error message. Value of 2
    /// means a full stack trace.
    pub(crate) static ref ERROR_VERBOSITY: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the number of execution threads for
    /// background tasks.
    pub(crate) static ref EXECUTION_THREADS: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the timeout for remote tasks that runs on a remote shard.
    pub(crate) static ref REMOTE_TASK_DEFAULT_TIMEOUT: AtomicI64 = AtomicI64::default();

    /// Configuration value indicates the timeout for locking Redis (except
    /// for the loading from RDB. For that, see the [`DB_LOADING_LOCK_REDIS_TIMEOUT`]).
    pub(crate) static ref LOCK_REDIS_TIMEOUT: LoadLockTimeout = LoadLockTimeout::default();

    /// Configuration value indicates the timeout for locking Redis when
    /// loading from persistency (either AOF, RDB or replication stream).
    pub(crate) static ref DB_LOADING_LOCK_REDIS_TIMEOUT: RdbLockTimeout = RdbLockTimeout::default();

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

    /// Configuration value indicates the V8 flags to pass to the V8 engine.
    pub(crate) static ref V8_FLAGS: RedisGILGuard<String> = RedisGILGuard::default();

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
