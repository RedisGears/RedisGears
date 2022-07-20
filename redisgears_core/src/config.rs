use redis_module::context::configuration::{
    ConfigFlags, RedisConfigCtx, RedisEnumConfigCtx, RedisNumberConfigCtx, RedisStringConfigCtx,
};
use redis_module::RedisString;

use redis_module::context::Context;
use redis_module::RedisError;

use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::LibraryFatalFailurePolicy;

use std::fmt;

pub(crate) struct ExecutionThreads {
    pub(crate) size: usize,
    flags: ConfigFlags,
}

impl ExecutionThreads {
    fn new() -> ExecutionThreads {
        ExecutionThreads {
            size: 1,
            flags: ConfigFlags::new().emmutable(),
        }
    }
}

impl fmt::Display for ExecutionThreads {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.size)
    }
}

impl RedisConfigCtx for ExecutionThreads {
    fn name(&self) -> &'static str {
        "execution-threads"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for ExecutionThreads {
    fn default(&self) -> i64 {
        1
    }

    fn min(&self) -> i64 {
        1
    }
    fn max(&self) -> i64 {
        32
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as usize;
        Ok(())
    }
}

pub(crate) struct LibraryMaxMemory {
    pub(crate) size: usize,
    flags: ConfigFlags,
}

impl LibraryMaxMemory {
    fn new() -> LibraryMaxMemory {
        LibraryMaxMemory {
            size: 1024 * 1024 * 1024, // 1G
            flags: ConfigFlags::new().emmutable().memory(),
        }
    }
}

impl fmt::Display for LibraryMaxMemory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.size)
    }
}

impl RedisConfigCtx for LibraryMaxMemory {
    fn name(&self) -> &'static str {
        "library-maxmemory"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for LibraryMaxMemory {
    fn default(&self) -> i64 {
        1024 * 1024 * 1024 // 1G
    }

    fn min(&self) -> i64 {
        16 * 1024 * 1024 // 16M
    }
    fn max(&self) -> i64 {
        2 * 1024 * 1024 * 1024 // 2G
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as usize;
        Ok(())
    }
}

pub(crate) struct GearBoxAddress {
    pub(crate) address: String,
    flags: ConfigFlags,
}

impl GearBoxAddress {
    fn new() -> GearBoxAddress {
        GearBoxAddress {
            address: "http://localhost:3000".to_string(),
            flags: ConfigFlags::new(),
        }
    }
}

impl fmt::Display for GearBoxAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.address)
    }
}

impl RedisConfigCtx for GearBoxAddress {
    fn name(&self) -> &'static str {
        "gearsbox-address"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisStringConfigCtx for GearBoxAddress {
    fn default(&self) -> Option<String> {
        Some("http://localhost:3000".to_string())
    }

    fn get(&self, _name: &str) -> RedisString {
        RedisString::create(std::ptr::null_mut(), &self.address)
    }

    fn set(&mut self, _name: &str, value: RedisString) -> Result<(), RedisError> {
        self.address = value.try_as_str().unwrap().to_string();
        Ok(())
    }
}

pub(crate) struct LibraryOnFatalFailurePolicy {
    pub(crate) policy: LibraryFatalFailurePolicy,
    flags: ConfigFlags,
}

impl LibraryOnFatalFailurePolicy {
    fn new() -> LibraryOnFatalFailurePolicy {
        LibraryOnFatalFailurePolicy {
            policy: LibraryFatalFailurePolicy::Abort,
            flags: ConfigFlags::new(),
        }
    }
}

impl fmt::Display for LibraryOnFatalFailurePolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.policy {
            LibraryFatalFailurePolicy::Abort => write!(f, "abort"),
            LibraryFatalFailurePolicy::Kill => write!(f, "kill"),
        }
    }
}

impl RedisConfigCtx for LibraryOnFatalFailurePolicy {
    fn name(&self) -> &'static str {
        "library-fatal-failure-policy"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisEnumConfigCtx for LibraryOnFatalFailurePolicy {
    fn default(&self) -> i32 {
        self.policy.clone() as i32
    }

    fn values(&self) -> Vec<(&str, i32)> {
        vec![
            ("abort", LibraryFatalFailurePolicy::Abort as i32),
            ("kill", LibraryFatalFailurePolicy::Kill as i32),
        ]
    }

    fn get(&self, _name: &str) -> i32 {
        self.policy.clone() as i32
    }

    fn set(&mut self, _name: &str, value: i32) -> Result<(), RedisError> {
        match value {
            x if x == LibraryFatalFailurePolicy::Abort as i32 => {
                self.policy = LibraryFatalFailurePolicy::Abort
            }
            x if x == LibraryFatalFailurePolicy::Kill as i32 => {
                self.policy = LibraryFatalFailurePolicy::Kill
            }
            _ => return Err(RedisError::Str("unsupported value were given")),
        }
        Ok(())
    }
}

pub(crate) struct LockRedisTimeout {
    pub(crate) size: u128,
    flags: ConfigFlags,
}

impl LockRedisTimeout {
    fn new() -> LockRedisTimeout {
        LockRedisTimeout {
            size: 1000,
            flags: ConfigFlags::new(),
        }
    }
}

impl fmt::Display for LockRedisTimeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.size)
    }
}

impl RedisConfigCtx for LockRedisTimeout {
    fn name(&self) -> &'static str {
        "lock-redis-timeout"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for LockRedisTimeout {
    fn default(&self) -> i64 {
        500
    }

    fn min(&self) -> i64 {
        100
    }
    fn max(&self) -> i64 {
        1000000000
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as u128;
        Ok(())
    }
}

pub(crate) struct Config {
    pub(crate) execution_threads: ExecutionThreads,
    pub(crate) library_maxmemory: LibraryMaxMemory,
    pub(crate) gears_box_address: GearBoxAddress,
    pub(crate) libraray_fatal_failure_policy: LibraryOnFatalFailurePolicy,
    pub(crate) lock_regis_timeout: LockRedisTimeout,
}

impl Config {
    pub(crate) fn new() -> Config {
        Config {
            execution_threads: ExecutionThreads::new(),
            library_maxmemory: LibraryMaxMemory::new(),
            gears_box_address: GearBoxAddress::new(),
            libraray_fatal_failure_policy: LibraryOnFatalFailurePolicy::new(),
            lock_regis_timeout: LockRedisTimeout::new(),
        }
    }

    pub fn set_numeric_value<T: RedisNumberConfigCtx>(
        config: &mut T,
        val: &str,
    ) -> Result<(), RedisError> {
        let v = match val.parse::<usize>() {
            Ok(v) => v,
            Err(e) => {
                return Err(RedisError::String(format!(
                    "Failed parsing value '{}' as usize, {}.",
                    val, e
                )))
            }
        };

        if v < config.min() as usize {
            return Err(RedisError::String(format!(
                "Value '{}' is less then minimum value allowed '{}'",
                v,
                config.min()
            )));
        }

        if v > config.max() as usize {
            return Err(RedisError::String(format!(
                "Value '{}' is greater then maximum value allowed '{}'",
                v,
                config.max()
            )));
        }

        config.set(config.name(), v as i64)
    }

    pub fn set_string_value<T: RedisStringConfigCtx>(
        config: &mut T,
        val: &str,
    ) -> Result<(), RedisError> {
        config.set(
            config.name(),
            RedisString::create(std::ptr::null_mut(), val),
        )
    }

    pub fn set_enum_value<T: RedisEnumConfigCtx>(
        config: &mut T,
        val: &str,
    ) -> Result<(), RedisError> {
        let values = config.values();
        for (n, v) in config.values() {
            if n == val {
                return config.set(config.name(), v);
            }
        }

        return Err(RedisError::String(format!(
            "Unknow configration value '{}', options are {:?}.",
            val,
            values.iter().map(|(k, _v)| *k).collect::<Vec<&str>>()
        )));
    }

    pub(crate) fn initial_set(&mut self, name: &str, val: &str) -> Result<(), RedisError> {
        match name {
            x if x == self.execution_threads.name() => {
                Self::set_numeric_value(&mut self.execution_threads, val)
            }
            x if x == self.library_maxmemory.name() => {
                Self::set_numeric_value(&mut self.library_maxmemory, val)
            }
            x if x == self.gears_box_address.name() => {
                Self::set_string_value(&mut self.gears_box_address, val)
            }
            x if x == self.libraray_fatal_failure_policy.name() => {
                Self::set_enum_value(&mut self.libraray_fatal_failure_policy, val)
            }
            x if x == self.lock_regis_timeout.name() => {
                Self::set_numeric_value(&mut self.lock_regis_timeout, val)
            }
            _ => {
                return Err(RedisError::String(format!(
                    "No such configuration {}",
                    name
                )))
            }
        }
    }

    pub fn is_emmutable<T: RedisConfigCtx>(config: &T) -> bool {
        config.flags().is_emmutable()
    }

    pub(crate) fn set(&mut self, name: &str, val: &str) -> Result<(), RedisError> {
        if match name {
            x if x == self.execution_threads.name() => {
                Self::is_emmutable(&mut self.execution_threads)
            }
            x if x == self.library_maxmemory.name() => {
                Self::is_emmutable(&mut self.library_maxmemory)
            }
            x if x == self.gears_box_address.name() => {
                Self::is_emmutable(&mut self.gears_box_address)
            }
            x if x == self.libraray_fatal_failure_policy.name() => {
                Self::is_emmutable(&mut self.libraray_fatal_failure_policy)
            }
            x if x == self.lock_regis_timeout.name() => {
                Self::is_emmutable(&mut self.lock_regis_timeout)
            }
            _ => {
                return Err(RedisError::String(format!(
                    "No such configuration {}",
                    name
                )))
            }
        } {
            return Err(RedisError::String(format!(
                "Configuration {} can not be changed at runtime",
                name
            )));
        }

        self.initial_set(name, val)
    }

    pub(crate) fn get(&self, name: &str) -> Result<String, RedisError> {
        match name {
            x if x == self.execution_threads.name() => Ok(format!("{}", self.execution_threads)),
            x if x == self.library_maxmemory.name() => Ok(format!("{}", self.library_maxmemory)),
            x if x == self.gears_box_address.name() => Ok(format!("{}", self.gears_box_address)),
            x if x == self.libraray_fatal_failure_policy.name() => {
                Ok(format!("{}", self.libraray_fatal_failure_policy))
            }
            x if x == self.lock_regis_timeout.name() => Ok(format!("{}", self.lock_regis_timeout)),
            _ => Err(RedisError::String(format!(
                "No such configuration {}",
                name
            ))),
        }
    }
}
