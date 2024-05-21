//! The NOOP module for RedisGears v2.

const NOOP_MODULE_ERROR: &str = "Unrecognized command.";

pub(crate) static REDIS_GEARS_VERSION: i32 = 1;
pub(crate) static REDIS_GEARS_TYPE: RedisType = RedisType::new(
    "GearsType",
    REDIS_GEARS_VERSION,
    RedisModuleTypeMethods {
        version: redis_module::TYPE_METHOD_VERSION,

        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None, // TODO add support
        free: None,
        mem_usage: None,
        digest: None,

        // Auxiliary data (v2)
        aux_load: Some(rdb::aux_load),
        aux_save: None,
        aux_save2: Some(rdb::aux_save),
        aux_save_triggers: REDISMODULE_AUX_BEFORE_RDB as i32,

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,

        free_effort2: None,
        unlink2: None,
        copy2: None,
        mem_usage2: None,
    },
);

use redis_module::{
    native_types::RedisType, raw::REDISMODULE_AUX_BEFORE_RDB, Context, RedisModuleTypeMethods,
};

mod rdb {
    use redis_module::{error::Error, raw, Context};
    use std::os::raw::c_int;

    use super::REDIS_GEARS_VERSION;

    pub(crate) extern "C" fn aux_save(_rdb: *mut raw::RedisModuleIO, _when: c_int) {}

    fn aux_load_internals(_ctx: &Context, rdb: *mut raw::RedisModuleIO) -> Result<(), Error> {
        let num_of_libs = raw::load_unsigned(rdb)?;

        for _ in 0..num_of_libs {
            let _name = raw::load_string_buffer(rdb)
                .map_err(|e| Error::generic(&format!("Failed loading name from rdb, {}.", e)))?
                .to_string()
                .map_err(|e| {
                    Error::generic(&format!("Failed parsing name from rdb as string, {}.", e))
                })?;
            let _code = raw::load_string_buffer(rdb)
                .map_err(|e| Error::generic(&format!("Failed loading code from rdb, {}.", e)))?
                .to_string()
                .map_err(|e| {
                    Error::generic(&format!("Failed parsing code from rdb as string, {}.", e))
                })?;
            let _user = raw::load_string(rdb)
                .map_err(|e| Error::generic(&format!("Failed loading user from rdb, {}.", e)))?;

            let has_config = raw::load_unsigned(rdb).map_err(|e| {
                Error::generic(&format!("Failed loading config indicator from rdb, {}.", e))
            })?;

            let _config = if has_config > 0 {
                Some(
                    raw::load_string_buffer(rdb)
                        .map_err(|e| {
                            Error::generic(&format!("Failed loading user from rdb, {}.", e))
                        })?
                        .to_string()
                        .map_err(|e| {
                            Error::generic(&format!(
                                "Failed parsing user from rdb as string, {}.",
                                e
                            ))
                        })?,
                )
            } else {
                None
            };

            // load stream consumers data
            let num_of_streams_consumers = raw::load_unsigned(rdb).map_err(|e| {
                Error::generic(&format!(
                    "Failed loading number of streams from rdb, {}.",
                    e
                ))
            })?;

            for _ in 0..num_of_streams_consumers {
                let consumer_name = raw::load_string_buffer(rdb)
                    .map_err(|e| {
                        Error::generic(&format!("Failed loading consumer name from rdb, {}.", e))
                    })?
                    .to_string()
                    .map_err(|e| {
                        Error::generic(&format!(
                            "Failed parsing consumer name from rdb as string, {}.",
                            e
                        ))
                    })?;

                // read the number of streams for this consumer
                let num_of_streams = raw::load_unsigned(rdb).map_err(|e| {
                    Error::generic(&format!(
                        "Failed loading number of streams for a consumer '{}', {}.",
                        consumer_name, e
                    ))
                })?;

                for _ in 0..num_of_streams {
                    let _stream_name = raw::load_string_buffer(rdb).map_err(|e| {
                        Error::generic(&format!(
                            "Failed loading stream name for consumer '{}', {}.",
                            consumer_name, e
                        ))
                    })?;
                    let _ms = raw::load_unsigned(rdb).map_err(|e| {
                        Error::generic(&format!(
                            "Failed loading ms value for consumer '{}', {}.",
                            consumer_name, e
                        ))
                    })?;
                    let _seq = raw::load_unsigned(rdb).map_err(|e| {
                        Error::generic(&format!(
                            "Failed loading seq value for consumer '{}', {}.",
                            consumer_name, e
                        ))
                    })?;
                }
            }
        }

        Ok(())
    }

    pub(crate) unsafe extern "C" fn aux_load(
        rdb: *mut raw::RedisModuleIO,
        encver: c_int,
        _when: c_int,
    ) -> c_int {
        if encver > REDIS_GEARS_VERSION {
            log::info!(
                "Can not load RedisGears data type version '{}', max supported version '{}'",
                encver,
                REDIS_GEARS_VERSION
            );
            return raw::REDISMODULE_ERR as i32;
        }

        let inner_ctx = unsafe { raw::RedisModule_GetContextFromIO.unwrap()(rdb) };
        let ctx = Context::new(inner_ctx);

        match aux_load_internals(&ctx, rdb) {
            Ok(_) => raw::REDISMODULE_OK as i32,
            Err(e) => {
                log::warn!("Failed loading functions from rdb, {}.", e);
                raw::REDISMODULE_ERR as i32
            }
        }
    }
}

mod commands {
    use redis_module::{RedisError, RedisResult, RedisString, RedisValue};
    use redis_module_macros::command;

    use super::*;

    #[command(
    {
        name: "tfcall",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -3,
        key_spec: [
            {
                flags: [ReadWrite, Access, Update],
                begin_search: Index({ index : 2}),
                find_keys: Keynum({ key_num_idx : 0, first_key : 1, key_step : 1 }),
            }
        ],
    }
)]
    fn function_call(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
        RedisResult::Err(RedisError::Str(NOOP_MODULE_ERROR))
    }

    #[command(
    {
        name: "tfcallasync",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -3,
        key_spec: [
            {
                flags: [ReadWrite, Access, Update],
                begin_search: Index({ index : 2}),
                find_keys: Keynum({ key_num_idx : 0, first_key : 1, key_step : 1 }),
            }
        ],
    }
)]
    fn function_call_async(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
        RedisResult::Err(RedisError::Str(NOOP_MODULE_ERROR))
    }

    #[command(
    {
        name: "_rg_internals.function",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        enterprise_flags: [ProxyFiltered],
        arity: -3,
        key_spec: [],
    }
)]
    fn function_command_on_replica(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
        Ok(RedisValue::SimpleStringStatic("OK"))
    }

    #[command(
    {
        name: "tfunction",
        flags: [MayReplicate, DenyScript, NoMandatoryKeys],
        arity: -2,
        key_spec: [],
    }
)]
    fn function_command(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
        RedisResult::Err(RedisError::Str(NOOP_MODULE_ERROR))
    }

    #[command(
    {
        name: "_rg_internals.update_stream_last_read_id",
        flags: [MayReplicate, DenyScript],
        enterprise_flags: [ProxyFiltered],
        arity: 6,
        key_spec: [
            {
                flags: [ReadWrite, Access, Update],
                begin_search: Index({ index : 3}),
                find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
            }
        ],
    }
)]
    fn update_stream_last_read_id(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
        Ok(RedisValue::SimpleStringStatic("OK"))
    }
}

pub mod gears_module {
    use redis_module::{configuration::ConfigurationFlags, RedisString, Status};

    use super::*;
    use crate::config::*;

    use crate::{
        BUILD_OS, BUILD_OS_ARCH, BUILD_OS_NICK, BUILD_TYPE, GIT_SHA, VERSION_NUM, VERSION_STR,
    };

    fn noop_init(_ctx: &Context, _args: &[RedisString]) -> Status {
        log::info!(
            "RedisGears v{}, sha='{}', build_type='{}', built_for='{}-{}.{}', noop mode.",
            VERSION_STR.unwrap_or_default(),
            GIT_SHA.unwrap_or_default(),
            BUILD_TYPE.unwrap_or_default(),
            BUILD_OS.unwrap_or_default(),
            BUILD_OS_NICK.unwrap_or_default(),
            BUILD_OS_ARCH.unwrap_or_default(),
        );

        Status::Ok
    }

    redis_module::redis_module! {
        name: "redisgears_2",
        version: VERSION_NUM.unwrap().parse::<i32>().unwrap(),
        allocator: (std::alloc::System, std::alloc::System),
        data_types: [REDIS_GEARS_TYPE],
        init: noop_init,
        commands: [],
        configurations:[
            i64: [
                ["error-verbosity", &*ERROR_VERBOSITY ,1, 1, 2, ConfigurationFlags::DEFAULT, None],
                ["execution-threads", &*EXECUTION_THREADS ,1, 1, 32, ConfigurationFlags::IMMUTABLE, None],
                ["remote-task-default-timeout", &*REMOTE_TASK_DEFAULT_TIMEOUT , 500, 1, i64::MAX, ConfigurationFlags::DEFAULT, None],
                ["lock-redis-timeout", &*LOCK_REDIS_TIMEOUT , 500, 100, 1000000000, ConfigurationFlags::DEFAULT, None],
                ["db-loading-lock-redis-timeout", &*DB_LOADING_LOCK_REDIS_TIMEOUT , 30000, 100, 1000000000, ConfigurationFlags::DEFAULT, None],

                [
                    "v8-maxmemory",
                    &*V8_MAX_MEMORY,
                    byte_unit::n_mb_bytes!(200) as i64,
                    byte_unit::n_mb_bytes!(50) as i64,
                    byte_unit::n_gb_bytes!(1) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-initial-memory-usage",
                    &*V8_LIBRARY_INITIAL_MEMORY_USAGE,
                    byte_unit::n_mb_bytes!(2) as i64,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(10) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-initial-memory-limit",
                    &*V8_LIBRARY_INITIAL_MEMORY_LIMIT,
                    byte_unit::n_mb_bytes!(3) as i64,
                    byte_unit::n_mb_bytes!(2) as i64,
                    byte_unit::n_mb_bytes!(20) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
                [
                    "v8-library-memory-usage-delta",
                    &*V8_LIBRARY_MEMORY_USAGE_DELTA,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(1) as i64,
                    byte_unit::n_mb_bytes!(10) as i64,
                    ConfigurationFlags::MEMORY | ConfigurationFlags::IMMUTABLE,
                    None
                ],
            ],
            string: [
                ["gearsbox-address", &*GEARS_BOX_ADDRESS , "http://localhost:3000", ConfigurationFlags::DEFAULT, None],
                ["v8-plugin-path", &*V8_PLUGIN_PATH , "libredisgears_v8_plugin.so", ConfigurationFlags::IMMUTABLE, None],
                ["v8-flags", &*V8_FLAGS, "'--noexpose-wasm'", ConfigurationFlags::IMMUTABLE, None],
                ["v8-debug-server-address", &*V8_DEBUG_SERVER_ADDRESS, "", ConfigurationFlags::IMMUTABLE, None],
            ],
            bool: [
                ["enable-debug-command", &*ENABLE_DEBUG_COMMAND , false, ConfigurationFlags::IMMUTABLE, None],
            ],
            enum: [
                ["library-fatal-failure-policy", &*FATAL_FAILURE_POLICY , FatalFailurePolicyConfiguration::Abort, ConfigurationFlags::DEFAULT, None],
            ],
            module_args_as_configuration: true,
            module_config_get: "TCONFIG_GET",
            module_config_set: "TCONFIG_SET",
        ]
    }
}
