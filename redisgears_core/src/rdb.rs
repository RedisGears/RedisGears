/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    function_load_command::function_load_internal, get_globals, get_globals_mut, get_libraries,
};

use mr::libmr::{calc_slot, is_my_slot};
use redis_module::{
    error::Error, native_types::RedisType, raw, raw::REDISMODULE_AUX_BEFORE_RDB, Context,
    RedisModuleTypeMethods,
};

use std::os::raw::c_int;

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
        aux_load: Some(aux_load),
        aux_save: None,
        aux_save2: Some(aux_save),
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

extern "C" fn aux_save(rdb: *mut raw::RedisModuleIO, _when: c_int) {
    let libraries = get_libraries();
    if libraries.is_empty() {
        // no libraries to save, we will save nothing to the RDB so it will be
        // possible to load the RDB even without loading RedisGears.
        return;
    }

    // save the number of libraries
    raw::save_unsigned(rdb, libraries.len() as u64);

    for val in libraries.values() {
        raw::save_string(rdb, &val.gears_lib_ctx.meta_data.name);
        raw::save_string(rdb, &val.gears_lib_ctx.meta_data.code);
        raw::save_redis_string(rdb, &val.gears_lib_ctx.meta_data.user);
        if let Some(config) = &val.gears_lib_ctx.meta_data.config.as_ref() {
            raw::save_unsigned(rdb, 1); // config exists
            raw::save_string(rdb, config);
        } else {
            raw::save_unsigned(rdb, 0); // no config
        }
        // save the number of streams consumer
        raw::save_unsigned(rdb, val.gears_lib_ctx.stream_consumers.len() as u64);
        for (name, stream_consumer) in val.gears_lib_ctx.stream_consumers.iter() {
            // save the consumer name
            raw::save_string(rdb, name);
            let streams_info = stream_consumer
                .ref_cell
                .borrow()
                .get_streams_info()
                .collect::<Vec<(Vec<u8>, u64, u64)>>();
            // save the number of streams for this consumer
            raw::save_unsigned(rdb, streams_info.len() as u64);
            for (stream, ms, seq) in streams_info {
                // save the stream name
                raw::save_slice(rdb, &stream);
                // save last read id
                raw::save_unsigned(rdb, ms);
                raw::save_unsigned(rdb, seq);
            }
        }
    }
}

fn aux_load_internals(ctx: &Context, rdb: *mut raw::RedisModuleIO) -> Result<(), Error> {
    let num_of_libs = raw::load_unsigned(rdb)?;

    for _ in 0..num_of_libs {
        let name = raw::load_string_buffer(rdb)
            .map_err(|e| Error::generic(&format!("Failed loading name from rdb, {}.", e)))?
            .to_string()
            .map_err(|e| {
                Error::generic(&format!("Failed parsing name from rdb as string, {}.", e))
            })?;
        let code = raw::load_string_buffer(rdb)
            .map_err(|e| Error::generic(&format!("Failed loading code from rdb, {}.", e)))?
            .to_string()
            .map_err(|e| {
                Error::generic(&format!("Failed parsing code from rdb as string, {}.", e))
            })?;
        let user = raw::load_string(rdb)
            .map_err(|e| Error::generic(&format!("Failed loading user from rdb, {}.", e)))?;

        let has_config = raw::load_unsigned(rdb).map_err(|e| {
            Error::generic(&format!("Failed loading config indicator from rdb, {}.", e))
        })?;

        let config = if has_config > 0 {
            Some(
                raw::load_string_buffer(rdb)
                    .map_err(|e| Error::generic(&format!("Failed loading user from rdb, {}.", e)))?
                    .to_string()
                    .map_err(|e| {
                        Error::generic(&format!("Failed parsing user from rdb as string, {}.", e))
                    })?,
            )
        } else {
            None
        };

        // allow upgrade on pseudo_slave (replica-of) because we might get the same function multiple time from different source shards.
        let is_pseudo_slave = get_globals().db_policy.is_pseudo_slave();
        match function_load_internal(ctx, user, &code, config, is_pseudo_slave, true) {
            Ok(_) => {}
            Err(e) => return Err(Error::generic(&format!("Failed loading librart, {}", e))),
        }

        // library was load, we must be able to find it
        let libraries = get_libraries();
        let lib = libraries.get(&name).unwrap();

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
            let consumer = lib
                .gears_lib_ctx
                .stream_consumers
                .get(&consumer_name)
                .unwrap();
            // read the number of streams for this consumer
            let num_of_streams = raw::load_unsigned(rdb).map_err(|e| {
                Error::generic(&format!(
                    "Failed loading number of streams for a consumer '{}', {}.",
                    consumer_name, e
                ))
            })?;
            for _ in 0..num_of_streams {
                let stream_name = raw::load_string_buffer(rdb).map_err(|e| {
                    Error::generic(&format!(
                        "Failed loading stream name for consumer '{}', {}.",
                        consumer_name, e
                    ))
                })?;
                let ms = raw::load_unsigned(rdb).map_err(|e| {
                    Error::generic(&format!(
                        "Failed loading ms value for consumer '{}', {}.",
                        consumer_name, e
                    ))
                })?;
                let seq = raw::load_unsigned(rdb).map_err(|e| {
                    Error::generic(&format!(
                        "Failed loading seq value for consumer '{}', {}.",
                        consumer_name, e
                    ))
                })?;
                // Add the stream only if it belong to our slot range.
                if is_pseudo_slave {
                    let slot = calc_slot(stream_name.as_ref());
                    if !is_my_slot(slot) {
                        continue;
                    }
                }
                get_globals_mut().stream_ctx.update_stream_for_consumer(
                    stream_name.as_ref(),
                    consumer,
                    ms,
                    seq,
                );
            }
        }
    }

    Ok(())
}

unsafe extern "C" fn aux_load(rdb: *mut raw::RedisModuleIO, encver: c_int, _when: c_int) -> c_int {
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
