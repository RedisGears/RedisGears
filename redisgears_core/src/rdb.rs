use crate::{function_load_intrernal, get_ctx, get_globals_mut, get_libraries};

use redis_module::{
    native_types::RedisType, raw, raw::REDISMODULE_AUX_BEFORE_RDB, RedisModuleTypeMethods,
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
        aux_save: Some(aux_save),
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

    // save the number of libraries
    raw::save_unsigned(rdb, libraries.len() as u64);

    for val in libraries.values() {
        raw::save_string(rdb, &val.gears_lib_ctx.meta_data.name);
        raw::save_string(rdb, &val.gears_lib_ctx.meta_data.code);
        raw::save_string(rdb, &val.gears_lib_ctx.user.ref_cell.borrow());
        if let Some(gears_box_info) = &val.gears_box_lib {
            raw::save_unsigned(rdb, 1);
            raw::save_string(rdb, &serde_json::to_string(gears_box_info).unwrap());
        } else {
            raw::save_unsigned(rdb, 0);
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
                .collect::<Vec<(String, u64, u64)>>();
            // save the number of streams for this consumer
            raw::save_unsigned(rdb, streams_info.len() as u64);
            for (stream, ms, seq) in streams_info {
                // save the stream name
                raw::save_string(rdb, &stream);
                // save last read id
                raw::save_unsigned(rdb, ms);
                raw::save_unsigned(rdb, seq);
            }
        }
    }
}

unsafe extern "C" fn aux_load(rdb: *mut raw::RedisModuleIO, encver: c_int, _when: c_int) -> c_int {
    if encver > REDIS_GEARS_VERSION {
        get_ctx().log_notice(&format!(
            "Can not load RedisGears data type version '{}', max supported version '{}'",
            encver, REDIS_GEARS_VERSION
        ));
        return raw::REDISMODULE_ERR as i32;
    }

    let num_of_libs = match raw::load_unsigned(rdb) {
        Ok(n) => n,
        Err(e) => {
            get_ctx().log_notice(&format!("Failed reading number of library from rdb, {}", e));
            return raw::REDISMODULE_ERR as i32;
        }
    };

    for _ in 0..num_of_libs {
        let name = match raw::load_string_buffer(rdb) {
            Ok(s) => match s.to_string() {
                Ok(s) => s,
                Err(e) => {
                    get_ctx()
                        .log_notice(&format!("Failed converting librart name to string, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            },
            Err(e) => {
                get_ctx().log_notice(&format!("Failed reading librart name from rdb, {}", e));
                return raw::REDISMODULE_ERR as i32;
            }
        };
        let code = match raw::load_string_buffer(rdb) {
            Ok(s) => match s.to_string() {
                Ok(s) => s,
                Err(e) => {
                    get_ctx()
                        .log_notice(&format!("Failed converting library code to string, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            },
            Err(e) => {
                get_ctx().log_notice(&format!("Failed reading library code from rdb, {}", e));
                return raw::REDISMODULE_ERR as i32;
            }
        };
        let user = match raw::load_string_buffer(rdb) {
            Ok(s) => match s.to_string() {
                Ok(s) => s,
                Err(e) => {
                    get_ctx()
                        .log_notice(&format!("Failed converting library user to string, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            },
            Err(e) => {
                get_ctx().log_notice(&format!("Failed reading library user from rdb, {}", e));
                return raw::REDISMODULE_ERR as i32;
            }
        };

        // load gears box info
        let has_gears_box_info = match raw::load_unsigned(rdb) {
            Ok(n) => n,
            Err(e) => {
                get_ctx().log_notice(&format!(
                    "Failed reading number of steams consumers from rdb, {}",
                    e
                ));
                return raw::REDISMODULE_ERR as i32;
            }
        };

        let gears_box_info = if has_gears_box_info > 0 {
            let gears_box_info_str = match raw::load_string_buffer(rdb) {
                Ok(s) => match s.to_string() {
                    Ok(s) => s,
                    Err(e) => {
                        get_ctx().log_notice(&format!(
                            "Failed converting library gears box to string, {}",
                            e
                        ));
                        return raw::REDISMODULE_ERR as i32;
                    }
                },
                Err(e) => {
                    get_ctx()
                        .log_notice(&format!("Failed reading library gears box from rdb, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            };
            Some(serde_json::from_str(&gears_box_info_str).unwrap())
        } else {
            None
        };

        match function_load_intrernal(user, &code, false, gears_box_info) {
            Ok(_) => {}
            Err(e) => {
                get_ctx().log_notice(&format!("Failed loading librart, {}", e));
                return raw::REDISMODULE_ERR as i32;
            }
        }

        // library was load, we must be able to find it
        let libraries = get_libraries();
        let lib = libraries.get(&name).unwrap();

        // load stream consumers data
        let num_of_streams_consumers = match raw::load_unsigned(rdb) {
            Ok(n) => n,
            Err(e) => {
                get_ctx().log_notice(&format!(
                    "Failed reading number of steams consumers from rdb, {}",
                    e
                ));
                return raw::REDISMODULE_ERR as i32;
            }
        };

        for _ in 0..num_of_streams_consumers {
            let consumer_name = match raw::load_string_buffer(rdb) {
                Ok(s) => match s.to_string() {
                    Ok(s) => s,
                    Err(e) => {
                        get_ctx().log_notice(&format!(
                            "Failed converting consumer name to string, {}",
                            e
                        ));
                        return raw::REDISMODULE_ERR as i32;
                    }
                },
                Err(e) => {
                    get_ctx().log_notice(&format!("Failed reading consumer name from rdb, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            };
            let consumer = lib
                .gears_lib_ctx
                .stream_consumers
                .get(&consumer_name)
                .unwrap();
            // read the number of streams for this consumer
            let num_of_streams = match raw::load_unsigned(rdb) {
                Ok(n) => n,
                Err(e) => {
                    get_ctx()
                        .log_notice(&format!("Failed reading number of steams from rdb, {}", e));
                    return raw::REDISMODULE_ERR as i32;
                }
            };
            for _ in 0..num_of_streams {
                let stream_name = match raw::load_string_buffer(rdb) {
                    Ok(s) => match s.to_string() {
                        Ok(s) => s,
                        Err(e) => {
                            get_ctx().log_notice(&format!(
                                "Failed converting stream name to string, {}",
                                e
                            ));
                            return raw::REDISMODULE_ERR as i32;
                        }
                    },
                    Err(e) => {
                        get_ctx()
                            .log_notice(&format!("Failed reading stream name from rdb, {}", e));
                        return raw::REDISMODULE_ERR as i32;
                    }
                };
                let ms = match raw::load_unsigned(rdb) {
                    Ok(n) => n,
                    Err(e) => {
                        get_ctx().log_notice(&format!("Failed reading ms from rdb, {}", e));
                        return raw::REDISMODULE_ERR as i32;
                    }
                };
                let seq = match raw::load_unsigned(rdb) {
                    Ok(n) => n,
                    Err(e) => {
                        get_ctx().log_notice(&format!("Failed reading seq from rdb, {}", e));
                        return raw::REDISMODULE_ERR as i32;
                    }
                };
                get_globals_mut().stream_ctx.update_stream_for_consumer(
                    &stream_name,
                    consumer,
                    ms,
                    seq,
                );
            }
        }
    }

    raw::REDISMODULE_OK as i32
}
