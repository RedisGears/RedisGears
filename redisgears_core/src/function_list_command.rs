/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::redisvalue::RedisValueKey;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue};
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;

use std::vec::IntoIter;
use std::{collections::HashMap, iter::Skip};

use crate::{get_libraries, get_msg_verbose, json_to_redis_value};

fn function_list_command_flags(flags: FunctionFlags) -> RedisValue {
    let mut res = Vec::new();
    if flags.contains(FunctionFlags::NO_WRITES) {
        res.push(RedisValue::BulkString("no-writes".to_string()));
    }
    if flags.contains(FunctionFlags::ALLOW_OOM) {
        res.push(RedisValue::BulkString("allow-oom".to_string()));
    }
    if flags.contains(FunctionFlags::RAW_ARGUMENTS) {
        res.push(RedisValue::BulkString("raw-arguments".to_string()));
    }
    RedisValue::Array(res)
}

pub(crate) fn function_list_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut with_code = false;
    let mut lib = None;
    let mut verbosity = 0;
    loop {
        let arg = args.next_arg();
        if arg.is_err() {
            break;
        }
        let arg = arg.unwrap();
        let arg_str = arg
            .try_as_str()
            .map_err(|_| RedisError::Str("Binary option is not allowed"))?;
        let arg_str = arg_str.to_lowercase();
        match arg_str.as_ref() {
            "withcode" => with_code = true,
            "verbose" => verbosity += 1,
            "v" => verbosity += 1,
            "vv" => verbosity += 2,
            "vvv" => verbosity += 3,
            "library" => {
                let lib_name = args
                    .next_arg()
                    .map_err(|_| RedisError::Str("Library name was not given"))?
                    .try_as_str()
                    .map_err(|_| RedisError::Str("Library name is not a string"))?;
                lib = Some(lib_name);
            }
            _ => return Err(RedisError::String(format!("Unknown option '{}'", arg_str))),
        }
    }
    let libraries = get_libraries();
    Ok(RedisValue::Array(
        libraries
            .values()
            .filter(|l| lib.map(|lib_name| l.gears_lib_ctx.meta_data.name == lib_name).unwrap_or(true))
            .map(|l| {
                let mut res =
                    HashMap::from([
                        (
                            RedisValueKey::String("engine".to_string()),
                            RedisValue::BulkString(l.gears_lib_ctx.meta_data.engine.to_string()),
                        ),
                        (
                            RedisValueKey::String("api_version".to_string()),
                            RedisValue::BulkString(l.gears_lib_ctx.meta_data.api_version.to_string()),
                        ),
                        (
                            RedisValueKey::String("name".to_string()),
                            RedisValue::BulkString(l.gears_lib_ctx.meta_data.name.to_string()),
                        ),
                        (
                            RedisValueKey::String("user".to_string()),
                            RedisValue::BulkString(l.gears_lib_ctx.meta_data.user.to_string()),
                        ),
                        (RedisValueKey::String("configuration".to_string()), {
                            l.gears_lib_ctx
                                .meta_data
                                .config
                                .as_ref()
                                .map(|v| RedisValue::BulkString(v.to_string()))
                                .unwrap_or(RedisValue::Null)
                        }),
                        (
                            RedisValueKey::String("pending_jobs".to_string()),
                            RedisValue::Integer(l.compile_lib_internals.pending_jobs() as i64),
                        ),
                        (
                            RedisValueKey::String("functions".to_string()),
                            RedisValue::Array(if verbosity > 0 {
                                l.gears_lib_ctx
                                    .functions
                                    .iter()
                                    .map(|(k, v)| {
                                        RedisValue::Map(HashMap::from([
                                            (
                                                RedisValueKey::String("name".to_string()),
                                                RedisValue::BulkString(k.to_string()),
                                            ),
                                            (
                                                RedisValueKey::String("flags".to_string()),
                                                function_list_command_flags(v.flags),
                                            ),
                                            (
                                                RedisValueKey::String("is_async".to_string()),
                                                RedisValue::Bool(v.is_async),
                                            ),
                                        ]))
                                    })
                                    .collect::<Vec<RedisValue>>()
                            } else {
                                l.gears_lib_ctx
                                    .functions
                                    .keys()
                                    .map(|k| RedisValue::BulkString(k.to_string()))
                                    .collect::<Vec<RedisValue>>()
                            }),
                        ),
                        (
                            RedisValueKey::String("remote_functions".to_string()),
                            RedisValue::Array(
                                l.gears_lib_ctx
                                    .remote_functions
                                    .keys()
                                    .map(|k| RedisValue::BulkString(k.to_string()))
                                    .collect::<Vec<RedisValue>>(),
                            ),
                        ),
                        (
                            RedisValueKey::String("stream_consumers".to_string()),
                            RedisValue::Array(
                                l.gears_lib_ctx
                                    .stream_consumers
                                    .iter()
                                    .map(|(k, v)| {
                                        let v = v.ref_cell.borrow();
                                        if verbosity > 0 {
                                            let mut res = HashMap::from([
                                                (
                                                    RedisValueKey::String("name".to_string()),
                                                    RedisValue::BulkString(k.to_string()),
                                                ),
                                                (
                                                    RedisValueKey::String("prefix".to_string()),
                                                    RedisValue::BulkRedisString(
                                                        ctx.create_string(v.prefix.as_slice()),
                                                    ),
                                                ),
                                                (
                                                    RedisValueKey::String("window".to_string()),
                                                    RedisValue::Integer(v.window as i64),
                                                ),
                                                (
                                                    RedisValueKey::String("trim".to_string()),
                                                    RedisValue::BulkString(
                                                        (if v.trim {
                                                            "enabled"
                                                        } else {
                                                            "disabled"
                                                        })
                                                        .to_string(),
                                                    ),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "num_streams".to_string(),
                                                    ),
                                                    RedisValue::Integer(
                                                        v.consumed_streams.len() as i64
                                                    ),
                                                ),
                                            ]);
                                            if verbosity > 1 {
                                                res.insert(
                                                    RedisValueKey::String("streams".to_string()),
                                                    RedisValue::Array(
                                                        v.consumed_streams
                                                            .iter()
                                                            .map(|(s, v)| {
                                                                let v = v.ref_cell.borrow();
                                                                let mut res = HashMap::from([
                                                                    (
                                                                        RedisValueKey::String(
                                                                            "name".to_string(),
                                                                        ),
                                                                        RedisValue::BulkRedisString(
                                                                            ctx.create_string(
                                                                                s.as_slice(),
                                                                            ),
                                                                        ),
                                                                    ),
                                                                    (
                                                                        RedisValueKey::String(
                                                                            "last_processed_time"
                                                                                .to_string(),
                                                                        ),
                                                                        RedisValue::Integer(
                                                                            v.last_processed_time
                                                                                as i64,
                                                                        ),
                                                                    ),
                                                                    (RedisValueKey::String(
                                                                        "avg_processed_time"
                                                                            .to_string(),
                                                                    ),
                                                                    RedisValue::Float(
                                                                        v.total_processed_time
                                                                            as f64
                                                                            / v.records_processed
                                                                                as f64,
                                                                    )),
                                                                    (RedisValueKey::String(
                                                                        "last_lag".to_string(),
                                                                    ),
                                                                    RedisValue::Integer(
                                                                        v.last_lag as i64,
                                                                    )),
                                                                    (RedisValueKey::String(
                                                                        "avg_lag".to_string(),
                                                                    ),
                                                                    RedisValue::Float(
                                                                        v.total_lag as f64
                                                                            / v.records_processed
                                                                                as f64,
                                                                    )),
                                                                    (RedisValueKey::String(
                                                                        "total_record_processed"
                                                                            .to_string(),
                                                                    ),
                                                                    RedisValue::Integer(
                                                                        v.records_processed as i64,
                                                                    )),
                                                                    (RedisValueKey::String(
                                                                        "id_to_read_from"
                                                                            .to_string(),
                                                                    ),
                                                                    RedisValue::BulkString(v.last_read_id
                                                                        .map(|id| format!("{}-{}",id.ms, id.seq)).
                                                                        unwrap_or_else(|| "None".to_string()))),
                                                                    (RedisValueKey::String(
                                                                        "last_error".to_string(),
                                                                    ),
                                                                    RedisValue::BulkString(v.last_error.as_ref()
                                                                        .map(|v| get_msg_verbose(v).to_string())
                                                                        .unwrap_or_else(|| "None".to_string()))),
                                                                ]);
                                                                if verbosity > 2 {
                                                                    let pending_ids = v
                                                                        .pending_ids
                                                                        .iter()
                                                                        .map(|e| {
                                                                            RedisValue::BulkString(
                                                                                format!(
                                                                                    "{}-{}",
                                                                                    e.ms, e.seq
                                                                                ),
                                                                            )
                                                                        })
                                                                        .collect::<Vec<RedisValue>>(
                                                                        );
                                                                    res.insert(
                                                                        RedisValueKey::String(
                                                                            "pending_ids"
                                                                                .to_string(),
                                                                        ),
                                                                        RedisValue::Array(
                                                                            pending_ids,
                                                                        ),
                                                                    );
                                                                }
                                                                RedisValue::Map(res)
                                                            })
                                                            .collect::<Vec<RedisValue>>(),
                                                    ),
                                                );
                                            }
                                            RedisValue::Map(res)
                                        } else {
                                            RedisValue::BulkString(k.to_string())
                                        }
                                    })
                                    .collect::<Vec<RedisValue>>(),
                            ),
                        ),
                        (
                            RedisValueKey::String("notifications_consumers".to_string()),
                            RedisValue::Array(
                                l.gears_lib_ctx
                                    .notifications_consumers
                                    .iter()
                                    .map(|(name, c)| {
                                        if verbosity == 0 {
                                            RedisValue::BulkString(name.to_string())
                                        } else {
                                            let stats = c.borrow().get_stats();
                                            RedisValue::Map(HashMap::from([
                                                (
                                                    RedisValueKey::String("name".to_string()),
                                                    RedisValue::BulkString(name.to_string()),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "num_triggered".to_string(),
                                                    ),
                                                    RedisValue::Integer(stats.num_trigger as i64),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "num_finished".to_string(),
                                                    ),
                                                    RedisValue::Integer(stats.num_finished as i64),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "num_success".to_string(),
                                                    ),
                                                    RedisValue::Integer(stats.num_success as i64),
                                                ),
                                                (
                                                    RedisValueKey::String("num_failed".to_string()),
                                                    RedisValue::Integer(stats.num_failed as i64),
                                                ),
                                                (
                                                    RedisValueKey::String("last_error".to_string()),
                                                    RedisValue::BulkString(
                                                        stats
                                                            .last_error
                                                            .as_ref()
                                                            .map(|v| get_msg_verbose(v).to_string())
                                                            .unwrap_or_else(|| "None".to_string()),
                                                    ),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "last_exection_time".to_string(),
                                                    ),
                                                    RedisValue::Integer(
                                                        stats.last_execution_time as i64,
                                                    ),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "total_exection_time".to_string(),
                                                    ),
                                                    RedisValue::Integer(
                                                        stats.total_execution_time as i64,
                                                    ),
                                                ),
                                                (
                                                    RedisValueKey::String(
                                                        "avg_exection_time".to_string(),
                                                    ),
                                                    RedisValue::Float(
                                                        stats.total_execution_time as f64
                                                            / stats.num_finished as f64,
                                                    ),
                                                ),
                                            ]))
                                        }
                                    })
                                    .collect::<Vec<RedisValue>>(),
                            ),
                        ),
                    ]);
                if with_code {
                    res.insert(
                        RedisValueKey::String("code".to_string()),
                        RedisValue::BulkString(l.gears_lib_ctx.meta_data.code.to_string()),
                    );
                }
                if verbosity > 0 {
                    let gears_box_info_str = serde_json::to_string(&l.gears_box_lib).unwrap();
                    res.insert(
                        RedisValueKey::String("gears_box_info".to_string()),
                        json_to_redis_value(serde_json::from_str(&gears_box_info_str).unwrap()),
                    );
                }
                RedisValue::Map(res)
            })
            .collect::<Vec<RedisValue>>(),
    ))
}
