/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue};

use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::FUNCTION_FLAG_ALLOW_OOM, load_library_ctx::FUNCTION_FLAG_NO_WRITES,
    load_library_ctx::FUNCTION_FLAG_RAW_ARGUMENTS,
};

use std::iter::Skip;
use std::vec::IntoIter;

use crate::{get_libraries, get_msg_verbose, json_to_redis_value};

fn function_list_command_flags(flags: u8) -> RedisValue {
    let mut res = Vec::new();
    if (flags & FUNCTION_FLAG_NO_WRITES) != 0 {
        res.push(RedisValue::BulkString("no-writes".to_string()));
    }
    if (flags & FUNCTION_FLAG_ALLOW_OOM) != 0 {
        res.push(RedisValue::BulkString("allow-oom".to_string()));
    }
    if (flags & FUNCTION_FLAG_RAW_ARGUMENTS) != 0 {
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
        let arg_str = match arg.try_as_str() {
            Ok(arg) => arg,
            Err(_) => return Err(RedisError::Str("Binary option is not allowed")),
        };
        let arg_str = arg_str.to_lowercase();
        match arg_str.as_ref() {
            "withcode" => with_code = true,
            "verbose" => verbosity += 1,
            "v" => verbosity += 1,
            "vv" => verbosity += 2,
            "vvv" => verbosity += 3,
            "library" => {
                let lib_name = match args.next_arg() {
                    Ok(n) => match n.try_as_str() {
                        Ok(n) => n,
                        Err(_) => return Err(RedisError::Str("Library name is not a string")),
                    },
                    Err(_) => return Err(RedisError::Str("Library name was not given")),
                };
                lib = Some(lib_name);
            }
            _ => return Err(RedisError::String(format!("Unknown option '{}'", arg_str))),
        }
    }
    let libraries = get_libraries();
    Ok(RedisValue::Array(
        libraries
            .values()
            .filter(|l| match lib {
                Some(lib_name) => l.gears_lib_ctx.meta_data.name == lib_name,
                None => true,
            })
            .map(|l| {
                let mut res = vec![
                    RedisValue::BulkString("engine".to_string()),
                    RedisValue::BulkString(l.gears_lib_ctx.meta_data.engine.to_string()),
                    RedisValue::BulkString("name".to_string()),
                    RedisValue::BulkString(l.gears_lib_ctx.meta_data.name.to_string()),
                    RedisValue::BulkString("user".to_string()),
                    RedisValue::BulkString(l.gears_lib_ctx.meta_data.user.to_string()),
                    RedisValue::BulkString("configuration".to_string()),
                    {
                        match l.gears_lib_ctx.meta_data.config.as_ref() {
                            Some(c) => RedisValue::BulkString(c.to_string()),
                            None => RedisValue::Null,
                        }
                    },
                    RedisValue::BulkString("pending_jobs".to_string()),
                    RedisValue::Integer(l.compile_lib_internals.pending_jobs() as i64),
                    RedisValue::BulkString("functions".to_string()),
                    RedisValue::Array(if verbosity > 0 {
                        l.gears_lib_ctx
                            .functions
                            .iter()
                            .map(|(k, v)| {
                                RedisValue::Array(vec![
                                    RedisValue::BulkString("name".to_string()),
                                    RedisValue::BulkString(k.to_string()),
                                    RedisValue::BulkString("flags".to_string()),
                                    function_list_command_flags(v.flags),
                                ])
                            })
                            .collect::<Vec<RedisValue>>()
                    } else {
                        l.gears_lib_ctx
                            .functions
                            .keys()
                            .map(|k| RedisValue::BulkString(k.to_string()))
                            .collect::<Vec<RedisValue>>()
                    }),
                    RedisValue::BulkString("remote_functions".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .remote_functions
                            .keys()
                            .map(|k| RedisValue::BulkString(k.to_string()))
                            .collect::<Vec<RedisValue>>(),
                    ),
                    RedisValue::BulkString("stream_consumers".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .stream_consumers
                            .iter()
                            .map(|(k, v)| {
                                let v = v.ref_cell.borrow();
                                if verbosity > 0 {
                                    let mut res = vec![
                                        RedisValue::BulkString("name".to_string()),
                                        RedisValue::BulkString(k.to_string()),
                                        RedisValue::BulkString("prefix".to_string()),
                                        RedisValue::BulkRedisString(
                                            ctx.create_string_from_slice(&v.prefix),
                                        ),
                                        RedisValue::BulkString("window".to_string()),
                                        RedisValue::Integer(v.window as i64),
                                        RedisValue::BulkString("trim".to_string()),
                                        RedisValue::BulkString(
                                            (if v.trim { "enabled" } else { "disabled" })
                                                .to_string(),
                                        ),
                                        RedisValue::BulkString("num_streams".to_string()),
                                        RedisValue::Integer(v.consumed_streams.len() as i64),
                                    ];
                                    if verbosity > 1 {
                                        res.push(RedisValue::BulkString("streams".to_string()));
                                        res.push(RedisValue::Array(
                                            v.consumed_streams
                                                .iter()
                                                .map(|(s, v)| {
                                                    let v = v.ref_cell.borrow();
                                                    let mut res = vec![
                                                        RedisValue::BulkString("name".to_string()),
                                                        RedisValue::BulkRedisString(
                                                            ctx.create_string_from_slice(s),
                                                        ),
                                                        RedisValue::BulkString(
                                                            "last_processed_time".to_string(),
                                                        ),
                                                        RedisValue::Integer(
                                                            v.last_processed_time as i64,
                                                        ),
                                                        RedisValue::BulkString(
                                                            "avg_processed_time".to_string(),
                                                        ),
                                                        RedisValue::Float(
                                                            v.total_processed_time as f64
                                                                / v.records_processed as f64,
                                                        ),
                                                        RedisValue::BulkString(
                                                            "last_lag".to_string(),
                                                        ),
                                                        RedisValue::Integer(v.last_lag as i64),
                                                        RedisValue::BulkString(
                                                            "avg_lag".to_string(),
                                                        ),
                                                        RedisValue::Float(
                                                            v.total_lag as f64
                                                                / v.records_processed as f64,
                                                        ),
                                                        RedisValue::BulkString(
                                                            "total_record_processed".to_string(),
                                                        ),
                                                        RedisValue::Integer(
                                                            v.records_processed as i64,
                                                        ),
                                                        RedisValue::BulkString(
                                                            "id_to_read_from".to_string(),
                                                        ),
                                                        match v.last_read_id {
                                                            Some(id) => RedisValue::BulkString(
                                                                format!("{}-{}", id.ms, id.seq),
                                                            ),
                                                            None => RedisValue::BulkString(
                                                                "None".to_string(),
                                                            ),
                                                        },
                                                        RedisValue::BulkString(
                                                            "last_error".to_string(),
                                                        ),
                                                        match &v.last_error {
                                                            Some(err) => RedisValue::BulkString(
                                                                get_msg_verbose(err).to_string(),
                                                            ),
                                                            None => RedisValue::BulkString(
                                                                "None".to_string(),
                                                            ),
                                                        },
                                                    ];
                                                    if verbosity > 2 {
                                                        res.push(RedisValue::BulkString(
                                                            "pending_ids".to_string(),
                                                        ));
                                                        let pending_ids = v
                                                            .pending_ids
                                                            .iter()
                                                            .map(|e| {
                                                                RedisValue::BulkString(format!(
                                                                    "{}-{}",
                                                                    e.ms, e.seq
                                                                ))
                                                            })
                                                            .collect::<Vec<RedisValue>>();
                                                        res.push(RedisValue::Array(pending_ids));
                                                    }
                                                    RedisValue::Array(res)
                                                })
                                                .collect::<Vec<RedisValue>>(),
                                        ));
                                    }
                                    RedisValue::Array(res)
                                } else {
                                    RedisValue::BulkString(k.to_string())
                                }
                            })
                            .collect::<Vec<RedisValue>>(),
                    ),
                    RedisValue::BulkString("notifications_consumers".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .notifications_consumers
                            .iter()
                            .map(|(name, c)| {
                                if verbosity == 0 {
                                    RedisValue::BulkString(name.to_string())
                                } else {
                                    let stats = c.borrow().get_stats();
                                    RedisValue::Array(vec![
                                        RedisValue::BulkString("name".to_string()),
                                        RedisValue::BulkString(name.to_string()),
                                        RedisValue::BulkString("num_triggered".to_string()),
                                        RedisValue::Integer(stats.num_trigger as i64),
                                        RedisValue::BulkString("num_finished".to_string()),
                                        RedisValue::Integer(stats.num_finished as i64),
                                        RedisValue::BulkString("num_success".to_string()),
                                        RedisValue::Integer(stats.num_success as i64),
                                        RedisValue::BulkString("num_failed".to_string()),
                                        RedisValue::Integer(stats.num_failed as i64),
                                        RedisValue::BulkString("last_error".to_string()),
                                        RedisValue::BulkString(match stats.last_error {
                                            Some(s) => get_msg_verbose(&s).to_string(),
                                            None => "None".to_string(),
                                        }),
                                        RedisValue::BulkString("last_exection_time".to_string()),
                                        RedisValue::Integer(stats.last_execution_time as i64),
                                        RedisValue::BulkString("total_exection_time".to_string()),
                                        RedisValue::Integer(stats.total_execution_time as i64),
                                        RedisValue::BulkString("avg_exection_time".to_string()),
                                        RedisValue::Float(
                                            stats.total_execution_time as f64
                                                / stats.num_finished as f64,
                                        ),
                                    ])
                                }
                            })
                            .collect::<Vec<RedisValue>>(),
                    ),
                ];
                if with_code {
                    res.push(RedisValue::BulkString("code".to_string()));
                    res.push(RedisValue::BulkString(
                        l.gears_lib_ctx.meta_data.code.to_string(),
                    ));
                }
                if verbosity > 0 {
                    res.push(RedisValue::BulkString("gears_box_info".to_string()));
                    let gears_box_info_str = serde_json::to_string(&l.gears_box_lib).unwrap();
                    res.push(json_to_redis_value(
                        serde_json::from_str(&gears_box_info_str).unwrap(),
                    ));
                }
                RedisValue::Array(res)
            })
            .collect::<Vec<RedisValue>>(),
    ))
}
