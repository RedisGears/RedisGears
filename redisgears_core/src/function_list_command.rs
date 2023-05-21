/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::redisvalue::RedisValueKey;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::FunctionFlags;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::gears_box::GearsBoxLibraryInfo;
use crate::keys_notifications::NotificationConsumerStats;
use crate::stream_reader::ConsumerInfo;
use crate::{get_libraries, get_msg_verbose, json_to_redis_value, GearsLibrary};

/// Contains information about a single stream that tracked
/// by a stream trigger.
struct StreamInfo {
    name: Vec<u8>,
    info: ConsumerInfo,
}

/// A struct that allows to translate a [StreamInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level.
struct RequestedStreamInfo {
    stream_info: StreamInfo,
    verbosity_level: usize,
}

impl From<RequestedStreamInfo> for RedisValue {
    fn from(value: RequestedStreamInfo) -> Self {
        let mut res = BTreeMap::from([
            (
                RedisValueKey::String("name".to_string()),
                RedisValue::StringBuffer(value.stream_info.name),
            ),
            (
                RedisValueKey::String("last_processed_time".to_string()),
                RedisValue::Integer(value.stream_info.info.last_processed_time as i64),
            ),
            (
                RedisValueKey::String("avg_processed_time".to_string()),
                RedisValue::Float(
                    value.stream_info.info.total_processed_time as f64
                        / value.stream_info.info.records_processed as f64,
                ),
            ),
            (
                RedisValueKey::String("last_lag".to_string()),
                RedisValue::Integer(value.stream_info.info.last_lag as i64),
            ),
            (
                RedisValueKey::String("avg_lag".to_string()),
                RedisValue::Float(
                    value.stream_info.info.total_lag as f64
                        / value.stream_info.info.records_processed as f64,
                ),
            ),
            (
                RedisValueKey::String("total_record_processed".to_string()),
                RedisValue::Integer(value.stream_info.info.records_processed as i64),
            ),
            (
                RedisValueKey::String("id_to_read_from".to_string()),
                value
                    .stream_info
                    .info
                    .last_read_id
                    .map(|id| RedisValue::BulkString(format!("{}-{}", id.ms, id.seq)))
                    .unwrap_or_else(|| RedisValue::Null),
            ),
            (
                RedisValueKey::String("last_error".to_string()),
                value
                    .stream_info
                    .info
                    .last_error
                    .as_ref()
                    .map(|v| RedisValue::BulkString(get_msg_verbose(v).to_string()))
                    .unwrap_or_else(|| RedisValue::Null),
            ),
        ]);

        if value.verbosity_level > 2 {
            let pending_ids = value
                .stream_info
                .info
                .pending_ids
                .iter()
                .map(|e| RedisValue::BulkString(format!("{}-{}", e.ms, e.seq)))
                .collect::<Vec<RedisValue>>();
            res.insert(
                RedisValueKey::String("pending_ids".to_string()),
                RedisValue::Array(pending_ids),
            );
        }

        RedisValue::OrderedMap(res)
    }
}

/// Contains all relevant information about RedisGears stream trigger.
struct StreamTriggersInfo {
    name: String,
    prefix: Vec<u8>,
    window: usize,
    trim: bool,
    streams: Vec<StreamInfo>,
}

/// A struct that allows to translate a [StreamTriggersInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level.
struct RequestedStreamTriggersInfo {
    stream_info: StreamTriggersInfo,
    verbosity_level: usize,
}

impl From<RequestedStreamTriggersInfo> for RedisValue {
    fn from(value: RequestedStreamTriggersInfo) -> Self {
        if value.verbosity_level == 0 {
            return RedisValue::BulkString(value.stream_info.name);
        }

        let mut res = BTreeMap::from([
            (
                RedisValueKey::String("name".to_string()),
                RedisValue::BulkString(value.stream_info.name),
            ),
            (
                RedisValueKey::String("prefix".to_string()),
                RedisValue::StringBuffer(value.stream_info.prefix),
            ),
            (
                RedisValueKey::String("window".to_string()),
                RedisValue::Integer(value.stream_info.window as i64),
            ),
            (
                RedisValueKey::String("trim".to_string()),
                RedisValue::BulkString(
                    (if value.stream_info.trim {
                        "enabled"
                    } else {
                        "disabled"
                    })
                    .to_string(),
                ),
            ),
            (
                RedisValueKey::String("num_streams".to_string()),
                RedisValue::Integer(value.stream_info.streams.len() as i64),
            ),
        ]);

        if value.verbosity_level > 1 {
            res.insert(
                RedisValueKey::String("streams".to_string()),
                RedisValue::Array(
                    value
                        .stream_info
                        .streams
                        .into_iter()
                        .map(|v| {
                            RequestedStreamInfo {
                                stream_info: v,
                                verbosity_level: value.verbosity_level,
                            }
                            .into()
                        })
                        .collect(),
                ),
            );
        }

        RedisValue::OrderedMap(res)
    }
}

/// Contains all relevant information about RedisGears key space trigger.
struct TriggersInfo {
    name: String,
    stats: NotificationConsumerStats,
}

impl From<TriggersInfo> for RedisValue {
    fn from(value: TriggersInfo) -> Self {
        RedisValue::OrderedMap(BTreeMap::from([
            (
                RedisValueKey::String("name".to_string()),
                RedisValue::BulkString(value.name.to_string()),
            ),
            (
                RedisValueKey::String("num_triggered".to_string()),
                RedisValue::Integer(value.stats.num_trigger as i64),
            ),
            (
                RedisValueKey::String("num_finished".to_string()),
                RedisValue::Integer(value.stats.num_finished as i64),
            ),
            (
                RedisValueKey::String("num_success".to_string()),
                RedisValue::Integer(value.stats.num_success as i64),
            ),
            (
                RedisValueKey::String("num_failed".to_string()),
                RedisValue::Integer(value.stats.num_failed as i64),
            ),
            (
                RedisValueKey::String("last_error".to_string()),
                value
                    .stats
                    .last_error
                    .as_ref()
                    .map(|v| RedisValue::BulkString(get_msg_verbose(v).to_string()))
                    .unwrap_or_else(|| RedisValue::Null),
            ),
            (
                RedisValueKey::String("last_exection_time".to_string()),
                RedisValue::Integer(value.stats.last_execution_time as i64),
            ),
            (
                RedisValueKey::String("total_exection_time".to_string()),
                RedisValue::Integer(value.stats.total_execution_time as i64),
            ),
            (
                RedisValueKey::String("avg_exection_time".to_string()),
                RedisValue::Float(
                    value.stats.total_execution_time as f64 / value.stats.num_finished as f64,
                ),
            ),
        ]))
    }
}

/// Contains all relevant information about RedisGears clustered function.
struct ClusterFunctionInfo {
    name: String,
}

/// Contains all relevant information about RedisGears function.
struct FunctionInfo {
    name: String,
    flags: FunctionFlags,
    is_async: bool,
}

/// A struct that allows to translate a [RequestedFunctionInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level.
struct RequestedFunctionInfo {
    function_info: FunctionInfo,
    verbosity_level: usize,
}

impl From<RequestedFunctionInfo> for RedisValue {
    fn from(value: RequestedFunctionInfo) -> Self {
        if value.verbosity_level > 0 {
            RedisValue::OrderedMap(BTreeMap::from([
                (
                    RedisValueKey::String("name".to_string()),
                    RedisValue::BulkString(value.function_info.name),
                ),
                (
                    RedisValueKey::String("flags".to_string()),
                    function_list_command_flags(value.function_info.flags),
                ),
                (
                    RedisValueKey::String("is_async".to_string()),
                    RedisValue::Bool(value.function_info.is_async),
                ),
            ]))
        } else {
            RedisValue::BulkString(value.function_info.name)
        }
    }
}

/// Contains all relevant information about RedisGears library.
struct LibararyInfo {
    engine: String,
    api_version: String,
    name: String,
    user: String,
    configuration: Option<String>,
    code: String,
    gears_box_info: Option<GearsBoxLibraryInfo>,
    pending_jobs: usize,
    functions: Vec<FunctionInfo>,
    cluster_functions: Vec<ClusterFunctionInfo>,
    triggers: Vec<TriggersInfo>,
    stream_triggers: Vec<StreamTriggersInfo>,
}

/// A struct that allows to translate a [LibararyInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level and the `withcode` parameter.
struct RequestedLibararyInfo {
    lib_info: LibararyInfo,
    verbosity_level: usize,
    with_code: bool,
}

impl From<RequestedLibararyInfo> for RedisValue {
    fn from(value: RequestedLibararyInfo) -> Self {
        let mut res = BTreeMap::from([
            (
                RedisValueKey::String("engine".to_string()),
                RedisValue::BulkString(value.lib_info.engine),
            ),
            (
                RedisValueKey::String("api_version".to_string()),
                RedisValue::BulkString(value.lib_info.api_version),
            ),
            (
                RedisValueKey::String("name".to_string()),
                RedisValue::BulkString(value.lib_info.name),
            ),
            (
                RedisValueKey::String("user".to_string()),
                RedisValue::BulkString(value.lib_info.user),
            ),
            (
                RedisValueKey::String("configuration".to_string()),
                value
                    .lib_info
                    .configuration
                    .map(|v| RedisValue::BulkString(v))
                    .unwrap_or(RedisValue::Null),
            ),
            (
                RedisValueKey::String("pending_jobs".to_string()),
                RedisValue::Integer(value.lib_info.pending_jobs as i64),
            ),
            (
                RedisValueKey::String("functions".to_string()),
                RedisValue::Array(
                    value
                        .lib_info
                        .functions
                        .into_iter()
                        .map(|v| {
                            RequestedFunctionInfo {
                                function_info: v,
                                verbosity_level: value.verbosity_level,
                            }
                            .into()
                        })
                        .collect(),
                ),
            ),
            (
                RedisValueKey::String("cluster_functions".to_string()),
                RedisValue::Array(
                    value
                        .lib_info
                        .cluster_functions
                        .into_iter()
                        .map(|v| RedisValue::BulkString(v.name))
                        .collect(),
                ),
            ),
            (
                RedisValueKey::String("stream_triggers".to_string()),
                RedisValue::Array(
                    value
                        .lib_info
                        .stream_triggers
                        .into_iter()
                        .map(|v| {
                            RequestedStreamTriggersInfo {
                                stream_info: v,
                                verbosity_level: value.verbosity_level,
                            }
                            .into()
                        })
                        .collect(),
                ),
            ),
            (
                RedisValueKey::String("triggers".to_string()),
                RedisValue::Array(
                    value
                        .lib_info
                        .triggers
                        .into_iter()
                        .map(|v| v.into())
                        .collect(),
                ),
            ),
        ]);
        if value.with_code {
            res.insert(
                RedisValueKey::String("code".to_string()),
                RedisValue::BulkString(value.lib_info.code),
            );
        }
        if value.verbosity_level > 0 {
            let gears_box_info_str = serde_json::to_string(&value.lib_info.gears_box_info).unwrap();
            res.insert(
                RedisValueKey::String("gears_box_info".to_string()),
                json_to_redis_value(serde_json::from_str(&gears_box_info_str).unwrap()),
            );
        }
        RedisValue::OrderedMap(res)
    }
}

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

/// Creates a [LibararyInfo] object that can be used to
/// transform into [RedisValue] in order to generate the
/// `FUNCTION LIST` reply. In the future we can reuse this
/// object to return the information on other locations
/// (for example, on Redis `INFO` command).
fn get_library_info(lib: &Arc<GearsLibrary>) -> LibararyInfo {
    LibararyInfo {
        engine: lib.gears_lib_ctx.meta_data.engine.to_owned(),
        api_version: lib.gears_lib_ctx.meta_data.api_version.to_string(),
        name: lib.gears_lib_ctx.meta_data.name.to_owned(),
        user: lib.gears_lib_ctx.meta_data.user.to_string_lossy(),
        configuration: lib.gears_lib_ctx.meta_data.config.clone(),
        code: lib.gears_lib_ctx.meta_data.code.to_owned(),
        gears_box_info: lib.gears_box_lib.clone(),
        pending_jobs: lib.compile_lib_internals.pending_jobs(),
        functions: lib
            .gears_lib_ctx
            .functions
            .iter()
            .map(|(name, val)| FunctionInfo {
                name: name.to_owned(),
                flags: val.flags,
                is_async: val.is_async,
            })
            .collect(),
        cluster_functions: lib
            .gears_lib_ctx
            .remote_functions
            .keys()
            .map(|name| ClusterFunctionInfo {
                name: name.to_owned(),
            })
            .collect(),
        triggers: lib
            .gears_lib_ctx
            .notifications_consumers
            .iter()
            .map(|(name, val)| {
                let stats = val.borrow().get_stats();
                TriggersInfo {
                    name: name.to_owned(),
                    stats: stats.clone(),
                }
            })
            .collect(),
        stream_triggers: lib
            .gears_lib_ctx
            .stream_consumers
            .iter()
            .map(|(name, val)| {
                let val = val.ref_cell.borrow();
                StreamTriggersInfo {
                    name: name.to_owned(),
                    prefix: val.prefix.clone(),
                    window: val.window,
                    trim: val.trim,
                    streams: val
                        .consumed_streams
                        .iter()
                        .map(|(name, val)| {
                            let val = val.ref_cell.borrow();
                            StreamInfo {
                                name: name.clone(),
                                info: val.clone(),
                            }
                        })
                        .collect(),
                }
            })
            .collect(),
    }
}

/// Implementation for `FUNCTION LIST` command.
pub(crate) fn function_list_command<T: Iterator<Item = RedisString>>(
    _ctx: &Context,
    mut args: T,
) -> RedisResult {
    let mut with_code = false;
    let mut lib = None;
    let mut verbosity_level = 0;
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
            "verbose" => verbosity_level += 1,
            "v" => verbosity_level += 1,
            "vv" => verbosity_level += 2,
            "vvv" => verbosity_level += 3,
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
            .filter(|l| {
                lib.map(|lib_name| l.gears_lib_ctx.meta_data.name == lib_name)
                    .unwrap_or(true)
            })
            .map(|l| {
                let lib_info = get_library_info(l);
                RequestedLibararyInfo {
                    lib_info,
                    verbosity_level,
                    with_code,
                }
                .into()
            })
            .collect::<Vec<RedisValue>>(),
    ))
}
