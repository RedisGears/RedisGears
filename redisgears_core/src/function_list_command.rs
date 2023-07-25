/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use redis_module_macros::RedisValue;
use redisgears_plugin_api::redisgears_plugin_api::load_library_ctx::{
    FunctionFlags, FUNCTION_FLAG_ALLOW_OOM_GLOBAL_VALUE, FUNCTION_FLAG_NO_WRITES_GLOBAL_VALUE,
    FUNCTION_FLAG_RAW_ARGUMENTS_GLOBAL_VALUE,
};
use std::sync::Arc;

use crate::{get_globals, get_libraries, get_msg_verbose, GearsLibrary};

/// Contains information about a single stream that tracked
/// by a stream trigger.
#[derive(RedisValue)]
struct StreamInfo {
    name: Vec<u8>,
    last_processed_time: usize,    // last processed time in ms
    total_processed_time: usize,   // last processed time in ms
    last_lag: usize,               // last lag in ms
    total_lag: usize,              // average lag in ms
    total_record_processed: usize, // average lag in ms
    pending_ids: Vec<String>,
    id_to_read_from: Option<String>,
    last_error: Option<String>,
}

#[derive(RedisValue)]
struct StreamTriggersInfoVerbose2 {
    #[RedisValueAttr{flatten: true}]
    stream_trigger_info: StreamTriggersInfoVerbose1,
    streams: Vec<StreamInfo>,
}

/// Contains all relevant information about RedisGears stream trigger.
#[derive(RedisValue)]
struct StreamTriggersInfoVerbose1 {
    name: String,
    prefix: Vec<u8>,
    window: usize,
    trim: bool,
    description: Option<String>,
}

/// A struct that allows to translate a [StreamTriggersInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level.
#[derive(RedisValue)]
enum StreamTriggersInfo {
    Verbose0(String),
    Verbose1(StreamTriggersInfoVerbose1),
    Verbose2(StreamTriggersInfoVerbose2),
}

/// Contains all relevant information about RedisGears key space trigger.
#[derive(RedisValue)]
struct TriggersInfoVerbose1 {
    name: String,
    description: Option<String>,
    num_trigger: usize,
    num_success: usize,
    num_failed: usize,
    num_finished: usize,
    last_error: Option<String>,
    last_execution_time: usize,
    total_execution_time: usize,
}

#[derive(RedisValue)]
enum TriggersInfo {
    Verbose0(String),
    Verbose1(TriggersInfoVerbose1),
}

/// Contains all relevant information about RedisGears function.
#[derive(RedisValue)]
struct FunctionInfoVerbose {
    name: String,
    flags: RedisValue,
    is_async: bool,
    description: Option<String>,
}

/// A struct that allows to translate a [RequestedFunctionInfo] into
/// [RedisValue] while taking into consideration the requested
/// verbosity level.
#[derive(RedisValue)]
enum FunctionInfo {
    Verbose0(String),
    Verbose1(FunctionInfoVerbose),
}

#[derive(RedisValue)]
struct LibraryInfoWithoutCode {
    engine: String,
    api_version: String,
    name: String,
    user: String,
    configuration: Option<String>,
    pending_jobs: usize,
    functions: Vec<FunctionInfo>,
    cluster_functions: Vec<String>,
    keyspace_triggers: Vec<TriggersInfo>,
    stream_triggers: Vec<StreamTriggersInfo>,
    pending_async_calls: Vec<String>,
}

/// Contains all relevant information about RedisGears library.
#[derive(RedisValue)]
struct LibraryInfoWithCode {
    code: String,
    #[RedisValueAttr{flatten: true}]
    lib_info: LibraryInfoWithoutCode,
}

enum LibraryInfo {
    WithCode(LibraryInfoWithCode),
    WithoutCode(LibraryInfoWithoutCode),
}

impl From<LibraryInfo> for RedisValue {
    fn from(value: LibraryInfo) -> Self {
        match value {
            LibraryInfo::WithCode(l) => l.into(),
            LibraryInfo::WithoutCode(l) => l.into(),
        }
    }
}

fn function_list_command_flags(flags: FunctionFlags) -> RedisValue {
    let mut res = Vec::new();
    if flags.contains(FunctionFlags::NO_WRITES) {
        res.push(RedisValue::BulkString(
            FUNCTION_FLAG_NO_WRITES_GLOBAL_VALUE.to_string(),
        ));
    }
    if flags.contains(FunctionFlags::ALLOW_OOM) {
        res.push(RedisValue::BulkString(
            FUNCTION_FLAG_ALLOW_OOM_GLOBAL_VALUE.to_string(),
        ));
    }
    if flags.contains(FunctionFlags::RAW_ARGUMENTS) {
        res.push(RedisValue::BulkString(
            FUNCTION_FLAG_RAW_ARGUMENTS_GLOBAL_VALUE.to_string(),
        ));
    }
    RedisValue::Array(res)
}

/// Creates a [LibraryInfo] object that can be used to
/// transform into [RedisValue] in order to generate the
/// `FUNCTION LIST` reply. In the future we can reuse this
/// object to return the information on other locations
/// (for example, on Redis `INFO` command).
fn get_library_info(
    ctx: &Context,
    lib: &Arc<GearsLibrary>,
    verbosity_level: usize,
    with_code: bool,
) -> LibraryInfo {
    let lib_info = LibraryInfoWithoutCode {
        engine: lib.gears_lib_ctx.meta_data.engine.to_owned(),
        api_version: lib.gears_lib_ctx.meta_data.api_version.to_string(),
        name: lib.gears_lib_ctx.meta_data.name.to_owned(),
        user: lib.gears_lib_ctx.meta_data.user.to_string_lossy(),
        configuration: lib.gears_lib_ctx.meta_data.config.clone(),
        pending_jobs: lib.compile_lib_internals.pending_jobs(),
        functions: lib
            .gears_lib_ctx
            .functions
            .iter()
            .map(|(name, val)| {
                if verbosity_level == 0 {
                    FunctionInfo::Verbose0(name.to_owned())
                } else {
                    FunctionInfo::Verbose1(FunctionInfoVerbose {
                        name: name.to_owned(),
                        flags: function_list_command_flags(val.flags),
                        is_async: val.is_async,
                        description: val.description.to_owned(),
                    })
                }
            })
            .collect(),
        cluster_functions: lib
            .gears_lib_ctx
            .remote_functions
            .keys()
            .map(|name| name.to_owned())
            .collect(),
        keyspace_triggers: lib
            .gears_lib_ctx
            .notifications_consumers
            .iter()
            .map(|(name, val)| {
                if verbosity_level == 0 {
                    return TriggersInfo::Verbose0(name.to_owned());
                }
                let val = val.borrow();
                let stats = val.get_stats();
                TriggersInfo::Verbose1(TriggersInfoVerbose1 {
                    name: name.to_owned(),
                    description: val.get_description(),
                    num_trigger: stats.num_trigger,
                    num_success: stats.num_success,
                    num_failed: stats.num_failed,
                    num_finished: stats.num_finished,
                    last_error: stats
                        .last_error
                        .as_ref()
                        .map(|v| get_msg_verbose(v).to_owned()),
                    last_execution_time: stats.last_execution_time as usize,
                    total_execution_time: stats.total_execution_time as usize,
                })
            })
            .collect(),
        stream_triggers: lib
            .gears_lib_ctx
            .stream_consumers
            .iter()
            .map(|(name, val)| {
                if verbosity_level == 0 {
                    return StreamTriggersInfo::Verbose0(name.to_owned());
                }
                let val = val.ref_cell.borrow();
                let stream_trigger_info = StreamTriggersInfoVerbose1 {
                    name: name.to_owned(),
                    prefix: val.prefix.clone(),
                    window: val.window,
                    trim: val.trim,
                    description: val.description.clone(),
                };
                if verbosity_level == 1 {
                    return StreamTriggersInfo::Verbose1(stream_trigger_info);
                }
                StreamTriggersInfo::Verbose2(StreamTriggersInfoVerbose2 {
                    stream_trigger_info,
                    streams: val
                        .consumed_streams
                        .iter()
                        .map(|(name, val)| {
                            let val = val.ref_cell.borrow();
                            StreamInfo {
                                name: name.to_owned(),
                                last_processed_time: val.last_processed_time as usize,
                                total_processed_time: val.total_processed_time as usize,
                                last_lag: val.last_lag as usize,
                                total_lag: val.total_lag as usize,
                                total_record_processed: val.records_processed,
                                pending_ids: val
                                    .pending_ids
                                    .iter()
                                    .map(|id| format!("{}-{}", id.ms, id.seq))
                                    .collect(),
                                id_to_read_from: val
                                    .last_read_id
                                    .map(|id| format!("{}-{}", id.ms, id.seq)),
                                last_error: val
                                    .last_error
                                    .as_ref()
                                    .map(|v| get_msg_verbose(v).to_owned()),
                            }
                        })
                        .collect(),
                })
            })
            .collect(),
        pending_async_calls: get_globals()
            .future_handlers
            .get(&lib.gears_lib_ctx.meta_data.name)
            .map_or_else(Vec::new, |v| {
                v.iter()
                    .filter_map(|v| {
                        let v = v.upgrade()?;
                        let v = v.lock(ctx);
                        let res: Vec<String> = v
                            .command
                            .iter()
                            .map(|v| String::from_utf8_lossy(v.as_slice()).into_owned())
                            .collect();
                        Some(res.join(" "))
                    })
                    .collect()
            }),
    };

    if !with_code {
        return LibraryInfo::WithoutCode(lib_info);
    }

    LibraryInfo::WithCode(LibraryInfoWithCode {
        code: lib.gears_lib_ctx.meta_data.code.to_owned(),
        lib_info,
    })
}

/// Implementation for `FUNCTION LIST` command.
pub(crate) fn function_list_command<T: Iterator<Item = RedisString>>(
    ctx: &Context,
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
            .map(|l| get_library_info(ctx, l, verbosity_level, with_code).into())
            .collect::<Vec<RedisValue>>(),
    ))
}
