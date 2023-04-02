#!/usr/bin/env bash

# A script to run Redis with RedisGears module.
# Supports running under release and debug modes, using locally-built
# Redis server instance path and may run under gdb.

GEARS_RELEASE_PATH=$PWD/target/release/libredisgears.so
GEARS_V8_RELEASE_PATH=$PWD/target/release/libredisgears_v8_plugin.so
GEARS_DEBUG_PATH=$PWD/target/debug/libredisgears.so
GEARS_V8_DEBUG_PATH=$PWD/target/debug/libredisgears_v8_plugin.so
REDIS_ARGUMENTS="--enable-debug-command yes"
REDIS_GLOBAL_PATH=redis-server
DEBUGGER_SCRIPT="gdb --args"
VALGRIND="valgrind --leak-check=full --show-leak-kinds=all"

function launch() {
    local redis_path=$1
    local gears_module_path=$2
    local gears_v8_plugin_path=$3
    local redis_arguments="$4 $5"

    $redis_path --loadmodule $gears_module_path v8-plugin-path $gears_v8_plugin_path $redis_arguments
}

function parse_args_and_launch() {
    local redis_path=$REDIS_GLOBAL_PATH
    local gears_module_path=$GEARS_RELEASE_PATH
    local gears_v8_plugin_path=$GEARS_V8_RELEASE_PATH
    local redis_arguments=$REDIS_ARGUMENTS
    local prefix=""

    while getopts 'dDs:l:vh' opt; do
    case "$opt" in
        d)
        echo "Setting up to run against the debug binaries."
        gears_module_path=$GEARS_DEBUG_PATH
        gears_v8_plugin_path=$GEARS_V8_DEBUG_PATH
        ;;

        D)
        echo "Setting up to run through a debugger."
        gears_module_path=$GEARS_DEBUG_PATH
        gears_v8_plugin_path=$GEARS_V8_DEBUG_PATH
        prefix="${DEBUGGER_SCRIPT}"
        ;;

        s)
        arg="$OPTARG"
        echo "Setting up to run a custom redis server: '${OPTARG}'"
        redis_path=${OPTARG}
        ;;

        l)
        arg="$OPTARG"
        echo "Setting up a custom gears library path: '${OPTARG}'"
        gears_module_path="${OPTARG}/libredisgears.so"
        gears_v8_plugin_path="${OPTARG}/libredisgears_v8_plugin.so"
        ;;

        v)
        echo "Setting up to run through valgrind."
        prefix="${VALGRIND}"
        ;;

        ?|h)
        printf "Usage: $(basename $0) [-d] [-D] [-v] [-s custom-redis-path] [-l custom-library-path]\nArguments:\n\t-d\tUse debug binaries\n\t-D\tRun in debugger\n\t-v\tRun via valgrind\n\t-s\tSpecify custom redis server\n\t-l\tSpecify custom library path\n\nExample: $(basename $0) -d -s ../redis/src/redis-server\n"
        exit 1
        ;;
    esac
    done

    launch "${prefix} ${redis_path}" $gears_module_path $gears_v8_plugin_path $redis_arguments
}

parse_args_and_launch $@
