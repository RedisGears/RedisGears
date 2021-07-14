
#pragma once

#include "utils/dict.h"
#include "redismodule.h"
#include "cluster.h"
#include "utils/arr_rm_alloc.h"

#include <stdio.h>

extern Gears_dictType* dictTypeHeapIdsPtr;

extern RedisModuleCtx *staticCtx;

#define VERIFY_CLUSTER_INITIALIZE(c) if(!Cluster_IsInitialized()) return RedisModule_ReplyWithError(c, CLUSTER_ERROR" Uninitialized cluster state")

int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...);
int ExecCommandVList(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, va_list __arg);
