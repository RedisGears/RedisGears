/*
 * common.h
 *
 *  Created on: 10 Nov 2018
 *      Author: root
 */

#pragma once

#include "version.h"
#include "utils/dict.h"
#include "redismodule.h"
#include "cluster.h"

#include "utils/arr_rm_alloc.h"

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#if defined(DEBUG) || !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

extern Gears_dictType* dictTypeHeapIdsPtr;

extern bool gearsIsCrdt;

extern RedisModuleCtx *staticCtx;

void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);
int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg);
int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...);
char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*));
const char* GetShardUniqueId();
int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...);
int IsKeyMatch(const char* prefix, const char* key, size_t prefixLen);
int ExecCommandVList(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, va_list __arg);

