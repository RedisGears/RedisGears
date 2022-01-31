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
#include "redisgears.h"
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

extern RedisVersion currVesion;
extern RedisVersion supportedVersion;

extern int gearsRlecMajorVersion;
extern int gearsRlecMinorVersion;
extern int gearsRlecPatchVersion;
extern int gearsRlecBuild;

extern bool gearsIsCrdt;

static inline int IsEnterprise() {
  return gearsRlecMajorVersion != -1;
}

typedef struct Plugin{
    char* name;
    int version;
    RedisModuleInfoFunc infoFunc;
    GearsPlugin_UnlinkSession unlinkSession;
    GearsPlugin_SerializeSession serializeSession;
    GearsPlugin_DeserializeSession deserializeSession;
    GearsPlugin_SetCurrSession setCurrSession;
}Plugin;

extern Gears_dict* plugins;

extern RedisModuleCtx *staticCtx;

#define VERIFY_CLUSTER_INITIALIZE(c) \
	do { \
		if(!Cluster_IsInitialized()) return RedisModule_ReplyWithError(c, CLUSTER_ERROR" Uninitialized cluster state"); \
	} while(0)

int GearsCompareVersions();
int GearsCheckSupportedVersion();
void GearsGetRedisVersion();
void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);
int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg);
int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...);
char* IntArrToStr(int* arr, size_t len, char*(*toStr)(int), char sep);
char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*), char sep);
const char* GetShardUniqueId();
int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...);
int IsKeyMatch(const char* prefix, const char* key, size_t prefixLen);
int ExecCommandVList(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, va_list __arg);

