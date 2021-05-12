/*
 * common.h
 *
 *  Created on: 10 Nov 2018
 *      Author: root
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#include "utils/dict.h"
#include "redismodule.h"
#include "cluster.h"
#include <stdio.h>
#include <stdbool.h>
#include "utils/arr_rm_alloc.h"

#if defined(DEBUG) || !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

extern Gears_dictType* dictTypeHeapIdsPtr;

typedef struct RedisVersion{
    int redisMajorVersion;
    int redisMinorVersion;
    int redisPatchVersion;
}RedisVersion;

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

extern RedisModuleCtx *staticCtx;

#define VERIFY_CLUSTER_INITIALIZE(c) if(!Cluster_IsInitialized()) return RedisModule_ReplyWithError(c, CLUSTER_ERROR" Uninitialized cluster state")

int GearsCompareVersions();
int GearsCheckSupportedVersion();
void GearsGetRedisVersion();
void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);
int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg);
int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...);
char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*));
const char* GetShardUniqueId();
int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...);
int IsKeyMatch(const char* prefix, const char* key, size_t prefixLen);
int ExecCommandVList(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, va_list __arg);

#endif /* SRC_COMMANDS_H_ */

