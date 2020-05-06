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
#include <stdio.h>

#if defined(DEBUG) || !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

#define ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0
#define STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

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

int GearsCheckSupportedVestion();
void GearsGetRedisVersion();
void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);
int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg);
int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...);
char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*));
const char* GetShardUniqueId();
int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...);

#endif /* SRC_COMMANDS_H_ */

