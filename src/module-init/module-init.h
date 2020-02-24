#ifndef RG_MODULE_INIT_H
#define RG_MODULE_INIT_H

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { GEARS_INIT_MODULE, GEARS_INIT_LIBRARY } GearsInitMode;
int RedisGears_Init(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, GearsInitMode mode);

#ifdef __cplusplus
}
#endif
#endif