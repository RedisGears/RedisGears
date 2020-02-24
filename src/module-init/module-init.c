/*
 * module_init.c
 *
 *  Created on: 6 Dec 2018
 *      Author: root
 */
#include "redismodule.h"
#include "module-init.h"
#include "version.h"
#include "globals.h"
#include "config.h"
#include "keys_reader.h"
#include "streams_reader.h"
#include "example.h"
#include "mappers.h"

#ifndef REDISGEARS_GIT_SHA
#define REDISGEARS_GIT_SHA "unknown"
#endif

void AddToStream(ExecutionCtx *rctx, Record *data, void *arg);
void Gears_RegisterGlobalReceivers(void);
int RedisGears_ModuleSetup(RedisModuleCtx *ctx);

int RedisGears_Init(RedisModuleCtx * ctx, RedisModuleString * *argv, int argc,
                    GearsInitMode mode) {
    RedisModule_Log(ctx, "notice", "RedisGears version %d.%d.%d, git_sha=%s",
                    REDISGEARS_VERSION_MAJOR, REDISGEARS_VERSION_MINOR, REDISGEARS_VERSION_PATCH,
                    REDISGEARS_GIT_SHA);

    if (LockHandler_Initialize() != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "could not initialize lock handler");
      return REDISMODULE_ERR;
    }

    if (RedisGears_RegisterApi(ctx) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "could not register RedisGears api");
      return REDISMODULE_ERR;
    }

    if (GearsConfig_Init(ctx, argv, argc) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "could not initialize gears config");
      return REDISMODULE_ERR;
    }

    if (RedisAI_Initialize(ctx) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning",
                      "could not initialize RediAI api, running without AI support.");
    } else {
      RedisModule_Log(ctx, "notice", "RedisAI api loaded successfully.");
      globals.redisAILoaded = true;
    }

    if (KeysReader_Initialize(ctx) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "could not initialize default keys reader.");
      return REDISMODULE_ERR;
    }

    Mgmt_Init();

    Cluster_Init();

    RGM_RegisterReader(KeysReader);
    RGM_RegisterReader(KeysOnlyReader);
    RGM_RegisterReader(StreamReader);
    RGM_RegisterFilter(Example_Filter, NULL);
    RGM_RegisterMap(GetValueMapper, NULL);
    RGM_RegisterForEach(AddToStream, NULL);

    ExecutionPlan_Initialize(1);

    #ifdef WITHPYTHON
    if (RedisGearsPy_Init(ctx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    #endif
    Gears_RegisterGlobalReceivers();
    if (mode == GEARS_INIT_MODULE) {
        return RedisGears_ModuleSetup(ctx);
    }
    return REDISMODULE_OK;
}