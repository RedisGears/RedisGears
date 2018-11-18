#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include "redistar.h"
#include <string.h>
#include "redistar_memory.h"

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    FlatExecutionPlan* rsctx = RSM_CreateCtx("example", KeysReader);
    RSM_Run(rsctx, RS_STRDUP("*"), NULL, NULL);
    return REDISMODULE_OK;
}
