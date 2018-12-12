#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include <string.h>

#include "redisgears.h"
#include "redisgears_memory.h"
#include "commands.h"

Record* Example_Accumulate(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err){
    if(!accumulate){
        accumulate = RedisGears_LongRecordCreate(0);
    }
    long long num = RedisGears_LongRecordGet(accumulate);
    RedisGears_LongRecordSet(accumulate, num + 1);
    RedisGears_FreeRecord(r);
    return accumulate;
}

static void onDone(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResults(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    FlatExecutionPlan* rsctx = RSM_CreateCtx(KeysReader);
    RSM_Accumulate(rsctx, Example_Accumulate, NULL);
    ExecutionPlan* ep = RSM_Run(rsctx, RG_STRDUP("*"), NULL, NULL);
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
    RedisGears_RegisterExecutionDoneCallback(ep, onDone);
    RedisGears_SetPrivateData(ep, bc, NULL);
    return REDISMODULE_OK;
}
