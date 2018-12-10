#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include <string.h>

#include "redisgears.h"
#include "redisgears_memory.h"

bool Example_Filter(RedisModuleCtx* rctx, Record *r, void* arg, char** err){
    if(RedisGears_RecordGetType(r) != HASH_SET_RECORD){
        return false;
    }
    size_t len;
    char** keys = RedisGears_HashSetRecordGetAllKeys(r, &len);
    for(size_t i = 0 ; i < len ; ++i){
        Record* val = RedisGears_HashSetRecordGet(r, keys[i]);
        if(RedisGears_RecordGetType(val) == STRING_RECORD){
            size_t len;
            char* valStr = RedisGears_StringRecordGet(val, &len);
            int valInt = atol(valStr);
            if(valInt > 9999990){
                RedisGears_HashSetRecordFreeKeysArray(keys);
                return true;
            }
        }
    }
    RedisGears_HashSetRecordFreeKeysArray(keys);
    return false;
}

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    FlatExecutionPlan* rsctx = RSM_CreateCtx(KeysReader);
    RSM_Filter(rsctx, Example_Filter, NULL);
    ExecutionPlan* ep = RSM_Run(rsctx, RG_STRDUP("*"), NULL, NULL);
    RedisModule_ReplyWithStringBuffer(ctx, RedisGears_GetId(ep), strlen(RedisGears_GetId(ep)));
    RedisGears_FreeFlatExecution(rsctx);
    return REDISMODULE_OK;
}
