#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include <string.h>

#include "redisgears.h"
#include "redisgears_memory.h"

bool Example_Filter(ExecutionCtx* rctx, Record *r, void* arg){
    if(RedisGears_RecordGetType(r) != hashSetRecordType){
        return false;
    }
    size_t len;
    Arr(char*) keys = RedisGears_HashSetRecordGetAllKeys(r);
    for(size_t i = 0 ; i < len ; ++i){
        Record* val = RedisGears_HashSetRecordGet(r, keys[i]);
        if(RedisGears_RecordGetType(val) == stringRecordType){
            size_t len;
            char* valStr = RedisGears_StringRecordGet(val, &len);
            int valInt = atol(valStr);
            if(valInt > 9999990){
                array_free(keys);
                return true;
            }
        }
    }
    array_free(keys);
    return false;
}

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    FlatExecutionPlan* rsctx = RGM_CreateCtx(KeysReader);
    RGM_Filter(rsctx, Example_Filter, NULL);
    ExecutionPlan* ep = RGM_Run(rsctx, ExecutionModeAsync, RG_STRDUP("*"), NULL, NULL, NULL);
    RedisModule_ReplyWithStringBuffer(ctx, RedisGears_GetId(ep), strlen(RedisGears_GetId(ep)));
    RedisGears_FreeFlatExecution(rsctx);
    return REDISMODULE_OK;
}
