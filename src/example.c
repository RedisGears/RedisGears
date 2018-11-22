#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include "redistar.h"
#include <string.h>
#include "redistar_memory.h"

static bool Example_Filter(RedisModuleCtx* rctx, Record *r, void* arg, char** err){
    if(RediStar_RecordGetType(r) != HASH_SET_RECORD){
        return false;
    }
    size_t len;
    char** keys = RediStar_HashSetRecordGetAllKeys(r, &len);
    for(size_t i = 0 ; i < len ; ++i){
        Record* val = RediStar_HashSetRecordGet(r, keys[i]);
        if(RediStar_RecordGetType(val) == STRING_RECORD){
            size_t len;
            char* valStr = RediStar_StringRecordGet(val, &len);
            int valInt = atol(valStr);
            if(valInt > 9999990){
                RediStar_HashSetRecordFreeKeysArray(keys);
                return true;
            }
        }
    }
    RediStar_HashSetRecordFreeKeysArray(keys);
    return false;
}

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    FlatExecutionPlan* rsctx = RSM_CreateCtx("example", KeysReader);
    RSM_RegisterFilter(Example_Filter, NULL);
    RSM_Filter(rsctx, Example_Filter, NULL);
    ExecutionPlan* ep = RSM_Run(rsctx, RS_STRDUP("*"), NULL, NULL);
    RedisModule_ReplyWithStringBuffer(ctx, RediStar_GetId(ep), strlen(RediStar_GetId(ep)));
    return REDISMODULE_OK;
}
