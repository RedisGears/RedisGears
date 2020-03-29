#include "redismodule.h"
#include "utils/arr_rm_alloc.h"
#include <assert.h>
#include <string.h>
#include "redisgears.h"
#include "redisgears_memory.h"


Record* GetValueMapper(ExecutionCtx* rctx, Record *record, void* arg){
    if(RedisGears_RecordGetType(record) != keyRecordType){
        RedisGears_SetError(rctx, RG_STRDUP("can not extract value for a none key value record."));
        RedisGears_FreeRecord(record);
        return NULL;
    }
    Record* res = RedisGears_KeyRecordGetVal(record);
    RedisGears_KeyRecordSetVal(record, NULL);
    RedisGears_FreeRecord(record);
    return res;
}
