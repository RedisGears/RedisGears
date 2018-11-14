#include "redismodule.h"
#include "redistar.h"
#include "utils/arr_rm_alloc.h"
#include <assert.h>
#include <redistar_memory.h>
#include <string.h>


Record* GetValueMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    if(RediStar_RecordGetType(record) != KEY_RECORD){
        *err = "can not extract value for a none key value record.";
        RediStar_FreeRecord(record);
        return NULL;
    }
    Record* res = RediStar_KeyRecordGetVal(record);
    RediStar_KeyRecordSetVal(record, NULL);
    RediStar_FreeRecord(record);
    return res;
}
