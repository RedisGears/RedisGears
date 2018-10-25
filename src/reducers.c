#include "redismodule.h"
#include "redistar.h"
#include <assert.h>
#include <redistar_memory.h>
#include <string.h>

Record* CountReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err){
    assert(RediStar_RecordGetType(records) == LIST_RECORD);
    Record* res =RediStar_LongRecordCreate(RediStar_ListRecordLen(records));
    RediStar_FreeRecord(records);
    return res;
}
