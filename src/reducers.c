#include "redismodule.h"
#include <assert.h>
#include <string.h>
#include "redisgears.h"
#include "redisgears_memory.h"

Record* CountReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err){
    RedisModule_Assert(RedisGears_RecordGetType(records) == listRecordType);
    Record* res =RedisGears_LongRecordCreate(RedisGears_ListRecordLen(records));
    RedisGears_FreeRecord(records);
    return res;
}
