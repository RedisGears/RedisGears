#include "redismodule.h"
#include <assert.h>
#include <string.h>
#include "redisgears.h"
#include "redisgears_memory.h"

char* KeyRecordStrValueExtractor(RedisModuleCtx* rctx, Record *record, void* arg, size_t* len, char** err){
    if(RedisGears_RecordGetType(record) != keyRecordType){
        *err = RG_STRDUP("KeyRecordStrValue extractor works only on key records");
        return NULL;
    }
    if(RedisGears_RecordGetType(RedisGears_KeyRecordGetVal(record)) != stringRecordType){
        *err = RG_STRDUP("KeyRecordStrValue extractor works only on key records with handler value");
        return NULL;
    }
    Record* strRecord = RedisGears_KeyRecordGetVal(record);
    char* str = RedisGears_StringRecordGet(strRecord, NULL);
    *len = strlen(str);
    return str;
}
