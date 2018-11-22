#include "redismodule.h"
#include "redistar.h"
#include "redistar_memory.h"
#include <assert.h>
#include <string.h>

char* KeyRecordStrValueExtractor(RedisModuleCtx* rctx, Record *record, void* arg, size_t* len, char** err){
    if(RediStar_RecordGetType(record) != KEY_RECORD){
        *err = RS_STRDUP("KeyRecordStrValue extractor works only on key records");
        return NULL;
    }
    if(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) != STRING_RECORD){
        *err = RS_STRDUP("KeyRecordStrValue extractor works only on key records with handler value");
        return NULL;
    }
    Record* strRecord = RediStar_KeyRecordGetVal(record);
    char* str = RediStar_StringRecordGet(strRecord, NULL);
    *len = strlen(str);
    return str;
}
