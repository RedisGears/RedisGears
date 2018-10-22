#include "redismodule.h"
#include "redistar.h"
#include "redistar_memory.h"
#include <assert.h>

bool TypeFilter(Record *record, void* arg, char** err){
    if(RediStar_RecordGetType(record) != KEY_RECORD){
        *err = RS_STRDUP("types filter works only on key records");
        return false;
    }
    if(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) != KEY_HANDLER_RECORD){
        *err = RS_STRDUP("types filter works only on key records with handler value");
        return false;
    }
    Record* handlerRecord = RediStar_KeyRecordGetVal(record);
    RedisModuleKey* handler = RediStar_KeyHandlerRecordGet(handlerRecord);
    return RedisModule_KeyType(handler) == (long)arg;
}
