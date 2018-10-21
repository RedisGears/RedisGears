#include "redismodule.h"
#include "redistar.h"
#include <assert.h>

bool TypeFilter(Record *record, void* arg){
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == KEY_HANDLER_RECORD);
    Record* handlerRecord = RediStar_KeyRecordGetVal(record);
    RedisModuleKey* handler = RediStar_KeyHandlerRecordGet(handlerRecord);
    return RedisModule_KeyType(handler) == (long)arg;
}
