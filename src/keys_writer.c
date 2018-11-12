#include "redistar.h"
#include <string.h>
#include <assert.h>
#include "redismodule.h"
#include <stdbool.h>
#include "record.h"
#include "redistar_memory.h"

void KeyRecordWriter_WriteValueByType(RedisModuleCtx* rctx, Record *record, void* arg, char** err, RedisModuleKey* keyHandler){
    char* str;
    switch(RediStar_RecordGetType(record)){
    case STRING_RECORD:
        str = RediStar_StringRecordGet(record);
        RedisModuleString* redisStr = RedisModule_CreateString(rctx, str, strlen(str));
        RedisModule_StringSet(keyHandler, redisStr);
        break;
    default:
        printf("can not write the given record type\r\n");
        // write a warning log
    }
}

void KeyRecordWriter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    if(RediStar_RecordGetType(record) != KEY_RECORD){
        *err = "key record writer can only write key records.";
        return;
    }
    RedisModule_ThreadSafeContextLock(rctx);
    size_t len;
    char* key = RediStar_KeyRecordGetKey(record, &len);
    RedisModuleString* keyStr = RedisModule_CreateString(rctx, key, len);
    RedisModuleKey* keyHandler = RedisModule_OpenKey(rctx, keyStr, REDISMODULE_WRITE);
    int type = RedisModule_KeyType(keyHandler);
    if(type != REDISMODULE_KEYTYPE_EMPTY){
        RedisModule_DeleteKey(keyHandler);
    }
    Record* val = RediStar_KeyRecordGetVal(record);
    KeyRecordWriter_WriteValueByType(rctx, val, arg, err, keyHandler);
    RedisModule_CloseKey(keyHandler);
    RedisModule_FreeString(rctx, keyStr);
    RedisModule_ThreadSafeContextUnlock(rctx);
}
