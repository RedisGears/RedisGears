#include <string.h>
#include <assert.h>
#include "redismodule.h"
#include <stdbool.h>
#include "record.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"

void KeyRecordWriter_WriteValueByType(RedisModuleCtx* rctx, Record *record, void* arg, char** err, RedisModuleKey* keyHandler){
    char* str;
    size_t len;
    switch(RedisGears_RecordGetType(record)){
    case STRING_RECORD:
        str = RedisGears_StringRecordGet(record, &len);
        RedisModuleString* redisStr = RedisModule_CreateString(rctx, str, strlen(str));
        RedisModule_StringSet(keyHandler, redisStr);
        break;
    default:
        printf("can not write the given record type\r\n");
        // write a warning log
    }
}

void KeyRecordWriter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    if(RedisGears_RecordGetType(record) != KEY_RECORD){
        *err = "key record writer can only write key records.";
        return;
    }
    LockHandler_Acquire(rctx);
    size_t len;
    char* key = RedisGears_KeyRecordGetKey(record, &len);
    RedisModuleString* keyStr = RedisModule_CreateString(rctx, key, len);
    RedisModuleKey* keyHandler = RedisModule_OpenKey(rctx, keyStr, REDISMODULE_WRITE);
    int type = RedisModule_KeyType(keyHandler);
    if(type != REDISMODULE_KEYTYPE_EMPTY){
        RedisModule_DeleteKey(keyHandler);
    }
    Record* val = RedisGears_KeyRecordGetVal(record);
    KeyRecordWriter_WriteValueByType(rctx, val, arg, err, keyHandler);
    RedisModule_CloseKey(keyHandler);
    RedisModule_FreeString(rctx, keyStr);
    LockHandler_Release(rctx);
}
