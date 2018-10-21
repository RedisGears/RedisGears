#include "redismodule.h"
#include "redistar.h"
#include "utils/arr_rm_alloc.h"
#include <assert.h>
#include <redistar_memory.h>
#include <string.h>

static Record* ValueToStringMapper(Record *record, void* arg){
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == KEY_HANDLER_RECORD);
    Record* handlerRecord = RediStar_KeyRecordGetVal(record);
    RedisModuleKey* handler = RediStar_KeyHandlerRecordGet(handlerRecord);
    size_t len;
    char* val = RedisModule_StringDMA(handler, &len, REDISMODULE_READ);
    char* strVal = RS_ALLOC(len + 1);
    memcpy(strVal, val, len);
    strVal[len] = '\0';

    Record* strRecord = RediStar_StringRecordCreate(strVal);

    RediStar_FreeRecord(handlerRecord);
    RediStar_KeyRecordSetVal(record, strRecord);
    return record;
}

static Record* ValueToListMapper(Record *record, void* arg){
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == KEY_HANDLER_RECORD);
    RedisModuleCtx* ctx = arg;
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "lrange", "cll", RediStar_KeyRecordGetKey(record, NULL), 0, -1);
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t len = RedisModule_CallReplyLength(reply);
    Record *listRecord = RediStar_ListRecordCreate(10);
    for(int i = 0 ; i < len ; ++i){
        RedisModuleCallReply *r = RedisModule_CallReplyArrayElement(reply, i);
        assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING);
        RedisModuleString* key = RedisModule_CreateStringFromCallReply(r);
        size_t vaLen;
        const char* val = RedisModule_StringPtrLen(key, &vaLen);
        char* str = RS_ALLOC(vaLen + 1);
        memcpy(str, val, vaLen);
        str[vaLen] = '\0';
        Record* strRecord = RediStar_StringRecordCreate(str);
        RedisModule_FreeString(ctx, key);
        RediStar_ListRecordAdd(listRecord, strRecord);
    }
    RedisModule_FreeCallReply(reply);
    RediStar_FreeRecord(RediStar_KeyRecordGetVal(record));
    RediStar_KeyRecordSetVal(record, listRecord);
    return record;
}

Record* ValueToRecordMapper(Record *record, void* redisModuleCtx){
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == KEY_HANDLER_RECORD);
    Record* handlerRecord = RediStar_KeyRecordGetVal(record);
    RedisModuleKey* handler = RediStar_KeyHandlerRecordGet(handlerRecord);
    switch(RedisModule_KeyType(handler)){
    case REDISMODULE_KEYTYPE_STRING:
        return ValueToStringMapper(record, redisModuleCtx);
        break;
    case REDISMODULE_KEYTYPE_LIST:
        return ValueToListMapper(record, redisModuleCtx);
        break;
    default:
        assert(false);
        return NULL;
    }
}
