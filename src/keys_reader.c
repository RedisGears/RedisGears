#include <redistar_memory.h>
#include "redistar.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include <stdbool.h>

typedef struct KeysReaderCtx{
    char* match;
    long long cursorIndex;
    bool isDone;
    Record** pendingRecords;
}KeysReaderCtx;

KeysReaderCtx* RS_KeysReaderCtxCreate(char* match){
#define PENDING_KEYS_INIT_CAP 10
    KeysReaderCtx* krctx = RS_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = match,
        .cursorIndex = 0,
        .isDone = false,
        .pendingRecords = array_new(Record*, PENDING_KEYS_INIT_CAP),
    };
    return krctx;
}

void RS_KeysReaderCtxSerialize(void* arg, BufferWriter* bw){
    char* match = arg;
    RediStar_BWWriteString(bw, match);
}

void* RS_KeysReaderCtxDeserialize(BufferReader* br){
    char* match = RediStar_BRReadString(br);
    return RS_STRDUP(match);
}

void KeysReader_Free(void* ctx){
    KeysReaderCtx* krctx = ctx;
    RS_FREE(krctx->match);
    for(size_t i = 0 ; i < array_len(krctx->pendingRecords) ; ++i){
        RediStar_FreeRecord(krctx->pendingRecords[i]);
    }
    array_free(krctx->pendingRecords);
    RS_FREE(krctx);
}

static ArgType KeysReaderType = (ArgType){
    .type = "KeysReaderType",
    .free = KeysReader_Free,
    .serialize = RS_KeysReaderCtxSerialize,
    .deserialize = RS_KeysReaderCtxDeserialize,
};

static Record* ValueToStringMapper(Record *record, RedisModuleKey* handler){
    size_t len;
    char* val = RedisModule_StringDMA(handler, &len, REDISMODULE_READ);
    char* strVal = RS_ALLOC(len + 1);
    memcpy(strVal, val, len);
    strVal[len] = '\0';

    Record* strRecord = RediStar_StringRecordCreate(strVal);

    RediStar_KeyRecordSetVal(record, strRecord);
    return record;
}

static Record* ValueToListMapper(Record *record, RedisModuleCtx* ctx){
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
    RediStar_KeyRecordSetVal(record, listRecord);
    return record;
}

static Record* ValueToRecordMapper(RedisModuleCtx* rctx, Record* record, RedisModuleKey* handler){
    switch(RedisModule_KeyType(handler)){
    case REDISMODULE_KEYTYPE_STRING:
        return ValueToStringMapper(record, handler);
        break;
    case REDISMODULE_KEYTYPE_LIST:
        return ValueToListMapper(record, rctx);
        break;
    default:
        assert(false);
        return NULL;
    }
}

static Record* KeysReader_NextKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx){
    if(array_len(readerCtx->pendingRecords) > 0){
        return array_pop(readerCtx->pendingRecords);
    }
    if(readerCtx->isDone){
        return NULL;
    }
    RedisModule_ThreadSafeContextLock(rctx);
    RedisModuleCallReply *reply = RedisModule_Call(rctx, "SCAN", "lcccc", readerCtx->cursorIndex, "COUNT", "10000", "MATCH", readerCtx->match);
    if (reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        if(reply) RedisModule_FreeCallReply(reply);
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }

    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

    if (RedisModule_CallReplyLength(reply) < 1) {
        RedisModule_FreeCallReply(reply);
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }

    assert(RedisModule_CallReplyLength(reply) <= 2);

    RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

    assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

    RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
    RedisModule_StringToLongLong(cursorStr, &readerCtx->cursorIndex);
    RedisModule_FreeString(rctx, cursorStr);

    if(readerCtx->cursorIndex == 0){
        readerCtx->isDone = true;
    }

    RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
    assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
    if(RedisModule_CallReplyLength(keysReply) < 1){
        RedisModule_FreeCallReply(reply);
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }
    for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
        RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
        assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
        RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
        RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
        if(!keyHandler){
            RedisModule_FreeString(rctx, key);
            continue;
        }
        size_t keyLen;
        const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);

        Record* record = RediStar_KeyRecordCreate();

        char* keyCStr = RS_ALLOC(keyLen + 1);
        memcpy(keyCStr, keyStr, keyLen);
        keyCStr[keyLen] = '\0';

        RediStar_KeyRecordSetKey(record, keyCStr, keyLen);

        ValueToRecordMapper(rctx, record, keyHandler);

        readerCtx->pendingRecords = array_append(readerCtx->pendingRecords, record);

        RedisModule_FreeString(rctx, key);
        RedisModule_CloseKey(keyHandler);
    }
    RedisModule_FreeCallReply(reply);
    RedisModule_ThreadSafeContextUnlock(rctx);
    return array_pop(readerCtx->pendingRecords);
}

Record* KeysReader_Next(RedisModuleCtx* rctx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    Record* record = KeysReader_NextKey(rctx, readerCtx);
    return record;
}

ArgType* GetKeysReaderArgType(){
    return &KeysReaderType;
}

Reader* KeysReader(void* arg){
    KeysReaderCtx* ctx = RS_KeysReaderCtxCreate(arg);
    Reader* r = RS_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .Next = KeysReader_Next,
    };
    return r;
}
