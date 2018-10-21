#include <redistar_memory.h>
#include "redistar.h"
#include "utils/arr_rm_alloc.h"
#include <stdbool.h>

typedef struct KeysReaderCtx{
    RedisModuleCtx* ctx;
    char* match;
    long long cursorIndex;
    bool isDone;
    RedisModuleString** pendingKeys;
}KeysReaderCtx;

static RedisModuleString* KeysReader_NextKey(KeysReaderCtx* readerCtx){
    if(array_len(readerCtx->pendingKeys) > 0){
        return array_pop(readerCtx->pendingKeys);
    }
    if(readerCtx->isDone){
        return NULL;
    }
    RedisModuleCallReply *reply = RedisModule_Call(readerCtx->ctx, "SCAN", "lcc", readerCtx->cursorIndex, "COUNT", "1");
    if (reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        if(reply) RedisModule_FreeCallReply(reply);
        return NULL;
    }

    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

    if (RedisModule_CallReplyLength(reply) < 1) {
        RedisModule_FreeCallReply(reply);
        return NULL;
    }

    assert(RedisModule_CallReplyLength(reply) <= 2);

    RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

    assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

    RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
    RedisModule_StringToLongLong(cursorStr, &readerCtx->cursorIndex);
    RedisModule_FreeString(readerCtx->ctx, cursorStr);

    if(readerCtx->cursorIndex == 0){
        readerCtx->isDone = true;
    }

    RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
    assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
    if(RedisModule_CallReplyLength(keysReply) < 1){
        RedisModule_FreeCallReply(reply);
        return NULL;
    }
    for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
        RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
        assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
        size_t keyLen;
        RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
        readerCtx->pendingKeys = array_append(readerCtx->pendingKeys, key);
    }
    RedisModule_FreeCallReply(reply);
    return array_pop(readerCtx->pendingKeys);
}

Record* KeysReader_Next(void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    RedisModuleString* key = NULL;
    while((key = KeysReader_NextKey(readerCtx))){
        RedisModuleKey *keyHandler = RedisModule_OpenKey(readerCtx->ctx, key, REDISMODULE_READ);
        if(!keyHandler){
            RedisModule_FreeString(readerCtx->ctx, key);
            continue;
        }
        size_t keyLen;
        const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);

        Record* record = RediStar_KeyRecordCreate();

        char* keyCStr = RS_ALLOC(keyLen + 1);
        memcpy(keyCStr, keyStr, keyLen);
        keyCStr[keyLen] = '\0';

        RediStar_KeyRecordSetKey(record, keyCStr, keyLen);

        Record* handlerRecord = RediStar_KeyHandlerRecordCreate(keyHandler);

        RediStar_KeyRecordSetVal(record, handlerRecord);

        RedisModule_FreeString(readerCtx->ctx, key);

        return record;
    }
    return NULL;
}

void KeysReader_Free(void* ctx){
    KeysReaderCtx* krctx = ctx;
    RS_FREE(krctx->match);
    array_free(krctx->pendingKeys);
    RS_FREE(krctx);
}

KeysReaderCtx* RS_KeysReaderCtxCreate(RedisModuleCtx* ctx, char* match){
#define PENDING_KEYS_INIT_CAP 10
    KeysReaderCtx* krctx = RS_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = RS_STRDUP(match),
        .ctx = ctx,
        .cursorIndex = 0,
        .isDone = false,
        .pendingKeys = array_new(RedisModuleString*, PENDING_KEYS_INIT_CAP),
    };
    return krctx;
}

Reader* KeysReader(void* arg){
    Reader* r = RS_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = arg,
        .Next = KeysReader_Next,
        .Free = KeysReader_Free,
    };
    return r;
}
