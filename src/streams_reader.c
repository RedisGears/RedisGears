#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "utils/dict.h"

#define STREAM_REGISTRATION_INIT_SIZE 10
Gears_list* streamsRegistration = NULL;

typedef struct StreamReaderCtx{
    char* streamKeyName;
    char* lastId;
    Record** records;
}StreamReaderCtx;

typedef struct StreamReaderTrigger{
    char* streamKeyName;
    Gears_dict* lastIds;
    FlatExecutionPlan* fep;
}StreamReaderTrigger;

StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId){
    StreamReaderCtx* readerCtx = RG_ALLOC(sizeof(StreamReaderCtx));
    *readerCtx = (StreamReaderCtx){
            .streamKeyName = streamName ? RG_STRDUP(streamName) : NULL,
            .lastId = streamId ? RG_STRDUP(streamId) : NULL,
            .records = NULL,
    };
    return readerCtx;
}

static char* StreamReader_ReadRecords(RedisModuleCtx* ctx, StreamReaderCtx* readerCtx){
    char* lastStreamId = NULL;
    assert(!readerCtx->records);
    readerCtx->records = array_new(Record*, 5);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "XREAD", "ccc", "STREAMS", readerCtx->streamKeyName, readerCtx->lastId);
    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL){
        return NULL;
    }
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    assert(RedisModule_CallReplyLength(reply) == 1);
    RedisModuleCallReply *streamReply = RedisModule_CallReplyArrayElement(reply, 0);
    assert(RedisModule_CallReplyType(streamReply) == REDISMODULE_REPLY_ARRAY);
    assert(RedisModule_CallReplyLength(streamReply) == 2);
    RedisModuleCallReply *elements = RedisModule_CallReplyArrayElement(streamReply, 1);
    assert(RedisModule_CallReplyType(elements) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(elements) ; ++i){
        Record* r = RedisGears_HashSetRecordCreate();
        RedisModuleCallReply *element = RedisModule_CallReplyArrayElement(elements, i);
        assert(RedisModule_CallReplyType(element) == REDISMODULE_REPLY_ARRAY);
        RedisModuleCallReply *id = RedisModule_CallReplyArrayElement(element, 0);
        assert(RedisModule_CallReplyType(id) == REDISMODULE_REPLY_STRING);
        size_t len;
        const char* idStr = RedisModule_CallReplyStringPtr(id, &len);
        char* idCStr = RG_ALLOC((len + 1)* sizeof(char));
        memcpy(idCStr, idStr, len);
        idCStr[len] = '\0';
        lastStreamId = idCStr;
        Record* valRecord = RedisGears_StringRecordCreate(idCStr, len);
        RedisGears_HashSetRecordSet(r, "streamId", valRecord);
        RedisModuleCallReply *values = RedisModule_CallReplyArrayElement(element, 1);
        assert(RedisModule_CallReplyType(values) == REDISMODULE_REPLY_ARRAY);
        assert(RedisModule_CallReplyLength(values) % 2 == 0);
        for(size_t j = 0 ; j < RedisModule_CallReplyLength(values) ; j+=2){
            RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(values, j);
            const char* keyStr = RedisModule_CallReplyStringPtr(key, &len);
            char keyCStr[len + 1];
            memcpy(keyCStr, keyStr, len);
            keyCStr[len] = '\0';
            RedisModuleCallReply *val = RedisModule_CallReplyArrayElement(values, j + 1);
            const char* valStr = RedisModule_CallReplyStringPtr(val, &len);
            char* valCStr = RG_ALLOC((len + 1)* sizeof(char));
            memcpy(valCStr, valStr, len);
            valCStr[len] = '\0';
            Record* valRecord = RedisGears_StringRecordCreate(valCStr, len);
            RedisGears_HashSetRecordSet(r, keyCStr, valRecord);
        }
        readerCtx->records = array_append(readerCtx->records, r);
    }

    RedisModule_FreeCallReply(reply);
    return lastStreamId;
}

static void StreamReader_CtxDeserialize(void* ctx, Gears_BufferReader* br){
    StreamReaderCtx* readerCtx = ctx;
    readerCtx->streamKeyName = RG_STRDUP(RedisGears_BRReadString(br));
    readerCtx->lastId = RG_STRDUP(RedisGears_BRReadString(br));
    readerCtx->records = array_new(Record*, 1);
}

static void StreamReader_CtxSerialize(void* ctx, Gears_BufferWriter* bw){
    StreamReaderCtx* readerCtx = ctx;
    RedisGears_BWWriteString(bw, readerCtx->streamKeyName);
    RedisGears_BWWriteString(bw, readerCtx->lastId);
}

static void StreamReader_Free(void* ctx){
    StreamReaderCtx* readerCtx = ctx;
    if(readerCtx->lastId){
        RG_FREE(readerCtx->lastId);
    }
    if(readerCtx->streamKeyName){
        RG_FREE(readerCtx->streamKeyName);
    }
    if(readerCtx->records){
        for(size_t i = 0 ; i < array_len(readerCtx->records) ; ++i){
            RedisGears_FreeRecord(readerCtx->records[i]);
        }
        array_free(readerCtx->records);
    }
    RG_FREE(ctx);
}

static Record* StreamReader_Next(ExecutionCtx* ectx, void* ctx){
    StreamReaderCtx* readerCtx = ctx;
    RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
    if(!readerCtx->records){
        LockHandler_Acquire(rctx);
        StreamReader_ReadRecords(rctx, readerCtx);
        LockHandler_Release(rctx);
    }
    if(array_len(readerCtx->records) == 0){
        return NULL;
    }
    return array_pop(readerCtx->records);
}

static int StreamReader_IsKeyMatch(const char* prefix, const char* key){
    size_t len = strlen(prefix);
    const char* data = prefix;
    int isPrefix = prefix[len - 1] == '*';
    if(isPrefix){
        --len;
    }
    if(isPrefix){
        return strncmp(data, key, len) == 0;
    }else{
        return strcmp(data, key) == 0;
    }
}

static int StreamReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    const char* keyName = RedisModule_StringPtrLen(key, NULL);
    while((node = Gears_listNext(iter))){
        StreamReaderTrigger* srctx = Gears_listNodeValue(node);
        if(StreamReader_IsKeyMatch(srctx->streamKeyName, keyName)){
            char* lastId = Gears_dictFetchValue(srctx->lastIds, (char*)keyName);
            if(!lastId){
                lastId = RG_STRDUP("0-0");
            }
            StreamReaderCtx* readerCtx = StreamReaderCtx_Create(keyName, lastId);
            char* lastStreamId = StreamReader_ReadRecords(ctx, readerCtx);
            if(lastStreamId){
                RG_FREE(lastId);
                lastId = RG_STRDUP(lastStreamId);
                Gears_dictEntry *entry = Gears_dictAddOrFind(srctx->lastIds, (char*)keyName);
                Gears_dictSetVal(srctx->lastIds, entry, lastId);
                if(!RedisGears_Run(srctx->fep, readerCtx, NULL, NULL)){
                    RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
                }
            }
        }
    }
    Gears_listReleaseIterator(iter);
    return REDISMODULE_OK;
}

static int StreamReader_RegisrterTrigger(FlatExecutionPlan* fep, void* arg){
    if(!streamsRegistration){
        streamsRegistration = Gears_listCreate();
        RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
        if(RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_STREAM, StreamReader_OnKeyTouched) != REDISMODULE_OK){
            // todo : print warning
        }
        RedisModule_FreeThreadSafeContext(ctx);
    }
    StreamReaderTrigger* srctx = RG_ALLOC(sizeof(StreamReaderTrigger));
    *srctx = (StreamReaderTrigger){
        .streamKeyName = arg,
        .lastIds = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .fep = fep,
    };
    Gears_listAddNodeHead(streamsRegistration, srctx);
    return 1;
}

static Reader* StreamReader_Create(void* arg){
    StreamReaderCtx* readerCtx = arg;
    if(!readerCtx){
        readerCtx = StreamReaderCtx_Create(NULL, NULL);
    }
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = readerCtx,
        .next = StreamReader_Next,
        .free = StreamReader_Free,
        .serialize = StreamReader_CtxSerialize,
        .deserialize = StreamReader_CtxDeserialize,
    };
    return r;
}

RedisGears_ReaderCallbacks StreamReader = {
        .create = StreamReader_Create,
        .registerTrigger = StreamReader_RegisrterTrigger,
};
