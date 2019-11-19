#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "utils/dict.h"
#include "execution_plan.h"

#define STREAM_REGISTRATION_INIT_SIZE 10
Gears_list* streamsRegistration = NULL;

typedef struct StreamReaderCtx{
    char* streamKeyName;
    char* lastId;
    Gears_list* records;
    bool isDone;
}StreamReaderCtx;

typedef struct StreamReaderTriggerArgs{
    size_t batchSize;
    char* stream;
}StreamReaderTriggerArgs;

typedef struct SingleStreamReaderCtx{
    char* lastId;
    size_t numTriggered;
}SingleStreamReaderCtx;

typedef struct StreamReaderTriggerCtx{
    size_t refCount;
    StreamReaderTriggerArgs* args;
    Gears_dict* lastIdsAndEvents;
    FlatExecutionPlan* fep;
    ExecutionMode mode;
}StreamReaderTriggerCtx;

StreamReaderTriggerArgs* StreamReaderTriggerArgs_Create(const char* streamName, size_t batchSize){
    StreamReaderTriggerArgs* readerArgs = RG_ALLOC(sizeof(StreamReaderTriggerArgs));
    readerArgs->stream = RG_STRDUP(streamName);
    readerArgs->batchSize = batchSize;
    return readerArgs;
}

static void StreamReaderTriggerArgs_Free(StreamReaderTriggerArgs* args){
    RG_FREE(args->stream);
    RG_FREE(args);
}

static StreamReaderTriggerCtx* StreamReaderTriggerCtx_GetShallowCopy(StreamReaderTriggerCtx* srtctx){
    ++srtctx->refCount;
    return srtctx;
}

static void StreamReaderTriggerCtx_Free(StreamReaderTriggerCtx* srtctx){
    if((--srtctx->refCount) == 0){
        Gears_dictIterator *iter = Gears_dictGetIterator(srtctx->lastIdsAndEvents);
        Gears_dictEntry* entry = NULL;
        while((entry = Gears_dictNext(iter))){
            SingleStreamReaderCtx* ssrctx = Gears_dictGetVal(entry);
            RG_FREE(ssrctx->lastId);
            RG_FREE(ssrctx);
        }
        Gears_dictReleaseIterator(iter);
        Gears_dictRelease(srtctx->lastIdsAndEvents);
        StreamReaderTriggerArgs_Free(srtctx->args);
        FlatExecutionPlan_Free(srtctx->fep);
        RG_FREE(srtctx);
    }
}

static StreamReaderTriggerCtx* StreamReaderTriggerCtx_Create(FlatExecutionPlan* fep, ExecutionMode mode,
                                                             StreamReaderTriggerArgs* args){
    StreamReaderTriggerCtx* srctx = RG_ALLOC(sizeof(StreamReaderTriggerCtx));
    *srctx = (StreamReaderTriggerCtx){
        .refCount = 1,
        .args = args,
        .lastIdsAndEvents = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .fep = fep,
        .mode = mode,
    };
    return srctx;
}

StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId){
    StreamReaderCtx* readerCtx = RG_ALLOC(sizeof(StreamReaderCtx));
    *readerCtx = (StreamReaderCtx){
            .streamKeyName = streamName ? RG_STRDUP(streamName) : NULL,
            .lastId = streamId ? RG_STRDUP(streamId) : NULL,
            .records = Gears_listCreate(),
            .isDone = false,
    };
    return readerCtx;
}

static char* StreamReader_ReadRecords(RedisModuleCtx* ctx, StreamReaderCtx* readerCtx){
    char* lastStreamId = NULL;
    if(readerCtx->isDone){
        return NULL;
    }
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "XREAD", "ccc", "STREAMS", readerCtx->streamKeyName, readerCtx->lastId);
    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
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
        Gears_listAddNodeHead(readerCtx->records, r);
    }

    RedisModule_FreeCallReply(reply);
    readerCtx->isDone = true;
    return lastStreamId;
}

static void StreamReader_CtxDeserialize(void* ctx, Gears_BufferReader* br){
    StreamReaderCtx* readerCtx = ctx;
    readerCtx->streamKeyName = RG_STRDUP(RedisGears_BRReadString(br));
    readerCtx->lastId = RG_STRDUP(RedisGears_BRReadString(br));
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
        while(Gears_listLength(readerCtx->records) > 0){
            Gears_listNode* node = Gears_listLast(readerCtx->records);
            Record* r = Gears_listNodeValue(node);
            RedisGears_FreeRecord(r);
            Gears_listDelNode(readerCtx->records, node);
        }
        Gears_listRelease(readerCtx->records);
    }
    RG_FREE(ctx);
}

static Record* StreamReader_Next(ExecutionCtx* ectx, void* ctx){
    StreamReaderCtx* readerCtx = ctx;
    RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
    if(Gears_listLength(readerCtx->records) == 0){
        LockHandler_Acquire(rctx);
        StreamReader_ReadRecords(rctx, readerCtx);
        LockHandler_Release(rctx);
    }
    if(Gears_listLength(readerCtx->records) == 0){
        return NULL;
    }
    Gears_listNode* node = Gears_listLast(readerCtx->records);
    Record* r = Gears_listNodeValue(node);
    Gears_listDelNode(readerCtx->records, node);
    return r;
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

static void StreamReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    StreamReaderTriggerCtx* srctx = privateData;
    if(srctx->mode == ExecutionModeSync){
        RedisGears_DropExecution(ctx);
    }
    StreamReaderTriggerCtx_Free(srctx);
}

static int StreamReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    const char* keyName = RedisModule_StringPtrLen(key, NULL);
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        if(StreamReader_IsKeyMatch(srctx->args->stream, keyName)){
            SingleStreamReaderCtx* ssrctx = Gears_dictFetchValue(srctx->lastIdsAndEvents, (char*)keyName);
            if(!ssrctx){
                ssrctx = RG_ALLOC(sizeof(*ssrctx));
                ssrctx->lastId = RG_STRDUP("0-0");
                ssrctx->numTriggered = 0;
                Gears_dictAdd(srctx->lastIdsAndEvents, (char*)keyName, ssrctx);
            }
            if(srctx->args->batchSize <= ++ssrctx->numTriggered){
                StreamReaderCtx* readerCtx = StreamReaderCtx_Create(keyName, ssrctx->lastId);
                char* lastStreamId = StreamReader_ReadRecords(ctx, readerCtx);
                if(lastStreamId){
                    RG_FREE(ssrctx->lastId);
                    ssrctx->lastId = RG_STRDUP(lastStreamId);
                    RedisGears_OnExecutionDoneCallback callback = NULL;
                    void* privateData = NULL;
                    if(srctx->mode == ExecutionModeSync){
                        callback = StreamReader_ExecutionDone;
                        privateData = StreamReaderTriggerCtx_GetShallowCopy(srctx);
                    }
                    if(!RedisGears_Run(srctx->fep, srctx->mode, readerCtx, callback, privateData)){
                        RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
                    }
                }
                ssrctx->numTriggered = 0;
            }
        }
    }
    Gears_listReleaseIterator(iter);
    return REDISMODULE_OK;
}

static void StreamReader_UnregisrterTrigger(FlatExecutionPlan* fep){
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        if(srctx->fep == fep){
            Gears_listReleaseIterator(iter);
            StreamReaderTriggerCtx_Free(srctx);
            Gears_listDelNode(streamsRegistration, node);
            return;
        }
    }
    assert(0);
}

static int StreamReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* arg){
    if(!streamsRegistration){
        streamsRegistration = Gears_listCreate();
        RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
        if(RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_STREAM, StreamReader_OnKeyTouched) != REDISMODULE_OK){
            // todo : print warning
        }
        RedisModule_FreeThreadSafeContext(ctx);
    }
    StreamReaderTriggerCtx* srctx = StreamReaderTriggerCtx_Create(fep, mode, arg);
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

static void StreamReader_SerializeArgs(void* args, Gears_BufferWriter* bw){
    StreamReaderTriggerArgs* triggerArgs = args;
    RedisGears_BWWriteString(bw, triggerArgs->stream);
    RedisGears_BWWriteLong(bw, triggerArgs->batchSize);
}

static void* StreamReader_DeserializeArgs(Gears_BufferReader* br){
    char* stream = RedisGears_BRReadString(br);
    size_t batchSize = RedisGears_BRReadLong(br);
    return StreamReaderTriggerArgs_Create(stream, batchSize);
}

RedisGears_ReaderCallbacks StreamReader = {
        .create = StreamReader_Create,
        .registerTrigger = StreamReader_RegisrterTrigger,
        .unregisterTrigger = StreamReader_UnregisrterTrigger,
        .serializeTriggerArgs = StreamReader_SerializeArgs,
        .deserializeTriggerArgs = StreamReader_DeserializeArgs,
};
