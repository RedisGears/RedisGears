#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "utils/dict.h"
#include "execution_plan.h"
#include "record.h"

#define STREAM_REGISTRATION_INIT_SIZE 10
Gears_list* streamsRegistration = NULL;

typedef struct StreamReaderCtx{
    char* streamKeyName;
    char* consumerGroup;
    char* streamId;
    size_t batchSize;
    Gears_list* records;
    bool isDone;
    RedisModuleString** batchIds;
}StreamReaderCtx;

typedef struct StreamReaderTriggerArgs{
    size_t batchSize;
    size_t durationMS;
    char* stream;
}StreamReaderTriggerArgs;

typedef struct StreamReaderTriggerCtx{
    size_t refCount;
    StreamReaderTriggerArgs* args;
    Gears_dict* singleStreamData;
    FlatExecutionPlan* fep;
    ExecutionMode mode;
    long long numTriggered;
    long long numAborted;
    long long numSuccess;
    long long numFailures;
    char* lastError;
}StreamReaderTriggerCtx;

typedef struct SingleStreamReaderCtx{
    size_t numTriggered;
    bool timerIsSet;
    RedisModuleTimerID lastTimerId;
    StreamReaderTriggerCtx* srtctx; // weak ptr to the StreamReaderCtx
    char* keyName;
}SingleStreamReaderCtx;

StreamReaderTriggerArgs* StreamReaderTriggerArgs_Create(const char* streamName, size_t batchSize, size_t durationMS){
    StreamReaderTriggerArgs* readerArgs = RG_ALLOC(sizeof(StreamReaderTriggerArgs));
    readerArgs->stream = RG_STRDUP(streamName);
    readerArgs->batchSize = batchSize;
    readerArgs->durationMS = durationMS;
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
        Gears_dictIterator *iter = Gears_dictGetIterator(srtctx->singleStreamData);
        Gears_dictEntry* entry = NULL;
        while((entry = Gears_dictNext(iter))){
            SingleStreamReaderCtx* ssrctx = Gears_dictGetVal(entry);
            RG_FREE(ssrctx->keyName);
            RG_FREE(ssrctx);
        }
        Gears_dictReleaseIterator(iter);
        Gears_dictRelease(srtctx->singleStreamData);
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
        .singleStreamData = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .fep = fep,
        .mode = mode,
        .numTriggered = 0,
        .numAborted = 0,
        .numSuccess = 0,
        .numFailures = 0,
        .lastError = NULL,
    };
    return srctx;
}

StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId){
    StreamReaderCtx* readerCtx = RG_ALLOC(sizeof(StreamReaderCtx));
    *readerCtx = (StreamReaderCtx){
            .streamKeyName = streamName ? RG_STRDUP(streamName) : NULL,
            .consumerGroup = NULL,
            .streamId = streamId ? RG_STRDUP(streamId) : NULL,
            .records = Gears_listCreate(),
            .isDone = false,
            .batchIds = NULL,
            .batchSize = 0,
    };
    return readerCtx;
}

StreamReaderCtx* StreamReaderCtx_CreateWithConsumerGroup(const char* streamName, const char* consumerGroup, size_t batchSize){
    StreamReaderCtx* readerCtx = RG_ALLOC(sizeof(StreamReaderCtx));
    *readerCtx = (StreamReaderCtx){
            .streamKeyName = streamName ? RG_STRDUP(streamName) : NULL,
            .consumerGroup = consumerGroup ? RG_STRDUP(consumerGroup) : NULL,
            .streamId = NULL,
            .records = Gears_listCreate(),
            .isDone = false,
            .batchIds = array_new(char*, 10), // todo : set to be batch size
            .batchSize = batchSize,
    };
    return readerCtx;
}

#define GEARS_CLIENT "__gears_client__"

static void StreamReader_ReadRecords(RedisModuleCtx* ctx, StreamReaderCtx* readerCtx){
    if(readerCtx->isDone){
        return;
    }
    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply;
    if(readerCtx->consumerGroup){
        if(readerCtx->batchSize > 0){
            reply = RedisModule_Call(ctx, "XREADGROUP", "cccclccc", "GROUP", readerCtx->consumerGroup, GEARS_CLIENT, "COUNT", readerCtx->batchSize, "STREAMS", readerCtx->streamKeyName, ">");
        }else{
            reply = RedisModule_Call(ctx, "XREADGROUP", "cccccc", "GROUP", readerCtx->consumerGroup, GEARS_CLIENT, "STREAMS", readerCtx->streamKeyName, ">");
        }
    }else{
        assert(readerCtx->streamId);
        reply = RedisModule_Call(ctx, "XREAD", "ccc", "STREAMS", readerCtx->streamKeyName, readerCtx->streamId);
    }
    LockHandler_Release(ctx);
    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        return;
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

        if(readerCtx->batchIds){
            readerCtx->batchIds = array_append(readerCtx->batchIds, RedisModule_CreateString(NULL, idCStr, len));
        }

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
}

static void StreamReader_CtxDeserialize(void* ctx, Gears_BufferReader* br){
    StreamReaderCtx* readerCtx = ctx;
    readerCtx->streamKeyName = RG_STRDUP(RedisGears_BRReadString(br));
    if(RedisGears_BRReadLong(br)){
        readerCtx->streamId = RG_STRDUP(RedisGears_BRReadString(br));
    }
    if(RedisGears_BRReadLong(br)){
        readerCtx->consumerGroup = RG_STRDUP(RedisGears_BRReadString(br));
    }

    readerCtx->batchSize = RedisGears_BRReadLong(br);
}

static void StreamReader_CtxSerialize(void* ctx, Gears_BufferWriter* bw){
    StreamReaderCtx* readerCtx = ctx;

    RedisGears_BWWriteString(bw, readerCtx->streamKeyName);

    if(readerCtx->streamId){
        RedisGears_BWWriteLong(bw, 1);
        RedisGears_BWWriteString(bw, readerCtx->streamId);
    }else{
        RedisGears_BWWriteLong(bw, 0);
    }

    if(readerCtx->consumerGroup){
        RedisGears_BWWriteLong(bw, 1);
        RedisGears_BWWriteString(bw, readerCtx->consumerGroup);
    }else{
        RedisGears_BWWriteLong(bw, 0);
    }

    RedisGears_BWWriteLong(bw, readerCtx->batchSize);
}

static void StreamReader_Free(void* ctx){
    StreamReaderCtx* readerCtx = ctx;
    if(readerCtx->streamKeyName){
        RG_FREE(readerCtx->streamKeyName);
    }
    if(readerCtx->streamId){
        RG_FREE(readerCtx->streamId);
    }
    if(readerCtx->consumerGroup){
        RG_FREE(readerCtx->consumerGroup);
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
    if(readerCtx->batchIds){
        for(size_t i = 0 ; i < array_len(readerCtx->batchIds) ; ++i){
            RedisModule_FreeString(NULL, readerCtx->batchIds[i]);
        }
        array_free(readerCtx->batchIds);
    }
    RG_FREE(ctx);
}

static Record* StreamReader_Next(ExecutionCtx* ectx, void* ctx){
    StreamReaderCtx* readerCtx = ctx;
    RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
    if(Gears_listLength(readerCtx->records) == 0){
        StreamReader_ReadRecords(rctx, readerCtx);
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

typedef struct ExecutionDoneCtx{
    char* ackId;
    SingleStreamReaderCtx ssrctx;
}ExecutionDoneCtx;

static void StreamReader_AckStream(StreamReaderCtx* readerCtx){
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_Call(rctx, "XACK", "ccv", readerCtx->streamKeyName, readerCtx->consumerGroup, readerCtx->batchIds, array_len(readerCtx->batchIds));
    RedisModule_FreeThreadSafeContext(rctx);
}

static void StreamReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    StreamReaderTriggerCtx* srctx = privateData;
    long long errorsLen = RedisGears_GetErrorsLen(ctx);

    Reader* reader = ExecutionPlan_GetReader(ctx);
    StreamReader_AckStream(reader->ctx);

    if(errorsLen > 0){
        ++srctx->numFailures;
        Record* r = RedisGears_GetError(ctx, 0);
        assert(RedisGears_RecordGetType(r) == ERROR_RECORD);
        if(srctx->lastError){
            RG_FREE(srctx->lastError);
        }
        srctx->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));
    } else {
        ++srctx->numSuccess;
    }

    if(srctx->mode == ExecutionModeSync){
        RedisGears_DropExecution(ctx);
    }

    StreamReaderTriggerCtx_Free(srctx);
}

#define GEARS_CONSUMER_GROUP "__gears_consumer_group__"

static void StreamReader_CreateConsumersGroup(RedisModuleCtx *ctx, const char* key){
    RedisModuleCallReply *r = RedisModule_Call(ctx, "XGROUP", "cccl", "CREATE", key, GEARS_CONSUMER_GROUP, 0);
    assert(r);
}

static void StreamReader_RunOnEvent(RedisModuleCtx *ctx, SingleStreamReaderCtx* ssrctx){
    StreamReaderTriggerCtx* srtctx = ssrctx->srtctx;
    StreamReaderCtx* readerCtx = StreamReaderCtx_CreateWithConsumerGroup(ssrctx->keyName, GEARS_CONSUMER_GROUP, srtctx->args->batchSize);
    RedisGears_OnExecutionDoneCallback callback = StreamReader_ExecutionDone;
    void* privateData = StreamReaderTriggerCtx_GetShallowCopy(srtctx);
    ++srtctx->numTriggered;
    if(!RedisGears_Run(srtctx->fep, srtctx->mode, readerCtx, callback, privateData)){
        ++srtctx->numAborted;
        RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
    }
    ssrctx->numTriggered = 0;
}

static void StreamReader_OnTime(RedisModuleCtx *ctx, void *data){
    SingleStreamReaderCtx* ssrctx = data;
    StreamReader_RunOnEvent(ctx, ssrctx);
    ssrctx->timerIsSet = false;
}

static int StreamReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    const char* keyName = RedisModule_StringPtrLen(key, NULL);
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        if(StreamReader_IsKeyMatch(srctx->args->stream, keyName)){
            SingleStreamReaderCtx* ssrctx = Gears_dictFetchValue(srctx->singleStreamData, (char*)keyName);
            if(!ssrctx){
                ssrctx = RG_ALLOC(sizeof(*ssrctx));
                StreamReader_CreateConsumersGroup(ctx, keyName);
                ssrctx->numTriggered = 0;
                ssrctx->srtctx = srctx;
                ssrctx->keyName = RG_STRDUP(keyName);
                ssrctx->timerIsSet = false;
                Gears_dictAdd(srctx->singleStreamData, (char*)keyName, ssrctx);
            }
            if(srctx->args->batchSize <= ++ssrctx->numTriggered){
                StreamReader_RunOnEvent(ctx, ssrctx);
                // we finish the run, if timer is set lets remove it.
                if(!ssrctx->timerIsSet && srctx->args->durationMS > 0){
                    RedisModule_StopTimer(ctx, ssrctx->lastTimerId, NULL);
                    ssrctx->timerIsSet = true;
                }
            }else{
                // if we did not run execution we need to set timer if its not already set
                if(!ssrctx->timerIsSet && srctx->args->durationMS > 0){
                    ssrctx->lastTimerId = RedisModule_CreateTimer(ctx, srctx->args->durationMS, StreamReader_OnTime, ssrctx);
                    ssrctx->timerIsSet = true;
                }
            }
        }
    }
    Gears_listReleaseIterator(iter);
    return REDISMODULE_OK;
}

#define STREAM_TRIGGER_FLAG_POP 0x01
static StreamReaderTriggerCtx* StreamReader_GetStreamTriggerCtxByFep(FlatExecutionPlan* fep, int flags){
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        if(srctx->fep == fep){
            Gears_listReleaseIterator(iter);
            if(flags & STREAM_TRIGGER_FLAG_POP){
                Gears_listDelNode(streamsRegistration, node);
            }
            return srctx;
        }
    }
    Gears_listReleaseIterator(iter);
    return NULL;
}

static void StreamReader_UnregisrterTrigger(FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, STREAM_TRIGGER_FLAG_POP);
    assert(srctx);
    StreamReaderTriggerCtx_Free(srctx);
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
    RedisGears_BWWriteLong(bw, triggerArgs->durationMS);
}

static void* StreamReader_DeserializeArgs(Gears_BufferReader* br){
    char* stream = RedisGears_BRReadString(br);
    size_t batchSize = RedisGears_BRReadLong(br);
    size_t durationMS = RedisGears_BRReadLong(br);
    return StreamReaderTriggerArgs_Create(stream, batchSize, durationMS);
}

static void StreamReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    assert(srctx);
    RedisModule_ReplyWithArray(ctx, 14);
    RedisModule_ReplyWithStringBuffer(ctx, "mode", strlen("mode"));
    if(srctx->mode == ExecutionModeSync){
        RedisModule_ReplyWithStringBuffer(ctx, "sync", strlen("sync"));
    } else if(srctx->mode == ExecutionModeAsync){
        RedisModule_ReplyWithStringBuffer(ctx, "async", strlen("async"));
    } else if(srctx->mode == ExecutionModeAsyncLocal){
        RedisModule_ReplyWithStringBuffer(ctx, "async_local", strlen("async_local"));
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "numTriggered", strlen("numTriggered"));
    RedisModule_ReplyWithLongLong(ctx, srctx->numTriggered);
    RedisModule_ReplyWithStringBuffer(ctx, "numSuccess", strlen("numSuccess"));
    RedisModule_ReplyWithLongLong(ctx, srctx->numSuccess);
    RedisModule_ReplyWithStringBuffer(ctx, "numFailures", strlen("numFailures"));
    RedisModule_ReplyWithLongLong(ctx, srctx->numFailures);
    RedisModule_ReplyWithStringBuffer(ctx, "numAborted", strlen("numAborted"));
    RedisModule_ReplyWithLongLong(ctx, srctx->numAborted);
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(srctx->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, srctx->lastError, strlen(srctx->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "agrs", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithStringBuffer(ctx, "batchSize", strlen("batchSize"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->batchSize);
    RedisModule_ReplyWithStringBuffer(ctx, "durationMS", strlen("durationMS"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->durationMS);
    RedisModule_ReplyWithStringBuffer(ctx, "stream", strlen("stream"));
    RedisModule_ReplyWithStringBuffer(ctx, srctx->args->stream, strlen(srctx->args->stream));
}

static void StreamReader_RdbSave(RedisModuleIO *rdb){
    if(!streamsRegistration){
        RedisModule_SaveUnsigned(rdb, 0); // done
        return;
    }
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        RedisModule_SaveUnsigned(rdb, 1); // has more
        size_t len;
        const char* serializedFep = FlatExecutionPlan_Serialize(srctx->fep, &len);
        assert(serializedFep); // fep already registered, must be serializable.
        RedisModule_SaveStringBuffer(rdb, serializedFep, len);

        Gears_Buffer* buff = Gears_BufferCreate();
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        StreamReader_SerializeArgs(srctx->args, &bw);
        RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

        RedisModule_SaveUnsigned(rdb, srctx->mode);
    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);
}

static void StreamReader_RdbLoad(RedisModuleIO *rdb, int encver){
    while(RedisModule_LoadUnsigned(rdb)){
        size_t len;
        char* data = RedisModule_LoadStringBuffer(rdb, &len);
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(data, len);
        RedisModule_Free(data);

        data = RedisModule_LoadStringBuffer(rdb, &len);
        Gears_Buffer buff = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader reader;
        Gears_BufferReaderInit(&reader, &buff);
        void* args = StreamReader_DeserializeArgs(&reader);
        RedisModule_Free(data);

        int mode = RedisModule_LoadUnsigned(rdb);
        StreamReader_RegisrterTrigger(fep, mode, args);
        FlatExecutionPlan_AddToRegisterDict(fep);
    }
}

static void StreamReader_Clear(){
    if(!streamsRegistration){
        return;
    }
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srtctx = Gears_listNodeValue(node);
        FlatExecutionPlan_RemoveFromRegisterDict(srtctx->fep);
        StreamReaderTriggerCtx_Free(srtctx);
    }
    Gears_listReleaseIterator(iter);
}

RedisGears_ReaderCallbacks StreamReader = {
        .create = StreamReader_Create,
        .registerTrigger = StreamReader_RegisrterTrigger,
        .unregisterTrigger = StreamReader_UnregisrterTrigger,
        .serializeTriggerArgs = StreamReader_SerializeArgs,
        .deserializeTriggerArgs = StreamReader_DeserializeArgs,
        .dumpRegistratioData = StreamReader_DumpRegistrationData,
        .rdbSave = StreamReader_RdbSave,
        .rdbLoad = StreamReader_RdbLoad,
        .clear = StreamReader_Clear,
};
