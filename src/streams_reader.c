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
    long long numTriggered;
    long long numAborted;
    long long numSuccess;
    long long numFailures;
    char* lastError;
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
    long long errorsLen = RedisGears_GetErrorsLen(ctx);

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
                    ++srctx->numTriggered;
                    if(!RedisGears_Run(srctx->fep, srctx->mode, readerCtx, callback, privateData)){
                        ++srctx->numAborted;
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
}

static void* StreamReader_DeserializeArgs(Gears_BufferReader* br){
    char* stream = RedisGears_BRReadString(br);
    size_t batchSize = RedisGears_BRReadLong(br);
    return StreamReaderTriggerArgs_Create(stream, batchSize);
}

static void StreamReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    assert(srctx);
    RedisModule_ReplyWithArray(ctx, 12);
    RedisModule_ReplyWithStringBuffer(ctx, "mode", strlen("mode"));
    if(srctx->mode == ExecutionModeSync){
        RedisModule_ReplyWithStringBuffer(ctx, "sync", strlen("sync"));
    } else {
        RedisModule_ReplyWithStringBuffer(ctx, "async", strlen("async"));
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
        Gears_Buffer* buff = Gears_BufferCreate();
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        FlatExecutionPlan_Serialize(srctx->fep, &bw);
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
        Gears_Buffer buff = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader reader;
        Gears_BufferReaderInit(&reader, &buff);
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&reader);
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
