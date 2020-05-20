#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "utils/dict.h"
#include "execution_plan.h"
#include "record.h"
#include "config.h"
#include <pthread.h>

#define STREAM_REGISTRATION_INIT_SIZE 10
Gears_list* streamsRegistration = NULL;

RedisModuleCtx *staticCtx = NULL;

typedef struct StreamId{
        long first;
        long second;
}StreamId;

typedef struct StreamReaderCtx{
    char* streamKeyName;
    char* consumerGroup;
    char* streamId;
    bool readPenging;
    size_t batchSize;
    Gears_list* records;
    bool isDone;
    RedisModuleString** batchIds;
    StreamId lastReadId;
}StreamReaderCtx;

typedef struct StreamReaderTriggerArgs{
    size_t batchSize;
    size_t durationMS;
    char* streamPrefix;
    OnFailedPolicy onFailedPolicy;
    size_t retryInterval;
    bool trimStream;
}StreamReaderTriggerArgs;

typedef enum StreamRegistrationStatus{
    StreamRegistrationStatus_OK,
    StreamRegistrationStatus_ABORTED,
    StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE,
    StreamRegistrationStatus_UNREGISTERED,
}StreamRegistrationStatus;

typedef struct StreamReaderTriggerCtx{
    size_t refCount;
    StreamRegistrationStatus status;
    StreamReaderTriggerArgs* args;
    Gears_dict* singleStreamData;
    FlatExecutionPlan* fep;
    ExecutionMode mode;
    long long numTriggered;
    long long numAborted;
    long long numSuccess;
    long long numFailures;
    char* lastError;
    pthread_t scanThread;
    Gears_list* localPendingExecutions;
    Gears_list* localDoneExecutions;
    WorkerData* wd;
}StreamReaderTriggerCtx;

typedef struct SingleStreamReaderCtx{
    size_t numTriggered;
    bool timerIsSet;
    bool freeOnNextTimeEvent;
    RedisModuleTimerID lastTimerId;
    StreamReaderTriggerCtx* srtctx; // weak ptr to the StreamReaderCtx
    char* keyName;
    StreamId lastId;
}SingleStreamReaderCtx;

static void* StreamReader_ScanForStreams(void* pd);
static void StreamReader_Free(void* ctx);

static bool StreamReader_VerifyCallReply(RedisModuleCtx* ctx, RedisModuleCallReply* reply, const char* msgPrefix, const char* logLevel){
    if (reply == NULL ||
            RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR ||
            RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL) {
        if(!msgPrefix){
            // no log message requested, just return false
            if(reply) RedisModule_FreeCallReply(reply);
            return false;
        }
        const char* err = "got null reply";
        if(reply && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
            err = RedisModule_CallReplyStringPtr(reply, NULL);
        }
        RedisModule_Log(ctx, logLevel, "%s : %s", msgPrefix, err);
        if(reply) RedisModule_FreeCallReply(reply);
        return false;
    }
    return true;
}

static void StreamReader_RunOnEvent(SingleStreamReaderCtx* ssrctx, size_t batch, bool readPending);

#define StreamIdZero (StreamId){.first = 0, .second = 0}

#define StreamIdIsZero(id) (id.first == 0 && id.second == 0)

static int StreamReader_StreamIdCompare(StreamId id1, StreamId id2){
    if(id1.second > id2.second){
        return 1;
    }

    if(id1.second < id2.second){
        return -1;
    }

    if(id1.first > id2.first){
        return 1;
    }

    if(id1.first < id2.first){
        return -1;
    }

    return 0;
}

static StreamId StreamReader_ParseStreamId(const char* streamId){
    StreamId ret;
    int res = sscanf(streamId, "%ld-%ld", &ret.second, &ret.first);
    RedisModule_Assert(res == 2);
    return ret;
}

StreamReaderTriggerArgs* StreamReaderTriggerArgs_Create(const char* streamPrefix, size_t batchSize, size_t durationMS, OnFailedPolicy onFailedPolicy, size_t retryInterval, bool trimStream){
    StreamReaderTriggerArgs* readerArgs = RG_ALLOC(sizeof(StreamReaderTriggerArgs));
    readerArgs->streamPrefix = RG_STRDUP(streamPrefix);
    readerArgs->batchSize = batchSize;
    readerArgs->durationMS = durationMS;
    readerArgs->onFailedPolicy = onFailedPolicy;
    readerArgs->retryInterval = retryInterval;
    readerArgs->trimStream = trimStream;
    return readerArgs;
}

void StreamReaderTriggerArgs_Free(StreamReaderTriggerArgs* args){
    RG_FREE(args->streamPrefix);
    RG_FREE(args);
}

static StreamReaderTriggerCtx* StreamReaderTriggerCtx_GetShallowCopy(StreamReaderTriggerCtx* srtctx){
    ++srtctx->refCount;
    return srtctx;
}

static void StreamReader_ReadLastId(RedisModuleCtx *rctx, SingleStreamReaderCtx* ssrctx){
    RedisModuleCallReply *reply = RedisModule_Call(rctx, "XINFO", "cc", "STREAM", ssrctx->keyName);
    bool ret = StreamReader_VerifyCallReply(rctx, reply, "Failed on XINFO command", "warning");
    RedisModule_Assert(ret);
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t lastGeneratedIdIndex = 0;
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(reply) ; i+=2){
        RedisModuleCallReply *currReply = RedisModule_CallReplyArrayElement(reply, i);
        size_t currReplyStrLen;
        const char* currReplyStr = RedisModule_CallReplyStringPtr(currReply, &currReplyStrLen);
        if(strncmp(currReplyStr, "last-generated-id", currReplyStrLen) == 0){
            lastGeneratedIdIndex = i + 1;
            break;
        }
    }
    RedisModule_Assert(lastGeneratedIdIndex > 0);
    RedisModuleCallReply *idReply = RedisModule_CallReplyArrayElement(reply, lastGeneratedIdIndex);
    RedisModule_Assert(RedisModule_CallReplyType(idReply) == REDISMODULE_REPLY_STRING);
    const char* idStr = RedisModule_CallReplyStringPtr(idReply, NULL);
    ssrctx->lastId = StreamReader_ParseStreamId(idStr);
    RedisModule_FreeCallReply(reply);
}

#define GEARS_CONSUMER_GROUP "__gears_consumer_group__"
static void StreamReader_CreateConsumersGroup(RedisModuleCtx *ctx, const char* key){
    RedisModuleCallReply *r = RedisModule_Call(ctx, "XGROUP", "!cccc", "CREATE", key, GEARS_CONSUMER_GROUP, "0");
    // if creation fails its ok, this is why we do not call VerifyCallReply
    if(r){
        RedisModule_FreeCallReply(r);
    }
}

static SingleStreamReaderCtx* SingleStreamReaderCtx_Create(RedisModuleCtx* ctx,
                                                             const char* keyName,
                                                             StreamReaderTriggerCtx* srtctx){
    SingleStreamReaderCtx* ssrctx = RG_ALLOC(sizeof(*ssrctx));
    ssrctx->numTriggered = 0;
    ssrctx->srtctx = srtctx;
    ssrctx->keyName = RG_STRDUP(keyName);
    ssrctx->timerIsSet = false;
    ssrctx->freeOnNextTimeEvent = false;
    StreamReader_ReadLastId(ctx, ssrctx);
    Gears_dictAdd(srtctx->singleStreamData, (char*)keyName, ssrctx);

    // The moment we create the SingleStreamReaderCtx we must trigger execution to read
    // pending messages to keep the read order correctly
    StreamReader_RunOnEvent(ssrctx, 0, true);
    return ssrctx;
}

static void StreamReaderTriggerCtx_CleanSingleStreamsData(StreamReaderTriggerCtx* srtctx){
    Gears_dictIterator *iter = Gears_dictGetIterator(srtctx->singleStreamData);
    Gears_dictEntry* entry = NULL;
    while((entry = Gears_dictNext(iter))){
        SingleStreamReaderCtx* ssrctx = Gears_dictGetVal(entry);
        if(ssrctx->timerIsSet){
            ssrctx->freeOnNextTimeEvent = true;
        }else{
            RG_FREE(ssrctx->keyName);
            RG_FREE(ssrctx);
        }
    }
    Gears_dictReleaseIterator(iter);
    Gears_dictRelease(srtctx->singleStreamData);
    srtctx->singleStreamData = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
}

static void StreamReaderTriggerCtx_Free(StreamReaderTriggerCtx* srtctx){
    RedisModule_Assert(srtctx->refCount > 0);

    if((--srtctx->refCount) == 0){
        Gears_listNode* n = NULL;

        // if we free registration there must not be any pending executions.
        // either all executions was finished or aborted
        RedisModule_Assert(Gears_listLength(srtctx->localPendingExecutions) == 0);

        while((n = Gears_listFirst(srtctx->localDoneExecutions))){
            char* epIdStr = Gears_listNodeValue(n);
            Gears_listDelNode(srtctx->localDoneExecutions, n);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            RG_FREE(epIdStr);
            if(!ep){
                RedisModule_Log(NULL, "info", "Failed finding done execution to drop on unregister. Execution was probably already dropped.");
                continue;
            }
            // all the executions here are done, will just drop it.
            RedisGears_DropExecution(ep);
        }

        Gears_listRelease(srtctx->localPendingExecutions);
        Gears_listRelease(srtctx->localDoneExecutions);

        StreamReaderTriggerCtx_CleanSingleStreamsData(srtctx);
        Gears_dictRelease(srtctx->singleStreamData);
        StreamReaderTriggerArgs_Free(srtctx->args);
        FlatExecutionPlan_Free(srtctx->fep);
        RedisGears_WorkerDataFree(srtctx->wd);
        RG_FREE(srtctx->lastError);
        RG_FREE(srtctx);
    }
}

static StreamReaderTriggerCtx* StreamReaderTriggerCtx_Create(FlatExecutionPlan* fep, ExecutionMode mode,
                                                             StreamReaderTriggerArgs* args){
    StreamReaderTriggerCtx* srctx = RG_ALLOC(sizeof(StreamReaderTriggerCtx));
    *srctx = (StreamReaderTriggerCtx){
        .refCount = 1,
        .status = StreamRegistrationStatus_OK,
        .args = args,
        .singleStreamData = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .fep = fep,
        .mode = mode,
        .numTriggered = 0,
        .numAborted = 0,
        .numSuccess = 0,
        .numFailures = 0,
        .lastError = NULL,
        .localPendingExecutions = Gears_listCreate(),
        .localDoneExecutions = Gears_listCreate(),
        .wd = RedisGears_WorkerDataCreate(NULL),
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
            .readPenging = false,
            .lastReadId = StreamIdZero,
    };
    return readerCtx;
}

void StreamReaderCtx_Free(StreamReaderCtx* readerCtx){
    StreamReader_Free(readerCtx);
}

StreamReaderCtx* StreamReaderCtx_CreateWithConsumerGroup(const char* streamName, const char* consumerGroup, size_t batchSize, bool readPenging){
    StreamReaderCtx* readerCtx = RG_ALLOC(sizeof(StreamReaderCtx));
    *readerCtx = (StreamReaderCtx){
            .streamKeyName = streamName ? RG_STRDUP(streamName) : NULL,
            .consumerGroup = consumerGroup ? RG_STRDUP(consumerGroup) : NULL,
            .streamId = NULL,
            .records = Gears_listCreate(),
            .isDone = false,
            .batchIds = array_new(char*, 10), // todo : set to be batch size
            .batchSize = batchSize,
            .readPenging = readPenging,
            .lastReadId = StreamIdZero,
    };
    return readerCtx;
}

#define GEARS_CLIENT "__gears_client__"

static RedisModuleCallReply* StreamReader_ReadFromGroup(RedisModuleCtx* ctx, StreamReaderCtx* readerCtx){
    RedisModuleCallReply *reply;
    const char* lastId = readerCtx->readPenging ? "0" : ">";
    if(readerCtx->batchSize > 0){
        reply = RedisModule_Call(ctx, "XREADGROUP", "!cccclccc", "GROUP", readerCtx->consumerGroup, GEARS_CLIENT, "COUNT", readerCtx->batchSize, "STREAMS", readerCtx->streamKeyName, lastId);
    }else{
        reply = RedisModule_Call(ctx, "XREADGROUP", "!cccccc", "GROUP", readerCtx->consumerGroup, GEARS_CLIENT, "STREAMS", readerCtx->streamKeyName, lastId);
    }
    return reply;
}

static void StreamReader_ReadRecords(RedisModuleCtx* ctx, StreamReaderCtx* readerCtx){
    if(readerCtx->isDone){
        return;
    }
    const char* lastReadId = NULL;
    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply;
    if(readerCtx->consumerGroup){
        reply = StreamReader_ReadFromGroup(ctx, readerCtx);
        if(reply && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL){
            readerCtx->isDone = true;
            if(reply){
                RedisModule_FreeCallReply(reply);
            }
            LockHandler_Release(ctx);
            return; // empty reply - no data
        }
        // NULL or ERROR
        if(!StreamReader_VerifyCallReply(ctx, reply, "Failed on XREADGROUP, will create consumer group and retry (if failed again its fatal)", "debug")){
            // NULL or ERROR, try create consumer group.
            StreamReader_CreateConsumersGroup(ctx, readerCtx->streamKeyName);
            reply = StreamReader_ReadFromGroup(ctx, readerCtx);
        }

    }else{
        RedisModule_Assert(readerCtx->streamId);
        reply = RedisModule_Call(ctx, "XREAD", "ccc", "STREAMS", readerCtx->streamKeyName, readerCtx->streamId);
        if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
            LockHandler_Release(ctx);
            size_t errorLen;
            const char* errorRecord = RedisModule_CallReplyStringPtr(reply, &errorLen);
            char* errorStr = RG_ALLOC((errorLen + 1)* sizeof(char));
            memcpy(errorStr, errorRecord, errorLen);
            errorStr[errorLen] = '\0';
            Record* errorRecrod = RG_ErrorRecordCreate(errorStr, errorLen);
            Gears_listAddNodeHead(readerCtx->records, errorRecrod);
            RedisModule_FreeCallReply(reply);
            return;
        }
    }
    LockHandler_Release(ctx);
    if(!StreamReader_VerifyCallReply(ctx, reply, NULL, NULL)){
        // failure here means no data or stream is gone
        readerCtx->isDone = true;
        return;
    }
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    RedisModule_Assert(RedisModule_CallReplyLength(reply) == 1);
    RedisModuleCallReply *streamReply = RedisModule_CallReplyArrayElement(reply, 0);
    RedisModule_Assert(RedisModule_CallReplyType(streamReply) == REDISMODULE_REPLY_ARRAY);
    RedisModule_Assert(RedisModule_CallReplyLength(streamReply) == 2);
    RedisModuleCallReply *elements = RedisModule_CallReplyArrayElement(streamReply, 1);
    RedisModule_Assert(RedisModule_CallReplyType(elements) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(elements) ; ++i){
        Record* r = RedisGears_HashSetRecordCreate();
        Record* recordValues = RedisGears_HashSetRecordCreate();
        RedisModuleCallReply *element = RedisModule_CallReplyArrayElement(elements, i);
        RedisModule_Assert(RedisModule_CallReplyType(element) == REDISMODULE_REPLY_ARRAY);
        RedisModuleCallReply *id = RedisModule_CallReplyArrayElement(element, 0);
        RedisModule_Assert(RedisModule_CallReplyType(id) == REDISMODULE_REPLY_STRING);

        size_t len;
        lastReadId = RedisModule_CallReplyStringPtr(id, &len);
        char* idCStr = RG_ALLOC((len + 1)* sizeof(char));
        memcpy(idCStr, lastReadId, len);
        idCStr[len] = '\0';

        if(readerCtx->batchIds){
            readerCtx->batchIds = array_append(readerCtx->batchIds, RedisModule_CreateString(NULL, lastReadId, len));
        }

        Record* valRecord = RedisGears_StringRecordCreate(idCStr, len);
        Record* keyRecord = RedisGears_StringRecordCreate(RG_STRDUP(readerCtx->streamKeyName), strlen(readerCtx->streamKeyName));
        RedisGears_HashSetRecordSet(r, "id", valRecord);
        RedisGears_HashSetRecordSet(r, "value", recordValues);
        RedisGears_HashSetRecordSet(r, "key", keyRecord);
        RedisModuleCallReply *values = RedisModule_CallReplyArrayElement(element, 1);
        RedisModule_Assert(RedisModule_CallReplyType(values) == REDISMODULE_REPLY_ARRAY);
        RedisModule_Assert(RedisModule_CallReplyLength(values) % 2 == 0);
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
            RedisGears_HashSetRecordSet(recordValues, keyCStr, valRecord);
        }
        Gears_listAddNodeHead(readerCtx->records, r);
    }

    if(lastReadId){
        readerCtx->lastReadId = StreamReader_ParseStreamId(lastReadId);
    }

    RedisModule_FreeCallReply(reply);
    readerCtx->isDone = true;
}

static void StreamReader_CtxDeserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){
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

static void StreamReader_AckAndTrimm(StreamReaderCtx* readerCtx, bool alsoTrimm){
    if(array_len(readerCtx->batchIds) == 0){
        // nothing to ack on
        return;
    }
    RedisModuleCallReply* reply = RedisModule_Call(staticCtx, "XACK", "!ccv", readerCtx->streamKeyName, readerCtx->consumerGroup, readerCtx->batchIds, (size_t)array_len(readerCtx->batchIds));
    bool ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed acking messages", "warning");
    RedisModule_Assert(ret);

    RedisModule_FreeCallReply(reply);

    if(alsoTrimm){

        reply = RedisModule_Call(staticCtx, "XLEN", "c", readerCtx->streamKeyName);
        ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed XLEN messages", "warning");
        RedisModule_Assert(ret);
        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER);

        long long streamLen = RedisModule_CallReplyInteger(reply);

        RedisModule_FreeCallReply(reply);

        long long streamLenForTrim = streamLen - array_len(readerCtx->batchIds);
        if(streamLenForTrim < 0){
            RedisModule_Log(NULL, "warning", "try to trim stream to negative len, fatal!!");
            RedisModule_Assert(false);
        }

        reply = RedisModule_Call(staticCtx, "XTRIM", "!ccl", readerCtx->streamKeyName, "MAXLEN", streamLenForTrim);
        ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed Trim messages", "warning");
        RedisModule_Assert(ret);

        RedisModule_FreeCallReply(reply);

    }
}

static void StreamReader_TriggerAnotherExecutionIfNeeded(StreamReaderTriggerCtx* srctx, StreamReaderCtx* readerCtx){
    if(StreamIdIsZero(readerCtx->lastReadId) && !readerCtx->readPenging){
        // we got no data from the stream and we were not asked to read pending,
        // there is no need to trigger another execution.
        return;
    }
    // If we reach here there is 2 options:
    //      1. we did not yet reach the last id
    //      2. we only read pending records
    // either way we need to trigger another execution.
    SingleStreamReaderCtx* ssrctx = Gears_dictFetchValue(srctx->singleStreamData, readerCtx->streamKeyName);
    if(StreamReader_StreamIdCompare(ssrctx->lastId, readerCtx->lastReadId) > 0){
        StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
    }
}

static void StreamReader_AbortPendings(StreamReaderTriggerCtx* srctx){
    // unregister require aborting all pending executions
    ExecutionPlan** abortEpArray = array_new(ExecutionPlan*, 10);

    Gears_listNode* n = NULL;
    Gears_listIter *iter = Gears_listGetIterator(srctx->localPendingExecutions, AL_START_HEAD);
    while((n = Gears_listNext(iter))){
        char* epIdStr = Gears_listNodeValue(n);
        ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
        if(!ep){
            RedisModule_Log(NULL, "warning", "Failed finding pending execution to abort on unregister.");
            continue;
        }

        // we can not abort right now cause aborting might cause values to be deleted
        // from localPendingExecutions and it will mess up with the iterator
        // so we must collect all the exeuctions first and then abort one by one
        abortEpArray = array_append(abortEpArray, ep);
    }
    Gears_listReleaseIterator(iter);

    for(size_t i = 0 ; i < array_len(abortEpArray) ; ++i){
        // we can not free while iterating so we add to the epArr and free after
        if(RedisGears_AbortExecution(abortEpArray[i]) != REDISMODULE_OK){
            RedisModule_Log(NULL, "warning", "Failed aborting execution on unregister.");
        }
    }

    array_free(abortEpArray);
}

static void StreamReader_StartScanThread(RedisModuleCtx *ctx, void *data){
    StreamReaderTriggerCtx* srctx = data;
    if(srctx->status == StreamRegistrationStatus_UNREGISTERED){
        StreamReaderTriggerCtx_Free(srctx);
        return;
    }
    srctx->status = StreamRegistrationStatus_OK;
    pthread_create(&srctx->scanThread, NULL, StreamReader_ScanForStreams, srctx);
    pthread_detach(srctx->scanThread);
}

static void StreamReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    StreamReaderTriggerCtx* srctx = privateData;

    if(EPIsFlagOn(ctx, EFIsLocal)){
        Gears_listNode *head = Gears_listFirst(srctx->localPendingExecutions);
        char* epIdStr = NULL;
        while(head){
            epIdStr = Gears_listNodeValue(head);
            Gears_listDelNode(srctx->localPendingExecutions, head);
            if(strcmp(epIdStr, ctx->idStr) != 0){
                RedisModule_Log(NULL, "warning", "Got an out of order execution on registration, ignoring execution.");
                RG_FREE(epIdStr);
                head = Gears_listFirst(srctx->localPendingExecutions);
                continue;
            }
            // Found the execution id, we can stop iterating
            break;
        }
        if(!epIdStr){
            epIdStr = RG_STRDUP(ctx->idStr);
        }
        // Add the execution id to the localDoneExecutions list
        Gears_listAddNodeTail(srctx->localDoneExecutions, epIdStr);
        if(GearsConfig_GetMaxExecutionsPerRegistration() > 0 && Gears_listLength(srctx->localDoneExecutions) > GearsConfig_GetMaxExecutionsPerRegistration()){
            Gears_listNode *head = Gears_listFirst(srctx->localDoneExecutions);
            epIdStr = Gears_listNodeValue(head);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            if(ep){
                RedisModule_Assert(EPIsFlagOn(ep, EFDone));
                RedisGears_DropExecution(ep);
            }
            RG_FREE(epIdStr);
            Gears_listDelNode(srctx->localDoneExecutions, head);
        }
    }

    long long errorsLen = RedisGears_GetErrorsLen(ctx);
    bool ackAndTrim = false;

    if(errorsLen > 0){
        ++srctx->numFailures;
        Record* r = RedisGears_GetError(ctx, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        if(srctx->lastError){
            RG_FREE(srctx->lastError);
        }
        srctx->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));

        if(srctx->args->onFailedPolicy != OnFailedPolicyContinue){
            // we abort pending execution, continue now will cause incorect processing order.
            StreamReader_AbortPendings(srctx);

            // lets clean all our data about all the streams, on restart we will read all the
            // pending data so we will not lose records
            StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);

            if(srctx->args->onFailedPolicy == OnFailedPolicyRetry){
                // Set the status to WAITING_FOR_TIMEOUT_ON_FAILURE, the status will be reflected to the user.
                srctx->status = StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE;
                RedisModule_CreateTimer(staticCtx, srctx->args->retryInterval * 1000, StreamReader_StartScanThread, StreamReaderTriggerCtx_GetShallowCopy(srctx));
            }else if(srctx->args->onFailedPolicy == OnFailedPolicyAbort){
                srctx->status = StreamRegistrationStatus_ABORTED;
            }else{
                RedisModule_Assert(false);
            }
        }else{
            ackAndTrim = true;
        }
    } else if(ctx->status == ABORTED){
        ++srctx->numAborted;
    } else {
        ackAndTrim = true;
        ++srctx->numSuccess;
    }

    if(ackAndTrim && EPIsFlagOn(ctx, EFIsLocal)){
        // we only ack and trim local executions.
        // distributed executions on stream is tricky and should be considered
        // carefully cause order (and in rare situations process only once) can not be promissed.
        // why?
        // 1. oreder can not be promised because because execution might take longer on
        //    another shard and so the finished while be delayed.
        // 2. because execution order might changed, the read pending might read
        //    data that is currently processed and this is why we can not promise exactly once
        //    processing.
        //
        // It might be possible to solve those issues but we decide not to handle with them currently.
        Reader* reader = ExecutionPlan_GetReader(ctx);
        StreamReader_AckAndTrimm(reader->ctx, srctx->args->trimStream);
        StreamReader_TriggerAnotherExecutionIfNeeded(srctx, reader->ctx);
    }

    StreamReaderTriggerCtx_Free(srctx);
}

static void StreamReader_RunOnEvent(SingleStreamReaderCtx* ssrctx, size_t batch, bool readPending){
    StreamReaderTriggerCtx* srtctx = ssrctx->srtctx;
    StreamReaderCtx* readerCtx = StreamReaderCtx_CreateWithConsumerGroup(ssrctx->keyName, GEARS_CONSUMER_GROUP, batch, readPending);
    RedisGears_OnExecutionDoneCallback callback = StreamReader_ExecutionDone;
    void* privateData = StreamReaderTriggerCtx_GetShallowCopy(srtctx);
    ++srtctx->numTriggered;
    char* err = NULL;
    ExecutionPlan* ep = RedisGears_Run(srtctx->fep, srtctx->mode, readerCtx, callback, privateData, srtctx->wd, &err);
    if(!ep){
        ++srtctx->numAborted;
        RedisModule_Log(staticCtx, "warning", "could not execute flat execution on trigger, %s", err);
        if(err){
            RG_FREE(err);
        }
        return;
    }
    if(EPIsFlagOn(ep, EFIsLocal) && srtctx->mode != ExecutionModeSync){
        // execution is local
        // If execution is SYNC it will be added to localDoneExecutions on done
        // Otherwise, save it to the registration pending execution list.
        // currently we are not save global executions and those will not be listed
        // in the registration execution list nor will be drop on unregister.
        // todo: handle none local executions
        char* idStr = RG_STRDUP(ep->idStr);
        Gears_listAddNodeTail(srtctx->localPendingExecutions, idStr);
    }
}

static bool turnedMasterTEOn = false;
static RedisModuleTimerID turnedMasterTimer;

static void StreamReader_CheckIfTurnedMaster(RedisModuleCtx *ctx, void *data){
    int flags = RedisModule_GetContextFlags(ctx);
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        // we are still slave, lets check again in one second
        turnedMasterTimer = RedisModule_CreateTimer(ctx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
        return;
    }

    RedisModule_Log(ctx, "notice", "Become master, trigger a scan on each stream registration.");

    // we become master, we need to trigger a scan for each registration
    turnedMasterTEOn = false;
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        pthread_create(&srctx->scanThread, NULL, StreamReader_ScanForStreams, StreamReaderTriggerCtx_GetShallowCopy(srctx));
        pthread_detach(srctx->scanThread);
    }

}

static void StreamReader_OnTime(RedisModuleCtx *ctx, void *data){
    SingleStreamReaderCtx* ssrctx = data;
    if(ssrctx->freeOnNextTimeEvent){
        RG_FREE(ssrctx->keyName);
        RG_FREE(ssrctx);
        return;
    }
    StreamReader_RunOnEvent(ssrctx, ssrctx->srtctx->args->batchSize,false);
    ssrctx->timerIsSet = false;
    ssrctx->numTriggered = 0;
}

static int StreamReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    int flags = RedisModule_GetContextFlags(ctx);
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        // we are not executing registrations on slave
        if(!turnedMasterTEOn){
            // we need to trigger timer to check if we turned to master
            turnedMasterTimer = RedisModule_CreateTimer(ctx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
            turnedMasterTEOn = true;
        }
        return REDISMODULE_OK;
    }
    if(flags & REDISMODULE_CTX_FLAGS_LOADING){
        // we are not executing registrations on loading
        return REDISMODULE_OK;
    }
    if(strcmp(event, "xadd") != 0 && strcmp(event, "XADD") != 0){
        // we are not trigger on any command other then xadd
        return REDISMODULE_OK;
    }
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    const char* keyName = RedisModule_StringPtrLen(key, NULL);
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        if(srctx->status != StreamRegistrationStatus_OK){
            // we ignore stopped executions
            continue;
        }
        if(StreamReader_IsKeyMatch(srctx->args->streamPrefix, keyName)){
            SingleStreamReaderCtx* ssrctx = Gears_dictFetchValue(srctx->singleStreamData, (char*)keyName);
            if(!ssrctx){
                ssrctx = SingleStreamReaderCtx_Create(ctx, keyName, srctx);
            }
            if(srctx->args->batchSize <= ++ssrctx->numTriggered){
                StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
                ssrctx->numTriggered = 0;
                // we finish the run, if timer is set lets remove it.
                if(!ssrctx->timerIsSet && srctx->args->durationMS > 0){
                    RedisModule_StopTimer(ctx, ssrctx->lastTimerId, NULL);
                    ssrctx->timerIsSet = false;
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

static void StreamReader_UnregisrterTrigger(FlatExecutionPlan* fep, bool abortPending){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, STREAM_TRIGGER_FLAG_POP);
    RedisModule_Assert(srctx);

    if(abortPending){
        StreamReader_AbortPendings(srctx);
    }

    srctx->status = StreamRegistrationStatus_UNREGISTERED;
    StreamReaderTriggerCtx_Free(srctx);
}

static bool StreamReader_IsStream(RedisModuleKey *kp){
    return RedisModule_KeyType(kp) == REDISMODULE_KEYTYPE_STREAM;
}

static void* StreamReader_ScanForStreams(void* pd){
    StreamReaderTriggerCtx* srctx = pd;
    long long cursor = 0;
    do{
        // we do not use the lockhandler cause this thread is temporary
        // and we do not want to allocate any unneeded extra data.
        RedisModule_ThreadSafeContextLock(staticCtx);
        RedisModuleCallReply *reply = RedisModule_Call(staticCtx, "SCAN", "lcccc", cursor, "COUNT", "10000", "MATCH", srctx->args->streamPrefix);
        RedisModule_ThreadSafeContextUnlock(staticCtx);

        bool ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed scanning keys on background", "warning");
        RedisModule_Assert(ret);

        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

        RedisModule_Assert(RedisModule_CallReplyLength(reply) == 2);

        RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

        RedisModule_Assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

        RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
        RedisModule_StringToLongLong(cursorStr, &cursor);
        RedisModule_FreeString(NULL, cursorStr);

        RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
        if(RedisModule_CallReplyLength(keysReply) < 1){
            RedisModule_FreeCallReply(reply);
            continue;
        }
        for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
            RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
            RedisModule_Assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
            RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);

            RedisModule_ThreadSafeContextLock(staticCtx);
            RedisModuleKey *kp = RedisModule_OpenKey(staticCtx, key, REDISMODULE_READ);
            if(kp == NULL){
                RedisModule_ThreadSafeContextUnlock(staticCtx);
                continue;
            }

            // here we need to check, on redis v5 we need to do RedisModule_KeyType(kp) == 0 || RedisModule_KeyType(kp)
            // on redis v6 and above we need RedisModule_KeyType(kp) == 7
            if(StreamReader_IsStream(kp)){
                // this is a stream, on v5 we do not have the stream type on the h file
                // so we compare by numbers directly.
                const char* keyName = RedisModule_StringPtrLen(key, NULL);
                SingleStreamReaderCtx* ssrctx = Gears_dictFetchValue(srctx->singleStreamData, (char*)keyName);
                if(!ssrctx){
                    ssrctx = SingleStreamReaderCtx_Create(staticCtx, keyName, srctx);
                }
            }
            RedisModule_FreeString(staticCtx, key);
            RedisModule_CloseKey(kp);

            RedisModule_ThreadSafeContextUnlock(staticCtx);
        }

        RedisModule_FreeCallReply(reply);
    }while(cursor != 0);

    RedisModule_ThreadSafeContextLock(staticCtx);
    StreamReaderTriggerCtx_Free(srctx);
    RedisModule_ThreadSafeContextUnlock(staticCtx);

    return NULL;
}

static int StreamReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, char** err){
    if(!streamsRegistration){
        streamsRegistration = Gears_listCreate();
        staticCtx = RedisModule_GetThreadSafeContext(NULL);
        if(RedisModule_SubscribeToKeyspaceEvents(staticCtx, REDISMODULE_NOTIFY_STREAM, StreamReader_OnKeyTouched) != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "could not register key space even.");
        }
    }
    RedisModule_Assert(staticCtx);
    StreamReaderTriggerCtx* srctx = StreamReaderTriggerCtx_Create(fep, mode, arg);
    Gears_listAddNodeHead(streamsRegistration, srctx);

    // we create a scan thread that will scan the key space for match streams
    // and trigger executions on them
    int flags = RedisModule_GetContextFlags(staticCtx);
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        // we are not executing registrations on slave
        if(!turnedMasterTEOn){
            turnedMasterTimer = RedisModule_CreateTimer(staticCtx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
            turnedMasterTEOn = true;
        }
    }else{
        // we are master, lets run the scan thread.
        StreamReader_StartScanThread(staticCtx, StreamReaderTriggerCtx_GetShallowCopy(srctx));
    }
    return REDISMODULE_OK;
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
    RedisGears_BWWriteString(bw, triggerArgs->streamPrefix);
    RedisGears_BWWriteLong(bw, triggerArgs->batchSize);
    RedisGears_BWWriteLong(bw, triggerArgs->durationMS);
    RedisGears_BWWriteLong(bw, triggerArgs->onFailedPolicy);
    RedisGears_BWWriteLong(bw, triggerArgs->retryInterval);
    RedisGears_BWWriteLong(bw, triggerArgs->trimStream);
}

static void* StreamReader_DeserializeArgs(Gears_BufferReader* br){
    char* stream = RedisGears_BRReadString(br);
    size_t batchSize = RedisGears_BRReadLong(br);
    size_t durationMS = RedisGears_BRReadLong(br);
    OnFailedPolicy onFailedPolicy = RedisGears_BRReadLong(br);
    size_t retryInterval = RedisGears_BRReadLong(br);
    bool trimStream = RedisGears_BRReadLong(br);
    return StreamReaderTriggerArgs_Create(stream, batchSize, durationMS, onFailedPolicy, retryInterval, trimStream);
}

static void StreamReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    RedisModule_Assert(srctx);
    RedisModule_ReplyWithArray(ctx, 16);
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
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithStringBuffer(ctx, "batchSize", strlen("batchSize"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->batchSize);
    RedisModule_ReplyWithStringBuffer(ctx, "durationMS", strlen("durationMS"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->durationMS);
    RedisModule_ReplyWithStringBuffer(ctx, "stream", strlen("stream"));
    RedisModule_ReplyWithStringBuffer(ctx, srctx->args->streamPrefix, strlen(srctx->args->streamPrefix));
    RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
    switch(srctx->status){
    case StreamRegistrationStatus_OK:
        RedisModule_ReplyWithStringBuffer(ctx, "OK", strlen("OK"));
        break;
    case StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE:
        RedisModule_ReplyWithStringBuffer(ctx, "WAITING_FOR_RETRY_ON_FAILURE", strlen("WAITING_FOR_RETRY_ON_FAILURE"));
        break;
    case StreamRegistrationStatus_ABORTED:
        RedisModule_ReplyWithStringBuffer(ctx, "ABORTED", strlen("ABORTED"));
        break;
    default:
        RedisModule_Assert(false);
    }
}

static void StreamReader_RdbSave(RedisModuleIO *rdb){
    if(!streamsRegistration){
        RedisModule_SaveUnsigned(rdb, 0); // done
        return;
    }

    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);

    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        RedisModule_SaveUnsigned(rdb, 1); // has more
        size_t len;
        int res = FlatExecutionPlan_Serialize(&bw, srctx->fep, NULL);
        RedisModule_Assert(res == REDISMODULE_OK); // fep already registered, must be serializable.

        StreamReader_SerializeArgs(srctx->args, &bw);

        RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

        RedisModule_SaveUnsigned(rdb, srctx->mode);

        Gears_BufferClear(buff);
    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);

    Gears_BufferFree(buff);
}

static void StreamReader_RdbLoad(RedisModuleIO *rdb, int encver){
    while(RedisModule_LoadUnsigned(rdb)){
        size_t len;
        char* data = RedisModule_LoadStringBuffer(rdb, &len);
        RedisModule_Assert(data);

        Gears_Buffer buff = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader reader;
        Gears_BufferReaderInit(&reader, &buff);

        char* err = NULL;
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&reader, &err, encver);
        if(!fep){
            RedisModule_Log(NULL, "warning", "Could not deserialize flat execution, error='%s'", err);
            RedisModule_Assert(false);
        }

        void* args = StreamReader_DeserializeArgs(&reader);
        RedisModule_Free(data);

        int mode = RedisModule_LoadUnsigned(rdb);
        int ret = StreamReader_RegisrterTrigger(fep, mode, args, &err);
        if(ret != REDISMODULE_OK){
            RedisModule_Log(NULL, "warning", "Could not register flat execution, error='%s'", err);
            RedisModule_Assert(false);
        }
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
        Gears_listDelNode(streamsRegistration, node);
    }
    Gears_listReleaseIterator(iter);
}

static void StreamReader_FreeArgs(void* args){
    StreamReaderTriggerArgs_Free(args);
}

RedisGears_ReaderCallbacks StreamReader = {
        .create = StreamReader_Create,
        .registerTrigger = StreamReader_RegisrterTrigger,
        .unregisterTrigger = StreamReader_UnregisrterTrigger,
        .serializeTriggerArgs = StreamReader_SerializeArgs,
        .deserializeTriggerArgs = StreamReader_DeserializeArgs,
        .freeTriggerArgs = StreamReader_FreeArgs,
        .dumpRegistratioData = StreamReader_DumpRegistrationData,
        .rdbSave = StreamReader_RdbSave,
        .rdbLoad = StreamReader_RdbLoad,
        .clear = StreamReader_Clear,
};
