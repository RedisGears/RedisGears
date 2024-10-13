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
#include "readers_common.h"
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>

#define STREAM_REGISTRATION_INIT_SIZE 10
static Gears_list* streamsRegistration = NULL;
static RedisModuleTimerID turnedMasterTimer;
static bool turnedMasterTEOn = false;

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
    StreamRegistrationStatus_PAUSED,
}StreamRegistrationStatus;

typedef struct StreamReaderTriggerCtx{
    size_t refCount;
    StreamRegistrationStatus status;
    bool isRetryTimmerSet;
    StreamReaderTriggerArgs* args;
    Gears_dict* singleStreamData;
    FlatExecutionPlan* fep;
    ExecutionMode mode;
    long long numTriggered;
    long long numAborted;
    long long numSuccess;
    long long numFailures;
    long long lastRunDuration;
    long long totalRunDuration;
    long long lastBatchLag;
    long long totalBatchLag;
    char* lastError;
    pthread_t scanThread;
    Gears_list* localDoneExecutions;
    WorkerData* wd;
    size_t epoc;
}StreamReaderTriggerCtx;

typedef struct SingleStreamReaderCtx{
    Gears_list *batchStartTime;
    bool timerIsSet;
    bool freeOnNextTimeEvent;
    RedisModuleTimerID lastTimerId;
    StreamReaderTriggerCtx* srtctx; // weak ptr to the StreamReaderCtx
    char* keyName;
    long long pendingMessages;
    long long nextBatch;
    bool isRunning;
    bool isFreeWhenDone;
    size_t createdEpoc;
}SingleStreamReaderCtx;

static void* StreamReader_ScanForStreams(void* pd);
static void StreamReader_Free(void* ctx);
static void StreamReader_CheckIfTurnedMaster(RedisModuleCtx *ctx, void *data);
static void StreamReader_OnTime(RedisModuleCtx *ctx, void *data);

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
        RedisModule_Log(staticCtx, logLevel, "%s : %s (errno=%d, errono_str=%s)", msgPrefix, err, errno, strerror(errno));
        if(reply) RedisModule_FreeCallReply(reply);
        return false;
    }
    return true;
}

static void StreamReader_RunOnEvent(SingleStreamReaderCtx* ssrctx, size_t batch, bool readPending);

#define StreamIdZero (StreamId){.first = 0, .second = 0}

#define StreamIdIsZero(id) (id.first == 0 && id.second == 0)

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

static int StreamReader_ReadStreamLen(RedisModuleCtx *rctx, SingleStreamReaderCtx* ssrctx){
    RedisModuleCallReply *reply = RedisModule_Call(rctx, "XLEN", "c", ssrctx->keyName);
    if(!StreamReader_VerifyCallReply(rctx, reply, "Failed on XLEN command", "warning")){
        return false;
    }
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER);
    ssrctx->pendingMessages = RedisModule_CallReplyInteger(reply);
    RedisModule_FreeCallReply(reply);
    return true;
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
                                                           StreamReaderTriggerCtx* srtctx,
                                                           bool setCurrentTime){
    SingleStreamReaderCtx* ssrctx = RG_ALLOC(sizeof(*ssrctx));
    ssrctx->srtctx = srtctx;
    ssrctx->keyName = RG_STRDUP(keyName);
    ssrctx->timerIsSet = false;
    ssrctx->freeOnNextTimeEvent = false;
    ssrctx->isRunning = false;
    ssrctx->isFreeWhenDone = false;
    ssrctx->nextBatch = 0;
    ssrctx->batchStartTime = Gears_listCreate();
    ssrctx->createdEpoc = srtctx->epoc;
    if (!StreamReader_ReadStreamLen(ctx, ssrctx)) {
        RG_FREE(ssrctx->keyName);
        RG_FREE(ssrctx);
        return NULL;
    }
    if (ssrctx->pendingMessages) {
        for (size_t i = 0 ; i < ssrctx->pendingMessages ; i+=srtctx->args->batchSize) {
            long long currTime = 0;
            if (setCurrentTime) {
                struct timespec t = {0};
                clock_gettime(CLOCK_REALTIME, &t);
                currTime = (long long)1000000000 * (t.tv_sec) + (t.tv_nsec);
            }
            Gears_listAddNodeTail(ssrctx->batchStartTime, (void*)currTime);
        }
    }
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
        } else if (ssrctx->isRunning){
            ssrctx->isFreeWhenDone = true;
        } else {
            Gears_listRelease(ssrctx->batchStartTime);
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

        while((n = Gears_listFirst(srtctx->localDoneExecutions))){
            char* epIdStr = Gears_listNodeValue(n);
            Gears_listDelNode(srtctx->localDoneExecutions, n);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            RG_FREE(epIdStr);
            if(!ep){
                RedisModule_Log(staticCtx, "info", "Failed finding done execution to drop on unregister. Execution was probably already dropped.");
                continue;
            }
            // all the executions here are done, will just drop it.
            RedisGears_DropExecution(ep);
        }

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
        .status = StreamRegistrationStatus_PAUSED,
        .args = args,
        .singleStreamData = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .fep = fep,
        .mode = mode,
        .numTriggered = 0,
        .numAborted = 0,
        .numSuccess = 0,
        .numFailures = 0,
        .lastRunDuration = 0,
        .totalRunDuration = 0,
        .lastBatchLag = 0,
        .totalBatchLag = 0,
        .lastError = NULL,
        .localDoneExecutions = Gears_listCreate(),
        .wd = RedisGears_WorkerDataCreate(fep->executionThreadPool),
        .isRetryTimmerSet = false,
        .epoc = 0,
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

static StreamReaderCtx* StreamReaderCtx_CreateWithConsumerGroup(const char* streamName, const char* consumerGroup, size_t batchSize, bool readPenging){
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
        if (RedisModule_CallReplyType(values) == REDISMODULE_REPLY_ARRAY) {
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
        }
        Gears_listAddNodeHead(readerCtx->records, r);
    }

    if(lastReadId){
        readerCtx->lastReadId = StreamReader_ParseStreamId(lastReadId);
    }

    RedisModule_FreeCallReply(reply);
    readerCtx->isDone = true;
}

static int StreamReader_CtxDeserialize(ExecutionCtx* ectx, void* ctx, Gears_BufferReader* br){
    StreamReaderCtx* readerCtx = ctx;
    readerCtx->streamKeyName = RG_STRDUP(RedisGears_BRReadString(br));
    if(RedisGears_BRReadLong(br)){
        readerCtx->streamId = RG_STRDUP(RedisGears_BRReadString(br));
    }
    if(RedisGears_BRReadLong(br)){
        readerCtx->consumerGroup = RG_STRDUP(RedisGears_BRReadString(br));
    }

    readerCtx->batchSize = RedisGears_BRReadLong(br);
    return REDISMODULE_OK;
}

static int StreamReader_CtxSerialize(ExecutionCtx* ectx, void* ctx, Gears_BufferWriter* bw){
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
    return REDISMODULE_OK;
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

static int StreamReader_HasData(StreamReaderCtx* readerCtx){
    return array_len(readerCtx->batchIds) > 0;
}

static void StreamReader_AckAndTrimm(StreamReaderCtx* readerCtx, SingleStreamReaderCtx* ssrctx, bool alsoTrimm){
    if(array_len(readerCtx->batchIds) == 0){
        // nothing to ack on
        return;
    }

    ssrctx->pendingMessages -= array_len(readerCtx->batchIds);
    if(ssrctx->pendingMessages < 0){
        ssrctx->pendingMessages = 0;
        RedisModule_Log(staticCtx, "warning", "Calculated stream length turned negative, set to 0.");
    }

    RedisModuleCallReply* reply = RedisModule_Call(staticCtx, "XACK", "!ccv", readerCtx->streamKeyName, readerCtx->consumerGroup, readerCtx->batchIds, (size_t)array_len(readerCtx->batchIds));
    bool ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed acking messages", "warning");
    if(!ret){
        // we all not crash on failure, someone is messing up with the stream.
        return;
    }

    RedisModule_FreeCallReply(reply);

    if(alsoTrimm){
        reply = RedisModule_Call(staticCtx, "XTRIM", "!ccl", readerCtx->streamKeyName, "MAXLEN", ssrctx->pendingMessages);
        ret = StreamReader_VerifyCallReply(staticCtx, reply, "Failed Trim messages", "warning");
        if(!ret){
            // we will not crash on failure, someone is messing up with the stream.
            return;
        }

        RedisModule_FreeCallReply(reply);
    }
}

static void StreamReader_TriggerAnotherExecutionIfNeeded(StreamReaderTriggerCtx* srctx, SingleStreamReaderCtx* ssrctx, int hasData, int readPending){
    if (!ssrctx->pendingMessages) {
        /* Nothing to run on */
        ssrctx->isRunning = false;
        return;
    }
    if (!hasData && !readPending && ssrctx->nextBatch == 0) {
        // Did not processed any data on this batch, and we do not have any new data added.
        // There is not need to trigger another execution.
        ssrctx->isRunning = false;
        // it is safe to set pending message to 0 here, we know we are at the end of the stream.
        ssrctx->pendingMessages = 0;
        return;
    }
    if(srctx->args->batchSize <= ssrctx->pendingMessages){
        StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
        return;
    }
    long long batchStartTime = 0;
    Gears_listNode *first = Gears_listFirst(ssrctx->batchStartTime);
    if (first){
        batchStartTime = (long long)Gears_listNodeValue(first);
    }
    if (!batchStartTime) {
        /* we do not know when the batch started, data was in the stream
         * when we started the run, we will trigger an execution. */
        StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
        ssrctx->nextBatch = 0; /* trigger execution in the middle of batch require us to reset the batch */
        return;
    }
    if(srctx->args->durationMS) {
        /* check if we should run on the pending messages anyway.
         * If we reached the configured timeout we will trigger
         * the run anyway, otherwise we will set a timer with the
         * left timeout. */
        struct timespec currTimeSpec = {0};
        clock_gettime(CLOCK_REALTIME, &currTimeSpec);
        long long currTime = ((long long)1000000000 * (currTimeSpec.tv_sec) + (currTimeSpec.tv_nsec));
        if ((currTime - batchStartTime >= srctx->args->durationMS * 1000000)) {
            /* Duration pass, trigger the run now. */
            StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
            ssrctx->nextBatch = 0; /* trigger execution in the middle of batch require us to reset the batch */
            return;
        }
        RedisModule_Assert(!ssrctx->timerIsSet);
        ssrctx->lastTimerId = RedisModule_CreateTimer(staticCtx, srctx->args->durationMS - (currTime - batchStartTime)/1000000, StreamReader_OnTime, ssrctx);
        ssrctx->timerIsSet = true;
    }
    ssrctx->isRunning = false;
}

static void StreamReader_StartScanThread(RedisModuleCtx *ctx, void *data){
    StreamReaderTriggerCtx* srctx = data;
    srctx->isRetryTimmerSet = false;
    if(srctx->status == StreamRegistrationStatus_UNREGISTERED){
        StreamReaderTriggerCtx_Free(srctx);
        return;
    }
    srctx->epoc++;
    srctx->status = StreamRegistrationStatus_OK;
    pthread_create(&srctx->scanThread, NULL, StreamReader_ScanForStreams, srctx);
    pthread_detach(srctx->scanThread);
}

typedef struct StreamReaderExecutionCtx {
    StreamReaderTriggerCtx* srctx;
    SingleStreamReaderCtx* ssrctx;
} StreamReaderExecutionCtx;

static void StreamReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    StreamReaderExecutionCtx *pd = privateData;
    StreamReaderTriggerCtx* srctx = pd->srctx;
    SingleStreamReaderCtx* ssrctx = pd->ssrctx;


    int flags = RedisModule_GetContextFlags(staticCtx);

    // Add the execution id to the localDoneExecutions list
    char *currentRunningExecution = RG_STRDUP(RedisGears_GetId(ctx));
    if (currentRunningExecution) {
        Gears_listAddNodeTail(srctx->localDoneExecutions, currentRunningExecution);
        if(GearsConfig_GetMaxExecutionsPerRegistration() > 0 && Gears_listLength(srctx->localDoneExecutions) > GearsConfig_GetMaxExecutionsPerRegistration()){
            Gears_listNode *head = Gears_listFirst(srctx->localDoneExecutions);
            char* epIdStr = Gears_listNodeValue(head);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            if(ep){
                RedisModule_Assert(EPIsFlagOn(ep, EFDone));
                RedisGears_DropExecution(ep);
            }
            RG_FREE(epIdStr);
            Gears_listDelNode(srctx->localDoneExecutions, head);
        }
    }

    srctx->lastRunDuration = FlatExecutionPlan_GetExecutionDuration(ctx);
    srctx->totalRunDuration += srctx->lastRunDuration;

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

        if (ssrctx->createdEpoc == srctx->epoc) {
            if (strncmp(srctx->lastError, "PAUSE", 5) == 0 || srctx->status == StreamRegistrationStatus_PAUSED) {
                StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);
                srctx->status = StreamRegistrationStatus_PAUSED;
            } else if(srctx->args->onFailedPolicy != OnFailedPolicyContinue){
                // lets clean all our data about all the streams, on restart we will read all the
                // pending data so we will not lose records
                StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);
                if(srctx->args->onFailedPolicy == OnFailedPolicyRetry){
                    // only retrigger on master
                    if(flags & REDISMODULE_CTX_FLAGS_MASTER){
                        // Set the status to WAITING_FOR_TIMEOUT_ON_FAILURE, the status will be reflected to the user.
                        srctx->status = StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE;
                        if (!srctx->isRetryTimmerSet) {
                            RedisModule_CreateTimer(staticCtx, srctx->args->retryInterval * 1000, StreamReader_StartScanThread, StreamReaderTriggerCtx_GetShallowCopy(srctx));
                            srctx->isRetryTimmerSet = true;
                        }
                    } else {
                        // set timer to check if we turn master
                        if(!turnedMasterTEOn){
                            turnedMasterTimer = RedisModule_CreateTimer(staticCtx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
                            turnedMasterTEOn = true;
                        }
                    }
                }else if(srctx->args->onFailedPolicy == OnFailedPolicyAbort){
                    srctx->status = StreamRegistrationStatus_ABORTED;
                }else{
                    RedisModule_Assert(false);
                }
            }else{
                ackAndTrim = true;
            }
        }
    } else if(ctx->status == ABORTED){
        ++srctx->numAborted;
    } else {
        ackAndTrim = true;
        ++srctx->numSuccess;
    }

    if(ackAndTrim && ssrctx->createdEpoc == srctx->epoc){
        Reader* reader = ExecutionPlan_GetReader(ctx);
        StreamReaderCtx* srCtx = ((StreamReaderCtx*)reader->ctx);
        if (!srCtx->readPenging) {
            /* calculate lag, no lag information on not yet acked data */
            Gears_listNode *first = Gears_listFirst(ssrctx->batchStartTime);
            if (first) {
                long long batchStartTime = (long long)Gears_listNodeValue(first);
                Gears_listDelNode(ssrctx->batchStartTime, first);
                if (batchStartTime) {
                    struct timespec currTimeSpec = {0};
                    clock_gettime(CLOCK_REALTIME, &currTimeSpec);
                    long long currTime = ((long long)1000000000 * (currTimeSpec.tv_sec) + (currTimeSpec.tv_nsec));
                    srctx->lastBatchLag = currTime - batchStartTime;
                    srctx->totalBatchLag += srctx->lastBatchLag;
                }
            } else {
                /* someone is messing up with the stream, and the stream size is not as we think, read it again */
                StreamReader_ReadStreamLen(staticCtx, ssrctx);
            }
        }
        if (flags & REDISMODULE_CTX_FLAGS_MASTER) {
            /* only if we are master we should continue trigger events */
            StreamReader_AckAndTrimm(reader->ctx, ssrctx, srctx->args->trimStream);
            if (!ssrctx->isFreeWhenDone) {
                StreamReader_TriggerAnotherExecutionIfNeeded(srctx, ssrctx, StreamReader_HasData(srCtx), srCtx->readPenging);
            } else {
                ssrctx->isRunning = false;
            }
        } else {
            ssrctx->isRunning = false;
            if(!turnedMasterTEOn){
                StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);
                // set timer to check if we turn master
                turnedMasterTimer = RedisModule_CreateTimer(staticCtx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
                turnedMasterTEOn = true;
            }
        }
    } else {
        ssrctx->isRunning = false;
    }

    if (ssrctx->isFreeWhenDone) {
        Gears_listRelease(ssrctx->batchStartTime);
        RG_FREE(ssrctx->keyName);
        RG_FREE(ssrctx);
    }
    StreamReaderTriggerCtx_Free(srctx);
    RG_FREE(pd);
}

static void StreamReader_RunOnEvent(SingleStreamReaderCtx* ssrctx, size_t batch, bool readPending){
    StreamReaderTriggerCtx* srtctx = ssrctx->srtctx;
    StreamReaderCtx* readerCtx = StreamReaderCtx_CreateWithConsumerGroup(ssrctx->keyName, GEARS_CONSUMER_GROUP, batch, readPending);
    RedisGears_OnExecutionDoneCallback callback = StreamReader_ExecutionDone;
    StreamReaderExecutionCtx *pd = RG_ALLOC(sizeof(*pd));
    pd->srctx = StreamReaderTriggerCtx_GetShallowCopy(srtctx);
    pd->ssrctx = ssrctx;
    ++srtctx->numTriggered;
    char* err = NULL;
    ssrctx->isRunning = true;
    ExecutionPlan* ep = RedisGears_Run(srtctx->fep, srtctx->mode, readerCtx, callback, pd, srtctx->wd, &err);
    if(!ep){
        ++srtctx->numAborted;
        RedisModule_Log(staticCtx, "warning", "could not execute flat execution on trigger, %s", err);
        if(err){
            RG_FREE(err);
        }
        return;
    }
}

static void StreamReader_CheckIfTurnedMaster(RedisModuleCtx *ctx, void *data){
    int flags = RedisModule_GetContextFlags(ctx);
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        // we are still slave, lets check again in one second
        turnedMasterTimer = RedisModule_CreateTimer(ctx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
        return;
    }

    RedisModule_Log(staticCtx, "notice", "Become master, trigger a scan on each stream registration.");

    // we become master, we need to trigger a scan for each registration
    turnedMasterTEOn = false;
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srctx = Gears_listNodeValue(node);
        pthread_create(&srctx->scanThread, NULL, StreamReader_ScanForStreams, StreamReaderTriggerCtx_GetShallowCopy(srctx));
        pthread_detach(srctx->scanThread);
    }
    Gears_listReleaseIterator(iter);

}

static void StreamReader_OnTime(RedisModuleCtx *ctx, void *data){
    int flags = RedisModule_GetContextFlags(staticCtx);
    SingleStreamReaderCtx* ssrctx = data;
    if(ssrctx->freeOnNextTimeEvent){
        Gears_listRelease(ssrctx->batchStartTime);
        RG_FREE(ssrctx->keyName);
        RG_FREE(ssrctx);
        return;
    }
    ssrctx->nextBatch = 0; /* trigger execution in the middle of batch require us to reset the batch */
    ssrctx->timerIsSet = false;
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        if(!turnedMasterTEOn){
            StreamReaderTriggerCtx_CleanSingleStreamsData(ssrctx->srtctx);
            // set timer to check if we turn master
            turnedMasterTimer = RedisModule_CreateTimer(staticCtx, 1000, StreamReader_CheckIfTurnedMaster, NULL);
            turnedMasterTEOn = true;
        }
        return;
    }
    StreamReader_RunOnEvent(ssrctx, ssrctx->srtctx->args->batchSize,false);
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
                ssrctx = SingleStreamReaderCtx_Create(ctx, keyName, srctx, true);
                RedisModule_Assert(ssrctx);
            } else {
                ++ssrctx->pendingMessages;
                ++ssrctx->nextBatch;
                if (ssrctx->nextBatch == 1) {
                    /* first element in a batch save current time for lag calculation */
                    struct timespec t = {0};
                    clock_gettime(CLOCK_REALTIME, &t);
                    Gears_listAddNodeTail(ssrctx->batchStartTime, (void*)((long long)1000000000 * (t.tv_sec) + (t.tv_nsec)));
                }
                if (ssrctx->nextBatch == srctx->args->batchSize) {
                    ssrctx->nextBatch = 0;
                }
            }
            if(srctx->args->batchSize <= ssrctx->pendingMessages && !ssrctx->isRunning){
                if(ssrctx->timerIsSet){
                    RedisModule_StopTimer(ctx, ssrctx->lastTimerId, NULL);
                    ssrctx->timerIsSet = false;
                }
                StreamReader_RunOnEvent(ssrctx, srctx->args->batchSize, false);
                // we finish the run, if timer is set lets remove it.
            } else if (!ssrctx->isRunning) {
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

static void StreamReader_PauseTrigger(FlatExecutionPlan* fep, bool abortPending){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    RedisModule_Assert(srctx);

    if (srctx->status == StreamRegistrationStatus_OK ||
        srctx->status == StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE) {
        StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);
        srctx->status = StreamRegistrationStatus_PAUSED;
    }
}

static void StreamReader_UnpauseTrigger(FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    RedisModule_Assert(srctx);
    StreamReader_StartScanThread(staticCtx, StreamReaderTriggerCtx_GetShallowCopy(srctx));
}

static void StreamReader_UnregisrterTrigger(FlatExecutionPlan* fep, bool abortPending){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, STREAM_TRIGGER_FLAG_POP);
    RedisModule_Assert(srctx);

    srctx->status = StreamRegistrationStatus_UNREGISTERED;
    StreamReaderTriggerCtx_CleanSingleStreamsData(srctx);
    StreamReaderTriggerCtx_Free(srctx);
}

static bool StreamReader_IsStream(RedisModuleKey *kp){
    return RedisModule_KeyType(kp) == REDISMODULE_KEYTYPE_STREAM;
}

typedef struct ScanCtx {
    Gears_list *keys;
    char *pattern;
} ScanCtx;

static void StreamReader_ScanFreeKey(void* pd){
    if (!pd) return;
    RedisModuleString *keyname = pd;
    RedisModule_FreeString(NULL, keyname);
}

static int stringmatchlen(const char *pattern, int patternLen,const char *string, int stringLen, int nocase, int *skipLongerMatches, int nesting)
{
    /* Protection against abusive patterns. */
    if (nesting > 1000) return 0;

    while(patternLen && stringLen) {
        switch(pattern[0]) {
        case '*':
            while (patternLen && pattern[1] == '*') {
                pattern++;
                patternLen--;
            }
            if (patternLen == 1)
                return 1; /* match */
            while(stringLen) {
                if (stringmatchlen(pattern+1, patternLen-1,
                            string, stringLen, nocase, skipLongerMatches, nesting+1))
                    return 1; /* match */
                if (*skipLongerMatches)
                    return 0; /* no match */
                string++;
                stringLen--;
            }
            /* There was no match for the rest of the pattern starting
             * from anywhere in the rest of the string. If there were
             * any '*' earlier in the pattern, we can terminate the
             * search early without trying to match them to longer
             * substrings. This is because a longer match for the
             * earlier part of the pattern would require the rest of the
             * pattern to match starting later in the string, and we
             * have just determined that there is no match for the rest
             * of the pattern starting from anywhere in the current
             * string. */
            *skipLongerMatches = 1;
            return 0; /* no match */
            break;
        case '?':
            string++;
            stringLen--;
            break;
        case '[':
        {
            int not, match;

            pattern++;
            patternLen--;
            not = pattern[0] == '^';
            if (not) {
                pattern++;
                patternLen--;
            }
            match = 0;
            while(1) {
                if (pattern[0] == '\\' && patternLen >= 2) {
                    pattern++;
                    patternLen--;
                    if (pattern[0] == string[0])
                        match = 1;
                } else if (pattern[0] == ']') {
                    break;
                } else if (patternLen == 0) {
                    pattern--;
                    patternLen++;
                    break;
                } else if (patternLen >= 3 && pattern[1] == '-') {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];
                    if (start > end) {
                        int t = start;
                        start = end;
                        end = t;
                    }
                    if (nocase) {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }
                    pattern += 2;
                    patternLen -= 2;
                    if (c >= start && c <= end)
                        match = 1;
                } else {
                    if (!nocase) {
                        if (pattern[0] == string[0])
                            match = 1;
                    } else {
                        if (tolower((int)pattern[0]) == tolower((int)string[0]))
                            match = 1;
                    }
                }
                pattern++;
                patternLen--;
            }
            if (not)
                match = !match;
            if (!match)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        }
        case '\\':
            if (patternLen >= 2) {
                pattern++;
                patternLen--;
            }
            /* fall through */
        default:
            if (!nocase) {
                if (pattern[0] != string[0])
                    return 0; /* no match */
            } else {
                if (tolower((int)pattern[0]) != tolower((int)string[0]))
                    return 0; /* no match */
            }
            string++;
            stringLen--;
            break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0) {
            while(*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringLen == 0)
        return 1;
    return 0;
}

static void StreamReader_ScanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key, void *privdata) {
    ScanCtx *scanCtx = privdata;
    size_t len;
    int skipLongerMatches = 0;
    const char *keyNameBuff = RedisModule_StringPtrLen(keyname, &len);
    if (!stringmatchlen(scanCtx->pattern, strlen(scanCtx->pattern), keyNameBuff, len, 0, &skipLongerMatches, 0)) {
        return;
    }
    RedisModule_RetainString(ctx, keyname);
    Gears_listAddNodeHead(scanCtx->keys, keyname);
}

static int StreamReader_HandleKeys(Gears_list *keys, StreamReaderTriggerCtx* srctx) {
    Gears_listNode *node = NULL;
    while (node = Gears_listFirst(keys)) {
        /* Get ownership of the value */
        RedisModuleString *key = Gears_listNodeValue(node);
        Gears_listNodeValue(node) = NULL;
        /* Must free the node when GIL is held because RedisString are not thread safe. */
        Gears_listDelNode(keys, node);
        LockHandler_Acquire(staticCtx);
        if (srctx->status != StreamRegistrationStatus_OK) {
            RedisModule_FreeString(staticCtx, key);
            LockHandler_Release(staticCtx);
            RedisModule_Log(staticCtx, "warning", "Stream registration was aborted while key space was scanned");
            return 0;
        }
        RedisModuleKey *kp = RedisModule_OpenKey(staticCtx, key, REDISMODULE_READ);
        if(kp == NULL){
            RedisModule_FreeString(staticCtx, key);
            LockHandler_Release(staticCtx);
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
                size_t retries = 0;
                while(!(ssrctx = SingleStreamReaderCtx_Create(staticCtx, keyName, srctx, false))) {
                    // creating stream reader ctx can failed if cluster is not initialized.
                    // sleep for 1 second and retry (but not more than 10 retries)
                    if(++retries > 10) {
                        RedisModule_Log(staticCtx, "warning", "Failed creating stream reader ctx for key %s, for more than 10 times, abort.", keyName);
                        break;
                    }
                    LockHandler_Release(staticCtx);
                    RedisModule_Log(staticCtx, "warning", "Failed creating stream reader ctx for key %s, will retry in 5 seconds.", keyName);
                    sleep(5);
                    LockHandler_Acquire(staticCtx);

                }
            }
        }
        RedisModule_FreeString(staticCtx, key);
        RedisModule_CloseKey(kp);

        LockHandler_Release(staticCtx);
    }
    return 1;
}

static void* StreamReader_ScanForStreams(void* pd){
    RedisGears_LockHanlderRegister();
    StreamReaderTriggerCtx* srctx = pd;
    while (!Cluster_IsInitialized()) {
    	/* Wait until cluster is initialized,
    	 * Notice that there might be an adge case
    	 * where cluster will be initialized here but
    	 * will get uninitialized when we try to read
    	 * the stream, for this we also have retry mechanism */
    	sleep(1);
    }
    ScanCtx scanCtx = (ScanCtx){
        .pattern = srctx->args->streamPrefix,
        .keys = Gears_listCreate(),
    };
    Gears_listSetFreeMethod(scanCtx.keys, StreamReader_ScanFreeKey);
    LockHandler_Acquire(staticCtx);
    RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
    bool isDone = false;
    while (!isDone && RedisModule_Scan(staticCtx, cursor, StreamReader_ScanCallback, &scanCtx)) {
        LockHandler_Release(staticCtx);
        if (!StreamReader_HandleKeys(scanCtx.keys, srctx)) {
            isDone = 1;
            LockHandler_Acquire(staticCtx);
            break;
        }
        LockHandler_Acquire(staticCtx);
        /* Must free the list when GIL is held because RedisString are not thread safe. */
        Gears_listRelease(scanCtx.keys);
        scanCtx.keys = Gears_listCreate();
        Gears_listSetFreeMethod(scanCtx.keys, StreamReader_ScanFreeKey);
    }
    if (!isDone) {
        LockHandler_Release(staticCtx);
        StreamReader_HandleKeys(scanCtx.keys, srctx);
        LockHandler_Acquire(staticCtx);
    }
    Gears_listRelease(scanCtx.keys);
    RedisModule_ScanCursorDestroy(cursor);
    StreamReaderTriggerCtx_Free(srctx);
    LockHandler_Release(staticCtx);

    return NULL;
}

static int StreamReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, char** err){
    if(!streamsRegistration){
        streamsRegistration = Gears_listCreate();
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
        srctx->status = StreamRegistrationStatus_OK;
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

static void* StreamReader_DeserializeArgs(Gears_BufferReader* br, int encver){
    char* stream = RedisGears_BRReadString(br);
    size_t batchSize = RedisGears_BRReadLong(br);
    size_t durationMS = RedisGears_BRReadLong(br);
    OnFailedPolicy onFailedPolicy = RedisGears_BRReadLong(br);
    size_t retryInterval = RedisGears_BRReadLong(br);
    bool trimStream = RedisGears_BRReadLong(br);
    return StreamReaderTriggerArgs_Create(stream, batchSize, durationMS, onFailedPolicy, retryInterval, trimStream);
}

static void StreamReader_DumpRegistrationInfo(FlatExecutionPlan* fep, RedisModuleInfoCtx *ctx, int for_crash_report) {
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);

    if(srctx->mode == ExecutionModeSync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "sync");
    } else if(srctx->mode == ExecutionModeAsync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async");
    } else if(srctx->mode == ExecutionModeAsyncLocal){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async_local");
    } else{
        RedisModule_InfoAddFieldCString(ctx, "mode", "unknown");
    }

    RedisModule_InfoAddFieldULongLong(ctx, "numTriggered", srctx->numTriggered);
    RedisModule_InfoAddFieldULongLong(ctx, "numSuccess", srctx->numSuccess);
    RedisModule_InfoAddFieldULongLong(ctx, "numFailures", srctx->numFailures);
    RedisModule_InfoAddFieldULongLong(ctx, "numAborted", srctx->numAborted);
    RedisModule_InfoAddFieldULongLong(ctx, "lastRunDurationMS", DURATION2MS(srctx->lastRunDuration));
    RedisModule_InfoAddFieldULongLong(ctx, "totalRunDurationMS", totalDurationMS(srctx));
    RedisModule_InfoAddFieldDouble(ctx, "avgRunDurationMS", avgDurationMS(srctx));
    RedisModule_InfoAddFieldLongLong(ctx, "lastEstimatedLagMS", DURATION2MS(srctx->lastBatchLag));
    if (srctx->numSuccess + srctx->numFailures + srctx->numAborted == 0) {
        RedisModule_InfoAddFieldDouble(ctx, "avgEstimatedLagMS", 0);
    } else {
        RedisModule_InfoAddFieldDouble(ctx, "avgEstimatedLagMS", ((double)DURATION2MS(srctx->totalBatchLag) / (srctx->numSuccess + srctx->numFailures + srctx->numAborted)));
    }
    RedisModule_InfoAddFieldCString(ctx, "lastError", srctx->lastError ? srctx->lastError : "None");
    RedisModule_InfoAddFieldULongLong(ctx, "batchSize", srctx->args->batchSize);
    RedisModule_InfoAddFieldULongLong(ctx, "durationMS", srctx->args->durationMS);
    RedisModule_InfoAddFieldCString(ctx, "stream", srctx->args->streamPrefix);
    switch(srctx->status){
    case StreamRegistrationStatus_OK:
        RedisModule_InfoAddFieldCString(ctx, "status", "OK");
        break;
    case StreamRegistrationStatus_WAITING_FOR_RETRY_ON_FAILURE:
        RedisModule_InfoAddFieldCString(ctx, "status", "WAITING_FOR_RETRY_ON_FAILURE");
        break;
    case StreamRegistrationStatus_ABORTED:
        RedisModule_InfoAddFieldCString(ctx, "status", "ABORTED");
        break;
    case StreamRegistrationStatus_PAUSED:
        RedisModule_InfoAddFieldCString(ctx, "status", "PAUSED");
        break;
    default:
        RedisModule_Assert(false);
    }
    RedisModule_InfoAddFieldULongLong(ctx, "trimStream", srctx->args->trimStream);

    switch(srctx->args->onFailedPolicy){
    case OnFailedPolicyContinue:
        RedisModule_InfoAddFieldCString(ctx, "onFailedPolicy", "continue");
        break;
    case OnFailedPolicyAbort:
        RedisModule_InfoAddFieldCString(ctx, "onFailedPolicy", "abort");
        break;
    case OnFailedPolicyRetry:
        RedisModule_InfoAddFieldCString(ctx, "onFailedPolicy", "retry");
        break;
    default:
        RedisModule_Assert(false);
    }

    RedisModule_InfoAddFieldULongLong(ctx, "retryInterval", srctx->args->retryInterval);
}

static void StreamReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    StreamReaderTriggerCtx* srctx = StreamReader_GetStreamTriggerCtxByFep(fep, 0);
    RedisModule_Assert(srctx);
    RedisModule_ReplyWithArray(ctx, 26);
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
    RedisModule_ReplyWithStringBuffer(ctx, "lastRunDurationMS", strlen("lastRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, DURATION2MS(srctx->lastRunDuration));
    RedisModule_ReplyWithStringBuffer(ctx, "totalRunDurationMS", strlen("totalRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, totalDurationMS(srctx));
    RedisModule_ReplyWithStringBuffer(ctx, "avgRunDurationMS", strlen("avgRunDurationMS"));
    RedisModule_ReplyWithDouble(ctx, avgDurationMS(srctx));
    RedisModule_ReplyWithStringBuffer(ctx, "lastEstimatedLagMS", strlen("lastEstimatedLagMS"));
    RedisModule_ReplyWithLongLong(ctx, DURATION2MS(srctx->lastBatchLag));
    RedisModule_ReplyWithStringBuffer(ctx, "avgEstimatedLagMS", strlen("avgEstimatedLagMS"));
    if (srctx->numSuccess + srctx->numFailures + srctx->numAborted == 0) {
        RedisModule_ReplyWithDouble(ctx, 0);
    } else {
        RedisModule_ReplyWithDouble(ctx, ((double)DURATION2MS(srctx->totalBatchLag) / (srctx->numSuccess + srctx->numFailures + srctx->numAborted)));
    }
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(srctx->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, srctx->lastError, strlen(srctx->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 12);
    RedisModule_ReplyWithStringBuffer(ctx, "batchSize", strlen("batchSize"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->batchSize);
    RedisModule_ReplyWithStringBuffer(ctx, "durationMS", strlen("durationMS"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->durationMS);
    RedisModule_ReplyWithStringBuffer(ctx, "stream", strlen("stream"));
    RedisModule_ReplyWithStringBuffer(ctx, srctx->args->streamPrefix, strlen(srctx->args->streamPrefix));
    RedisModule_ReplyWithStringBuffer(ctx, "trimStream", strlen("trimStream"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->trimStream);
    RedisModule_ReplyWithStringBuffer(ctx, "onFailedPolicy", strlen("onFailedPolicy"));
    switch(srctx->args->onFailedPolicy){
    case OnFailedPolicyContinue:
        RedisModule_ReplyWithStringBuffer(ctx, "continue", strlen("continue"));
        break;
    case OnFailedPolicyAbort:
        RedisModule_ReplyWithStringBuffer(ctx, "abort", strlen("abort"));
        break;
    case OnFailedPolicyRetry:
        RedisModule_ReplyWithStringBuffer(ctx, "retry", strlen("retry"));
        break;
    default:
        RedisModule_Assert(false);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "retryInterval", strlen("retryInterval"));
    RedisModule_ReplyWithLongLong(ctx, srctx->args->retryInterval);

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
    case StreamRegistrationStatus_PAUSED:
        RedisModule_ReplyWithStringBuffer(ctx, "PAUSED", strlen("PAUSED"));
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

        char* err = NULL;
        int res = FlatExecutionPlan_Serialize(&bw, srctx->fep, &err);
        if(res != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "Failed serializing fep, err='%s'", err);
            RedisModule_Assert(false); // fep already registered, must be serializable.
        }

        StreamReader_SerializeArgs(srctx->args, &bw);

        RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

        RedisModule_SaveUnsigned(rdb, srctx->mode);

        Gears_BufferClear(buff);
    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);

    Gears_BufferFree(buff);
}

static int StreamReader_RdbLoad(RedisModuleIO *rdb, int encver){
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
            RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution, error='%s'", err);
            RedisModule_Free(data);
            return REDISMODULE_ERR;
        }

        void* args = StreamReader_DeserializeArgs(&reader, encver);
        RedisModule_Free(data);
        if(!args){
            RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution args");
            FlatExecutionPlan_Free(fep);
            return REDISMODULE_ERR;
        }


        int mode = RedisModule_LoadUnsigned(rdb);
        int ret = StreamReader_RegisrterTrigger(fep, mode, args, &err);
        if(ret != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "Could not register flat execution, error='%s'", err);
            StreamReaderTriggerArgs_Free(args);
            FlatExecutionPlan_Free(fep);
            return REDISMODULE_ERR;
        }
        FlatExecutionPlan_AddToRegisterDict(fep);
    }
    return REDISMODULE_OK;
}

static void StreamReader_ClearStats(){
    if(!streamsRegistration){
        return;
    }
    Gears_listIter *iter = Gears_listGetIterator(streamsRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        StreamReaderTriggerCtx* srtctx = Gears_listNodeValue(node);
        resetStats(srtctx);
        srtctx->lastBatchLag = 0;
        srtctx->totalBatchLag = 0;
    }
    Gears_listReleaseIterator(iter);
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
        .pauseTrigger = StreamReader_PauseTrigger,
        .unpauseTrigger = StreamReader_UnpauseTrigger,
        .serializeTriggerArgs = StreamReader_SerializeArgs,
        .deserializeTriggerArgs = StreamReader_DeserializeArgs,
        .freeTriggerArgs = StreamReader_FreeArgs,
        .dumpRegistratioData = StreamReader_DumpRegistrationData,
        .dumpRegistratioInfo = StreamReader_DumpRegistrationInfo,
        .rdbSave = StreamReader_RdbSave,
        .rdbLoad = StreamReader_RdbLoad,
        .clear = StreamReader_Clear,
        .clearStats = StreamReader_ClearStats,
};
