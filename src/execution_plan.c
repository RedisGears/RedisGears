#include "execution_plan.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include "record.h"
#include "cluster.h"
#include "config.h"
#include <assert.h>
#include <stdbool.h>
#include "utils/adlist.h"
#include "utils/buffer.h"
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include <event2/event.h>
#include "lock_handler.h"
#include "utils/thpool.h"
#include "version.h"

#define INIT_TIMER  struct timespec _ts = {0}, _te = {0}; \
                    bool timerInitialized = false;
#define GETTIME(t)  clock_gettime(CLOCK_REALTIME, t);
#define START_TIMER if(GearsConfig_GetProfileExecutions()){ \
                        timerInitialized = true; \
                        GETTIME(&_ts); \
                    }
#define STOP_TIMER  if(GearsConfig_GetProfileExecutions()) GETTIME(&_te);
#define DURATION    ((long long)1000000000 * (_te.tv_sec - _ts.tv_sec) \
                    + (_te.tv_nsec - _ts.tv_nsec))
#define ADD_DURATION(d) if(GearsConfig_GetProfileExecutions()){ \
                            if(timerInitialized){ \
                                STOP_TIMER; \
                                d += DURATION; \
                            } \
                        }
#define DURATION2MS(d)  (long long)(d/(long long)1000000)

char* stepsNames[] = {
#define X(a, b) b,
    STEP_TYPES
#undef X
};

char* statusesNames[] = {
#define X(a, b, c) b,
    EXECUTION_PLAN_STATUSES
#undef X
};

typedef ActionResult (*EPStatus_ActionCallback)(ExecutionPlan*);

EPStatus_ActionCallback statusesActions[] = {
#define X(a, b, c) c,
    EXECUTION_PLAN_STATUSES
#undef X
};

typedef struct LimitExecutionStepArg{
    size_t offset;
    size_t len;
}LimitExecutionStepArg;

static void FreeLimitArg(void* arg){
    RG_FREE(arg);
}

static void* DupLimitArg(void* arg){
    LimitExecutionStepArg* limitArg = arg;
    LimitExecutionStepArg* ret = RG_ALLOC(sizeof(*ret));
    ret->len = limitArg->len;
    ret->offset = limitArg->offset;
    return ret;
}

static int LimitArgSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    LimitExecutionStepArg* limitArg = arg;
    RedisGears_BWWriteLong(bw, limitArg->offset);
    RedisGears_BWWriteLong(bw, limitArg->len);
    return REDISMODULE_OK;
}

#define limitArgVersion 1

static void* LimitArgDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > limitArgVersion){
        return NULL;
    }
    LimitExecutionStepArg* limitArg = RG_ALLOC(sizeof(*limitArg));
    limitArg->offset = RedisGears_BRReadLong(br);
    limitArg->len = RedisGears_BRReadLong(br);
    return limitArg;
}

static ArgType LimitArgType = {
        .free = FreeLimitArg,
        .dup = DupLimitArg,
        .serialize = LimitArgSerialize,
        .deserialize = LimitArgDeserialize,
};

typedef struct ExecutionPlansData{
    // protected by mutex, mutex must be acquire when access those vars
    Gears_dict* epDict;
    Gears_list* epList;

    // protected by the GIL, GIL must be acquire when access this dict
    Gears_dict* registeredFepDict;

    ExecutionThreadPool* defaultPool;
}ExecutionPlansData;

ExecutionPlansData epData;

static long long lastEPId = 0;
static long long lastFEPId = 0;

static Record* ExecutionPlan_NextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx);
static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep, ExecutionMode mode, void* arg);
static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader);
static void ExecutionPlan_RegisterForRun(ExecutionPlan* ep);
static ReaderStep ExecutionPlan_NewReader(FlatExecutionReader* reader, void* arg);
static void ExecutionPlan_NotifyReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);
static void ExecutionPlan_NotifyRun(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);
static void ExecutionPlan_SetID(ExecutionPlan* ep, char* id);
static void FlatExecutionPlan_SetID(FlatExecutionPlan* fep, char* id);
static FlatExecutionPlan* FlatExecutionPlan_ShallowCopy(FlatExecutionPlan* fep);
static void ExecutionPlan_MessageThreadMain(void *arg);
static void ExecutionPlan_FreeWorkerInternal(WorkerData* wd);

typedef enum MsgType{
    RUN_MSG, ADD_RECORD_MSG, SHARD_COMPLETED_MSG, EXECUTION_DONE, EXECUTION_TERMINATE, WORKER_FREE
}MsgType;

typedef struct RunWorkerMsg{
}RunWorkerMsg;

typedef struct ExecutionDoneMsg{
}ExecutionDoneMsg;

typedef struct ExecutionFreeMsg{
}ExecutionFreeMsg;

typedef struct WorkerFreeMsg{
}WorkerFreeMsg;

typedef struct ShardCompletedWorkerMsg{
	size_t stepId;
	enum StepType stepType;
}ShardCompletedWorkerMsg;

typedef struct AddRecordWorkerMsg{
	Record* record;
	size_t stepId;
	enum StepType stepType;
}AddRecordWorkerMsg;

typedef struct WorkerMsg{
    char id[ID_LEN];
    union{
    	RunWorkerMsg runWM;
    	AddRecordWorkerMsg addRecordWM;
    	ShardCompletedWorkerMsg shardCompletedWM;
    	ExecutionDoneMsg executionDone;
    	ExecutionFreeMsg executionFree;
    	WorkerFreeMsg workerFreeMsg;
    };
    MsgType type;
}WorkerMsg;

typedef struct ExecutionThreadPool{
    Gears_threadpool pool;
    char* name;
}ExecutionThreadPool;

static Gears_dict* poolDictionary;

ExecutionThreadPool* ExectuionPlan_GetThreadPool(const char* name){
    return Gears_dictFetchValue(poolDictionary, name);
}

ExecutionThreadPool* ExecutionPlan_CreateThreadPool(const char* name, size_t numOfThreads){
    if(ExectuionPlan_GetThreadPool(name)){
        RedisModule_Log(NULL, "warning", "Pool name already exists, %s", name);
        return NULL;
    }
    ExecutionThreadPool* ret = RG_ALLOC(sizeof(*ret));
    ret->pool = Gears_thpool_init(numOfThreads);
    ret->name = RG_STRDUP(name);
    Gears_dictAdd(poolDictionary, ret->name, ret);
    return ret;
}

static void ExectuionPlan_WorkerMsgSend(WorkerData* wd, WorkerMsg* msg){
    if(wd->status == WorkerStatus_ShuttingDown){
        RedisModule_Log(NULL, "warning", "Got a message to a shuttingdown worker, fatal!!!");
        RedisModule_Assert(false);
    }
    if(msg->type == WORKER_FREE){
        wd->status = WorkerStatus_ShuttingDown;
    }
    pthread_mutex_lock(&wd->lock);
    size_t lenBeforeMsg = Gears_listLength(wd->notifications);
	Gears_listAddNodeTail(wd->notifications, msg);
	if(lenBeforeMsg == 0){
	    Gears_thpool_add_work(wd->pool->pool, ExecutionPlan_MessageThreadMain, wd);
	}
	pthread_mutex_unlock(&wd->lock);
}

static void ExectuionPlan_WorkerMsgFree(WorkerMsg* msg){
    if(msg->type == ADD_RECORD_MSG && msg->addRecordWM.record){
        RedisGears_FreeRecord(msg->addRecordWM.record);
    }
	RG_FREE(msg);
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateRun(ExecutionPlan* ep){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = RUN_MSG;
	memcpy(ret->id, ep->id, ID_LEN);
	return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgFreeWorker(){
    WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
    ret->type = WORKER_FREE;
    return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateTerminate(ExecutionPlan* ep){
    WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
    ret->type = EXECUTION_TERMINATE;
    memcpy(ret->id, ep->id, ID_LEN);
    return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateDone(ExecutionPlan* ep){
    WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
    ret->type = EXECUTION_DONE;
    memcpy(ret->id, ep->id, ID_LEN);
    return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateAddRecord(ExecutionPlan* ep, size_t stepId, Record* r, enum StepType stepType){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = ADD_RECORD_MSG;
	memcpy(ret->id, ep->id, ID_LEN);
	ret->addRecordWM.record = r;
	ret->addRecordWM.stepId = stepId;
	ret->addRecordWM.stepType = stepType;
	return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateShardCompleted(ExecutionPlan* ep, size_t stepId, enum StepType stepType){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = SHARD_COMPLETED_MSG;
	memcpy(ret->id, ep->id, ID_LEN);
	ret->shardCompletedWM.stepId = stepId;
	ret->shardCompletedWM.stepType = stepType;
	return ret;
}

static ArgType* FlatExecutionPlan_GetArgTypeByStepType(enum StepType type, const char* name){
    switch(type){
    case MAP:
    case FLAT_MAP:
        return MapsMgmt_GetArgType(name);
    case FILTER:
        return FiltersMgmt_GetArgType(name);
    case EXTRACTKEY:
        return ExtractorsMgmt_GetArgType(name);
    case REDUCE:
        return ReducersMgmt_GetArgType(name);
    case FOREACH:
        return ForEachsMgmt_GetArgType(name);
    case ACCUMULATE:
        return AccumulatesMgmt_GetArgType(name);
    case ACCUMULATE_BY_KEY:
    	return AccumulateByKeysMgmt_GetArgType(name);
    case READER:
        // todo: fix reader args handling for now we free the reader on execution plan itself
        return NULL;
    case LIMIT:
        return &LimitArgType;
    default:
        return NULL;
    }
}

ExecutionPlan* ExecutionPlan_FindById(const char* id){
    ExecutionPlan* ep = Gears_dictFetchValue(epData.epDict, id);
    return ep;
}

ExecutionPlan* ExecutionPlan_FindByStrId(const char* id){
    char realId[ID_LEN] = {0};
    if(strlen(id) < REDISMODULE_NODE_ID_LEN + 2){
        return NULL;
    }
    if(id[REDISMODULE_NODE_ID_LEN] != '-'){
        return NULL;
    }
    memcpy(realId, id, REDISMODULE_NODE_ID_LEN);
    int match = sscanf(id + REDISMODULE_NODE_ID_LEN + 1, "%lld", (long long*)(&realId[REDISMODULE_NODE_ID_LEN]));
    if(match != 1){
        return NULL;
    }
    return ExecutionPlan_FindById(realId);
}

static FlatExecutionPlan* FlatExecutionPlan_FindId(const char* id){
    FlatExecutionPlan* fep = Gears_dictFetchValue(epData.registeredFepDict, id);
    return fep;
}

static FlatExecutionPlan* FlatExecutionPlan_FindByStrId(const char* id){
    char realId[ID_LEN] = {0};
    if(strlen(id) < REDISMODULE_NODE_ID_LEN + 2){
        return NULL;
    }
    if(id[REDISMODULE_NODE_ID_LEN] != '-'){
        return NULL;
    }
    memcpy(realId, id, REDISMODULE_NODE_ID_LEN);
    int match = sscanf(id + REDISMODULE_NODE_ID_LEN + 1, "%lld", (long long*)(&realId[REDISMODULE_NODE_ID_LEN]));
    if(match != 1){
        return NULL;
    }

    return FlatExecutionPlan_FindId(realId);
}

static void FlatExecutionPlan_SerializeReader(FlatExecutionReader* rfep, Gears_BufferWriter* bw){
    RedisGears_BWWriteString(bw, rfep->reader);
}

static int FlatExecutionPlan_SerializeArg(ArgType* type, void* arg, Gears_BufferWriter* bw, char** err){
    RedisModule_Assert(type && type->serialize);
    RedisGears_BWWriteLong(bw, type->version);
    return type->serialize(arg, bw, err);
}

static int FlatExecutionPlan_SerializeStepArg(ArgType* type, void* stepArg, Gears_BufferWriter* bw, char** err){
    if(stepArg){
        RedisGears_BWWriteLong(bw, 1); // has step args
        return FlatExecutionPlan_SerializeArg(type, stepArg, bw, err);
    }else{
        RedisGears_BWWriteLong(bw, 0); // do not have step args
    }
    return REDISMODULE_OK;
}

static int FlatExecutionPlan_SerializeStep(FlatExecutionStep* step, Gears_BufferWriter* bw, char** err){
    RedisGears_BWWriteLong(bw, step->type);
    RedisGears_BWWriteString(bw, step->bStep.stepName);
    ArgType* type = step->bStep.arg.type;
    return FlatExecutionPlan_SerializeStepArg(type, step->bStep.arg.stepArg, bw, err);
}

static const char* FlatExecutionPlan_SerializeInternal(FlatExecutionPlan* fep, size_t *len, char** err){
    if(fep->serializedFep){
        // notice that this is not only an optimization,
        // when calling register on fep we call this function to serialize
        // The execution might changed during run (for example, args might change).
        // It is important to save the initial version of the execution.
        if(len){
            *len = fep->serializedFep->size;
        }
        return fep->serializedFep->buff;
    }
    fep->serializedFep = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, fep->serializedFep);

    FlatExecutionPlan_SerializeReader(fep->reader, &bw);
    RedisGears_BWWriteLong(&bw, array_len(fep->steps));
    for(int i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* step = fep->steps + i;
        if(FlatExecutionPlan_SerializeStep(step, &bw, err) != REDISMODULE_OK){
            Gears_BufferFree(fep->serializedFep);
            fep->serializedFep = NULL;
            return NULL;
        }
    }

    // serialize FEP id
    RedisGears_BWWriteBuffer(&bw, fep->id, ID_LEN);
    if(fep->desc){
        RedisGears_BWWriteLong(&bw, 1); // has desc
        RedisGears_BWWriteString(&bw, fep->desc);
    }else{
        RedisGears_BWWriteLong(&bw, 0); // no desc
    }

    if(fep->onExecutionStartStep.stepName){
        RedisGears_BWWriteLong(&bw, 1); // has onExecutionStartStep
        RedisGears_BWWriteString(&bw, fep->onExecutionStartStep.stepName);
        ArgType* type = fep->onExecutionStartStep.arg.type;
        int res = FlatExecutionPlan_SerializeStepArg(type, fep->onExecutionStartStep.arg.stepArg, &bw, err);
        if(res != REDISMODULE_OK){
            Gears_BufferFree(fep->serializedFep);
            fep->serializedFep = NULL;
            return NULL;
        }
    }else{
        RedisGears_BWWriteLong(&bw, 0); // no onExecutionStartStep
    }

    if(fep->onUnpausedStep.stepName){
        RedisGears_BWWriteLong(&bw, 1); // has onExecutionStartStep
        RedisGears_BWWriteString(&bw, fep->onUnpausedStep.stepName);
        ArgType* type = fep->onUnpausedStep.arg.type;
        int res = FlatExecutionPlan_SerializeStepArg(type, fep->onUnpausedStep.arg.stepArg, &bw, err);
        if(res != REDISMODULE_OK){
            Gears_BufferFree(fep->serializedFep);
            fep->serializedFep = NULL;
            return NULL;
        }
    }else{
        RedisGears_BWWriteLong(&bw, 0); // no onExecutionStartStep
    }

    if(fep->onRegisteredStep.stepName){
        RedisGears_BWWriteLong(&bw, 1); // has onExecutionStartStep
        RedisGears_BWWriteString(&bw, fep->onRegisteredStep.stepName);
        ArgType* type = fep->onRegisteredStep.arg.type;
        int res = FlatExecutionPlan_SerializeStepArg(type, fep->onRegisteredStep.arg.stepArg, &bw, err);
        if(res != REDISMODULE_OK){
            Gears_BufferFree(fep->serializedFep);
            fep->serializedFep = NULL;
            return NULL;
        }
    }else{
        RedisGears_BWWriteLong(&bw, 0); // no onExecutionStartStep
    }

    if(len){
        *len = fep->serializedFep->size;
    }

    return fep->serializedFep->buff;
}

int FlatExecutionPlan_Serialize(Gears_BufferWriter* bw, FlatExecutionPlan* fep, char** err){
    // we serialize the PD of a fep each time cause it might be very big (contains file
    // deps and we do not want to hold it in the memory all the time)
    // Als private data must serialize and deserialized first cause other
    // stages of the deserialization process might need it.
    if(fep->PD){
        RedisGears_BWWriteLong(bw, 1); // PD exists
        RedisGears_BWWriteString(bw, fep->PDType);
        ArgType* type = FepPrivateDatasMgmt_GetArgType(fep->PDType);
        RedisModule_Assert(type);
        if(FlatExecutionPlan_SerializeArg(type, fep->PD, bw, err) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // PD do exists
    }

    size_t serializedFepLen;
    const char* serializedFep = FlatExecutionPlan_SerializeInternal(fep, &serializedFepLen, err);

    if(!serializedFep){
        return REDISMODULE_ERR;
    }

    RedisGears_BWWriteBuffer(bw, serializedFep, serializedFepLen);

    return REDISMODULE_OK;
}

static FlatExecutionReader* FlatExecutionPlan_DeserializeReader(Gears_BufferReader* br){
    char* readerName = RedisGears_BRReadString(br);
    FlatExecutionReader* reader = FlatExecutionPlan_NewReader(readerName);
    return reader;
}

static void* FlatExecutionPlan_DeserializeArg(FlatExecutionPlan* fep, ArgType* type, Gears_BufferReader* br, int encver, char** err){
    void* arg = NULL;
    if(RedisGears_BRReadLong(br)){
        int version = 0;
        if(encver >= VERSION_WITH_ARG_TYPE){
            version = RedisGears_BRReadLong(br);
        }
        arg = type->deserialize(fep, br, version, err);
        if(!arg){
            if(!(*err)){
                *err = RG_STRDUP("Failed deserialize argument");
            }
        }
    }
    return arg;
}

static int FlatExecutionPlan_DeserializeStep(FlatExecutionPlan* fep, FlatExecutionStep* step, Gears_BufferReader* br, char** err, int encver){
    step->type = RedisGears_BRReadLong(br);
    step->bStep.stepName = RG_STRDUP(RedisGears_BRReadString(br));
    step->bStep.arg.type = FlatExecutionPlan_GetArgTypeByStepType(step->type, step->bStep.stepName);
    step->bStep.arg.stepArg = FlatExecutionPlan_DeserializeArg(fep, step->bStep.arg.type, br, encver, err);
    if(*err){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static int FlatExecutionPlan_DeserializeInternal(FlatExecutionPlan* ret, const char* data, size_t dataLen, char** err, int encver){
    Gears_Buffer buff = {
            .buff = (char*)data,
            .size = dataLen,
            .cap = dataLen,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);

    ret->reader = FlatExecutionPlan_DeserializeReader(&br);
    long numberOfSteps = RedisGears_BRReadLong(&br);
    for(int i = 0 ; i < numberOfSteps ; ++i){
        FlatExecutionStep s = {0};
        if(FlatExecutionPlan_DeserializeStep(ret, &s, &br, err, encver) != REDISMODULE_OK){
            goto error;
        }
        ret->steps = array_append(ret->steps, s);
    }

    // read FEP id
    size_t len;
    char* idBuff = RedisGears_BRReadBuffer(&br, &len);
    RedisModule_Assert(len == ID_LEN);
    FlatExecutionPlan_SetID(ret, idBuff);

    long long hasDesc = RedisGears_BRReadLong(&br);
    if(hasDesc){
        ret->desc = RG_STRDUP(RedisGears_BRReadString(&br));
    }

    long long hasOnStartCallback = RedisGears_BRReadLong(&br);
    if(hasOnStartCallback){
        const char* onStartCallbackName = RedisGears_BRReadString(&br);
        ArgType* type = ExecutionOnStartsMgmt_GetArgType(onStartCallbackName);
        void* arg = FlatExecutionPlan_DeserializeArg(ret, type, &br, encver, err);
        if(*err){
            goto error;
        }
        ret->onExecutionStartStep = (FlatBasicStep){
                .stepName = RG_STRDUP(onStartCallbackName),
                .arg = {
                        .stepArg = arg,
                        .type = type,
                },
        };
    }

    long long hasOnUnpausedCallback = RedisGears_BRReadLong(&br);
    if(hasOnUnpausedCallback){
        const char* onUnpausedCallbackName = RedisGears_BRReadString(&br);
        ArgType* type = ExecutionOnUnpausedsMgmt_GetArgType(onUnpausedCallbackName);
        void* arg = FlatExecutionPlan_DeserializeArg(ret, type, &br, encver, err);
        if(*err){
            goto error;
        }
        ret->onUnpausedStep = (FlatBasicStep){
                .stepName = RG_STRDUP(onUnpausedCallbackName),
                .arg = {
                        .stepArg = arg,
                        .type = type,
                },
        };
    }

    long long hasOnRegisteredCallback = RedisGears_BRReadLong(&br);
    if(hasOnRegisteredCallback){
        const char* onRegisteredCallbackName = RedisGears_BRReadString(&br);
        ArgType* type = FlatExecutionOnRegisteredsMgmt_GetArgType(onRegisteredCallbackName);
        void* arg = FlatExecutionPlan_DeserializeArg(ret, type, &br, encver, err);
        if(*err){
            goto error;
        }
        ret->onRegisteredStep = (FlatBasicStep){
                .stepName = RG_STRDUP(onRegisteredCallbackName),
                .arg = {
                        .stepArg = arg,
                        .type = type,
                },
        };
    }

    // we need to deserialize the fep now so we will have the deserialize clean version of it.
    // it might changed after to something we can not serialize
    const char* d = FlatExecutionPlan_SerializeInternal(ret, NULL, NULL);
    RedisModule_Assert(d);
    return REDISMODULE_OK;

error:
    FlatExecutionPlan_Free(ret);
    return REDISMODULE_ERR;
}

FlatExecutionPlan* FlatExecutionPlan_Deserialize(Gears_BufferReader* br, char** err, int encver){
    FlatExecutionPlan* ret = FlatExecutionPlan_New();

    bool PDExists = RedisGears_BRReadLong(br);
    if(PDExists){
        ret->PDType = RG_STRDUP(RedisGears_BRReadString(br));
        ArgType* type = FepPrivateDatasMgmt_GetArgType(ret->PDType);
        RedisModule_Assert(type);
        int version = 0;
        if(encver >= VERSION_WITH_ARG_TYPE){
            version = RedisGears_BRReadLong(br);
        }
        ret->PD = type->deserialize(ret, br, version, err);
        if(!ret->PD){
            goto error;
        }
    }

    size_t len;
    const char* data = RedisGears_BRReadBuffer(br, &len);

    if(FlatExecutionPlan_DeserializeInternal(ret, data, len, err, encver) != REDISMODULE_OK){
        goto error;
    }

    return ret;

error:
    FlatExecutionPlan_Free(ret);
    return NULL;
}

static void ExecutionPlan_SendRunRequest(ExecutionPlan* ep){
	Cluster_SendMsgM(NULL, ExecutionPlan_NotifyRun, ep->id, ID_LEN);
}

static void ExecutionPlan_SendRecievedNotification(ExecutionPlan* ep){
	Cluster_SendMsgM(ep->id, ExecutionPlan_NotifyReceived, ep->id, ID_LEN);
}

static void ExecutionPlan_Distribute(ExecutionPlan* ep){
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    size_t len;
    int res = FlatExecutionPlan_Serialize(&bw, ep->fep, NULL);
    RedisModule_Assert(res == REDISMODULE_OK); // if we reached here execution must be serialized
    RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution id
    ExecutionStep* readerStep = ep->steps[array_len(ep->steps) - 1];
    readerStep->reader.r->serialize(readerStep->reader.r->ctx, &bw);

    // send the ThreadPool name
    if(ep->assignWorker->pool == epData.defaultPool){
        // optimization of not doing a lookup on dictionary when using default pool
        // (which will happened almost always)
        RedisGears_BWWriteLong(&bw, 1); // running on default pool
    }else{
        RedisGears_BWWriteLong(&bw, 0); // running on constume pool
        RedisGears_BWWriteString(&bw, ep->assignWorker->pool->name);
    }

    Cluster_SendMsgM(NULL, ExecutionPlan_OnReceived, buff->buff, buff->size);
    Gears_BufferFree(buff);
}

static Record* ExecutionPlan_FilterNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = NULL;
    INIT_TIMER;
    while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx))){
        if(record == &StopRecord){
            return record;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            return record;
        }
	    START_TIMER;
        ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
        bool filterRes = step->filter.filter(&ectx, record, step->filter.stepArg.stepArg);
        if(ectx.err){
            RedisGears_FreeRecord(record);
            record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
            goto end;
        }
        if(filterRes){
            goto end;
        }else{
            RedisGears_FreeRecord(record);
        }
    }
    record = NULL;
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_MapNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = ExecutionPlan_NextRecord(ep, step->prev, rctx);

    INIT_TIMER;
	START_TIMER;
	if(record == NULL){
        goto end;
    }
    if(record == &StopRecord){
    	goto end;
    }
    if(RedisGears_RecordGetType(record) == errorRecordType){
        goto end;
    }
    if(record != NULL){
        ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
        record = step->map.map(&ectx, record, step->map.stepArg.stepArg);
        if(ectx.err){
            if(record){
                RedisGears_FreeRecord(record);
            }
            record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
        }
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_FlatMapNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
	Record* r = NULL;

    INIT_TIMER;
	if(step->flatMap.pendings){
        START_TIMER;
        r = RedisGears_ListRecordPop(step->flatMap.pendings);
        if(RedisGears_ListRecordLen(step->flatMap.pendings) == 0){
            RedisGears_FreeRecord(step->flatMap.pendings);
            step->flatMap.pendings = NULL;
        }
        goto end;
    }
    do{
        if(r){
            // if we reach here r is an empty list record
            RedisGears_FreeRecord(r);
        }
        r = ExecutionPlan_MapNextRecord(ep, step, rctx);
        START_TIMER;
        if(r == NULL){
            goto end;
        }
        if(r == &StopRecord){
            goto end;
        }
        if(RedisGears_RecordGetType(r) == errorRecordType){
            goto end;
        }
        if(RedisGears_RecordGetType(r) != listRecordType){
            goto end;
        }
    	ADD_DURATION(step->executionDuration);
    }while(RedisGears_ListRecordLen(r) == 0);
    START_TIMER;
    if(RedisGears_ListRecordLen(r) == 1){
        Record* ret;
        ret = RedisGears_ListRecordPop(r);
        RedisGears_FreeRecord(r);
        r = ret;
        goto end;
    }
    step->flatMap.pendings = r;
    r = RedisGears_ListRecordPop(step->flatMap.pendings);
end:
	ADD_DURATION(step->executionDuration);
    return r;
}

static Record* ExecutionPlan_ExtractKeyNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    size_t buffLen;
    Record* r = NULL;
    Record* record = ExecutionPlan_NextRecord(ep, step->prev, rctx);

    INIT_TIMER;
    START_TIMER;
    if(record == NULL){
        goto end;
    }
    if(record == &StopRecord){
    	r = record;
    	goto end;
    }
    if(RedisGears_RecordGetType(record) == errorRecordType){
        r = record;
        goto end;
    }
    ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
    char* buff = step->extractKey.extractor(&ectx, record, step->extractKey.extractorArg.stepArg, &buffLen);
    if(ectx.err){
        RedisGears_FreeRecord(record);
        r = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
        goto end;
    }
    r = RedisGears_KeyRecordCreate();
    RedisGears_KeyRecordSetKey(r, buff, buffLen);
    RedisGears_KeyRecordSetVal(r, record);
end:
	ADD_DURATION(step->executionDuration);
    return r;
}

static Record* ExecutionPlan_GroupNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
#define GROUP_RECORD_INIT_LEN 10
    Record* record = NULL;

    INIT_TIMER;
    if(step->group.isGrouped){
        if(array_len(step->group.groupedRecords) == 0){
            return NULL;
        }
        return array_pop(step->group.groupedRecords);
    }
    while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx))){
        START_TIMER;
        if(record == &StopRecord){
            goto end;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            goto end;
        }
        RedisModule_Assert(RedisGears_RecordGetType(record) == keyRecordType);
        size_t keyLen;
        char* key = RedisGears_KeyRecordGetKey(record, &keyLen);
        Gears_dictEntry* entry = Gears_dictFind(step->group.d, key);
        Record* r = NULL;
        if(!entry){
            r = RedisGears_KeyRecordCreate();
            RedisGears_KeyRecordSetKey(r, key, keyLen);
            RedisGears_KeyRecordSetKey(record, NULL, 0);
            Record* val  = RedisGears_ListRecordCreate(GROUP_RECORD_INIT_LEN);
            RedisGears_KeyRecordSetVal(r, val);
            Gears_dictAdd(step->group.d, key, r);
            step->group.groupedRecords = array_append(step->group.groupedRecords, r);
        }else{
            r = Gears_dictGetVal(entry);
        }
        Record* listRecord = RedisGears_KeyRecordGetVal(r);
        RedisGears_ListRecordAdd(listRecord, RedisGears_KeyRecordGetVal(record));
        RedisGears_KeyRecordSetVal(record, NULL);
        RedisGears_FreeRecord(record);
        ADD_DURATION(step->executionDuration);
    }
    START_TIMER;
    step->group.isGrouped = true;
    if(array_len(step->group.groupedRecords) == 0){
        record = NULL;
    }else{
        record = array_pop(step->group.groupedRecords);
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_ReduceNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = ExecutionPlan_NextRecord(ep, step->prev, rctx);

    INIT_TIMER;
    START_TIMER;
    if(record == NULL){
        goto end;
    }
    if(record == &StopRecord){
        goto end;
    }
    if(RedisGears_RecordGetType(record) == errorRecordType){
        goto end;
    }
    RedisModule_Assert(RedisGears_RecordGetType(record) == keyRecordType);
    size_t keyLen;
    char* key = RedisGears_KeyRecordGetKey(record, &keyLen);
    ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
    Record* r = step->reduce.reducer(&ectx, key, keyLen, RedisGears_KeyRecordGetVal(record), step->reduce.reducerArg.stepArg);
    RedisGears_KeyRecordSetVal(record, r);
    if(ectx.err){
        RedisGears_FreeRecord(record);
        record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_RepartitionNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = NULL;
    Gears_Buffer* buff;
    Gears_BufferWriter bw;

    if(!Cluster_IsClusterMode() || EPIsFlagOn(ep, EFIsLocal)){
        return ExecutionPlan_NextRecord(ep, step->prev, rctx);
    }

    INIT_TIMER;
    START_TIMER;
    if(step->repartion.stoped){
        if(array_len(step->repartion.pendings) > 0){
            record = array_pop(step->repartion.pendings);
            goto end;
        }
        if((Cluster_GetSize() - 1) == step->repartion.totalShardsCompleted){
			goto end; // we are done!!
		}
        record = &StopRecord;
        goto end;
    }
    buff = Gears_BufferCreate();
    STOP_TIMER;
	step->executionDuration += DURATION;

    while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx)) != NULL){
        START_TIMER;
        if(record == &StopRecord){
            Gears_BufferFree(buff);
            goto end;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            // this is an error record which should stay with us so lets return it
            Gears_BufferFree(buff);
            goto end;
        }
        size_t len;
        char* key = RedisGears_KeyRecordGetKey(record, &len);
        const char* shardIdToSendRecord = Cluster_GetNodeIdByKey(key);
        if(memcmp(shardIdToSendRecord, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0){
            // this record should stay with us, lets return it.
        	Gears_BufferFree(buff);
            goto end;
        }
        else{
            // we need to send the record to another shard
            char* err = NULL;
            Gears_BufferWriterInit(&bw, buff);
            RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
            RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
            int serializationRes = RG_SerializeRecord(&bw, record, &err);
            RedisGears_FreeRecord(record);
            if(serializationRes != REDISMODULE_OK){
                // we need to clear and rewrite cause buff contains garbage
                Gears_BufferClear(buff);
                RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
                RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
                record = RG_ErrorRecordCreate(err, strlen(err));
                err = NULL;
                serializationRes = RG_SerializeRecord(&bw, record, &err);
                RedisModule_Assert(serializationRes == REDISMODULE_OK);
                RedisGears_FreeRecord(record);
            }

            Cluster_SendMsgM(shardIdToSendRecord, ExecutionPlan_OnRepartitionRecordReceived, buff->buff, buff->size);

            Gears_BufferClear(buff);
        }
    	ADD_DURATION(step->executionDuration);
    }

    START_TIMER;
    Gears_BufferWriterInit(&bw, buff);
    RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
    RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id

    Cluster_SendMsgM(NULL, ExecutionPlan_DoneRepartition, buff->buff, buff->size);

    Gears_BufferFree(buff);
    step->repartion.stoped = true;
    if(array_len(step->repartion.pendings) > 0){
        record = array_pop(step->repartion.pendings);
        goto end;
	}
	if((Cluster_GetSize() - 1) == step->repartion.totalShardsCompleted){
		record = NULL; // we are done!!
        goto end;
	}
    record = &StopRecord;
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_CollectNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
	Record* record = NULL;
	Gears_Buffer* buff;;
	Gears_BufferWriter bw;

	if(!Cluster_IsClusterMode() || EPIsFlagOn(ep, EFIsLocal)){
		return ExecutionPlan_NextRecord(ep, step->prev, rctx);
	}

    INIT_TIMER;
    START_TIMER;
	if(step->collect.stoped){
		if(array_len(step->collect.pendings) > 0){
            record = array_pop(step->collect.pendings);
            goto end;
		}
		if((Cluster_GetSize() - 1) == step->collect.totalShardsCompleted){
			record = NULL; // we are done!!
            goto end;
		}
		record = &StopRecord;
        goto end;
	}

	buff = Gears_BufferCreate();
	ADD_DURATION(step->executionDuration);

	while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx)) != NULL){
        START_TIMER;
		if(record == &StopRecord){
			Gears_BufferFree(buff);
			goto end;
		}
		if(Cluster_IsMyId(ep->id)){
			Gears_BufferFree(buff);
			goto end; // record should stay here, just return it.
		}else{
		    char* err = NULL;
			Gears_BufferWriterInit(&bw, buff);
			RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
			RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
			int serializationRes = RG_SerializeRecord(&bw, record, &err);
			RedisGears_FreeRecord(record);
			if(serializationRes != REDISMODULE_OK){
			    // we need to clear and rewrite cause buff contains garbage
			    Gears_BufferClear(buff);
			    RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
			    RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
			    record = RG_ErrorRecordCreate(err, strlen(err));
			    err = NULL;
			    serializationRes = RG_SerializeRecord(&bw, record, &err);
			    RedisModule_Assert(serializationRes == REDISMODULE_OK);
			    RedisGears_FreeRecord(record);
			}


			Cluster_SendMsgM(ep->id, ExecutionPlan_CollectOnRecordReceived, buff->buff, buff->size);

			Gears_BufferClear(buff);
		}
    	ADD_DURATION(step->executionDuration);
	}

    START_TIMER;
	step->collect.stoped = true;

	if(Cluster_IsMyId(ep->id)){
		Gears_BufferFree(buff);
		if(array_len(step->collect.pendings) > 0){
			record = array_pop(step->collect.pendings);
            goto end;
		}
		if((Cluster_GetSize() - 1) == step->collect.totalShardsCompleted){
			record = NULL; // we are done!!
            goto end;
		}
		record = &StopRecord; // now we should wait for record to arrive from the other shards
	}else{
		Gears_BufferWriterInit(&bw, buff);
		RedisGears_BWWriteBuffer(&bw, ep->id, ID_LEN); // serialize execution plan id
		RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id

		Cluster_SendMsgM(ep->id, ExecutionPlan_CollectDoneSendingRecords, buff->buff, buff->size);
		Gears_BufferFree(buff);
		record = NULL;
	}
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_ForEachNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = ExecutionPlan_NextRecord(ep, step->prev, rctx);
    INIT_TIMER;
    START_TIMER;
    if(record == &StopRecord){
        goto end;
    }
    if(record == NULL){
        goto end;
    }
    if(RedisGears_RecordGetType(record) == errorRecordType){
        goto end;
    }
    ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
    step->forEach.forEach(&ectx, record, step->forEach.stepArg.stepArg);
    if(ectx.err){
        RedisGears_FreeRecord(record);
        record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_LimitNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = NULL;    

    INIT_TIMER;
    while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx))){
        START_TIMER;
        if(record == NULL){
            goto end;
        }
        if(record == &StopRecord){
            goto end;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            goto end;
        }

        LimitExecutionStepArg* arg = (LimitExecutionStepArg*)step->limit.stepArg.stepArg;
        if(step->limit.currRecordIndex >= arg->offset &&
                step->limit.currRecordIndex < arg->offset + arg->len){
            ++step->limit.currRecordIndex;
            goto end;
        }else{
            RedisGears_FreeRecord(record);
        }
        ++step->limit.currRecordIndex;
        if(step->limit.currRecordIndex >= arg->offset + arg->len){
            record = NULL;
            goto end;
        }
    	ADD_DURATION(step->executionDuration);
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_AccumulateNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = NULL;    

    INIT_TIMER;
    if(step->accumulate.isDone){
    	return NULL;
    }
    while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx))){
        START_TIMER;
        if(record == &StopRecord){
            goto end;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            goto end;
        }
        ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
        step->accumulate.accumulator = step->accumulate.accumulate(&ectx, step->accumulate.accumulator, record, step->accumulate.stepArg.stepArg);
        if(ectx.err){
            if(step->accumulate.accumulator){
                RedisGears_FreeRecord(step->accumulate.accumulator);
                step->accumulate.accumulator = NULL;
            }
            record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
            goto end;
        }
    	ADD_DURATION(step->executionDuration);
    }
    START_TIMER;
    record = step->accumulate.accumulator;
    step->accumulate.accumulator = NULL;
    step->accumulate.isDone = true;
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_AccumulateByKeyNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
	Record* record = NULL;

    INIT_TIMER;
	if(!step->accumulateByKey.accumulators){
	    return NULL;
	}
	while((record = ExecutionPlan_NextRecord(ep, step->prev, rctx))){
        START_TIMER;
		if(record == &StopRecord){
			goto end;
		}
		if(RedisGears_RecordGetType(record) == errorRecordType){
		    goto end;
		}
		RedisModule_Assert(RedisGears_RecordGetType(record) == keyRecordType);
		char* key = RedisGears_KeyRecordGetKey(record, NULL);
		Record* val = RedisGears_KeyRecordGetVal(record);
		RedisGears_KeyRecordSetVal(record, NULL);
		Record* accumulator = NULL;
		Gears_dictEntry *entry = Gears_dictFind(step->accumulateByKey.accumulators, key);
		Record* keyRecord = NULL;
		if(entry){
			keyRecord = Gears_dictGetVal(entry);
			accumulator = RedisGears_KeyRecordGetVal(keyRecord);
		}
		ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
		accumulator = step->accumulateByKey.accumulate(&ectx, key, accumulator, val, step->accumulate.stepArg.stepArg);
		if(ectx.err){
		    if(accumulator){
		        RedisGears_FreeRecord(accumulator);
		    }
		    if(keyRecord){
                RedisGears_KeyRecordSetVal(keyRecord, NULL);
                RedisGears_FreeRecord(keyRecord);
		    }
		    Gears_dictDelete(step->accumulateByKey.accumulators, key);
			RedisGears_FreeRecord(record);
            record = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
            goto end;
		}
		if(!keyRecord){
			keyRecord = RedisGears_KeyRecordCreate();
			RedisGears_KeyRecordSetKey(keyRecord, RG_STRDUP(key), strlen(key));
			RedisModule_Assert(Gears_dictFetchValue(step->accumulateByKey.accumulators, key) == NULL);
			Gears_dictAdd(step->accumulateByKey.accumulators, key, keyRecord);
		}
		RedisGears_KeyRecordSetVal(keyRecord, accumulator);
		RedisGears_FreeRecord(record);
    	ADD_DURATION(step->executionDuration);
	}
	START_TIMER;
    if(!step->accumulateByKey.iter){
		step->accumulateByKey.iter = Gears_dictGetIterator(step->accumulateByKey.accumulators);
	}
	Gears_dictEntry *entry = Gears_dictNext(step->accumulateByKey.iter);
	if(!entry){
		Gears_dictReleaseIterator(step->accumulateByKey.iter);
		Gears_dictRelease(step->accumulateByKey.accumulators);
		step->accumulateByKey.iter = NULL;
		step->accumulateByKey.accumulators = NULL;
		record = NULL;
        goto end;
	}
	record = Gears_dictGetVal(entry);
	RedisModule_Assert(RedisGears_RecordGetType(record) == keyRecordType);
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_NextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* r = NULL;

    INIT_TIMER;
    switch(step->type){
    case READER:
    	if(array_len(ep->errors) == 0){
            GETTIME(&_ts);
            ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, ep);
    	    r = step->reader.r->next(&ectx, step->reader.r->ctx);
            GETTIME(&_te);
    	    step->executionDuration += DURATION;
    	    if(!r && ectx.err){
    	        r = RG_ErrorRecordCreate(ectx.err, strlen(ectx.err) + 1);
    	    }
    	}
        break;
    case MAP:
    	r = ExecutionPlan_MapNextRecord(ep, step, rctx);
    	break;
    case FLAT_MAP:
    	r = ExecutionPlan_FlatMapNextRecord(ep, step, rctx);
    	break;
    case FILTER:
    	r = ExecutionPlan_FilterNextRecord(ep, step, rctx);
    	break;
    case EXTRACTKEY:
    	r = ExecutionPlan_ExtractKeyNextRecord(ep, step, rctx);
    	break;
    case GROUP:
    	r = ExecutionPlan_GroupNextRecord(ep, step, rctx);
    	break;
    case REDUCE:
    	r = ExecutionPlan_ReduceNextRecord(ep, step, rctx);
    	break;
    case REPARTITION:
    	r = ExecutionPlan_RepartitionNextRecord(ep, step, rctx);
    	break;
    case COLLECT:
    	r = ExecutionPlan_CollectNextRecord(ep, step, rctx);
    	break;
    case FOREACH:
    	r = ExecutionPlan_ForEachNextRecord(ep, step, rctx);
    	break;
    case LIMIT:
    	r = ExecutionPlan_LimitNextRecord(ep, step, rctx);
    	break;
    case ACCUMULATE:
    	r = ExecutionPlan_AccumulateNextRecord(ep, step, rctx);
    	break;
    case ACCUMULATE_BY_KEY:
    	r = ExecutionPlan_AccumulateByKeyNextRecord(ep, step, rctx);
		break;
    default:
        RedisModule_Assert(false);
        return NULL;
    }
    return r;
}

static void ExecutionPlan_WriteResult(ExecutionPlan* ep, Record* record){
    ep->results = array_append(ep->results, record);
}

static void ExecutionPlan_WriteError(ExecutionPlan* ep, Record* record){
    ep->errors = array_append(ep->errors, record);
}

static bool ExecutionPlan_Execute(ExecutionPlan* ep, RedisModuleCtx* rctx){
    Record* record = NULL;

    while((record = ExecutionPlan_NextRecord(ep, ep->steps[0], rctx))){
        if(record == &StopRecord){
            // Execution need to be stopped, lets wait for a while.
            return false;
        }
        if(RedisGears_RecordGetType(record) == errorRecordType){
            ExecutionPlan_WriteError(ep, record);
        }else{
            ExecutionPlan_WriteResult(ep, record);
        }
    }

    return true;
}

ActionResult EPStatus_CreatedAction(ExecutionPlan* ep){
    if(Cluster_IsClusterMode() && EPIsFlagOff(ep, EFIsLocal)){
        // we are in a cluster mode, we must first distribute the exection to all the shards.
        if(memcmp(ep->id, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0){
            ExecutionPlan_Distribute(ep);
            ep->status = WAITING_FOR_RECIEVED_NOTIFICATION;
        }else{
            ExecutionPlan_SendRecievedNotification(ep);
            ep->status = WAITING_FOR_RUN_NOTIFICATION;
        }
        return STOP;
    }
    // we are not in cluster mode or local execution is requested, we can just start the execution
    ep->status = RUNNING;
    return CONTINUE;
}

ActionResult EPStatus_PendingReceiveAction(ExecutionPlan* ep){
    // we got recieved notification from all the cluster. Lets tell everyone to start the execution!
    ExecutionPlan_SendRunRequest(ep);
    ep->status = RUNNING;
    return CONTINUE;
}

ActionResult EPStatus_PendingRunAction(ExecutionPlan* ep){
    // got run notification. Lets start the execution!
    ep->status = RUNNING;
    return CONTINUE;
}

ActionResult EPStatus_AbortedAction(ExecutionPlan* ep){
    return COMPLETED;
}

ActionResult EPStatus_DoneAction(ExecutionPlan* ep){
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);

    if(ep->maxIdleTimerSet){
        RedisModule_StopTimer(rctx, ep->maxIdleTimer, NULL);
    }
    EPTurnOnFlag(ep, EFDone);

    // we set it to true so if execution will be freed during done callbacks we
    // will free it only after all the callbacks are executed
    EPTurnOnFlag(ep, EFIsOnDoneCallback);
    for(size_t i = 0 ; i < array_len(ep->onDoneData) ; ++i){
        ep->onDoneData[i].callback(ep, ep->onDoneData[i].privateData);
    }
    EPTurnOffFlag(ep, EFIsOnDoneCallback);
    if(EPIsFlagOn(ep, EFIsFreedOnDoneCallback)){
        RedisGears_DropExecution(ep);
    }else if(EPIsFlagOn(ep, EFIsLocalyFreedOnDoneCallback)){
        ExecutionPlan_Free(ep);
    }
    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return COMPLETED;
}

ActionResult EPStatus_RunningAction(ExecutionPlan* ep){
    INIT_TIMER;
    GETTIME(&_ts);
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    bool isDone = ExecutionPlan_Execute(ep, rctx);
    GETTIME(&_te);
    ep->executionDuration += DURATION;
    RedisModule_FreeThreadSafeContext(rctx);

    if(!isDone){
        // if we reach here and we are not done we should stop and wait for notification
        // to continue the execution
        return STOP;
    }

    // we are done :)
    if(!Cluster_IsClusterMode() || EPIsFlagOn(ep, EFIsLocal)){
        // no cluster mode or execution is local, we can just complete the execution
        ep->status = DONE;
        return CONTINUE;
    }

    // we are done the local execution but we are on cluster mode
    // we need to initiate termination protocol:
    // if (we are the initiator){
    //     1. wait for all shards to tell us they are done
    //     2. sent complete notification to all shards
    // }
    // else{
    //     1. tell the initiator that we are done
    //     2. wait for the initiator to tell us that everyone done and it safe to finish the execution
    // }

    if(memcmp(ep->id, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0){
        if((Cluster_GetSize() - 1) == ep->totalShardsCompleted){
            // todo: check if this can really happened (I think it can not)
            // all the shards are done
            // notify them that its safe to complete the execution
            Cluster_SendMsgM(NULL, ExecutionPlan_TeminateExecution, ep->id, ID_LEN);
            ep->status = DONE;
            return CONTINUE;
        }else{
            ep->status = WAITING_FOR_CLUSTER_TO_COMPLETE;
        }
    }else{
        // we are not the initiator, notifying the initiator that we are done and wait
        // for him to tell us that it safe to complete the execution
        Cluster_SendMsgM(ep->id, ExecutionPlan_NotifyExecutionDone, ep->id, ID_LEN);
        ep->status = WAITING_FOR_INITIATOR_TERMINATION;
    }
    return STOP;
}

ActionResult EPStatus_PendingClusterAction(ExecutionPlan* ep){
    // if we are here we must be the initiator
    RedisModule_Assert(memcmp(ep->id, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0);

    // we are the initiator, lets notify everyone that its safe to complete the execution
    Cluster_SendMsgM(NULL, ExecutionPlan_TeminateExecution, ep->id, ID_LEN);

    ep->status = DONE;
    return CONTINUE;
}

ActionResult EPStatus_InitiatorTerminationAction(ExecutionPlan* ep){
    // initiator notifies us that its safe to complete the execution. Lets finish it!!!
    ep->status = DONE;
    return CONTINUE;
}

static void ExecutionPlan_OnMaxIdleReacher(RedisModuleCtx *ctx, void *data){
    // If we reached here we know the execution is not running so its enough
    // to set its status to abort and call the DoneAction
    // This will for sure will not break order on local exeuctions
    // because on local execution we do not consider MaxIdleTime, they just start and
    // finish without any stops in the middle.
#define EXECUTION_MAX_IDLE_REACHED_MSG "Execution max idle reached"
    ExecutionPlan* ep = data;
    ep->status = ABORTED;
    Record* err = RG_ErrorRecordCreate(RG_STRDUP(EXECUTION_MAX_IDLE_REACHED_MSG), strlen(EXECUTION_MAX_IDLE_REACHED_MSG));
    ExecutionPlan_WriteError(ep, err);
    ep->maxIdleTimerSet = false;
    EPStatus_DoneAction(ep);
}

static void ExecutionPlan_Pause(RedisModuleCtx* ctx, ExecutionPlan* ep){
    LockHandler_Acquire(ctx);
    ep->isPaused = true;
    ep->maxIdleTimer = RedisModule_CreateTimer(ctx, ep->fep->executionMaxIdleTime, ExecutionPlan_OnMaxIdleReacher, ep);
    ep->maxIdleTimerSet = true;
    LockHandler_Release(ctx);
}

static void ExecutionPlan_Main(RedisModuleCtx* ctx, ExecutionPlan* ep){
    ActionResult result;
    EPTurnOffFlag(ep, EFSentRunRequest);
    while(true){
        result = statusesActions[ep->status](ep);
        switch(result){
        case CONTINUE:
            break;
        case STOP:
            ExecutionPlan_Pause(ctx, ep);
            return;
        case COMPLETED:
            return;
        default:
            RedisModule_Assert(false);
        }
    }
}

static void ExecutionPlan_RegisterForRun(ExecutionPlan* ep){
	if(EPIsFlagOn(ep, EFSentRunRequest)){
		return;
	}
	WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateRun(ep);
	EPTurnOnFlag(ep, EFSentRunRequest);
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

void FlatExecutionPlan_AddToRegisterDict(FlatExecutionPlan* fep){
    Gears_dictAdd(epData.registeredFepDict, fep->id, fep);
    // call the on registered callback if set
    if(fep->onRegisteredStep.stepName){
        RedisGears_FlatExecutionOnRegisteredCallback onRegistered = FlatExecutionOnRegisteredsMgmt_Get(fep->onRegisteredStep.stepName);
        RedisModule_Assert(onRegistered);
        onRegistered(fep, fep->onRegisteredStep.arg.stepArg);
    }
}

void FlatExecutionPlan_RemoveFromRegisterDict(FlatExecutionPlan* fep){
    int res = Gears_dictDelete(epData.registeredFepDict, fep->id);
    RedisModule_Assert(res == DICT_OK);
}

static int FlatExecutionPlan_RegisterInternal(FlatExecutionPlan* fep, RedisGears_ReaderCallbacks* callbacks, ExecutionMode mode, void* arg, char** err){
    RedisModule_Assert(callbacks->registerTrigger);
    if(callbacks->registerTrigger(fep, mode, arg, err) != REDISMODULE_OK){
        return REDISMODULE_ERR;
    }

    // the registeredFepDict holds a weak pointer to the fep struct. It does not increase
    // the refcount and will be remove when the fep will be unregistered
    FlatExecutionPlan_AddToRegisterDict(fep);
    return REDISMODULE_OK;
}

static void FlatExecutionPlan_RegisterKeySpaceEvent(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff = (Gears_Buffer){
        .buff = (char*)payload,
        .size = len,
        .cap = len,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    char* err = NULL;
    // this reached from another shard it safe to assume it the same as our version
    FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br, &err, REDISGEARS_DATATYPE_VERSION);
    if(!fep){
        if(!err){
            err = RG_STRDUP("Unknown error");
        }
        RedisModule_Log(ctx, "warning", "Could not deserialize flat execution plan sent by another shard : %s, error='%s'", sender_id, err);
        RG_FREE(err);
        return;
    }

    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);
    RedisModule_Assert(callbacks);
    RedisModule_Assert(callbacks->deserializeTriggerArgs);

    void* args = callbacks->deserializeTriggerArgs(&br);
    ExecutionMode mode = RedisGears_BRReadLong(&br);


    if(FlatExecutionPlan_RegisterInternal(fep, callbacks, mode, args, &err) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Could not register flat execution plan sent by another shard : %s, error='%s'", sender_id, err);
        if(err){
            RG_FREE(err);
        }
        FlatExecutionPlan_Free(fep);
        callbacks->freeTriggerArgs(args);
        return;
    }

    // replicate to oaf and slaves
    RedisModule_SelectDb(ctx, 0);
    RedisModule_Replicate(ctx, RG_INNER_REGISTER_COMMAND, "b", payload, len);
}

Reader* ExecutionPlan_GetReader(ExecutionPlan* ep){
    ExecutionStep* readerStep = ep->steps[array_len(ep->steps) - 1];
    RedisModule_Assert(readerStep->type == READER);
    return readerStep->reader.r;
}

static ExecutionPlan* FlatExecutionPlan_CreateExecution(FlatExecutionPlan* fep, char* eid, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData){
    ExecutionPlan* ep;
    if(fep->executionPoolSize > 0){
        ep = fep->executionPool[--fep->executionPoolSize];
        Reader* r = ExecutionPlan_GetReader(ep);
        // we need to reset the reader with the new arguments
        if(r->reset){
            // reader is support reset, lets use it.
            r->reset(r->ctx, arg);
        }else{
            // reader do not support reset, lets free and recreate
            ExecutionStep* readerStep = array_pop(ep->steps);
            RedisModule_Assert(readerStep->type == READER);
            readerStep->reader.r->free(readerStep->reader.r->ctx);
            RG_FREE(readerStep->reader.r);
            ReaderStep rs = ExecutionPlan_NewReader(fep->reader, arg);
            readerStep->reader = rs;
            if(array_len(ep->steps) > 0){
                ep->steps[array_len(ep->steps) - 1]->prev = readerStep;
            }
            ep->steps = array_append(ep->steps, readerStep);
        }

        // set the mode
        ep->mode = mode;
        if(ep->mode == ExecutionModeSync ||
                ep->mode == ExecutionModeAsyncLocal ||
                !Cluster_IsClusterMode()){
            EPTurnOnFlag(ep, EFIsLocal);
        }else{
            EPTurnOffFlag(ep, EFIsLocal);
        }

    } else {
        ep = ExecutionPlan_New(fep, mode, arg);
    }
    if(!ep){
        return NULL;
    }

    if(callback){
        OnDoneData onDoneData = (OnDoneData){.callback = callback, .privateData = privateData};
        ep->onDoneData = array_append(ep->onDoneData, onDoneData);
    }
    ep->fep = FlatExecutionPlan_ShallowCopy(fep);

    // set onStartCallback
    if(fep->onExecutionStartStep.stepName){
        ep->onStartCallback = ExecutionOnStartsMgmt_Get(fep->onExecutionStartStep.stepName);
    }else{
        ep->onStartCallback = NULL;
    }

    // set onUnpaused
    if(fep->onUnpausedStep.stepName){
        ep->onUnpausedCallback = ExecutionOnUnpausedsMgmt_Get(fep->onUnpausedStep.stepName);
    }else{
        ep->onUnpausedCallback = NULL;
    }

    ExecutionPlan_SetID(ep, eid);

    if(GearsConfig_GetMaxExecutions() > 0 && Gears_listLength(epData.epList) >= GearsConfig_GetMaxExecutions()){
        Gears_listNode *head = Gears_listFirst(epData.epList);
        ExecutionPlan* ep0 = Gears_listNodeValue(head);
        if(EPIsFlagOff(ep0, EFDone)){
            // we are not done yet, we will drop the execution when it finished.
            // also lets delete this execution from the execution list
            Gears_listDelNode(epData.epList, head);
            ep0->nodeOnExecutionsList = NULL;
            RedisGears_AddOnDoneCallback(ep0, RedisGears_DropLocalyOnDone, NULL);
        }else{
            ExecutionPlan_Free(ep0);
        }
    }
    Gears_listAddNodeTail(epData.epList, ep);
    ep->nodeOnExecutionsList = Gears_listLast(epData.epList);
    Gears_dictAdd(epData.epDict, ep->id, ep);

    ep->assignWorker = NULL;
    ep->isPaused = true;
    ep->maxIdleTimerSet = false;

    return ep;
}

static void ExecutionPlan_Run(ExecutionPlan* ep){
    if(!ep->assignWorker){
        ep->assignWorker = ExecutionPlan_CreateWorker(NULL);
    }
    ExecutionPlan_RegisterForRun(ep);
}

static void ExecutionStep_Reset(ExecutionStep* es){
    Gears_dictIterator * iter = NULL;
    Gears_dictEntry *entry = NULL;
    es->executionDuration = 0;
    if(es->prev){
        ExecutionStep_Reset(es->prev);
    }
    switch(es->type){
    case LIMIT:
        es->limit.currRecordIndex = 0;
        break;
    case MAP:
    case FILTER:
    case EXTRACTKEY:
    case REDUCE:
    case FOREACH:
        break;
    case FLAT_MAP:
        if(es->flatMap.pendings){
            RedisGears_FreeRecord(es->flatMap.pendings);
        }
        es->flatMap.pendings = NULL;
        break;
    case REPARTITION:
        if(es->repartion.pendings){
            while(array_len(es->repartion.pendings) > 0){
                Record* r = array_pop(es->repartion.pendings);
                RedisGears_FreeRecord(r);
            }
        }
        es->repartion.stoped = false;
        es->repartion.totalShardsCompleted = 0;
        break;
    case COLLECT:
        if(es->collect.pendings){
            while(array_len(es->collect.pendings) > 0){
                Record* r = array_pop(es->collect.pendings);
                RedisGears_FreeRecord(r);
            }
        }
        es->collect.totalShardsCompleted = 0;
        es->collect.stoped = false;
        break;
    case GROUP:
        if(es->group.groupedRecords){
            while(array_len(es->group.groupedRecords) > 0){
                Record* r = array_pop(es->group.groupedRecords);
                RedisGears_FreeRecord(r);
            }
        }
        Gears_dictEmpty(es->group.d, NULL);
        es->group.isGrouped = false;
        break;
    case READER:
        // the reader will be reset with the new args or will be freed ...
        break;
    case ACCUMULATE:
        if(es->accumulate.accumulator){
            RedisGears_FreeRecord(es->accumulate.accumulator);
        }
        es->accumulate.isDone = false;
        break;
    case ACCUMULATE_BY_KEY:
        if(es->accumulateByKey.accumulators){
            if(es->accumulateByKey.iter){
                iter = es->accumulateByKey.iter;
                es->accumulateByKey.iter = NULL;
            }else{
                iter = Gears_dictGetIterator(es->accumulateByKey.accumulators);
            }
            while((entry = Gears_dictNext(iter))){
                Record* r = Gears_dictGetVal(entry);
                RedisGears_FreeRecord(r);
            }
            Gears_dictReleaseIterator(iter);
            Gears_dictEmpty(es->accumulateByKey.accumulators, NULL);

        }else{
            es->accumulateByKey.accumulators = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
        }
        break;
    default:
        RedisModule_Assert(false);
    }
}

static void ExecutionPlan_Reset(ExecutionPlan* ep){
    while(array_len(ep->results) > 0){
        Record* record = array_pop(ep->results);
        RedisGears_FreeRecord(record);
    }

    while(array_len(ep->errors) > 0){
        Record* record = array_pop(ep->errors);
        RedisGears_FreeRecord(record);
    }

    ep->executionDuration = 0;
    ep->totalShardsRecieved = 0;
    ep->totalShardsCompleted = 0;
    ep->status = CREATED;
    EPTurnOffFlag(ep, EFSentRunRequest);
    EPTurnOffFlag(ep, EFDone);

    ep->onDoneData = array_trimm_len(ep->onDoneData, 0);
    EPTurnOffFlag(ep, EFIsOnDoneCallback);
    EPTurnOffFlag(ep, EFIsFreedOnDoneCallback);
    EPTurnOffFlag(ep, EFIsLocalyFreedOnDoneCallback);
    EPTurnOffFlag(ep, EFStarted);

    ExecutionStep_Reset(ep->steps[0]);
}

static void ExecutionPlan_RunSync(ExecutionPlan* ep){
    RedisModule_Assert(ep->mode == ExecutionModeSync);

    INIT_TIMER;
    GETTIME(&_ts);
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);

    bool isDone = ExecutionPlan_Execute(ep, rctx);
    GETTIME(&_te);

    // Sync execution can not stop in the middle
    RedisModule_Assert(isDone);
    ep->executionDuration += DURATION;

    ep->status = DONE;
    EPTurnOnFlag(ep, EFDone);

    for(size_t i = 0 ; i < array_len(ep->onDoneData) ; ++i){
        ep->onDoneData[i].callback(ep, ep->onDoneData[i].privateData);
    }

    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
}

static ExecutionPlan* FlatExecutionPlan_RunOnly(FlatExecutionPlan* fep, char* eid, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker){
    ExecutionPlan* ep = FlatExecutionPlan_CreateExecution(fep, eid, mode, arg, callback, privateData);
    if(mode == ExecutionModeSync){
        ExecutionPlan_RunSync(ep);
    } else{
        if(worker){
            ep->assignWorker = ExecutionPlan_WorkerGetShallowCopy(worker);
        }
        ExecutionPlan_Run(ep);
    }
    return ep;
}

static void ExecutionPlan_NotifyReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = ExecutionPlan_FindById(payload);
	if(!ep){
	    RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyReceived, Could not find execution");
	    return;
	}
	if(ep->status == ABORTED){
	    RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyReceived, execution aborted");
        return;
	}
	++ep->totalShardsRecieved;
	if((Cluster_GetSize() - 1) == ep->totalShardsRecieved){ // no need to wait to myself
	    ExecutionPlan_RegisterForRun(ep);
	}
}

static void ExecutionPlan_NotifyRun(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = ExecutionPlan_FindById(payload);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyRun, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyRun, execution aborted");
        return;
    }
	ExecutionPlan_RegisterForRun(ep);
}

static void ExecutionPlan_UnregisterExecutionInternal(RedisModuleCtx *ctx, FlatExecutionPlan* fep, bool abortPending){
    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);
    RedisModule_Assert(callbacks->unregisterTrigger);

    // replicate to slave and aof
    RedisModule_SelectDb(ctx, 0);
    if(abortPending){
        RedisModule_Replicate(ctx, RG_INNER_UNREGISTER_COMMAND, "cc", fep->idStr, "abortpending");
    }else{
        RedisModule_Replicate(ctx, RG_INNER_UNREGISTER_COMMAND, "c", fep->idStr);
    }

    FlatExecutionPlan_RemoveFromRegisterDict(fep);
    callbacks->unregisterTrigger(fep, abortPending);
}

static void ExecutionPlan_UnregisterExecutionReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff = {
            .buff = (char*)payload,
            .size = len,
            .cap = len,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    size_t idLen;
    char* id = RedisGears_BRReadBuffer(&br, &idLen);
    RedisModule_Assert(idLen == ID_LEN);
    bool abortPendind = RedisGears_BRReadLong(&br);
    FlatExecutionPlan* fep = FlatExecutionPlan_FindId(id);
    if(!fep){
        printf("warning: execution not found %s !!!\r\n", id);
        return;
    }
    ExecutionPlan_UnregisterExecutionInternal(ctx, fep, abortPendind);
}

static void ExecutionPlan_OnReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff = (Gears_Buffer){
        .buff = (char*)payload,
        .size = len,
        .cap = len,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    char* err = NULL;
    FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br, &err, REDISGEARS_DATATYPE_VERSION);
    if(!fep){
        RedisModule_Log(ctx, "warning", "Could not deserialize flat execution plan for execution, shard : %s, error='%s'", sender_id, err);
        if(err){
            RG_FREE(err);
        }
        return;
    }
    size_t idLen;
    char* eid = RedisGears_BRReadBuffer(&br, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    // Execution recieved from another shards is always async
    ExecutionPlan* ep = FlatExecutionPlan_CreateExecution(fep, eid, ExecutionModeAsync, NULL, NULL, NULL);
    Reader* reader = ExecutionPlan_GetReader(ep);
    reader->deserialize(fep, reader->ctx, &br);
    FlatExecutionPlan_Free(fep);

    ExecutionThreadPool* pool;
    long defaultPool = RedisGears_BRReadLong(&br);
    if(defaultPool){
        pool = epData.defaultPool;
    }else{
        const char* threadPoolName = RedisGears_BRReadString(&br);
        pool = ExectuionPlan_GetThreadPool(threadPoolName);
        if(!pool){
            RedisModule_Log(ctx, "warning", "Failed findin pool for execution %s", threadPoolName);
            RedisModule_Assert(false);
        }
    }

    ep->assignWorker = ExecutionPlan_CreateWorker(pool);
    ExecutionPlan_Run(ep);
}

static void ExecutionPlan_CollectOnRecordReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff;
    buff.buff = (char*)payload;
    buff.size = len;
    buff.cap = len;
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    size_t epIdLen;
    char* epId = RedisGears_BRReadBuffer(&br, &epIdLen);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_CollectOnRecordReceived, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_CollectOnRecordReceived, execution aborted");
        return;
    }
    size_t stepId = RedisGears_BRReadLong(&br);
    RedisModule_Assert(epIdLen == ID_LEN);
    Record* r = RG_DeserializeRecord(&br);
    WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateAddRecord(ep, stepId, r, COLLECT);
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_CollectDoneSendingRecords(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	Gears_Buffer buff;
	buff.buff = (char*)payload;
	buff.size = len;
	buff.cap = len;
	Gears_BufferReader br;
	Gears_BufferReaderInit(&br, &buff);
	size_t epIdLen;
	char* epId = RedisGears_BRReadBuffer(&br, &epIdLen);
	ExecutionPlan* ep = ExecutionPlan_FindById(epId);
	if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_CollectDoneSendingRecords, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_CollectDoneSendingRecords, execution aborted");
        return;
    }
	size_t stepId = RedisGears_BRReadLong(&br);
	WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateShardCompleted(ep, stepId, COLLECT);
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_OnRepartitionRecordReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff;
    buff.buff = (char*)payload;
    buff.size = len;
    buff.cap = len;
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    size_t epIdLen;
    char* epId = RedisGears_BRReadBuffer(&br, &epIdLen);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_OnRepartitionRecordReceived, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_OnRepartitionRecordReceived, execution aborted");
        return;
    }
    size_t stepId = RedisGears_BRReadLong(&br);
    RedisModule_Assert(epIdLen == ID_LEN);
    Record* r = RG_DeserializeRecord(&br);
    WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateAddRecord(ep, stepId, r, REPARTITION);
    ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void FlatExecutionPlan_AddBasicStep(FlatExecutionPlan* fep, const char* callbackName, void* arg, enum StepType type){
    FlatExecutionStep s;
    s.type = type;
    s.bStep.arg = (ExecutionStepArg){
        .stepArg = arg,
        .type = FlatExecutionPlan_GetArgTypeByStepType(type, callbackName),
    };
    if(callbackName){
        s.bStep.stepName = RG_STRDUP(callbackName);
    }else{
        s.bStep.stepName = NULL;
    }
    fep->steps = array_append(fep->steps, s);
}

static FlatExecutionPlan* FlatExecutionPlan_ShallowCopy(FlatExecutionPlan* fep){
    __atomic_add_fetch(&fep->refCount, 1, __ATOMIC_SEQ_CST);
    return fep;
}

static void ExecutionPlan_TeminateExecution(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    ExecutionPlan* ep = ExecutionPlan_FindById(payload);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_TeminateExecution, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_TeminateExecution, execution aborted");
        return;
    }
    WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateTerminate(ep);
    ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_NotifyExecutionDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    ExecutionPlan* ep = ExecutionPlan_FindById(payload);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyExecutionDone, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_NotifyExecutionDone, execution aborted");
        return;
    }
    WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateDone(ep);
    ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_DoneRepartition(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff;
    buff.buff = (char*)payload;
    buff.size = len;
    buff.cap = len;
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    size_t epIdLen;
    char* epId = RedisGears_BRReadBuffer(&br, &epIdLen);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
    if(!ep){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_DoneRepartition, Could not find execution");
        return;
    }
    if(ep->status == ABORTED){
        RedisModule_Log(NULL, "warning", "On ExecutionPlan_DoneRepartition, execution aborted");
        return;
    }
    size_t stepId = RedisGears_BRReadLong(&br);
	WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateShardCompleted(ep, stepId, REPARTITION);
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_ExecutionTerminate(RedisModuleCtx* ctx, ExecutionPlan* ep){
    RedisModule_Assert(ep->status == WAITING_FOR_INITIATOR_TERMINATION);
    ExecutionPlan_Main(ctx, ep);
}

static void ExecutionPlan_ExecutionDone(RedisModuleCtx* ctx, ExecutionPlan* ep){
    ep->totalShardsCompleted++;
    if((Cluster_GetSize() - 1) == ep->totalShardsCompleted){ // no need to wait to myself
        ExecutionPlan_Main(ctx, ep);
    }else{
        ExecutionPlan_Pause(ctx, ep);
    }
}

static void ExecutionPlan_StepDone(RedisModuleCtx* ctx, ExecutionPlan* ep, size_t stepId, enum StepType stepType){
	size_t totalShardsCompleted;
	switch(stepType){
	case REPARTITION:
	    RedisModule_Assert(ep->steps[stepId]->type == REPARTITION);
		totalShardsCompleted = ++ep->steps[stepId]->repartion.totalShardsCompleted;
		break;
	case COLLECT:
	    RedisModule_Assert(ep->steps[stepId]->type == COLLECT);
		totalShardsCompleted = ++ep->steps[stepId]->collect.totalShardsCompleted;
		break;
	default:
	    RedisModule_Assert(false);
	}

	RedisModule_Assert(Cluster_GetSize() - 1 >= totalShardsCompleted);
	if((Cluster_GetSize() - 1) == totalShardsCompleted){ // no need to wait to myself
	    ExecutionPlan_Main(ctx, ep);
	}else{
	    ExecutionPlan_Pause(ctx, ep);
	}
}

static void ExecutionPlan_AddStepRecord(RedisModuleCtx* ctx, ExecutionPlan* ep, size_t stepId, Record* r, enum StepType stepType){
#define MAX_PENDING_TO_START_RUNNING 10000
	Record*** pendings = NULL;
	switch(stepType){
	case REPARTITION:
	    RedisModule_Assert(ep->steps[stepId]->type == REPARTITION);
		pendings = &(ep->steps[stepId]->repartion.pendings);
		break;
	case COLLECT:
	    RedisModule_Assert(ep->steps[stepId]->type == COLLECT);
		pendings = &(ep->steps[stepId]->collect.pendings);
		break;
	default:
	    RedisModule_Assert(false);
	}
	*pendings = array_append(*pendings, r);
	if(array_len(*pendings) >= MAX_PENDING_TO_START_RUNNING){
	    ExecutionPlan_Main(ctx, ep);
	}else{
	    ExecutionPlan_Pause(ctx, ep);
	}
}

static void ExecutionPlan_MsgArrive(RedisModuleCtx* ctx, WorkerMsg* msg){
    ExecutionPlan* ep;
    LockHandler_Acquire(ctx);
    ep = ExecutionPlan_FindById(msg->id);
    if(!ep){
        // execution was probably already deleted
        LockHandler_Release(ctx);
        ExectuionPlan_WorkerMsgFree(msg);
        return;
    }
    if(ep->status == ABORTED){
        LockHandler_Release(ctx);
        ExectuionPlan_WorkerMsgFree(msg);
        return;
    }
    if(EPIsFlagOff(ep, EFStarted)){
        // lets mark execution as started, dropping it now require some extra work.
        EPTurnOnFlag(ep, EFStarted);

        // calling the onStart callback if exists
        if(ep->onStartCallback){
            ExecutionCtx ectx = {
                    .rctx = ctx,
                    .ep = ep,
                    .err = NULL,
            };
            ep->onStartCallback(&ectx, ep->fep->onExecutionStartStep.arg.stepArg);
        }
    }else{
        // here we need to cancle an existing maxIdleTimer
        RedisModule_StopTimer(ctx, ep->maxIdleTimer, NULL);
        ep->maxIdleTimerSet = false;
    }
    if(ep->onUnpausedCallback){
        ExecutionCtx ectx = {
                .rctx = ctx,
                .ep = ep,
                .err = NULL,
        };
        ep->onUnpausedCallback(&ectx, ep->fep->onUnpausedStep.arg.stepArg);
    }
    ep->isPaused = false;
    LockHandler_Release(ctx);
	switch(msg->type){
	case RUN_MSG:
        ExecutionPlan_Main(ctx, ep);
		break;
	case ADD_RECORD_MSG:
		ExecutionPlan_AddStepRecord(ctx, ep, msg->addRecordWM.stepId, msg->addRecordWM.record, msg->addRecordWM.stepType);
		// setting it to NULL to indicate that we move responsibility
		// on the record to the execution and it should not be free on ExectuionPlan_WorkerMsgFree
		msg->addRecordWM.record = NULL;
		break;
	case SHARD_COMPLETED_MSG:
		ExecutionPlan_StepDone(ctx, ep, msg->shardCompletedWM.stepId, msg->shardCompletedWM.stepType);
		break;
	case EXECUTION_DONE:
	    ExecutionPlan_ExecutionDone(ctx, ep);
	    break;
	case EXECUTION_TERMINATE:
        ExecutionPlan_ExecutionTerminate(ctx, ep);
        break;
	default:
	    RedisModule_Assert(false);
	}
	ExectuionPlan_WorkerMsgFree(msg);
}

static void ExecutionPlan_MessageThreadMain(void *arg){
    WorkerData* wd = arg;

    pthread_mutex_lock(&wd->lock);
    Gears_listNode* n = Gears_listFirst(wd->notifications);
    WorkerMsg* msg = Gears_listNodeValue(n);
    pthread_mutex_unlock(&wd->lock);

    if(msg->type == WORKER_FREE){
        ExectuionPlan_WorkerMsgFree(msg);
        ExecutionPlan_FreeWorkerInternal(wd);
        return;
    }

    ExecutionPlan_MsgArrive(wd->ctx, msg);

    pthread_mutex_lock(&wd->lock);
    Gears_listDelNode(wd->notifications, n);
    if(Gears_listLength(wd->notifications) > 0){
        // need to trigger another call (we are not doing it directly for fairness)
        Gears_thpool_add_work(wd->pool->pool, ExecutionPlan_MessageThreadMain, wd);
    }
    pthread_mutex_unlock(&wd->lock);
}

WorkerData* ExecutionPlan_CreateWorker(ExecutionThreadPool* pool){
    WorkerData* wd = RG_ALLOC(sizeof(WorkerData));

    pthread_mutex_init(&wd->lock, NULL);
    wd->notifications = Gears_listCreate();
    wd->ctx = RedisModule_GetThreadSafeContext(NULL);
    wd->refCount = 1;
    wd->status = WorkerStatus_Running;
    wd->pool = pool;
    if(!wd->pool){
        wd->pool = epData.defaultPool;
    }
    return wd;
}

static void ExecutionPlan_FreeWorkerInternal(WorkerData* wd){
    if(Gears_listLength(wd->notifications) > 1){
        RedisModule_Log(NULL, "warning", "Worker was freed but not empty, fatal!!!");
        RedisModule_Assert(false);
    }
    Gears_listRelease(wd->notifications);
    RedisModule_FreeThreadSafeContext(wd->ctx);
    RG_FREE(wd);
}

void ExecutionPlan_FreeWorker(WorkerData* wd){
    if(--wd->refCount){
        return;
    }

    WorkerMsg* msg = ExectuionPlan_WorkerMsgFreeWorker();
    ExectuionPlan_WorkerMsgSend(wd, msg);
    // We can not directly free the worker here, it might running on messages
    // right now and we will cause illegal memory access.
    // We will send a message to the worker to free itself and deny any
    // future messages
}

WorkerData* ExecutionPlan_WorkerGetShallowCopy(WorkerData* wd){
    ++wd->refCount;
    return wd;
}

void ExecutionPlan_Initialize(){
    poolDictionary = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    epData.epDict = Gears_dictCreate(dictTypeHeapIdsPtr, NULL);
    epData.registeredFepDict = Gears_dictCreate(dictTypeHeapIdsPtr, NULL);
    epData.epList = Gears_listCreate();

    Cluster_RegisterMsgReceiverM(ExecutionPlan_UnregisterExecutionReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_OnReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyRun);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_CollectOnRecordReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_CollectDoneSendingRecords);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_OnRepartitionRecordReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_DoneRepartition);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyExecutionDone);
    Cluster_RegisterMsgReceiverM(FlatExecutionPlan_RegisterKeySpaceEvent);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_TeminateExecution);

    epData.defaultPool = ExecutionPlan_CreateThreadPool("DefaultPool", GearsConfig_ExecutionThreads());
}

const char* FlatExecutionPlan_GetReader(FlatExecutionPlan* fep){
    return fep->reader->reader;
}

int FlatExecutionPlan_Register(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);
    RedisModule_Assert(callbacks); // todo: handle as error in future
    if(!callbacks->registerTrigger){
        return 0;
    }

    RedisModule_Assert(callbacks->serializeTriggerArgs);

    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);

    size_t len;
    int res = FlatExecutionPlan_Serialize(&bw, fep, err);
    if(res != REDISMODULE_OK){
        Gears_BufferFree(buff);
        callbacks->freeTriggerArgs(args);
        return 0;
    }

    callbacks->serializeTriggerArgs(args, &bw);
    RedisGears_BWWriteLong(&bw, mode);

    if(FlatExecutionPlan_RegisterInternal(FlatExecutionPlan_ShallowCopy(fep), callbacks, mode, args, err) != REDISMODULE_OK){
        Gears_BufferFree(buff);
        FlatExecutionPlan_Free(fep);
        callbacks->freeTriggerArgs(args);
        return 0;
    }

    if(Cluster_IsClusterMode()){
        Cluster_SendMsgM(NULL, FlatExecutionPlan_RegisterKeySpaceEvent, buff->buff, buff->size);
    }

    // replicating to slave and aof
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_SelectDb(ctx, 0);
    RedisModule_Replicate(ctx, RG_INNER_REGISTER_COMMAND, "b", buff->buff, buff->size);
    RedisModule_FreeThreadSafeContext(ctx);
    Gears_BufferFree(buff);
    return 1;
}

ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err){
    if(Cluster_IsClusterMode()){
        // on cluster mode, we must make sure we can distribute the execution to all shards.
        if(!FlatExecutionPlan_SerializeInternal(fep, NULL, err)){
            return NULL;
        }
    }

    return FlatExecutionPlan_RunOnly(fep, NULL, mode, arg, callback, privateData, worker);
}

static ReaderStep ExecutionPlan_NewReader(FlatExecutionReader* reader, void* arg){
    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(reader->reader);
    RedisModule_Assert(callbacks); // todo: handle as error in future
    return (ReaderStep){.r = callbacks->create(arg)};
}

static ExecutionStep* ExecutionPlan_NewExecutionStep(FlatExecutionStep* step){
#define PENDING_INITIAL_SIZE 10
    ExecutionStep* es = RG_ALLOC(sizeof(*es));
    es->type = step->type;
    switch(step->type){
    case MAP:
        es->map.map = MapsMgmt_Get(step->bStep.stepName);
        es->map.stepArg = step->bStep.arg;
        break;
    case FLAT_MAP:
        es->flatMap.mapStep.map = MapsMgmt_Get(step->bStep.stepName);
        es->flatMap.mapStep.stepArg = step->bStep.arg;
        es->flatMap.pendings = NULL;
        break;
    case FILTER:
        es->filter.filter = FiltersMgmt_Get(step->bStep.stepName);
        es->filter.stepArg = step->bStep.arg;
        break;
    case EXTRACTKEY:
        es->extractKey.extractor = ExtractorsMgmt_Get(step->bStep.stepName);
        es->extractKey.extractorArg = step->bStep.arg;
        break;
    case REDUCE:
        es->reduce.reducer = ReducersMgmt_Get(step->bStep.stepName);
        es->reduce.reducerArg = step->bStep.arg;
        break;
    case GROUP:
#define GROUP_RECORD_INIT_LEN 10
        es->group.groupedRecords = array_new(Record*, GROUP_RECORD_INIT_LEN);;
        es->group.d = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
        es->group.isGrouped = false;
        break;
    case REPARTITION:
        es->repartion.stoped = false;
        es->repartion.pendings = array_new(Record*, PENDING_INITIAL_SIZE);
        es->repartion.totalShardsCompleted = 0;
        break;
    case COLLECT:
    	es->collect.totalShardsCompleted = 0;
    	es->collect.stoped = false;
    	es->collect.pendings = array_new(Record*, PENDING_INITIAL_SIZE);
    	break;
    case FOREACH:
        es->forEach.forEach = ForEachsMgmt_Get(step->bStep.stepName);
        es->forEach.stepArg = step->bStep.arg;
        break;
    case LIMIT:
        es->limit.stepArg = step->bStep.arg;
        es->limit.currRecordIndex = 0;
        break;
    case ACCUMULATE:
        es->accumulate.stepArg = step->bStep.arg;
        es->accumulate.accumulate = AccumulatesMgmt_Get(step->bStep.stepName);
        es->accumulate.accumulator = NULL;
        es->accumulate.isDone = false;
        break;
    case ACCUMULATE_BY_KEY:
    	es->accumulateByKey.stepArg = step->bStep.arg;
		es->accumulateByKey.accumulate = AccumulateByKeysMgmt_Get(step->bStep.stepName);
		es->accumulateByKey.accumulators = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
		es->accumulateByKey.iter = NULL;
		break;
    default:
        RedisModule_Assert(false);
    }
    es->executionDuration = 0;
    return es;
}

static ExecutionStep* ExecutionPlan_NewReaderExecutionStep(ReaderStep reader){
    ExecutionStep* es = RG_ALLOC(sizeof(*es));
    es->type = READER;
    es->reader = reader;
    es->prev = NULL;
    es->executionDuration = 0;
    return es;
}

static void ExecutionPlan_SetID(ExecutionPlan* ep, char* id){
    SetId(id, ep->id, ep->idStr, &lastEPId);
}

static void FlatExecutionPlan_SetID(FlatExecutionPlan* fep, char* id){
    SetId(id, fep->id, fep->idStr, &lastFEPId);
}

static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep, ExecutionMode mode, void* arg){
    ExecutionPlan* ret = RG_ALLOC(sizeof(*ret));
    ret->steps = array_new(FlatExecutionStep*, array_len(fep->steps));
    ret->executionDuration = 0;
    ExecutionStep* last = NULL;
    for(int i = array_len(fep->steps) - 1 ; i >= 0 ; --i){
        FlatExecutionStep* s = fep->steps + i;
        ExecutionStep* es = ExecutionPlan_NewExecutionStep(s);
        es->stepId = array_len(fep->steps) - 1 - i;
        if(array_len(ret->steps) > 0){
            ret->steps[array_len(ret->steps) - 1]->prev = es;
        }
        ret->steps = array_append(ret->steps, es);
    }
    ReaderStep rs = ExecutionPlan_NewReader(fep->reader, arg);
    ExecutionStep* readerStep = ExecutionPlan_NewReaderExecutionStep(rs);
    if(array_len(ret->steps) > 0){
        ret->steps[array_len(ret->steps) - 1]->prev = readerStep;
    }
    ret->steps = array_append(ret->steps, readerStep);
    ret->totalShardsRecieved = 0;
    ret->totalShardsCompleted = 0;
    ret->results = array_new(Record*, 100);
    ret->errors = array_new(Record*, 1);
    ret->status = CREATED;
    EPTurnOffFlag(ret, EFSentRunRequest);
    ret->onDoneData = array_new(OnDoneData, 10);
    EPTurnOffFlag(ret, EFDone);
    ret->mode = mode;
    if(ret->mode == ExecutionModeSync ||
            ret->mode == ExecutionModeAsyncLocal ||
            !Cluster_IsClusterMode()){
        EPTurnOnFlag(ret, EFIsLocal);
    }else{
        EPTurnOffFlag(ret, EFIsLocal);
    }
    EPTurnOffFlag(ret, EFIsFreedOnDoneCallback);
    EPTurnOffFlag(ret, EFIsLocalyFreedOnDoneCallback);
    EPTurnOffFlag(ret, EFIsOnDoneCallback);
    EPTurnOffFlag(ret, EFStarted);
    return ret;
}

static void ExecutionStep_Free(ExecutionStep* es){
	Gears_dictIterator * iter = NULL;
	Gears_dictEntry *entry = NULL;
    if(es->prev){
        ExecutionStep_Free(es->prev);
    }
    switch(es->type){
    case LIMIT:
    case MAP:
    case FILTER:
    case EXTRACTKEY:
    case REDUCE:
    case FOREACH:
        break;
    case FLAT_MAP:
        if(es->flatMap.pendings){
            RedisGears_FreeRecord(es->flatMap.pendings);
        }
        break;
    case REPARTITION:
    	if(es->repartion.pendings){
			for(size_t i = 0 ; i < array_len(es->repartion.pendings) ; ++i){
				Record* r = es->repartion.pendings[i];
				RedisGears_FreeRecord(r);
			}
			array_free(es->repartion.pendings);
		}
		break;
    case COLLECT:
    	if(es->collect.pendings){
    		for(size_t i = 0 ; i < array_len(es->collect.pendings) ; ++i){
				Record* r = es->collect.pendings[i];
				RedisGears_FreeRecord(r);
			}
			array_free(es->collect.pendings);
    	}
		break;
    case GROUP:
        if(es->group.groupedRecords){
            for(size_t i = 0 ; i < array_len(es->group.groupedRecords) ; ++i){
                Record* r = es->group.groupedRecords[i];
                RedisGears_FreeRecord(r);
            }
            array_free(es->group.groupedRecords);
            Gears_dictRelease(es->group.d);
        }
        break;
    case READER:
        if(es->reader.r->free){
            es->reader.r->free(es->reader.r->ctx);
        }
        RG_FREE(es->reader.r);
        break;
    case ACCUMULATE:
    	if(es->accumulate.accumulator){
    		RedisGears_FreeRecord(es->accumulate.accumulator);
    	}
    	break;
    case ACCUMULATE_BY_KEY:
    	if(es->accumulateByKey.accumulators){
			if(es->accumulateByKey.iter){
				iter = es->accumulateByKey.iter;
			}else{
				iter = Gears_dictGetIterator(es->accumulateByKey.accumulators);
			}
			while((entry = Gears_dictNext(iter))){
				Record* r = Gears_dictGetVal(entry);
				RedisGears_FreeRecord(r);
			}
			Gears_dictReleaseIterator(iter);
			Gears_dictRelease(es->accumulateByKey.accumulators);
    	}
		break;
	default:
	    RedisModule_Assert(false);
    }
    RG_FREE(es);
}

static void ExecutionPlan_FreeRaw(ExecutionPlan* ep){
    ExecutionStep_Free(ep->steps[0]);
    array_free(ep->steps);
    array_free(ep->results);
    array_free(ep->errors);
    array_free(ep->onDoneData);
    RG_FREE(ep);
}

void ExecutionPlan_Free(ExecutionPlan* ep){

    if(ep->assignWorker){
        ExecutionPlan_FreeWorker(ep->assignWorker);
    }

    if(ep->nodeOnExecutionsList){
        Gears_listDelNode(epData.epList, ep->nodeOnExecutionsList);
    }
    Gears_dictDelete(epData.epDict, ep->id);

    ExecutionPlan_Reset(ep);

    FlatExecutionPlan* fep = ep->fep;
    if(fep->executionPoolSize < EXECUTION_POOL_SIZE){
        fep->executionPool[fep->executionPoolSize++] = ep;
    }else{
        ExecutionPlan_FreeRaw(ep);
    }

    FlatExecutionPlan_Free(fep);
}

static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader){
    FlatExecutionReader* res = RG_ALLOC(sizeof(*res));
    res->reader = RG_STRDUP(reader);
    return res;
}

FlatExecutionPlan* FlatExecutionPlan_New(){
#define STEPS_INITIAL_CAP 10
    FlatExecutionPlan* res = RG_ALLOC(sizeof(*res));
    res->refCount = 1;
    res->reader = NULL;
    res->steps = array_new(FlatExecutionStep, STEPS_INITIAL_CAP);
    res->PD = NULL;
    res->PDType = NULL;
    res->desc = NULL;
    res->executionPoolSize = 0;
    res->serializedFep = NULL;
    res->executionMaxIdleTime = GearsConfig_ExecutionMaxIdleTime();
    res->onExecutionStartStep = (FlatBasicStep){
            .stepName = NULL,
            .arg = {
                    .stepArg = NULL,
                    .type = NULL,
            },
    };

    res->onRegisteredStep = (FlatBasicStep){
            .stepName = NULL,
            .arg = {
                    .stepArg = NULL,
                    .type = NULL,
            },
    };

    res->onUnpausedStep = (FlatBasicStep){
            .stepName = NULL,
            .arg = {
                    .stepArg = NULL,
                    .type = NULL,
            },
    };

    FlatExecutionPlan_SetID(res, NULL);

    return res;
}

void FlatExecutionPlan_FreeArg(FlatExecutionStep* step){
    if (step->bStep.arg.type && step->bStep.arg.type->free){
        step->bStep.arg.type->free(step->bStep.arg.stepArg);
    }
}

void FlatExecutionPlan_Free(FlatExecutionPlan* fep){
    if(__atomic_sub_fetch(&fep->refCount, 1, __ATOMIC_SEQ_CST) > 0){
        return;
    }

    RedisModule_Assert(fep->refCount == 0);

    for(size_t i = 0 ; i < fep->executionPoolSize ; ++i){
        ExecutionPlan_FreeRaw(fep->executionPool[i]);
    }

    if(fep->PD){
        ArgType* type = FepPrivateDatasMgmt_GetArgType(fep->PDType);
        if(type && type->free){
            type->free(fep->PD);
        }
        RG_FREE(fep->PDType);
    }
    if(fep->reader){
        RG_FREE(fep->reader->reader);
        RG_FREE(fep->reader);
    }
    for(size_t i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* step = fep->steps + i;
        RG_FREE(step->bStep.stepName);
        FlatExecutionPlan_FreeArg(step);
    }
    array_free(fep->steps);
    if(fep->serializedFep){
        Gears_BufferFree(fep->serializedFep);
    }
    if(fep->desc){
        RG_FREE(fep->desc);
    }

    if(fep->onExecutionStartStep.stepName){
        RG_FREE(fep->onExecutionStartStep.stepName);
        if(fep->onExecutionStartStep.arg.stepArg){
            RedisModule_Assert(fep->onExecutionStartStep.arg.type);
            fep->onExecutionStartStep.arg.type->free(fep->onExecutionStartStep.arg.stepArg);
        }
    }

    if(fep->onRegisteredStep.stepName){
        RG_FREE(fep->onRegisteredStep.stepName);
        if(fep->onRegisteredStep.arg.stepArg){
            RedisModule_Assert(fep->onRegisteredStep.arg.type);
            fep->onRegisteredStep.arg.type->free(fep->onRegisteredStep.arg.stepArg);
        }
    }

    if(fep->onUnpausedStep.stepName){
        RG_FREE(fep->onUnpausedStep.stepName);
        if(fep->onUnpausedStep.arg.stepArg){
            RedisModule_Assert(fep->onUnpausedStep.arg.type);
            fep->onUnpausedStep.arg.type->free(fep->onUnpausedStep.arg.stepArg);
        }
    }

    RG_FREE(fep);
}

bool FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader){
    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(reader);
    if(!callbacks){
        return false;
    }
    fep->reader = FlatExecutionPlan_NewReader(reader);
    return true;
}

void* FlatExecutionPlan_GetPrivateData(FlatExecutionPlan* fep){
    return fep->PD;
}

void FlatExecutionPlan_SetPrivateData(FlatExecutionPlan* fep, const char* type, void* PD){
    fep->PD = PD;
    fep->PDType = RG_STRDUP(type);
}

void FlatExecutionPlan_SetDesc(FlatExecutionPlan* fep, const char* desc){
    fep->desc = RG_STRDUP(desc);
}

void FlatExecutionPlan_AddForEachStep(FlatExecutionPlan* fep, char* forEach, void* writerArg){
    FlatExecutionPlan_AddBasicStep(fep, forEach, writerArg, FOREACH);
}

void FlatExecutionPlan_SetOnStartStep(FlatExecutionPlan* fep, char* onStartCallback, void* onStartArg){
    fep->onExecutionStartStep.stepName = onStartCallback;
    fep->onExecutionStartStep.arg.stepArg = onStartArg;
    fep->onExecutionStartStep.arg.type = ExecutionOnStartsMgmt_GetArgType(onStartCallback);
    RedisModule_Assert(fep->onExecutionStartStep.arg.type);
}

void FlatExecutionPlan_SetOnUnPausedStep(FlatExecutionPlan* fep, char* onUnpausedCallback, void* onUnpausedArg){
    fep->onUnpausedStep.stepName = onUnpausedCallback;
    fep->onUnpausedStep.arg.stepArg = onUnpausedArg;
    fep->onUnpausedStep.arg.type = ExecutionOnUnpausedsMgmt_GetArgType(onUnpausedCallback);
    RedisModule_Assert(fep->onUnpausedStep.arg.type);
}

void FlatExecutionPlan_SetOnRegisteredStep(FlatExecutionPlan* fep, char* onRegisteredCallback, void* onRegisteredArg){
    fep->onRegisteredStep.stepName = onRegisteredCallback;
    fep->onRegisteredStep.arg.stepArg = onRegisteredArg;
    fep->onRegisteredStep.arg.type = FlatExecutionOnRegisteredsMgmt_GetArgType(onRegisteredCallback);
    RedisModule_Assert(fep->onRegisteredStep.arg.type);
}

void FlatExecutionPlan_AddAccumulateStep(FlatExecutionPlan* fep, char* accumulator, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, accumulator, arg, ACCUMULATE);
}

void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, callbackName, arg, MAP);
}

void FlatExecutionPlan_AddFlatMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, callbackName, arg, FLAT_MAP);
}

void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, callbackName, arg, FILTER);
}

void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                  const char* reducerName, void* reducerArg){
    FlatExecutionStep extractKey;
    FlatExecutionPlan_AddBasicStep(fep, extraxtorName, extractorArg, EXTRACTKEY);
    FlatExecutionPlan_AddBasicStep(fep, stepsNames[REPARTITION], NULL, REPARTITION);
    FlatExecutionPlan_AddBasicStep(fep, stepsNames[GROUP], NULL, GROUP);
    FlatExecutionPlan_AddBasicStep(fep, reducerName, reducerArg, REDUCE);
}

void FlatExecutionPlan_AddAccumulateByKeyStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                              const char* accumulateName, void* accumulateArg){
    FlatExecutionStep extractKey;
    FlatExecutionPlan_AddBasicStep(fep, extraxtorName, extractorArg, EXTRACTKEY);
    FlatExecutionPlan_AddBasicStep(fep, stepsNames[REPARTITION], NULL, REPARTITION);
    FlatExecutionPlan_AddBasicStep(fep, accumulateName, accumulateArg, ACCUMULATE_BY_KEY);
}

void FlatExecutionPlan_AddLocalAccumulateByKeyStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                                   const char* accumulateName, void* accumulateArg){
    FlatExecutionPlan_AddBasicStep(fep, extraxtorName, extractorArg, EXTRACTKEY);
    FlatExecutionPlan_AddBasicStep(fep, accumulateName, accumulateArg, ACCUMULATE_BY_KEY);
}

void FlatExecutionPlan_AddCollectStep(FlatExecutionPlan* fep){
	FlatExecutionPlan_AddBasicStep(fep, stepsNames[COLLECT], NULL, COLLECT);
}

void FlatExecutionPlan_AddLimitStep(FlatExecutionPlan* fep, size_t offset, size_t len){
    LimitExecutionStepArg* arg = RG_ALLOC(sizeof(*arg));
    *arg = (LimitExecutionStepArg){
        .offset = offset,
        .len = len,
    };
    FlatExecutionPlan_AddBasicStep(fep, stepsNames[LIMIT], arg, LIMIT);
}

void FlatExecutionPlan_AddRepartitionStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg){
    FlatExecutionPlan_AddBasicStep(fep, extraxtorName, extractorArg, EXTRACTKEY);
    FlatExecutionPlan_AddBasicStep(fep, stepsNames[REPARTITION], NULL, REPARTITION);
    FlatExecutionPlan_AddMapStep(fep, "GetValueMapper", NULL);
}

int ExecutionPlan_DumpRegistrations(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 1){
        return RedisModule_WrongArity(ctx);
    }
    Gears_dictIterator* iter = Gears_dictGetIterator(epData.registeredFepDict);
    Gears_dictEntry *curr = NULL;
    size_t numElements = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    while((curr = Gears_dictNext(iter))){
        FlatExecutionPlan* fep = Gears_dictGetVal(curr);
        RedisModule_ReplyWithArray(ctx, 10);
        RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
        RedisModule_ReplyWithStringBuffer(ctx, fep->idStr, strlen(fep->idStr));
        RedisModule_ReplyWithStringBuffer(ctx, "reader", strlen("reader"));
        RedisModule_ReplyWithStringBuffer(ctx, fep->reader->reader, strlen(fep->reader->reader));
        RedisModule_ReplyWithStringBuffer(ctx, "desc", strlen("desc"));
        if(fep->desc){
            RedisModule_ReplyWithStringBuffer(ctx, fep->desc, strlen(fep->desc));
        }else{
            RedisModule_ReplyWithNull(ctx);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "RegistrationData", strlen("RegistrationData"));

        RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);

        if(!callbacks->dumpRegistratioData){
            RedisModule_ReplyWithNull(ctx);
        } else {
            callbacks->dumpRegistratioData(ctx, fep);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "PD", strlen("PD"));
        if(fep->PD){
            ArgType* type = FepPrivateDatasMgmt_GetArgType(fep->PDType);
            char* pdStr = type->tostring(fep->PD);
            RedisModule_ReplyWithStringBuffer(ctx, pdStr, strlen(pdStr));
            RG_FREE(pdStr);
        }else{
            RedisModule_ReplyWithNull(ctx);
        }
        ++numElements;
    }
    Gears_dictReleaseIterator(iter);

    RedisModule_ReplySetArrayLength(ctx, numElements);
    return REDISMODULE_OK;
}

static int ExecutionPlan_UnregisterCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool sendOnCluster){
    if(argc < 2 || argc > 3){
        return RedisModule_WrongArity(ctx);
    }

    const char* id = RedisModule_StringPtrLen(argv[1], NULL);
    FlatExecutionPlan* fep = FlatExecutionPlan_FindByStrId(id);

    if(!fep){
        RedisModule_ReplyWithError(ctx, "execution does not registered");
        return REDISMODULE_OK;
    }

    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);

    if(!callbacks->unregisterTrigger){
        RedisModule_ReplyWithError(ctx, "reader does not support unregister");
        return REDISMODULE_OK;
    }

    bool abortPending = false;
    if(argc == 3){
        const char* abortPendingStr = RedisModule_StringPtrLen(argv[2], NULL);
        if(strcasecmp(abortPendingStr, "abortpending") == 0){
            abortPending = true;
        }
    }

    if(sendOnCluster && Cluster_IsClusterMode()){
        Gears_Buffer* buff = Gears_BufferNew(50);
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        RedisGears_BWWriteBuffer(&bw, fep->id, ID_LEN);
        RedisGears_BWWriteLong(&bw, abortPending);
        Cluster_SendMsgM(NULL, ExecutionPlan_UnregisterExecutionReceived, buff->buff, buff->size);
        Gears_BufferFree(buff);
    }

    ExecutionPlan_UnregisterExecutionInternal(ctx, fep, abortPending);

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

int ExecutionPlan_InnerUnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    return ExecutionPlan_UnregisterCommon(ctx, argv, argc, false);
}

int ExecutionPlan_UnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    return ExecutionPlan_UnregisterCommon(ctx, argv, argc, true);
}

int ExecutionPlan_InnerRegister(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    const char* val = RedisModule_StringPtrLen(argv[1], &len);
    FlatExecutionPlan_RegisterKeySpaceEvent(ctx, NULL, 0, val, len);
    return REDISMODULE_OK;
}

int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	size_t numOfEntries = 0;
    Gears_dictIterator* it = Gears_dictGetIterator(epData.epDict);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(it))) {
        ExecutionPlan* ep = Gears_dictGetVal(entry);
		RedisModule_ReplyWithArray(ctx, 4);
		RedisModule_ReplyWithStringBuffer(ctx, "executionId", strlen("executionId"));
		RedisModule_ReplyWithStringBuffer(ctx, ep->idStr, strlen(ep->idStr));
		RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
        RedisModule_ReplyWithStringBuffer(ctx, statusesNames[ep->status], strlen(statusesNames[ep->status]));
		++numOfEntries;
    }
    Gears_dictReleaseIterator(it);
	RedisModule_ReplySetArrayLength(ctx, numOfEntries);
	return REDISMODULE_OK;
}

static void onDoneResultsOnly(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResults(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

int ExecutionPlan_ExecutionGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2 || argc > 3){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
    bool bClusterPlan = Cluster_IsClusterMode();

    ExecutionPlan* ep = RedisGears_GetExecution(id);

    if(!ep){
        RedisModule_ReplyWithError(ctx, "execution plan does not exist");
        return REDISMODULE_OK;
    }

    if(argc == 3){
        const char *subcommand = RedisModule_StringPtrLen(argv[2], NULL);
        if(!strcasecmp(subcommand, "shard")){
            bClusterPlan = false;
        }else if(!strcasecmp(subcommand, "cluster")){
            if(!bClusterPlan){
                RedisModule_ReplyWithError(ctx, "no cluster detected - use `RG.GETEXECUTION <id> [SHARD]` instead");
                return REDISMODULE_OK;            
            }
#ifndef WITHPYTHON
            RedisModule_ReplyWithError(ctx, "cluster execution plan requires Python enabled - only `RG.GETEXECUTION <id> SHARD` is supported");
            return REDISMODULE_OK;            
#endif
        }else{
            RedisModule_ReplyWithError(ctx, "unknown subcommand");
            return REDISMODULE_OK;
        }
    }

    if(bClusterPlan){
#ifndef WITHPYTHON
        assert(true);
#else
        RedisModuleString **fargv = RG_CALLOC(2, sizeof(RedisModuleString*));
        const char *eid = RedisModule_StringPtrLen(argv[1], NULL);
        fargv[1] = RedisModule_CreateStringPrintf(ctx,
            "GB('ShardsIDReader')"
            ".map(lambda x: execute('RG.GETEXECUTION', '%s', 'SHARD'))"
            ".collect()"
            ".flatmap(lambda x: [i for i in x])"
            ".run(convertToStr=False, collect=False)", eid);
        // TODO: we create the fake args array with size 2 because the first should be the commmand but we only use the second one.
        int res = RedisGearsPy_ExecuteWithCallback(ctx, fargv, 2, onDoneResultsOnly);
        RedisModule_FreeString(ctx, fargv[1]);
        RG_FREE(fargv);
        return res;
#endif
    }else{
        RedisModule_ReplyWithArray(ctx, 1);
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithStringBuffer(ctx, "shard_id", strlen("shard_id"));
        char myId[REDISMODULE_NODE_ID_LEN];
        if(Cluster_IsClusterMode()){
            memcpy(myId, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN);
        }else{
            memset(myId, '0', REDISMODULE_NODE_ID_LEN);
        }
        RedisModule_ReplyWithStringBuffer(ctx, myId, REDISMODULE_NODE_ID_LEN);
        RedisModule_ReplyWithStringBuffer(ctx, "execution_plan", strlen("execution_plan"));
        RedisModule_ReplyWithArray(ctx, 16);
        RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
        RedisModule_ReplyWithStringBuffer(ctx, statusesNames[ep->status], strlen(statusesNames[ep->status]));
        RedisModule_ReplyWithStringBuffer(ctx, "shards_received", strlen("shards_received"));
        RedisModule_ReplyWithLongLong(ctx, ep->totalShardsRecieved);
        RedisModule_ReplyWithStringBuffer(ctx, "shards_completed", strlen("shards_completed"));
        RedisModule_ReplyWithLongLong(ctx, ep->totalShardsCompleted);
        RedisModule_ReplyWithStringBuffer(ctx, "results", strlen("results"));
        // TODO: once results and errors are linked lists we can provide more insight here
        if(RedisGears_IsDone(ep)){
            RedisModule_ReplyWithLongLong(ctx, RedisGears_GetRecordsLen(ep));
        }else{
            RedisModule_ReplyWithLongLong(ctx, -1);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "errors", strlen("errors"));
        if(RedisGears_IsDone(ep)){
            long long errorsLen = RedisGears_GetErrorsLen(ep);
            RedisModule_ReplyWithArray(ctx,errorsLen);
            for(long long i = 0; i < errorsLen; i++){
                Record* error = RedisGears_GetError(ep, i);
                size_t errorStrLen;
                char* errorStr = RedisGears_StringRecordGet(error, &errorStrLen);
                RedisModule_ReplyWithStringBuffer(ctx, errorStr, errorStrLen);
            }
        }else{
            RedisModule_ReplyWithArray(ctx, 0);
        }

        RedisModule_ReplyWithStringBuffer(ctx, "total_duration", strlen("total_duration"));
        RedisModule_ReplyWithLongLong(ctx, DURATION2MS(FlatExecutionPlan_GetExecutionDuration(ep)));
        RedisModule_ReplyWithStringBuffer(ctx, "read_duration", strlen("read_duration"));
        RedisModule_ReplyWithLongLong(ctx, DURATION2MS(FlatExecutionPlan_GetReadDuration(ep)));

        uint32_t fstepsLen = array_len(ep->fep->steps);
        RedisModule_ReplyWithStringBuffer(ctx, "steps", strlen("steps"));
        RedisModule_ReplyWithArray(ctx, fstepsLen);
        for(size_t i = 0; i < fstepsLen; i++){
            ExecutionStep *step = ep->steps[i];
            FlatExecutionStep fstep = ep->fep->steps[fstepsLen - i - 1];
            RedisModule_ReplyWithArray(ctx, 8);
            RedisModule_ReplyWithStringBuffer(ctx, "type", strlen("type"));
            RedisModule_ReplyWithStringBuffer(ctx, stepsNames[step->type], strlen(stepsNames[step->type]));
            RedisModule_ReplyWithStringBuffer(ctx, "duration", strlen("duration"));
            RedisModule_ReplyWithLongLong(ctx, DURATION2MS(step->executionDuration));
            RedisModule_ReplyWithStringBuffer(ctx, "name", strlen("name"));
            RedisModule_ReplyWithStringBuffer(ctx, fstep.bStep.stepName, strlen(fstep.bStep.stepName));
            RedisModule_ReplyWithStringBuffer(ctx, "arg", strlen("arg"));
            ExecutionStepArg arg = fstep.bStep.arg;
            if(arg.stepArg){
                ArgType* type = arg.type;
                if(type && type->tostring){
                    char* argCstr = type->tostring(arg.stepArg);
                    RedisModule_ReplyWithStringBuffer(ctx, argCstr, strlen(argCstr));
                    RG_FREE(argCstr);
                }else{
                    RedisModule_ReplyWithStringBuffer(ctx, "", strlen(""));
                }
            }else{
                RedisModule_ReplyWithStringBuffer(ctx, "", strlen(""));
            }
        }
    }
	return REDISMODULE_OK;
}


long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep){
	return ep->executionDuration;
}

long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep){
	return ep->steps[array_len(ep->steps) - 1]->executionDuration;
}

void ExecutionPlan_Clean() {
    // remove all registrations
    Gears_dictIterator* iter = Gears_dictGetIterator(Readerdict);
    Gears_dictEntry *curr = NULL;
    while((curr = Gears_dictNext(iter))){
        MgmtDataHolder* holder = Gears_dictGetVal(curr);
        RedisGears_ReaderCallbacks* callbacks = holder->callback;
        if(!callbacks->clear){
            continue;
        }
        callbacks->clear();
    }
    Gears_dictReleaseIterator(iter);

    // free all executions
    ExecutionPlan** epToFree = array_new(ExecutionPlan*, 10);

    Gears_dictIterator* it = Gears_dictGetIterator(epData.epDict);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(it))) {
        ExecutionPlan* ep = Gears_dictGetVal(entry);
        epToFree = array_append(epToFree, ep);
    }
    Gears_dictReleaseIterator(it);

    for(size_t i = 0 ; i < array_len(epToFree) ; ++i){
        ExecutionPlan_Free(epToFree[i]);
    }

    array_free(epToFree);
}

