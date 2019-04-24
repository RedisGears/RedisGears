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

#define INIT_TIMER  struct timespec _ts = {0}, _te = {0};
#define GETTIME(t)  clock_gettime(CLOCK_REALTIME, t);
#define START_TIMER if(GearsConfig_GetProfileExecutions()) GETTIME(&_ts);
#define STOP_TIMER  if(GearsConfig_GetProfileExecutions()) GETTIME(&_te);
#define DURATION    ((long long)1000000000 * (_te.tv_sec - _ts.tv_sec) \
                    + (_te.tv_nsec - _ts.tv_nsec))
#define ADD_DURATION(d) if(GearsConfig_GetProfileExecutions()){ \
                            STOP_TIMER; \
                            d += DURATION; \
                        }
#define DURATION2MS(d)  (long long)(d/(long long)1000000)

char* stepsNames[] = {
#define X(a, b) b,
    STEP_TYPES
#undef X
};

char* statusesNames[] = {
#define X(a, b) b,
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

static void LimitArgSerialize(void* arg, Gears_BufferWriter* bw){
    LimitExecutionStepArg* limitArg = arg;
    RedisGears_BWWriteLong(bw, limitArg->offset);
    RedisGears_BWWriteLong(bw, limitArg->len);
}

static void* LimitArgDeserialize(Gears_BufferReader* br){
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
    Gears_dict* epDict;
    Gears_list* epList;
    pthread_mutex_t mutex;
    WorkerData** workers;
}ExecutionPlansData;

ExecutionPlansData epData;

static long long lastId = 0;

static Record* ExecutionPlan_NextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx);
static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep, char* eid, void* arg);
static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader);
static void ExecutionPlan_RegisterForRun(ExecutionPlan* ep);
static ReaderStep ExecutionPlan_NewReader(FlatExecutionReader* reader, void* arg);
static void ExecutionPlan_NotifyReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);
static void ExecutionPlan_NotifyRun(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len);

static uint64_t idHashFunction(const void *key){
    return Gears_dictGenHashFunction(key, EXECUTION_PLAN_ID_LEN);
}

static int idKeyCompare(void *privdata, const void *key1, const void *key2){
    return memcmp(key1, key2, EXECUTION_PLAN_ID_LEN) == 0;
}

static void idKeyDestructor(void *privdata, void *key){
    RG_FREE(key);
}

static void* idKeyDup(void *privdata, const void *key){
	char* ret = RG_ALLOC(EXECUTION_PLAN_ID_LEN);
	memcpy(ret, key , EXECUTION_PLAN_ID_LEN);
    return ret;
}

Gears_dictType dictTypeHeapIds = {
        .hashFunction = idHashFunction,
        .keyDup = idKeyDup,
        .valDup = NULL,
        .keyCompare = idKeyCompare,
        .keyDestructor = idKeyDestructor,
        .valDestructor = NULL,
};

typedef enum MsgType{
    RUN_MSG, ADD_RECORD_MSG, SHARD_COMPLETED_MSG, EXECUTION_DONE, EXECUTION_FREE
}MsgType;

typedef struct RunWorkerMsg{
	ExecutionPlan* ep;
}RunWorkerMsg;

typedef struct ExecutionDoneMsg{
    ExecutionPlan* ep;
}ExecutionDoneMsg;

typedef struct ExecutionFreeMsg{
    ExecutionPlan* ep;
}ExecutionFreeMsg;

typedef struct ShardCompletedWorkerMsg{
	ExecutionPlan* ep;
	size_t stepId;
	enum StepType stepType;
}ShardCompletedWorkerMsg;

typedef struct AddRecordWorkerMsg{
	ExecutionPlan* ep;
	Record* record;
	size_t stepId;
	enum StepType stepType;
}AddRecordWorkerMsg;

typedef struct WorkerMsg{
    union{
    	RunWorkerMsg runWM;
    	AddRecordWorkerMsg addRecordWM;
    	ShardCompletedWorkerMsg shardCompletedWM;
    	ExecutionDoneMsg executionDone;
    	ExecutionFreeMsg executionFree;
    };
    MsgType type;
}WorkerMsg;

static void ExectuionPlan_WorkerMsgSend(WorkerData* wd, WorkerMsg* msg){
	write(wd->notifyPipe[1], &msg, sizeof(WorkerMsg*));
}

static void ExectuionPlan_WorkerMsgFree(WorkerMsg* msg){
	RG_FREE(msg);
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateRun(ExecutionPlan* ep){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = RUN_MSG;
	ret->runWM.ep = ep;
	return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateDone(ExecutionPlan* ep){
    WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
    ret->type = EXECUTION_DONE;
    ret->executionDone.ep = ep;
    return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateFree(ExecutionPlan* ep){
    WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
    ret->type = EXECUTION_FREE;
    ret->executionFree.ep = ep;
    return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateAddRecord(ExecutionPlan* ep, size_t stepId, Record* r, enum StepType stepType){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = ADD_RECORD_MSG;
	ret->addRecordWM.ep = ep;
	ret->addRecordWM.record = r;
	ret->addRecordWM.stepId = stepId;
	ret->addRecordWM.stepType = stepType;
	return ret;
}

static WorkerMsg* ExectuionPlan_WorkerMsgCreateShardCompleted(ExecutionPlan* ep, size_t stepId, enum StepType stepType){
	WorkerMsg* ret = RG_ALLOC(sizeof(WorkerMsg));
	ret->type = SHARD_COMPLETED_MSG;
	ret->shardCompletedWM.ep = ep;
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
    pthread_mutex_lock(&epData.mutex);
    ExecutionPlan* ep = Gears_dictFetchValue(epData.epDict, id);
    pthread_mutex_unlock(&epData.mutex);
    return ep;
}

ExecutionPlan* ExecutionPlan_FindByStrId(const char* id){
    char realId[EXECUTION_PLAN_ID_LEN] = {0};
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

static void FlatExecutionPlan_SerializeReader(FlatExecutionReader* rfep, Gears_BufferWriter* bw){
    RedisGears_BWWriteString(bw, rfep->reader);
}

static void FlatExecutionPlan_SerializeStep(FlatExecutionStep* step, Gears_BufferWriter* bw){
    RedisGears_BWWriteLong(bw, step->type);
    RedisGears_BWWriteString(bw, step->bStep.stepName);
    ArgType* type = step->bStep.arg.type;
    if(type && type->serialize){
        type->serialize(step->bStep.arg.stepArg, bw);
    }
}

static void FlatExecutionPlan_Serialize(FlatExecutionPlan* fep, Gears_BufferWriter* bw){
    FlatExecutionPlan_SerializeReader(fep->reader, bw);
    RedisGears_BWWriteLong(bw, array_len(fep->steps));
    for(int i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* step = fep->steps + i;
        FlatExecutionPlan_SerializeStep(step, bw);
    }
}

static FlatExecutionReader* FlatExecutionPlan_DeserializeReader(Gears_BufferReader* br){
    char* readerName = RedisGears_BRReadString(br);
    FlatExecutionReader* reader = FlatExecutionPlan_NewReader(readerName);
    return reader;
}

static FlatExecutionStep FlatExecutionPlan_DeserializeStep(Gears_BufferReader* br){
    FlatExecutionStep step;
    step.type = RedisGears_BRReadLong(br);
    step.bStep.stepName = RG_STRDUP(RedisGears_BRReadString(br));
    step.bStep.arg.stepArg = NULL;
    step.bStep.arg.type = FlatExecutionPlan_GetArgTypeByStepType(step.type, step.bStep.stepName);
    if(step.bStep.arg.type && step.bStep.arg.type->deserialize){
        step.bStep.arg.stepArg = step.bStep.arg.type->deserialize(br);
    }
    return step;
}

static FlatExecutionPlan* FlatExecutionPlan_Deserialize(Gears_BufferReader* br){
    FlatExecutionPlan* ret = FlatExecutionPlan_New();
    ret->reader = FlatExecutionPlan_DeserializeReader(br);
    long numberOfSteps = RedisGears_BRReadLong(br);
    for(int i = 0 ; i < numberOfSteps ; ++i){
        ret->steps = array_append(ret->steps, FlatExecutionPlan_DeserializeStep(br));
    }
    return ret;
}

static void ExecutionPlan_SendRunRequest(ExecutionPlan* ep){
	Cluster_SendMsgM(NULL, ExecutionPlan_NotifyRun, ep->id, EXECUTION_PLAN_ID_LEN);
}

static void ExecutionPlan_SendRecievedNotification(ExecutionPlan* ep){
	Cluster_SendMsgM(ep->id, ExecutionPlan_NotifyReceived, ep->id, EXECUTION_PLAN_ID_LEN);
}

static void ExecutionPlan_Distribute(ExecutionPlan* ep){
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    FlatExecutionPlan_Serialize(ep->fep, &bw);
    RedisGears_BWWriteBuffer(&bw, ep->id, EXECUTION_PLAN_ID_LEN); // serialize execution id
    ExecutionStep* readerStep = ep->steps[array_len(ep->steps) - 1];
    readerStep->reader.r->serialize(readerStep->reader.r->ctx, &bw);
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
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
            return record;
        }
	    START_TIMER;
        char* err = NULL;
        bool filterRes = step->filter.filter(rctx, record, step->filter.stepArg.stepArg, &err);
        if(err){
            RedisGears_FreeRecord(record);
            record = RG_ErrorRecordCreate(err, strlen(err) + 1);
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
    if(RedisGears_RecordGetType(record) == ERROR_RECORD){
        goto end;
    }
    if(record != NULL){
        char* err = NULL;
        record = step->map.map(rctx, record, step->map.stepArg.stepArg, &err);
        if(err){
            if(record){
                RedisGears_FreeRecord(record);
            }
            record = RG_ErrorRecordCreate(err, strlen(err) + 1);
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
        if(RedisGears_RecordGetType(r) == ERROR_RECORD){
            goto end;
        }
        if(RedisGears_RecordGetType(r) != LIST_RECORD){
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
    if(RedisGears_RecordGetType(record) == ERROR_RECORD){
        r = record;
        goto end;
    }
    char* err = NULL;
    char* buff = step->extractKey.extractor(rctx, record, step->extractKey.extractorArg.stepArg, &buffLen, &err);
    if(err){
        RedisGears_FreeRecord(record);
        r = RG_ErrorRecordCreate(err, strlen(err) + 1);
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
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
            goto end;
        }
        assert(RedisGears_RecordGetType(record) == KEY_RECORD);
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
    if(RedisGears_RecordGetType(record) == ERROR_RECORD){
        goto end;
    }
    assert(RedisGears_RecordGetType(record) == KEY_RECORD);
    size_t keyLen;
    char* key = RedisGears_KeyRecordGetKey(record, &keyLen);
    char* err = NULL;
    Record* r = step->reduce.reducer(rctx, key, keyLen, RedisGears_KeyRecordGetVal(record), step->reduce.reducerArg.stepArg, &err);
    RedisGears_KeyRecordSetVal(record, r);
    if(err){
        RedisGears_FreeRecord(record);
        record = RG_ErrorRecordCreate(err, strlen(err) + 1);
    }
end:
	ADD_DURATION(step->executionDuration);
    return record;
}

static Record* ExecutionPlan_RepartitionNextRecord(ExecutionPlan* ep, ExecutionStep* step, RedisModuleCtx* rctx){
    Record* record = NULL;
    Gears_Buffer* buff;
    Gears_BufferWriter bw;

    if(!Cluster_IsClusterMode()){
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
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
            // this is an error record which should stay with us so lets return it
            Gears_BufferFree(buff);
            goto end;
        }
        size_t len;
        char* key = RedisGears_KeyRecordGetKey(record, &len);
        char* shardIdToSendRecord = Cluster_GetNodeIdByKey(key);
        if(memcmp(shardIdToSendRecord, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0){
            // this record should stay with us, lets return it.
        	Gears_BufferFree(buff);
            goto end;
        }
        else{
            // we need to send the record to another shard
            Gears_BufferWriterInit(&bw, buff);
            RedisGears_BWWriteBuffer(&bw, ep->id, EXECUTION_PLAN_ID_LEN); // serialize execution plan id
            RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
            RG_SerializeRecord(&bw, record);
            RedisGears_FreeRecord(record);

            LockHandler_Acquire(rctx);
            Cluster_SendMsgM(shardIdToSendRecord, ExecutionPlan_OnRepartitionRecordReceived, buff->buff, buff->size);
            LockHandler_Release(rctx);

            Gears_BufferClear(buff);
        }
    	ADD_DURATION(step->executionDuration);
    }

    START_TIMER;
    Gears_BufferWriterInit(&bw, buff);
    RedisGears_BWWriteBuffer(&bw, ep->id, EXECUTION_PLAN_ID_LEN); // serialize execution plan id
    RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id

    LockHandler_Acquire(rctx);
    Cluster_SendMsgM(NULL, ExecutionPlan_DoneRepartition, buff->buff, buff->size);
    LockHandler_Release(rctx);

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

	if(!Cluster_IsClusterMode()){
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
			Gears_BufferWriterInit(&bw, buff);
			RedisGears_BWWriteBuffer(&bw, ep->id, EXECUTION_PLAN_ID_LEN); // serialize execution plan id
			RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id
			RG_SerializeRecord(&bw, record);
			RedisGears_FreeRecord(record);

			LockHandler_Acquire(rctx);
			Cluster_SendMsgM(ep->id, ExecutionPlan_CollectOnRecordReceived, buff->buff, buff->size);
			LockHandler_Release(rctx);

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
		RedisGears_BWWriteBuffer(&bw, ep->id, EXECUTION_PLAN_ID_LEN); // serialize execution plan id
		RedisGears_BWWriteLong(&bw, step->stepId); // serialize step id

		LockHandler_Acquire(rctx);
		Cluster_SendMsgM(ep->id, ExecutionPlan_CollectDoneSendingRecords, buff->buff, buff->size);
		LockHandler_Release(rctx);
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
    if(RedisGears_RecordGetType(record) == ERROR_RECORD){
        goto end;
    }
    char* err = NULL;
    step->forEach.forEach(rctx, record, step->forEach.stepArg.stepArg, &err);
    if(err){
        RedisGears_FreeRecord(record);
        record = RG_ErrorRecordCreate(err, strlen(err) + 1);
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
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
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
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
            goto end;
        }
        char* err = NULL;
        step->accumulate.accumulator = step->accumulate.accumulate(rctx, step->accumulate.accumulator, record, step->accumulate.stepArg.stepArg, &err);
        if(err){
            if(step->accumulate.accumulator){
                RedisGears_FreeRecord(step->accumulate.accumulator);
            }
            record = RG_ErrorRecordCreate(err, strlen(err) + 1);
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
		if(RedisGears_RecordGetType(record) == ERROR_RECORD){
		    goto end;
		}
		assert(RedisGears_RecordGetType(record) == KEY_RECORD);
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
		char* err = NULL;
		accumulator = step->accumulateByKey.accumulate(rctx, key, accumulator, val, step->accumulate.stepArg.stepArg, &err);
		if(err){
		    if(accumulator){
		        RedisGears_FreeRecord(accumulator);
		    }
			RedisGears_FreeRecord(record);
            record = RG_ErrorRecordCreate(err, strlen(err) + 1);
            goto end;
		}
		if(!keyRecord){
			keyRecord = RedisGears_KeyRecordCreate();
			RedisGears_KeyRecordSetKey(keyRecord, RG_STRDUP(key), strlen(key));
			assert(Gears_dictFetchValue(step->accumulateByKey.accumulators, key) == NULL);
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
	assert(RedisGears_RecordGetType(record) == KEY_RECORD);
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
    	    r = step->reader.r->next(rctx, step->reader.r->ctx);
            GETTIME(&_te);
    	    step->executionDuration += DURATION;
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
        assert(false);
        return NULL;
    }
    return r;
}

static void ExecutionPlan_WriteResult(ExecutionPlan* ep, RedisModuleCtx* rctx, Record* record){
    LockHandler_Acquire(rctx);
    ep->results = array_append(ep->results, record);
    LockHandler_Release(rctx);
}

static void ExecutionPlan_WriteError(ExecutionPlan* ep, RedisModuleCtx* rctx, Record* record){
    LockHandler_Acquire(rctx);
    ep->errors = array_append(ep->errors, record);
    LockHandler_Release(rctx);
}

static bool ExecutionPlan_Execute(ExecutionPlan* ep, RedisModuleCtx* rctx){
    Record* record = NULL;

    while((record = ExecutionPlan_NextRecord(ep, ep->steps[0], rctx))){
        if(record == &StopRecord){
            // Execution need to be stopped, lets wait for a while.
            return false;
        }
        if(RedisGears_RecordGetType(record) == ERROR_RECORD){
            ExecutionPlan_WriteError(ep, rctx, record);
        }else{
            ExecutionPlan_WriteResult(ep, rctx, record);
        }
    }

    return true;
}

static void ExecutionPlan_DoExecutionDoneActions(ExecutionPlan* ep, RedisModuleCtx* rctx){
    LockHandler_Acquire(rctx);
    ep->status = DONE;
    FreePrivateData freeC = ep->freeCallback;
    void* pd = ep->privateData;
    if(ep->callback){
        ep->callback(ep, ep->privateData);
    }
    if(freeC){
        freeC(pd);
    }
    LockHandler_Release(rctx);
}

static void ExecutionPlan_Main(ExecutionPlan* ep){
	assert(ep->status != DONE);
	ep->sentRunRequest = false;
	if(ep->status == WAITING_FOR_CLUSTER_TO_COMPLETE){
	    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
	    ExecutionPlan_DoExecutionDoneActions(ep, rctx);
        RedisModule_FreeThreadSafeContext(rctx);
	    return;
	}
	if(ep->status == CREATED){
		if(Cluster_IsClusterMode()){
			if(memcmp(ep->id, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) == 0){
				ExecutionPlan_Distribute(ep);
				ep->status = WAITING_FOR_RECIEVED_NOTIFICATION;
			}else{
				ExecutionPlan_SendRecievedNotification(ep);
				ep->status = WAITING_FOR_RUN_NOTIFICATION;
			}
			return;
		}
		ep->status = RUNNING;
	}

	if(ep->status == WAITING_FOR_RECIEVED_NOTIFICATION){
		ExecutionPlan_SendRunRequest(ep);
		ep->status = RUNNING;
	}

	if(ep->status == WAITING_FOR_RUN_NOTIFICATION){
		ep->status = RUNNING;
	}

    INIT_TIMER;
    GETTIME(&_ts);
	RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
	bool isDone = ExecutionPlan_Execute(ep, rctx);
    GETTIME(&_te);
	ep->executionDuration += DURATION;

	if(isDone){
	    if(Cluster_IsClusterMode()){
	        LockHandler_Acquire(rctx);
            Cluster_SendMsgM(NULL, ExecutionPlan_NotifyExecutionDone, ep->id, EXECUTION_PLAN_ID_LEN);
            LockHandler_Release(rctx);
            if((Cluster_GetSize() - 1) == ep->totalShardsCompleted){ // no need to wait to myself
                ExecutionPlan_DoExecutionDoneActions(ep, rctx);
            }else{
                ep->status = WAITING_FOR_CLUSTER_TO_COMPLETE;
            }
	    }else{
	        ExecutionPlan_DoExecutionDoneActions(ep, rctx);
	    }
	}
	RedisModule_FreeThreadSafeContext(rctx);
}

static void ExecutionPlan_RegisterForRun(ExecutionPlan* ep){
	if(ep->sentRunRequest){
		return;
	}
	WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateRun(ep);
	ep->sentRunRequest = true;
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void FlatExecutionPlan_RegisterKeySpaceEvent(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff = (Gears_Buffer){
        .buff = (char*)payload,
        .size = len,
        .cap = len,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br);
    if(!fep){
        // todo: big big warning
        return;
    }
    ReaderStep rs = ExecutionPlan_NewReader(fep->reader, NULL);
    assert(rs.r->registerTrigger);
    rs.r->registerTrigger(fep, RG_STRDUP(RedisGears_BRReadString(&br)));
    if(rs.r->free){
        rs.r->free(rs.r->ctx);
    }
    RG_FREE(rs.r);
}

static ExecutionPlan* FlatExecutionPlan_CreateExecution(FlatExecutionPlan* fep, char* eid, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData){
    ExecutionPlan* ep = ExecutionPlan_New(fep, eid, arg);
    if(!ep){
        return NULL;
    }
    ep->callback = callback;
    ep->privateData = privateData;
    return ep;
}

static int currAssignWorker = 0;

static void ExecutionPlan_Run(ExecutionPlan* ep){
    pthread_mutex_lock(&epData.mutex);
	WorkerData* wd = epData.workers[currAssignWorker];
	currAssignWorker = (currAssignWorker + 1) % array_len(epData.workers);
	pthread_mutex_unlock(&epData.mutex);
	ep->assignWorker = wd;
    ExecutionPlan_RegisterForRun(ep);
}

static ExecutionPlan* FlatExecutionPlan_RunOnly(FlatExecutionPlan* fep, char* eid, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData){
    ExecutionPlan* ep = FlatExecutionPlan_CreateExecution(fep, eid, arg, callback, privateData);
    ExecutionPlan_Run(ep);
    return ep;
}

static void ExecutionPlan_NotifyReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = ExecutionPlan_FindById(payload);
	assert(ep);
	++ep->totalShardsRecieved;
	if((Cluster_GetSize() - 1) == ep->totalShardsRecieved){ // no need to wait to myself
	    ExecutionPlan_RegisterForRun(ep);
	}
}

static void ExecutionPlan_NotifyRun(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = ExecutionPlan_FindById(payload);
	assert(ep);
	ExecutionPlan_RegisterForRun(ep);
}

static void ExecutionPlan_OnReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Gears_Buffer buff = (Gears_Buffer){
        .buff = (char*)payload,
        .size = len,
        .cap = len,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br);
    size_t idLen;
    char* eid = RedisGears_BRReadBuffer(&br, &idLen);
    assert(idLen == EXECUTION_PLAN_ID_LEN);
    ExecutionPlan* ep = FlatExecutionPlan_CreateExecution(fep, eid, NULL, NULL, NULL);
    ExecutionStep* rs = ep->steps[array_len(ep->steps) - 1];
    rs->reader.r->deserialize(rs->reader.r->ctx, &br);
    FlatExecutionPlan_Free(fep);
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
    size_t stepId = RedisGears_BRReadLong(&br);
    assert(epIdLen == EXECUTION_PLAN_ID_LEN);
    Record* r = RG_DeserializeRecord(&br);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
    assert(ep);
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
	size_t stepId = RedisGears_BRReadLong(&br);
	ExecutionPlan* ep = ExecutionPlan_FindById(epId);
	assert(ep);
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
    size_t stepId = RedisGears_BRReadLong(&br);
    assert(epIdLen == EXECUTION_PLAN_ID_LEN);
    Record* r = RG_DeserializeRecord(&br);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
    assert(ep);
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

static FlatExecutionPlan* FlatExecutionPlan_Duplicate(FlatExecutionPlan* fep){
    FlatExecutionPlan* ret = FlatExecutionPlan_New();
    bool res = FlatExecutionPlan_SetReader(ret, fep->reader->reader);
    assert(res);
    for(size_t i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* s = fep->steps + i;
        void* arg = NULL;
        if(s->bStep.arg.type){
            arg = s->bStep.arg.type->dup(s->bStep.arg.stepArg);
        }
        FlatExecutionPlan_AddBasicStep(ret, s->bStep.stepName, arg, s->type);
    }
    return ret;
}

static void ExecutionPlan_NotifyExecutionDone(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    ExecutionPlan* ep = ExecutionPlan_FindById(payload);
    assert(ep);
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
    size_t stepId = RedisGears_BRReadLong(&br);
    ExecutionPlan* ep = ExecutionPlan_FindById(epId);
	assert(ep);
	WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateShardCompleted(ep, stepId, REPARTITION);
	ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

static void ExecutionPlan_ExecutionDone(ExecutionPlan* ep){
    ep->totalShardsCompleted++;
    if((Cluster_GetSize() - 1) == ep->totalShardsCompleted){ // no need to wait to myself
        ExecutionPlan_Main(ep);
    }
}

static void ExecutionPlan_StepDone(ExecutionPlan* ep, size_t stepId, enum StepType stepType){
	size_t totalShardsCompleted;
	switch(stepType){
	case REPARTITION:
		assert(ep->steps[stepId]->type == REPARTITION);
		totalShardsCompleted = ++ep->steps[stepId]->repartion.totalShardsCompleted;
		break;
	case COLLECT:
		assert(ep->steps[stepId]->type == COLLECT);
		totalShardsCompleted = ++ep->steps[stepId]->collect.totalShardsCompleted;
		break;
	default:
		assert(false);
	}

	assert(Cluster_GetSize() - 1 >= totalShardsCompleted);
	if((Cluster_GetSize() - 1) == totalShardsCompleted){ // no need to wait to myself
	    ExecutionPlan_Main(ep);
	}
}

static void ExecutionPlan_AddStepRecord(ExecutionPlan* ep, size_t stepId, Record* r, enum StepType stepType){
#define MAX_PENDING_TO_START_RUNNING 10000
	Record*** pendings = NULL;
	switch(stepType){
	case REPARTITION:
		assert(ep->steps[stepId]->type == REPARTITION);
		pendings = &(ep->steps[stepId]->repartion.pendings);
		break;
	case COLLECT:
		assert(ep->steps[stepId]->type == COLLECT);
		pendings = &(ep->steps[stepId]->collect.pendings);
		break;
	default:
		assert(false);
	}
	*pendings = array_append(*pendings, r);
	if(array_len(*pendings) >= MAX_PENDING_TO_START_RUNNING){
	    ExecutionPlan_Main(ep);
	}
}

static void ExecutionPlan_MsgArrive(evutil_socket_t s, short what, void *arg){
	WorkerMsg* msg;
	read(s, &msg, sizeof(WorkerMsg*));
	switch(msg->type){
	case RUN_MSG:
		ExecutionPlan_Main(msg->runWM.ep);
		break;
	case ADD_RECORD_MSG:
		ExecutionPlan_AddStepRecord(msg->addRecordWM.ep, msg->addRecordWM.stepId, msg->addRecordWM.record, msg->addRecordWM.stepType);
		break;
	case SHARD_COMPLETED_MSG:
		ExecutionPlan_StepDone(msg->shardCompletedWM.ep, msg->shardCompletedWM.stepId, msg->shardCompletedWM.stepType);
		break;
	case EXECUTION_DONE:
	    ExecutionPlan_ExecutionDone(msg->executionDone.ep);
        break;
	case EXECUTION_FREE:
        ExecutionPlan_Free(msg->executionFree.ep, true);
        break;
	default:
		assert(false);
	}
	ExectuionPlan_WorkerMsgFree(msg);
}

static void* ExecutionPlan_MessageThreadMain(void *arg){
	WorkerData* wd = arg;
    event_base_loop(wd->eb, 0);
    return NULL;
}

static WorkerData* ExecutionPlan_StartThread(){
	WorkerData* wd = RG_ALLOC(sizeof(WorkerData));
    pipe(wd->notifyPipe);
    wd->eb = (struct event_base*)event_base_new();
    struct event *readEvent = event_new(wd->eb,
    									wd->notifyPipe[0],
                                        EV_READ | EV_PERSIST,
										ExecutionPlan_MsgArrive,
                                        NULL);
    event_base_set(wd->eb, readEvent);
    event_add(readEvent, 0);

    pthread_create(&wd->thread, NULL, ExecutionPlan_MessageThreadMain, wd);
    return wd;
}

void ExecutionPlan_Initialize(size_t numberOfworkers){
    epData.epDict = Gears_dictCreate(&dictTypeHeapIds, NULL);
    epData.epList = Gears_listCreate();
    pthread_mutex_init(&epData.mutex, NULL);
    epData.workers = array_new(WorkerData*, numberOfworkers);

    Cluster_RegisterMsgReceiverM(ExecutionPlan_OnReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyRun);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_CollectOnRecordReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_CollectDoneSendingRecords);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_OnRepartitionRecordReceived);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_DoneRepartition);
    Cluster_RegisterMsgReceiverM(ExecutionPlan_NotifyExecutionDone);
    Cluster_RegisterMsgReceiverM(FlatExecutionPlan_RegisterKeySpaceEvent);

    for(size_t i = 0 ; i < numberOfworkers ; ++i){
    	WorkerData* wd = ExecutionPlan_StartThread();
        epData.workers = array_append(epData.workers, wd);
    }
}

const char* FlatExecutionPlan_GetReader(FlatExecutionPlan* fep){
    return fep->reader->reader;
}

int FlatExecutionPlan_Register(FlatExecutionPlan* fep, char* key){
    ReaderStep rs = ExecutionPlan_NewReader(fep->reader, NULL);
    if(!rs.r->registerTrigger){
        return 0;
    }
    if(Cluster_IsClusterMode()){
        Gears_Buffer* buff = Gears_BufferCreate();
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        FlatExecutionPlan_Serialize(fep, &bw);
        RedisGears_BWWriteString(&bw, key);
        Cluster_SendMsgM(NULL, FlatExecutionPlan_RegisterKeySpaceEvent, buff->buff, buff->size);
        Gears_BufferFree(buff);
    }
    rs.r->registerTrigger(FlatExecutionPlan_Duplicate(fep), key);
    return 1;
}

ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, char* eid, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData){
    return FlatExecutionPlan_RunOnly(fep, eid, arg, callback, privateData);
}

static ReaderStep ExecutionPlan_NewReader(FlatExecutionReader* reader, void* arg){
    RedisGears_ReaderCallback callback = ReadersMgmt_Get(reader->reader);
    assert(callback); // todo: handle as error in future
    return (ReaderStep){.r = callback(arg)};
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
        assert(false);
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

static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep, char* finalId, void* arg){
    ExecutionPlan* ret = RG_ALLOC(sizeof(*ret));
    fep = FlatExecutionPlan_Duplicate(fep);
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
    ret->fep = fep;
    ret->totalShardsRecieved = 0;
    ret->totalShardsCompleted = 0;
    ret->results = array_new(Record*, 100);
    ret->errors = array_new(Record*, 1);
    ret->status = CREATED;
    ret->sentRunRequest = false;
    ret->callback = NULL;
    ret->privateData = NULL;
    ret->freeCallback = NULL;
    char generatedId[EXECUTION_PLAN_ID_LEN] = {0};
    if(!finalId){
        char noneClusterId[REDISMODULE_NODE_ID_LEN] = {0};
        char* id;
        if(Cluster_IsClusterMode()){
            id = Cluster_GetMyId();
        }else{
            memset(noneClusterId, '0', REDISMODULE_NODE_ID_LEN);
            id = noneClusterId;
        }
        memcpy(generatedId, id, REDISMODULE_NODE_ID_LEN);
        memcpy(generatedId + REDISMODULE_NODE_ID_LEN, &lastId, sizeof(long long));
        finalId = generatedId;
        ++lastId;
    }
    memcpy(ret->id, finalId, EXECUTION_PLAN_ID_LEN);
    snprintf(ret->idStr, EXECUTION_PLAN_STR_ID_LEN, "%.*s-%lld", REDISMODULE_NODE_ID_LEN, ret->id, *(long long*)&ret->id[REDISMODULE_NODE_ID_LEN]);
    pthread_mutex_lock(&epData.mutex);
    while (Gears_listLength(epData.epList) >= GearsConfig_GetMaxExecutions()) {
        Gears_listNode* n0 = Gears_listFirst(epData.epList);
        ExecutionPlan* ep0 = Gears_listNodeValue(n0);
        ExecutionPlan_Free(ep0, false);
    }
    Gears_listAddNodeTail(epData.epList, ret);
    Gears_dictAdd(epData.epDict, ret->id, ret);
    pthread_mutex_unlock(&epData.mutex);
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
        assert(false);
    }
    RG_FREE(es);
}

void ExecutionPlan_SendFreeMsg(ExecutionPlan* ep){
    WorkerMsg* msg = ExectuionPlan_WorkerMsgCreateFree(ep);
    ExectuionPlan_WorkerMsgSend(ep->assignWorker, msg);
}

void ExecutionPlan_Free(ExecutionPlan* ep, bool needLock){
    FlatExecutionPlan_Free(ep->fep);
    if (needLock) pthread_mutex_lock(&epData.mutex);
    Gears_listIter* it = Gears_listGetIterator(epData.epList, AL_START_TAIL);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(it))){
        ExecutionPlan* it_ep = Gears_listNodeValue(node);
		if (!memcmp(it_ep->id, ep->id, EXECUTION_PLAN_ID_LEN)) {
			Gears_listDelNode(epData.epList, node);
			break;
		}
    }
    Gears_listReleaseIterator(it);  
    Gears_dictDelete(epData.epDict, ep->id);
    if (needLock) pthread_mutex_unlock(&epData.mutex);

    ExecutionStep_Free(ep->steps[0]);
    array_free(ep->steps);

    for(int i = 0 ; i < array_len(ep->results) ; ++i){
        RedisGears_FreeRecord(ep->results[i]);
    }
    array_free(ep->results);

    for(int i = 0 ; i < array_len(ep->errors) ; ++i){
        RedisGears_FreeRecord(ep->errors[i]);
    }
    array_free(ep->errors);
    RG_FREE(ep);
}

static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader){
    FlatExecutionReader* res = RG_ALLOC(sizeof(*res));
    res->reader = RG_STRDUP(reader);
    return res;
}

FlatExecutionPlan* FlatExecutionPlan_New(){
#define STEPS_INITIAL_CAP 10
    FlatExecutionPlan* res = RG_ALLOC(sizeof(*res));
    res->reader = NULL;
    res->steps = array_new(FlatExecutionStep, STEPS_INITIAL_CAP);
    return res;
}

void FlatExecutionPlan_FreeArg(FlatExecutionStep* step){
    if (step->bStep.arg.type && step->bStep.arg.type->free){
        step->bStep.arg.type->free(step->bStep.arg.stepArg);
    }
}

void FlatExecutionPlan_Free(FlatExecutionPlan* fep){
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
    RG_FREE(fep);
}

bool FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader){
    RedisGears_ReaderCallback callback = ReadersMgmt_Get(reader);
    if(!callback){
        return false;
    }
    fep->reader = FlatExecutionPlan_NewReader(reader);
    return true;
}

void FlatExecutionPlan_AddForEachStep(FlatExecutionPlan* fep, char* forEach, void* writerArg){
    FlatExecutionPlan_AddBasicStep(fep, forEach, writerArg, FOREACH);
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

int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    pthread_mutex_lock(&epData.mutex);
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	size_t numOfEntries = 0;
    Gears_listIter* it = Gears_listGetIterator(epData.epList, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(it))) {
        ExecutionPlan* ep = Gears_listNodeValue(node);
		RedisModule_ReplyWithArray(ctx, 4);
		RedisModule_ReplyWithStringBuffer(ctx, "executionId", strlen("executionId"));
		RedisModule_ReplyWithStringBuffer(ctx, ep->idStr, strlen(ep->idStr));
		RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
        RedisModule_ReplyWithStringBuffer(ctx, statusesNames[ep->status], strlen(statusesNames[ep->status]));
		++numOfEntries;
    }
    Gears_listReleaseIterator(it);  
    pthread_mutex_unlock(&epData.mutex);
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

    // TODO: big potential issue if another thread releases the ep before we acquire the lock <- maybe just lock the eplist?
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
        RedisModule_Free(fargv[1]);
        RG_FREE(fargv);
        return res;
#endif
    }else{

        pthread_mutex_lock(&epData.mutex);
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
        if(ep->status == DONE){
            RedisModule_ReplyWithLongLong(ctx, RedisGears_GetRecordsLen(ep));
        }else{
            RedisModule_ReplyWithLongLong(ctx, -1);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "errors", strlen("errors"));
        if(ep->status == DONE){
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
        pthread_mutex_unlock(&epData.mutex);
    }
	return REDISMODULE_OK;
}


long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep){
	return ep->executionDuration;
}

long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep){
	return ep->steps[array_len(ep->steps) - 1]->executionDuration;
}

