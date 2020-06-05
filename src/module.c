/*
 * module.c
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */


#ifdef WITHPYTHON
#include "redisgears_python.h"
#endif
#include "redismodule.h"
#include "version.h"
#include "mgmt.h"
#include "execution_plan.h"
#include "example.h"
#include "cluster.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "utils/arr_rm_alloc.h"
#include "utils/buffer.h"
#include "record.h"
#include "commands.h"
#include "redisai.h"
#include "config.h"
#include "globals.h"
#include "readers/keys_reader.h"
#include "readers/streams_reader.h"
#include "readers/command_reader.h"
#include "readers/shardid_reader.h"
#include "mappers.h"
#include <stdbool.h>
#include <unistd.h>
#include "lock_handler.h"

#ifndef REDISGEARS_GIT_SHA
#define REDISGEARS_GIT_SHA "unknown"
#endif

#ifndef REDISGEARS_OS_VERSION
#define REDISGEARS_OS_VERSION "unknown"
#endif

#define REGISTER_API(name, ctx) \
    do{\
        RedisGears_ ## name = RG_ ## name;\
        if(RedisModule_ExportSharedAPI){\
            if(RedisModule_ExportSharedAPI(ctx, "RedisGears_" #name, RG_ ## name) != REDISMODULE_OK){\
                printf("could not register RedisGears_" #name "\r\n");\
                return REDISMODULE_ERR;\
            }\
        }\
    } while(0)

static int RG_GetLLApiVersion(){
    return REDISGEARS_LLAPI_VERSION;
}

static int RG_RegisterReader(char* name, RedisGears_ReaderCallbacks* reader){
    return ReadersMgmt_Add(name, reader, NULL);
}

static int RG_RegisterForEach(char* name, RedisGears_ForEachCallback writer, ArgType* type){
    return ForEachsMgmt_Add(name, writer, type);
}

static int RG_RegisterFlatExecutionPrivateDataType(ArgType* type){
    return FepPrivateDatasMgmt_Add(type->type, NULL, type);
}

static int RG_RegisterMap(char* name, RedisGears_MapCallback map, ArgType* type){
    return MapsMgmt_Add(name, map, type);
}

static int RG_RegisterAccumulator(char* name, RedisGears_AccumulateCallback accumulator, ArgType* type){
    return AccumulatesMgmt_Add(name, accumulator, type);
}

static int RG_RegisterAccumulatorByKey(char* name, RedisGears_AccumulateByKeyCallback accumulator, ArgType* type){
	return AccumulateByKeysMgmt_Add(name, accumulator, type);
}

static int RG_RegisterFilter(char* name, RedisGears_FilterCallback filter, ArgType* type){
    return FiltersMgmt_Add(name, filter, type);
}

static int RG_RegisterGroupByExtractor(char* name, RedisGears_ExtractorCallback extractor, ArgType* type){
    return ExtractorsMgmt_Add(name, extractor, type);
}

static int RG_RegisterReducer(char* name, RedisGears_ReducerCallback reducer, ArgType* type){
    return ReducersMgmt_Add(name, reducer, type);
}

static int RG_RegisterExecutionOnStartCallback(char* name, RedisGears_ExecutionOnStartCallback callback, ArgType* type){
    return ExecutionOnStartsMgmt_Add(name, callback, type);
}

static int RG_RegisterExecutionOnUnpausedCallback(char* name, RedisGears_ExecutionOnUnpausedCallback callback, ArgType* type){
    return ExecutionOnUnpausedsMgmt_Add(name, callback, type);
}

static int RG_RegisterFlatExecutionOnRegisteredCallback(char* name, RedisGears_FlatExecutionOnRegisteredCallback callback, ArgType* type){
    return FlatExecutionOnRegisteredsMgmt_Add(name, callback, type);
}

static int RG_SetFlatExecutionOnStartCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_ExecutionOnStartCallback c = ExecutionOnStartsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnStartStep(fep, RG_STRDUP(callback), arg);
    return REDISMODULE_OK;
}

static int RG_SetFlatExecutionOnUnpausedCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_ExecutionOnStartCallback c = ExecutionOnUnpausedsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnUnPausedStep(fep, RG_STRDUP(callback), arg);
    return REDISMODULE_OK;
}

static int RG_SetFlatExecutionOnRegisteredCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_FlatExecutionOnRegisteredCallback c = FlatExecutionOnRegisteredsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnRegisteredStep(fep, RG_STRDUP(callback), arg);
    return REDISMODULE_OK;
}

static FlatExecutionPlan* RG_CreateCtx(char* readerName){
    FlatExecutionPlan* fep = FlatExecutionPlan_New();
    if(!FlatExecutionPlan_SetReader(fep, readerName)){
        FlatExecutionPlan_Free(fep);
        return NULL;
    }
    return fep;
}

static int RG_SetDesc(FlatExecutionPlan* fep, const char* desc){
    FlatExecutionPlan_SetDesc(fep, desc);
    return 1;
}

static void RG_SetMaxIdleTime(FlatExecutionPlan* fep, long long executionMaxIdleTime){
    fep->executionMaxIdleTime = executionMaxIdleTime;
}

static void RG_SetFlatExecutionPrivateData(FlatExecutionPlan* fep, const char* type, void* PD){
    FlatExecutionPlan_SetPrivateData(fep, type, PD);
}

static void* RG_GetFlatExecutionPrivateDataFromFep(FlatExecutionPlan* fep){
    return FlatExecutionPlan_GetPrivateData(fep);
}

static int RG_Map(FlatExecutionPlan* fep, char* name, void* arg){
    FlatExecutionPlan_AddMapStep(fep, name, arg);
    return 1;
}

int RG_FlatMap(FlatExecutionPlan* fep, char* name, void* arg){
    FlatExecutionPlan_AddFlatMapStep(fep, name, arg);
    return 1;
}

static int RG_Filter(FlatExecutionPlan* fep, char* name, void* arg){
    FlatExecutionPlan_AddFilterStep(fep, name, arg);
    return 1;
}

static int RG_GroupBy(FlatExecutionPlan* fep, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg){
    FlatExecutionPlan_AddGroupByStep(fep, extraxtorName, extractorArg, reducerName, reducerArg);
    return 1;
}

static int RG_AccumulateBy(FlatExecutionPlan* fep, char* extraxtorName, void* extractorArg, char* accumulatorName, void* accumulatorArg){
	FlatExecutionPlan_AddAccumulateByKeyStep(fep, extraxtorName, extractorArg, accumulatorName, accumulatorArg);
	return 1;
}

static int RG_LocalAccumulateBy(FlatExecutionPlan* fep, char* extraxtorName, void* extractorArg, char* accumulatorName, void* accumulatorArg){
    FlatExecutionPlan_AddLocalAccumulateByKeyStep(fep, extraxtorName, extractorArg, accumulatorName, accumulatorArg);
    return 1;
}

static int RG_Collect(FlatExecutionPlan* fep){
    FlatExecutionPlan_AddCollectStep(fep);
	return 1;
}

static int RG_Repartition(FlatExecutionPlan* fep, char* extraxtorName, void* extractorArg){
    FlatExecutionPlan_AddRepartitionStep(fep, extraxtorName, extractorArg);
    return 1;
}

static int RG_ForEach(FlatExecutionPlan* fep, char* name, void* arg){
    FlatExecutionPlan_AddForEachStep(fep, name, arg);
    return 1;
}

static int RG_Accumulate(FlatExecutionPlan* fep, char* name, void* arg){
    FlatExecutionPlan_AddAccumulateStep(fep, name, arg);
    return 1;
}

static int RG_Limit(FlatExecutionPlan* fep, size_t offset, size_t len){
    FlatExecutionPlan_AddLimitStep(fep, offset, len);
    return 1;
}

static int RG_Register(FlatExecutionPlan* fep, ExecutionMode mode, void* key, char** err){

    return FlatExecutionPlan_Register(fep, mode, key, err);
}

static ExecutionPlan* RG_Run(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err){
    return FlatExecutionPlan_Run(fep, mode, arg, callback, privateData, worker, err);
}

static const char* RG_GetReader(FlatExecutionPlan* fep){
    return FlatExecutionPlan_GetReader(fep);
}

static StreamReaderCtx* RG_StreamReaderCtxCreate(const char* streamName, const char* streamId){
    return StreamReaderCtx_Create(streamName, streamId);
}

static void RG_StreamReaderCtxFree(StreamReaderCtx* readerCtx){
    StreamReaderCtx_Free(readerCtx);
}

static KeysReaderCtx* RG_KeysReaderCtxCreate(const char* match, bool readValue, const char* event, bool noScan){
    return KeysReaderCtx_Create(match, readValue, event, noScan);
}

static void RG_KeysReaderCtxFree(KeysReaderCtx* readerCtx){
    KeysReaderCtx_Free(readerCtx);
}

static StreamReaderTriggerArgs* RG_StreamReaderTriggerArgsCreate(const char* prefix, size_t batchSize, size_t durationMS, OnFailedPolicy onFailedPolicy, size_t retryInterval, bool trimStream){
    return StreamReaderTriggerArgs_Create(prefix, batchSize, durationMS, onFailedPolicy, retryInterval, trimStream);
}

static void RG_StreamReaderTriggerArgsFree(StreamReaderTriggerArgs* args){
    return StreamReaderTriggerArgs_Free(args);
}

static KeysReaderTriggerArgs* RG_KeysReaderTriggerArgsCreate(const char* prefix, char** eventTypes, int* keyTypes, bool readValue){
    return KeysReaderTriggerArgs_Create(prefix, eventTypes, keyTypes, readValue);
}

static CommandReaderTriggerArgs* RG_CommandReaderTriggerArgsCreate(const char* trigger){
    return CommandReaderTriggerArgs_Create(trigger);
}
static void RG_CommandReaderTriggerArgsFree(CommandReaderTriggerArgs* args){
    CommandReaderTriggerArgs_Free(args);
}

static void RG_KeysReaderTriggerArgsFree(KeysReaderTriggerArgs* args){
    return KeysReaderTriggerArgs_Free(args);
}

static void RG_FreeFlatExecution(FlatExecutionPlan* fep){
    FlatExecutionPlan_Free(fep);
}

static bool RG_IsDone(ExecutionPlan* ep){
	return EPIsFlagOn(ep, EFDone);
}

static const char* RG_GetId(ExecutionPlan* ep){
    return ep->idStr;
}

static long long RG_GetRecordsLen(ExecutionPlan* ep){
    // TODO: move results and errors to linked lists for partial parallelism w/o locking
    RedisModule_Assert(ep && RedisGears_IsDone(ep));
	return array_len(ep->results);
}

static long long RG_GetErrorsLen(ExecutionPlan* ep){
    // TODO: move results and errors to linked lists for partial parallelism w/o locking
    RedisModule_Assert(ep && RedisGears_IsDone(ep));
	return array_len(ep->errors);
}

static bool RG_AddOnDoneCallback(ExecutionPlan* ep, RedisGears_OnExecutionDoneCallback callback, void* privateData){
    if(EPIsFlagOn(ep, EFDone)){
        return false;
    }
    OnDoneData onDoneData = {
            .callback = callback,
            .privateData = privateData,
    };
    ep->onDoneData = array_append(ep->onDoneData, onDoneData);
    return true;
}

static Record* RG_GetRecord(ExecutionPlan* ep, long long i){
    // TODO: move results and errors to linked lists for partial parallelism w/o locking
    RedisModule_Assert(ep && RedisGears_IsDone(ep));
    RedisModule_Assert(i >= 0 && i < array_len(ep->results));
	return ep->results[i];
}

static Record* RG_GetError(ExecutionPlan* ep, long long i){
    // TODO: move results and errors to linked lists for partial parallelism w/o locking
    RedisModule_Assert(ep && RedisGears_IsDone(ep));
    RedisModule_Assert(i >= 0 && i < array_len(ep->errors));
	return ep->errors[i];
}

/**
 * Abort a running or created (and not yet started) execution
 *
 * Currently we can only abort a local execution.
 * Aborting a global execution can only be done via RG.ABORTEXECUTION command
 *
 * return REDISMODULE_OK if the execution was aborted and REDISMODULE_ERR otherwise
 */
static int RG_AbortExecution(ExecutionPlan* ep){

    // exection did not yet started.
    if(EPIsFlagOff(ep, EFStarted)){
        if(EPIsFlagOff(ep, EFIsLocal) &&
                memcmp(ep->id, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN) != 0){
            // we did not created the execution,  which mean its already started
            // on some other node and can not be aborted
            return REDISMODULE_ERR;
        }
        // abort the execution and execute its Done Actions
        ep->status = ABORTED;
        EPStatus_DoneAction(ep);
        return REDISMODULE_OK;
    }

    // execution is not local and already started, we can not abort it now.
    if(EPIsFlagOff(ep, EFIsLocal)){
        return REDISMODULE_ERR;
    }

    // execution is done, no need to abort
    if(RedisGears_IsDone(ep)){
        return REDISMODULE_OK;
    }

    while(ep->status != DONE){
        // we are checking for DONE status cause this one is set without getting the lock.
        // Once status changed to DONE we know that no more python code will be executed and
        // we can finish sending cancel signal
#ifdef WITHPYTHON
        unsigned long threadID = (unsigned long)ep->executionPD;
        RedisGearsPy_ForceStop(threadID);
#endif
        usleep(1000);
    }

    return REDISMODULE_OK;
}

/**
 * Drop the execution once the execution is done
 */
static void RG_DropExecution(ExecutionPlan* ep){
    if(EPIsFlagOn(ep, EFIsOnDoneCallback)){
        EPTurnOnFlag(ep, EFIsFreedOnDoneCallback);
        return;
    }
    if(Cluster_IsClusterMode() && EPIsFlagOff(ep, EFIsLocal)){
        // We need to distributed the drop execution to all the shards
        // only if execution is not local and not aborted (aborted exectuion is eather
        // local or not yet distributed to all the shards)
        Cluster_SendMsgM(NULL, RG_OnDropExecutionMsgReceived, ep->idStr, strlen(ep->idStr));
    }
    ExecutionPlan_Free(ep);
}

static ExecutionPlan* RG_GetExecution(const char* id){
	ExecutionPlan* ep =	ExecutionPlan_FindByStrId(id);
	return ep;
}

static ArgType* RG_CreateType(char* name, int version, ArgFree free, ArgDuplicate dup, ArgSerialize serialize, ArgDeserialize deserialize, ArgToString tostring){
    ArgType* ret = RG_ALLOC(sizeof(*ret));
    *ret = (ArgType){
        .type = RG_STRDUP(name),
        .version = version,
        .free = free,
        .dup = dup,
        .serialize = serialize,
        .deserialize = deserialize,
        .tostring = tostring,
    };
    return ret;
}

static void RG_BWWriteLong(Gears_BufferWriter* bw, long val){
    Gears_BufferWriterWriteLong(bw, val);
}

static void RG_BWWriteString(Gears_BufferWriter* bw, const  char* str){
    Gears_BufferWriterWriteString(bw, str);
}

static void RG_BWWriteBuffer(Gears_BufferWriter* bw, const char* buff, size_t len){
    Gears_BufferWriterWriteBuff(bw, buff, len);
}

static long RG_BRReadLong(Gears_BufferReader* br){
    return Gears_BufferReaderReadLong(br);
}

static char* RG_BRReadString(Gears_BufferReader* br){
    return Gears_BufferReaderReadString(br);
}

static char* RG_BRReadBuffer(Gears_BufferReader* br, size_t* len){
    return Gears_BufferReaderReadBuff(br, len);
}

static long long RG_GetTotalDuration(ExecutionPlan* ep){
    return FlatExecutionPlan_GetExecutionDuration(ep);
}

static long long RG_GetReadDuration(ExecutionPlan* ep){
	return FlatExecutionPlan_GetReadDuration(ep);
}

static void RG_SetError(ExecutionCtx* ectx, char* err){
    ectx->err = err;
}

static const char* RG_GetMyHashTag(){
   return Cluster_GetMyHashTag();
}

static RedisModuleCtx* RG_GetRedisModuleCtx(ExecutionCtx* ectx){
    return ectx->rctx;
}

static void* RG_GetFlatExecutionPrivateData(ExecutionCtx* ectx){
    if(ectx->ep){
        return ectx->ep->fep->PD;
    }
    return NULL;
}

static void* RG_GetPrivateData(ExecutionCtx* ectx){
    return ectx->ep->executionPD;
}

static void RG_SetPrivateData(ExecutionCtx* ectx, void* PD){
    ectx->ep->executionPD = PD;
}

static ExecutionThreadPool* RG_ExecutionThreadPoolCreate(const char* name, size_t numOfThreads){
    return ExecutionPlan_CreateThreadPool(name, numOfThreads);
}

static WorkerData* RG_WorkerDataCreate(ExecutionThreadPool* pool){
    return ExecutionPlan_CreateWorker(pool);
}
static void RG_WorkerDataFree(WorkerData* worker){
    ExecutionPlan_FreeWorker(worker);
}

static WorkerData* RG_WorkerDataGetShallowCopy(WorkerData* worker){
    return ExecutionPlan_WorkerGetShallowCopy(worker);
}

static void RG_ReturnResultsAndErrors(ExecutionPlan* ep, RedisModuleCtx *ctx){
    Command_ReturnResultsAndErrors(ep, ctx);
}

static void RedisGears_SaveRegistrations(RedisModuleIO *rdb, int when){
    if(when == REDISMODULE_AUX_BEFORE_RDB){
        return;
    }
    Gears_dictIterator* iter = Gears_dictGetIterator(Readerdict);
    Gears_dictEntry *curr = NULL;
    while((curr = Gears_dictNext(iter))){
        const char* readerName = Gears_dictGetKey(curr);
        MgmtDataHolder* holder = Gears_dictGetVal(curr);
        RedisGears_ReaderCallbacks* callbacks = holder->callback;
        if(!callbacks->rdbSave){
            continue;
        }
        RedisModule_SaveStringBuffer(rdb, readerName, strlen(readerName) + 1 /* for \0 */);
        callbacks->rdbSave(rdb);
    }
    Gears_dictReleaseIterator(iter);
    RedisModule_SaveStringBuffer(rdb, "", 1); // empty str mean the end!
}

static int RedisGears_LoadRegistrations(RedisModuleIO *rdb, int encver, int when){
    if(encver > REDISGEARS_DATATYPE_VERSION){
        RedisModule_LogIOError(rdb, "warning", "could not load rdb created with higher RedisGears version!");
        return REDISMODULE_ERR; // could not load rdb created with higher Gears version!
    }
    if(when == REDISMODULE_AUX_BEFORE_RDB){
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
    } else {
        // when loading keys phase finished, we load the registrations.
        char* readerName = NULL;
        for(readerName = RedisModule_LoadStringBuffer(rdb, NULL) ;
                strlen(readerName) > 0 ;
                readerName = RedisModule_LoadStringBuffer(rdb, NULL)){
            RedisModule_Assert(readerName);
            RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(readerName);
            RedisModule_Assert(callbacks->rdbLoad);
            callbacks->rdbLoad(rdb, encver);
            RedisModule_Free(readerName);
        }
        RedisModule_Free(readerName);
    }
    return REDISMODULE_OK;
}

RedisModuleType *GearsType = NULL;

static int RedisGears_CreateGearsDataType(RedisModuleCtx* ctx){
    RedisModuleTypeMethods methods = {
            .version = REDISMODULE_TYPE_METHOD_VERSION,
            .rdb_load = NULL,
            .rdb_save = NULL,
            .aof_rewrite = NULL,
            .mem_usage = NULL,
            .digest = NULL,
            .free = NULL,
            .aux_load = RedisGears_LoadRegistrations,
            .aux_save = RedisGears_SaveRegistrations,
            .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB|REDISMODULE_AUX_AFTER_RDB,
        };

    GearsType = RedisModule_CreateDataType(ctx, REDISGEARS_DATATYPE_NAME, REDISGEARS_DATATYPE_VERSION, &methods);
    return GearsType ? REDISMODULE_OK : REDISMODULE_ERR;
}

static void RG_DropLocalyOnDone(ExecutionPlan* ctx, void* privateData){
    EPTurnOnFlag(ctx, EFIsLocalyFreedOnDoneCallback);
}

static const char* RG_GetCompiledOs(){
    return REDISGEARS_OS_VERSION;
}

static int RedisGears_RegisterApi(RedisModuleCtx* ctx){
    if(!RedisModule_ExportSharedAPI){
        RedisModule_Log(ctx, "warning", "redis version are not compatible with shared api, running without expose c level api to other modules.");
    }
    REGISTER_API(GetLLApiVersion, ctx);

    REGISTER_API(CreateType, ctx);
    REGISTER_API(BWWriteLong, ctx);
    REGISTER_API(BWWriteString, ctx);
    REGISTER_API(BWWriteBuffer, ctx);
    REGISTER_API(BRReadLong, ctx);
    REGISTER_API(BRReadString, ctx);
    REGISTER_API(BRReadBuffer, ctx);

    REGISTER_API(RegisterReader, ctx);
    REGISTER_API(RegisterForEach, ctx);
    REGISTER_API(RegisterMap, ctx);
    REGISTER_API(RegisterAccumulator, ctx);
    REGISTER_API(RegisterAccumulatorByKey, ctx);
    REGISTER_API(RegisterFilter, ctx);
    REGISTER_API(RegisterGroupByExtractor, ctx);
    REGISTER_API(RegisterReducer, ctx);
    REGISTER_API(CreateCtx, ctx);
    REGISTER_API(SetDesc, ctx);
    REGISTER_API(SetMaxIdleTime, ctx);
    REGISTER_API(RegisterFlatExecutionPrivateDataType, ctx);
    REGISTER_API(SetFlatExecutionPrivateData, ctx);
    REGISTER_API(GetFlatExecutionPrivateDataFromFep, ctx);
    REGISTER_API(Map, ctx);
    REGISTER_API(Accumulate, ctx);
    REGISTER_API(AccumulateBy, ctx);
    REGISTER_API(LocalAccumulateBy, ctx);
    REGISTER_API(FlatMap, ctx);
    REGISTER_API(Filter, ctx);
    REGISTER_API(GroupBy, ctx);
    REGISTER_API(Collect, ctx);
    REGISTER_API(Repartition, ctx);
    REGISTER_API(ForEach, ctx);
    REGISTER_API(Limit, ctx);
    REGISTER_API(Run, ctx);
    REGISTER_API(Register, ctx);
    REGISTER_API(FreeFlatExecution, ctx);
    REGISTER_API(GetReader, ctx);
    REGISTER_API(StreamReaderCtxCreate, ctx);
    REGISTER_API(StreamReaderCtxFree, ctx);
    REGISTER_API(KeysReaderCtxCreate, ctx);
    REGISTER_API(KeysReaderCtxFree, ctx);
    REGISTER_API(StreamReaderTriggerArgsCreate, ctx);
    REGISTER_API(StreamReaderTriggerArgsFree, ctx);
    REGISTER_API(KeysReaderTriggerArgsCreate, ctx);
    REGISTER_API(KeysReaderTriggerArgsFree, ctx);
    REGISTER_API(CommandReaderTriggerArgsCreate, ctx);
    REGISTER_API(CommandReaderTriggerArgsFree, ctx);

    REGISTER_API(GetExecution, ctx);
    REGISTER_API(IsDone, ctx);
    REGISTER_API(GetRecordsLen, ctx);
    REGISTER_API(GetRecord, ctx);
    REGISTER_API(GetErrorsLen, ctx);
    REGISTER_API(GetError, ctx);
    REGISTER_API(AddOnDoneCallback, ctx);
    REGISTER_API(DropExecution, ctx);
    REGISTER_API(AbortExecution, ctx);
    REGISTER_API(GetId, ctx);

    REGISTER_API(RecordCreate, ctx);
    REGISTER_API(RecordTypeCreate, ctx);
    REGISTER_API(FreeRecord, ctx);
    REGISTER_API(RecordGetType, ctx);
    REGISTER_API(KeyRecordCreate, ctx);
    REGISTER_API(KeyRecordSetKey, ctx);
    REGISTER_API(KeyRecordSetVal, ctx);
    REGISTER_API(KeyRecordGetVal, ctx);
    REGISTER_API(KeyRecordGetKey, ctx);
    REGISTER_API(ListRecordCreate, ctx);
    REGISTER_API(ListRecordLen, ctx);
    REGISTER_API(ListRecordAdd, ctx);
    REGISTER_API(ListRecordGet, ctx);
    REGISTER_API(ListRecordPop, ctx);
    REGISTER_API(StringRecordCreate, ctx);
    REGISTER_API(StringRecordGet, ctx);
    REGISTER_API(StringRecordSet, ctx);
    REGISTER_API(DoubleRecordCreate, ctx);
    REGISTER_API(DoubleRecordGet, ctx);
    REGISTER_API(DoubleRecordSet, ctx);
    REGISTER_API(LongRecordCreate, ctx);
    REGISTER_API(LongRecordGet, ctx);
    REGISTER_API(LongRecordSet, ctx);
    REGISTER_API(KeyHandlerRecordCreate, ctx);
    REGISTER_API(KeyHandlerRecordGet, ctx);
    REGISTER_API(HashSetRecordCreate, ctx);
    REGISTER_API(HashSetRecordSet, ctx);
    REGISTER_API(HashSetRecordGet, ctx);
    REGISTER_API(HashSetRecordGetAllKeys, ctx);

    REGISTER_API(GetTotalDuration, ctx);
    REGISTER_API(GetReadDuration, ctx);

    REGISTER_API(SetError, ctx);
    REGISTER_API(GetRedisModuleCtx, ctx);
    REGISTER_API(GetFlatExecutionPrivateData, ctx);
    REGISTER_API(GetPrivateData, ctx);
    REGISTER_API(SetPrivateData, ctx);
    REGISTER_API(RegisterExecutionOnStartCallback, ctx);
    REGISTER_API(RegisterExecutionOnUnpausedCallback, ctx);
    REGISTER_API(RegisterFlatExecutionOnRegisteredCallback, ctx);
    REGISTER_API(SetFlatExecutionOnStartCallback, ctx);
    REGISTER_API(SetFlatExecutionOnUnpausedCallback, ctx);
    REGISTER_API(SetFlatExecutionOnRegisteredCallback, ctx);

    REGISTER_API(DropLocalyOnDone, ctx);

    REGISTER_API(GetMyHashTag, ctx);

    REGISTER_API(WorkerDataCreate, ctx);
    REGISTER_API(ExecutionThreadPoolCreate, ctx);
    REGISTER_API(WorkerDataFree, ctx);
    REGISTER_API(WorkerDataGetShallowCopy, ctx);
    REGISTER_API(GetCompiledOs, ctx);
    REGISTER_API(ReturnResultsAndErrors, ctx);

    return REDISMODULE_OK;
}

static void RG_OnDropExecutionMsgReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = RedisGears_GetExecution(payload);
	if(!ep){
		RedisModule_Log(ctx, "notice", "got msg to drop an unexists execution : %s", payload);
		return;
	}
	if(EPIsFlagOff(ep, EFDone)){
	    // we are not done yet, we will drop the execution when it finished.
	    // Notice that we got this from another shard that told us to drop the execution
	    // so we only need to drop the local exeuciton when we done.
	    RedisGears_AddOnDoneCallback(ep, RG_DropLocalyOnDone, NULL);
	}else{
	    ExecutionPlan_Free(ep);
	}
}

static void RG_NetworkTest(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    printf("Got test network message\r\n");
}

static int Command_NetworkTest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define TEST_MSG "test"
    Cluster_SendMsgM(NULL, RG_NetworkTest, TEST_MSG, strlen(TEST_MSG));
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

void AddToStream(ExecutionCtx* rctx, Record *data, void* arg){
    const char* keyName = RedisGears_KeyRecordGetKey(data, NULL);
    if(strcmp(keyName, "ChangedStream") == 0){
        return;
    }
    Record* valRecord = RedisGears_KeyRecordGetVal(data);
    const char* val = RedisGears_StringRecordGet(valRecord, NULL);
    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);
    LockHandler_Acquire(ctx);
    RedisModule_Call(ctx, "xadd", "cccccc", "ChangedStream", "*", "key", keyName, "value", val);
    LockHandler_Release(ctx);
}

static void RedisGears_OnModuleLoad(struct RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data){
    if(subevent == REDISMODULE_SUBEVENT_MODULE_LOADED){
        RedisModule_Log(ctx, "notice", "Got module load event, trying to reinitialize RedisAI api");
        if(RedisAI_Initialize(ctx) != REDISMODULE_OK){
            RedisModule_Log(ctx, "warning", "could not initialize RediAI api, running without AI support.");
        }else{
            RedisModule_Log(ctx, "notice", "RedisAI api loaded successfully.");
            globals.redisAILoaded = true;
        }
    }
}

static bool isInitiated = false;

int RedisGears_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_Log(ctx, "notice", "RedisGears version %s, git_sha=%s, compiled_os=%s",
	        REDISGEARS_VERSION_STR,
			REDISGEARS_GIT_SHA,
			REDISGEARS_OS_VERSION);

    GearsGetRedisVersion();
    RedisModule_Log(ctx, "notice", "Redis version found by RedisGears : %d.%d.%d - %s",
                    currVesion.redisMajorVersion, currVesion.redisMinorVersion, currVesion.redisPatchVersion,
	                IsEnterprise() ? (gearsIsCrdt ? "enterprise-crdt" : "enterprise") : "oss");
    if (IsEnterprise()) {
        RedisModule_Log(ctx, "notice", "Redis Enterprise version found by RedisGears : %d.%d.%d-%d",
                        gearsRlecMajorVersion, gearsRlecMinorVersion, gearsRlecPatchVersion, gearsRlecBuild);
    }

    if(GearsCheckSupportedVestion() != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Redis version is to old, please upgrade to redis %d.%d.%d and above.", supportedVersion.redisMajorVersion,
                                                                                                                supportedVersion.redisMinorVersion,
                                                                                                                supportedVersion.redisPatchVersion);
        return REDISMODULE_ERR;
    }

    if(LockHandler_Initialize() != REDISMODULE_OK){
	    RedisModule_Log(ctx, "warning", "could not initialize lock handler");
        return REDISMODULE_ERR;
    }

    if(RedisGears_RegisterApi(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not register RedisGears api");
        return REDISMODULE_ERR;
    }

    if(GearsConfig_Init(ctx, argv, argc) != REDISMODULE_OK){
    	RedisModule_Log(ctx, "warning", "could not initialize gears config");
    	return REDISMODULE_ERR;
    }

    if(RedisAI_Initialize(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not initialize RediAI api, running without AI support.");
    }else{
        RedisModule_Log(ctx, "notice", "RedisAI api loaded successfully.");
        globals.redisAILoaded = true;
    }

    Record_Initialize();

    if(KeysReader_Initialize(ctx) != REDISMODULE_OK){
    	RedisModule_Log(ctx, "warning", "could not initialize default keys reader.");
		return REDISMODULE_ERR;
    }

    if(CommandReader_Initialize(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not initialize default keys reader.");
        return REDISMODULE_ERR;
    }

    Mgmt_Init();

    Cluster_Init();

    RGM_RegisterReader(KeysReader);
    RGM_RegisterReader(StreamReader);
    RGM_RegisterReader(CommandReader);
    RGM_RegisterReader(ShardIDReader);
    RGM_RegisterFilter(Example_Filter, NULL);
    RGM_RegisterMap(GetValueMapper, NULL);
    RGM_RegisterForEach(AddToStream, NULL);

    ExecutionPlan_Initialize();

    Cluster_RegisterMsgReceiverM(RG_OnDropExecutionMsgReceived);
    Cluster_RegisterMsgReceiverM(RG_NetworkTest);

    if(RedisGears_CreateGearsDataType(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "failed create RedisGear DataType");
        return REDISMODULE_ERR;
    }

#ifdef WITHPYTHON
    if(RedisGearsPy_Init(ctx) != REDISMODULE_OK){
        return REDISMODULE_ERR;
    }
#endif

    if(Command_Init() != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not initialize commands.");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.example", Example_CommandCallback, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.example");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.refreshcluster", Cluster_RefreshCluster, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.clusterset", Cluster_ClusterSet, "readonly", 0, 0, -1) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.infocluster", Cluster_GetClusterInfo, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.getclusterinfo");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.hello", Cluster_RedisGearsHello, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.getclusterinfo");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_MSG_COMMAND, Cluster_OnMsgArrive, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "RG_INNER_MSG_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_REGISTER_COMMAND, ExecutionPlan_InnerRegister, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "RG_INNER_REGISTER_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.dumpexecutions", ExecutionPlan_ExecutionsDump, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.dumpexecutions");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.dumpregistrations", ExecutionPlan_DumpRegistrations, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.dumpregistrations");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_UNREGISTER_COMMAND, ExecutionPlan_InnerUnregisterExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "RG_INNER_UNREGISTER_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.unregister", ExecutionPlan_UnregisterExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.unregister");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.getexecution", ExecutionPlan_ExecutionGet, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.getexecution");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.getresults", Command_GetResults, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.getresults");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.getresultsblocking", Command_GetResultsBlocking, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.getresultsblocking");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.dropexecution", Command_DropExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.dropexecution");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.abortexecution", Command_AbortExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.abortexecution");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.networktest", Command_NetworkTest, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.networktest");
        return REDISMODULE_ERR;
    }

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange, RedisGears_OnModuleLoad);

    isInitiated = true;
    return REDISMODULE_OK;
}

#ifdef VALGRIND
static void __attribute__((destructor)) RedisGears_Clean(void) {
    // for valgrind check perposes, here we will free memory
    // that confuses the valgrind.
    if(!isInitiated){
        return;
    }
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);
    RedisGearsPy_Clean();
    ExecutionPlan_Clean();
    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
}
#endif

