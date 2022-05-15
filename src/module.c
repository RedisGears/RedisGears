/*
 * module.c
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

#include "version.h"
#include "mgmt.h"
#include "execution_plan.h"
#include "cluster.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "utils/arr_rm_alloc.h"
#include "utils/buffer.h"
#include "record.h"
#include "commands.h"
#include "config.h"
#include "readers/keys_reader.h"
#include "readers/streams_reader.h"
#include "readers/command_reader.h"
#include "readers/shardid_reader.h"
#include "mappers.h"
#include "lock_handler.h"
#include "command_hook.h"
#include "../plugins/python/redisgears_python.h"

#include <unistd.h>
#include <dlfcn.h>
#include <dirent.h>

#ifndef REDISGEARS_GIT_SHA
#define REDISGEARS_GIT_SHA "unknown"
#endif

#ifndef REDISGEARS_OS_VERSION
#define REDISGEARS_OS_VERSION "unknown"
#endif

Gears_dict* plugins = NULL;

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

static int RG_RegisterFlatExecutionOnUnregisteredCallback(char* name, RedisGears_FlatExecutionOnUnregisteredCallback callback, ArgType* type){
    return FlatExecutionOnUnregisteredsMgmt_Add(name, callback, type);
}

static int RG_SetFlatExecutionOnStartCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_ExecutionOnStartCallback c = ExecutionOnStartsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnStartStep(fep, callback, arg);
    return REDISMODULE_OK;
}

static int RG_SetFlatExecutionOnUnpausedCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_ExecutionOnStartCallback c = ExecutionOnUnpausedsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnUnPausedStep(fep, callback, arg);
    return REDISMODULE_OK;
}

static int RG_SetFlatExecutionOnRegisteredCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_FlatExecutionOnRegisteredCallback c = FlatExecutionOnRegisteredsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnRegisteredStep(fep, callback, arg);
    return REDISMODULE_OK;
}

static int RG_SetFlatExecutionOnUnregisteredCallback(FlatExecutionPlan* fep, const char* callback, void* arg){
    RedisGears_FlatExecutionOnUnregisteredCallback c = FlatExecutionOnUnregisteredsMgmt_Get(callback);
    if(!c){
        return REDISMODULE_ERR;
    }
    FlatExecutionPlan_SetOnUnregisteredStep(fep, callback, arg);
    return REDISMODULE_OK;
}

static FlatExecutionPlan* RG_CreateCtx(char* readerName, char** err){
    if(!LockHandler_IsRedisGearsThread()){
        *err = RG_STRDUP("Can only create a gearsCtx on registered gears thread");
        return NULL;
    }
    LockHandler_Acquire(staticCtx);
    FlatExecutionPlan* fep = FlatExecutionPlan_New();
    if(!FlatExecutionPlan_SetReader(fep, readerName)){
        FlatExecutionPlan_Free(fep);
        *err = RG_STRDUP("The given reader does not exists");
        LockHandler_Release(staticCtx);
        return NULL;
    }
    LockHandler_Release(staticCtx);
    return fep;
}

static int RG_SetDesc(FlatExecutionPlan* fep, const char* desc){
    FlatExecutionPlan_SetDesc(fep, desc);
    return 1;
}

static void RG_SetExecutionThreadPool(FlatExecutionPlan* ctx, ExecutionThreadPool* pool){
    FlatExecutionPlan_SetThreadPool(ctx, pool);
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

static SessionRegistrationCtx* RG_SessionRegisterCtxCreate(Plugin *p) {
    return SessionRegistrationCtx_Create(p);
}

static void RG_SessionRegisterCtxFree(SessionRegistrationCtx* s) {
    return SessionRegistrationCtx_Free(s);
}

static void RG_SessionRegisterSetMaxIdle(SessionRegistrationCtx* s, long long maxIdle) {
    s->maxIdle = maxIdle;
}

static void RG_AddRegistrationToUnregister(SessionRegistrationCtx* srctx, const char* registrationId) {
    FlatExecutionPlan_AddRegistrationToUnregister(srctx, registrationId);
}

static void RG_AddSessionToUnlink(SessionRegistrationCtx* srctx, const char* sessionId) {
    FlatExecutionPlan_AddSessionToUnlink(srctx, sessionId);
}

static int RG_PrepareForRegister(SessionRegistrationCtx* srctx, FlatExecutionPlan* fep, ExecutionMode mode, void* key, char** err, char** registrationId){
    // we need a deep copy in case this fep will be registered again with different args
    // this is a hack because a fep represent registration, we need create
    // a registration object that will hold a shared reference of the fep for future.
    // The plane is to reimplement this all API so its not worth inve
    fep = FlatExecutionPlan_DeepCopy(fep);

    int ret = FlatExecutionPlan_PrepareForRegister(srctx, fep, mode, key, err);

    if(!ret) {
        FlatExecutionPlan_Free(fep);
    } else {
        if(registrationId){
            *registrationId = RG_STRDUP(fep->idStr);
        }
    }
    return ret;
}

static int RG_Register(SessionRegistrationCtx* srctx, SessionRegistrationCtx_OnDone onDone, void *pd, char **err){
    Gears_BufferWriterWriteLong(&srctx->bw, SESSION_REGISTRATION_OP_CODE_DONE);
    return Command_Register(srctx, onDone, pd, err);
}

static int RG_RegisterFep(Plugin *p, FlatExecutionPlan* fep, ExecutionMode mode, void* key, char** err, char** registrationId){
    if(!LockHandler_IsRedisGearsThread()){
        *err = RG_STRDUP("Can only register execution on registered gears thread");
        return 0;
    }
    LockHandler_Acquire(staticCtx);
    SessionRegistrationCtx* srctx = SessionRegistrationCtx_Create(p);
    if (!RG_PrepareForRegister(srctx, fep, mode, key, err, registrationId)) {
        SessionRegistrationCtx_Free(srctx);
        LockHandler_Release(staticCtx);
        return 0;
    }

    Gears_BufferWriterWriteLong(&srctx->bw, SESSION_REGISTRATION_OP_CODE_DONE);
    FlatExecutionPlan_Register(srctx);

    LockHandler_Release(staticCtx);

    return 1;
}

static ExecutionPlan* RG_RunWithFlags(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err, RunFlags flags){
    if(!LockHandler_IsRedisGearsThread()){
        *err = RG_STRDUP("Can only run execution on registered gears thread");
        return NULL;
    }
    LockHandler_Acquire(staticCtx);
    if(mode == ExecutionModeAsync && !Cluster_IsInitialized()){
        *err = RG_STRDUP("Cluster is not initialized, can not start executions.");
        LockHandler_Release(staticCtx);
        return NULL;
    }
    ExecutionPlan* res = FlatExecutionPlan_Run(fep, mode, arg, callback, privateData, worker, err, flags);
    LockHandler_Release(staticCtx);
    return res;
}

static ExecutionPlan* RG_Run(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err){
    return RG_RunWithFlags(fep, mode, arg, callback, privateData, worker, err, 0);
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

static void RG_KeysReaderTriggerArgsSetHookCommands(KeysReaderTriggerArgs* krta, Arr(char*) hookCommands){
    return KeysReaderTriggerArgs_SetTriggerHookCommands(krta, hookCommands);
}

static CommandReaderTriggerArgs* RG_CommandReaderTriggerArgsCreate(const char* trigger, int inOrder){
    return CommandReaderTriggerArgs_CreateTrigger(trigger, inOrder);
}

static CommandReaderTriggerArgs* RG_CommandReaderTriggerArgsCreateHook(const char* hook, const char* prefix, int inOrder){
    return CommandReaderTriggerArgs_CreateHook(hook, prefix, inOrder);
}

static void RG_CommandReaderTriggerArgsFree(CommandReaderTriggerArgs* args){
    CommandReaderTriggerArgs_Free(args);
}

static CommandReaderTriggerCtx* RG_GetCommandReaderTriggerCtx(ExecutionCtx* ectx){
    return CommandReaderTriggerCtx_Get(ectx);
}

static CommandReaderTriggerCtx* RG_CommandReaderTriggerCtxGetShallowCopy(CommandReaderTriggerCtx* crtCtx){
    return CommandReaderTriggerCtx_GetShallowCopy(crtCtx);
}

static RedisModuleCallReply* RG_CommandReaderTriggerCtxNext(CommandReaderTriggerCtx* crtCtx, RedisModuleString** argv, size_t argc){
    return CommandReaderTriggerCtx_CallNext(crtCtx, argv, argc);
}

static void RG_CommandReaderTriggerCtxFree(CommandReaderTriggerCtx* crtCtx){
    CommandReaderTriggerCtx_Free(crtCtx);
}

static void RG_KeysReaderTriggerArgsFree(KeysReaderTriggerArgs* args){
    return KeysReaderTriggerArgs_Free(args);
}

static int RG_KeysReaderSetAvoidEvents(int avoidEvents){
    return KeyReader_SetAvoidEvents(avoidEvents);
}

static CommandCtx* RG_CommandCtxGetShallowCopy(CommandCtx* cmdCtx){
    return KeyReader_CommandCtxGetShallowCopy(cmdCtx);
}
static void RG_CommandCtxFree(CommandCtx* cmdCtx){
    KeyReader_CommandCtxFree(cmdCtx);
}

static int RG_CommandCtxOverrideReply(CommandCtx* cmdCtx, Record* r, char** err){
    return KeyReader_CommandCtxOverrideReply(cmdCtx, r, err);
}

static RedisModuleString** RG_CommandCtxGetCommand(CommandCtx* cmdCtx, size_t* len){
    return KeyReader_CommandCtxGetCommand(cmdCtx, len);
}

static CommandCtx* RG_CommandCtxGet(ExecutionCtx* ectx){
    return KeyReader_CommandCtxGet(ectx);
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

static const char* RG_FepGetId(FlatExecutionPlan* fep){
    return fep->idStr;
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
    ExecutionCallbacData onDoneData = {
            .callback = callback,
            .pd = privateData,
    };
    ep->onDoneData = array_append(ep->onDoneData, onDoneData);
    return true;
}

static bool RG_AddOnRunningCallback(ExecutionPlan* ep, RedisGears_ExecutionCallback callback, void* privateData) {
    if(EPIsFlagOn(ep, EFDone)){
        return false;
    }
    ExecutionCallbacData callbackData = {
            .callback = callback,
            .pd = privateData,
    };
    if (!ep->runCallbacks) {
        ep->runCallbacks = array_new(ExecutionCallbacData, 2);
    }
    ep->runCallbacks = array_append(ep->runCallbacks, callbackData);
    return true;
}

static bool RG_AddOnHoldingCallback(ExecutionPlan* ep, RedisGears_ExecutionCallback callback, void* privateData) {
    if(EPIsFlagOn(ep, EFDone)){
        return false;
    }
    ExecutionCallbacData callbackData = {
            .callback = callback,
            .pd = privateData,
    };
    if (!ep->holdCallbacks) {
        ep->holdCallbacks = array_new(ExecutionCallbacData, 2);
    }
    ep->holdCallbacks = array_append(ep->holdCallbacks, callbackData);
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

    // execution is done, no need to abort
    if(RedisGears_IsDone(ep)){
        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
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

static FlatExecutionPlan* RG_GetFepById(const char* id){
    return FlatExecutionPlan_FindByStrId(id);
}

static void RG_DumpRegistration(RedisModuleCtx *ctx, FlatExecutionPlan *fep, int flags) {
    ExecutionPlan_DumpSingleRegistration(ctx, fep, flags);
}

static RunFlags RG_GetRunFlags(ExecutionCtx* ectx){
    return ectx->ep->runFlags;
}

static ArgType* RG_CreateType(char* name,
                              int version,
                              ArgFree free,
                              ArgDuplicate dup,
                              ArgSerialize serialize,
                              ArgDeserialize deserialize,
                              ArgToString tostring,
                              ArgOnFepDeserialized onDeserialized){
    ArgType* ret = RG_ALLOC(sizeof(*ret));
    *ret = (ArgType){
        .type = RG_STRDUP(name),
        .version = version,
        .free = free,
        .dup = dup,
        .serialize = serialize,
        .deserialize = deserialize,
        .tostring = tostring,
        .onDeserialized = onDeserialized,
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

static ExecutionThreadPool* RG_ExecutionThreadPoolDefine(const char* name, void* poolCtx, ExecutionPoolAddJob addJob){
    return ExecutionPlan_DefineThreadPool(name, poolCtx, addJob);
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

static void RG_GetShardUUID(char* finalId, char* idBuf, char* idStrBuf, long long* lastID){
    SetId(finalId, idBuf, idStrBuf, lastID);
}

static void RG_LockHanlderRegister(){
    LockHandler_Register();
}

static void RG_LockHanlderAcquire(RedisModuleCtx* ctx){
    LockHandler_Acquire(ctx);
}

static void RG_LockHanlderRelease(RedisModuleCtx* ctx){
    LockHandler_Release(ctx);
}

static RecordType* RG_GetListRecordType(){
    return listRecordType;
}

static RecordType* RG_GetStringRecordType(){
    return stringRecordType;
}

static RecordType* RG_GetErrorRecordType(){
    return errorRecordType;
}

static RecordType* RG_GetLongRecordType(){
    return longRecordType;
}

static RecordType* RG_GetDoubleRecordType(){
    return doubleRecordType;
}

static RecordType* RG_GetKeyRecordType(){
    return keyRecordType;
}

static RecordType* RG_GetKeysHandlerRecordType(){
    return keysHandlerRecordType;
}

static RecordType* RG_GetHashSetRecordType(){
    return hashSetRecordType;
}

static RecordType* RG_GetNullRecordType(){
    return nullRecordType;
}

static ExecutionPlan* RG_GetExecutionFromCtx(ExecutionCtx* ectx){
    return ectx->ep;
}

static int RG_ExecuteCommand(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, ...) {
    char* command;
    va_list ap;
    va_start(ap, __fmt);

    int exitCode = ExecCommandVList(ctx, logLevel, __fmt, ap);

    va_end(ap);

    return exitCode;
}

static const char* RG_GetConfig(const char* name) {
    return GearsConfig_GetExtraConfigVals(name);
}

static bool RG_ProfileEnabled() {
    return GearsConfig_GetProfileExecutions();
}

static const int RG_ExecutionPlanIsLocal(ExecutionPlan* ep) {
    return EPIsFlagOn(ep, EFIsLocal);
}

static const int RG_GetVersion() {
    return REDISGEARS_MODULE_VERSION;
}

static const char* RG_GetVersionStr(){
    return REDISGEARS_VERSION_STR;
}

static Plugin* RG_RegisterPlugin(const char* name, int version) {
    if(Gears_dictFetchValue(plugins, name)){
        RedisModule_Log(staticCtx, "warning", "Plugin %s already exists", name);
        return NULL;
    }
    Plugin* plugin = RG_ALLOC(sizeof(*plugin));
    plugin->name = RG_STRDUP(name);
    plugin->version = version;
    plugin->infoFunc = NULL;
    plugin->unlinkSession = NULL;
    Gears_dictAdd(plugins, (char*)name, plugin);
    RedisModule_Log(staticCtx, "warning", "Loading plugin %s version %d ", name, version);
    return plugin;
}

static void RG_PluginSetInfoCallback(Plugin* p, RedisModuleInfoFunc infoFunc) {
    p->infoFunc = infoFunc;
}

static void RG_PluginSetUnlinkSessionCallback(Plugin* p, GearsPlugin_UnlinkSession unlinkSession) {
    p->unlinkSession = unlinkSession;
}

static void RG_PluginSetSerializeSessionCallback(Plugin* p, GearsPlugin_SerializeSession serializeSession) {
    p->serializeSession = serializeSession;
}

static void RG_PluginSetDeserializeSessionCallback(Plugin* p, GearsPlugin_DeserializeSession deserializeSession) {
    p->deserializeSession = deserializeSession;
}

static int RG_PutUsedSession(SessionRegistrationCtx* srctx, void *session, char **err) {
    RedisGears_BWWriteLong(&srctx->bw, SESSION_REGISTRATION_OP_CODE_SESSION_DESERIALIZE);
    int ret = srctx->p->serializeSession(session, &srctx->bw, err);
    if (ret == REDISMODULE_OK) {
        srctx->usedSession = session;
    }
    return ret;
}

static void RG_PluginSetSetCurrSessionCallback(Plugin* p, GearsPlugin_SetCurrSession setCurrSession) {
    p->setCurrSession = setCurrSession;
}

static int RG_KeysReaderSetReadRecordCallback(KeysReaderCtx* krCtx, const char* name){
    return KeysReaderCtx_SetReadRecordCallback(krCtx, name);
}

static int RG_KeysReaderTriggerArgsSetReadRecordCallback(KeysReaderTriggerArgs* krta, const char* name){
    return KeysReaderTriggerArgs_SetReadRecordCallback(krta, name);
}

static void RG_KeysReaderRegisterReadRecordCallback(const char* name, RedisGears_KeysReaderReadRecordCallback callback){
    KeysReaderReadRecordsMgmt_Add(name, callback, NULL);
}

static Gears_Buffer* RG_BufferCreate(size_t initCap){
    return Gears_BufferNew(initCap);
}

static void RG_BufferFree(Gears_Buffer* buff){
    Gears_BufferFree(buff);
}

static void RG_BufferClear(Gears_Buffer* buff){
    Gears_BufferClear(buff);
}

const char* RG_BufferGet(Gears_Buffer* buff, size_t* len){
    if(len){
        *len = buff->size;
    }
    return buff->buff;
}

static void RG_BufferReaderInit(Gears_BufferReader* br,Gears_Buffer* buff){
    Gears_BufferReaderInit(br, buff);
}

static void RG_BufferWriterInit(Gears_BufferWriter* bw,Gears_Buffer* buff){
    Gears_BufferWriterInit(bw, buff);
}

FlatExecutionPlan* RG_GetFep(ExecutionPlan* ep){
    return ep->fep;
}

int RG_IsCrdt(){
    return gearsIsCrdt;
}

int RG_IsEnterprise(){
    return IsEnterprise();
}

static const char* RG_GetShardIdentifier(){
    return GetShardUniqueId();
}

static int RG_ASprintf(char **__ptr, const char *__restrict __fmt, ...){
    va_list ap;
    va_start(ap, __fmt);

    int res = rg_vasprintf(__ptr, __fmt, ap);

    va_end(ap);

    return res;
}

static char* RG_ArrToStr(void** arr, size_t len, char*(*toStr)(void*), char sep){
    return ArrToStr(arr, len, toStr, sep);
}

static int RG_IsClusterMode(){
    return Cluster_IsClusterMode();
}

static const char* RG_GetNodeIdByKey(const char* key){
    return Cluster_GetNodeIdByKey(key);
}

static int RG_ClusterIsMyId(const char* id){
    return Cluster_IsMyId(id);
}

static int RG_ClusterIsInitialized(){
    return Cluster_IsInitialized();
}

static void RG_AddLockStateHandler(SaveState save, RestoreState restore){
    LockHandler_AddStateHanlder(save, restore);
}

static void RG_SetAbortCallback(ExecutionCtx *ctx, AbortCallback abort, void* abortPD){
    ctx->ep->abort = abort;
    ctx->ep->abortPD = abortPD;
}

static void RG_AddConfigHooks(BeforeConfigSet before, AfterConfigSet after, GetConfig getConfig){
    GearsConfig_AddHooks(before, after, getConfig);
}

RedisModuleEventCallback* pluginLoadingCallbacks;

static void RG_RegisterLoadingEvent(RedisModuleEventCallback pluginLoadingCallback){
    if(!pluginLoadingCallbacks){
        pluginLoadingCallbacks = array_new(RedisModuleEventCallback, 10);
    }
    pluginLoadingCallbacks = array_append(pluginLoadingCallbacks, pluginLoadingCallback);

}

static RedisVersion* RG_GetRedisVersion() {
    return &currVesion;
}

static void RedisGears_OnLoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data){
    if(subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
                subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
                subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START){
        ExecutionPlan_Clean();
    }
    if(pluginLoadingCallbacks){
        for(size_t i = 0 ; i < array_len(pluginLoadingCallbacks) ; ++i){
            pluginLoadingCallbacks[i](ctx, eid, subevent, data);
        }
    }
}

static void RedisGears_SaveRegistrations(RedisModuleIO *rdb, int when){
    if(when == REDISMODULE_AUX_BEFORE_RDB){
        // save loaded plugins
        RedisModule_SaveUnsigned(rdb, Gears_dictSize(plugins));
        Gears_dictIterator* iter = Gears_dictGetIterator(plugins);
        Gears_dictEntry *curr = NULL;
        while((curr = Gears_dictNext(iter))){
            Plugin* p = Gears_dictGetVal(curr);
            RedisModule_SaveStringBuffer(rdb, p->name, strlen(p->name) + 1 /* for \0*/);
            RedisModule_SaveUnsigned(rdb, p->version);
        }
        Gears_dictReleaseIterator(iter);
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

        if(encver >= VERSION_WITH_PLUGINS_NAMES){
            // verify loaded plugins
            size_t nplugins = RedisModule_LoadUnsigned(rdb);
            for(size_t i = 0 ; i < nplugins ; ++i){
                size_t nameLen;
                char* name = RedisModule_LoadStringBuffer(rdb, &nameLen);
                int version = RedisModule_LoadUnsigned(rdb);
                Plugin* p = Gears_dictFetchValue(plugins, name);
                if(!p){
                    RedisModule_LogIOError(rdb, "warning", "Plugin %s does not exists and required to load rdb", name);
                    RedisModule_Free(name);
                    return REDISMODULE_ERR;
                }
                if(p->version < version){
                    RedisModule_LogIOError(rdb, "warning", "Plugin %s with version %d (or above) is required but found version %d", name, version, p->version);
                    RedisModule_Free(name);
                    return REDISMODULE_ERR;
                }
                RedisModule_Free(name);
            }
        } else {
            // RDB was save on version without plugins (v1.0.x)
            // on this version we have the python plugin by default.
            // We need to verify that we have the python plugin loaded
            // otherwise we can not load this RDB
            if (!Gears_dictFetchValue(plugins, REDISGEARSPYTHON_PLUGIN_NAME)) {
                // we do not have the python plugin, we can not load the RDB.
                RedisModule_LogIOError(rdb, "warning", "RDB created on RedisGears v1.0.x require the python plugin which is currently not loaded.");
                return REDISMODULE_ERR;
            }
        }

        // clear registrations
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
            if(callbacks->rdbLoad(rdb, encver) != REDISMODULE_OK){
                RedisModule_Free(readerName);
                return REDISMODULE_ERR;
            }
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
        RedisModule_Log(staticCtx, "warning", "redis version are not compatible with shared api, running without expose c level api to other modules.");
    }
    REGISTER_API(GetLLApiVersion, ctx);

    REGISTER_API(CreateType, ctx);
    REGISTER_API(BufferCreate, ctx);
    REGISTER_API(BufferFree, ctx);
    REGISTER_API(BufferClear, ctx);
    REGISTER_API(BufferGet, ctx);
    REGISTER_API(BufferReaderInit, ctx);
    REGISTER_API(BufferWriterInit, ctx);
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
    REGISTER_API(SetExecutionThreadPool, ctx);
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
    REGISTER_API(RunWithFlags, ctx);
    REGISTER_API(Register, ctx);
    REGISTER_API(PrepareForRegister, ctx);
    REGISTER_API(RegisterFep, ctx);
    REGISTER_API(AddRegistrationToUnregister, ctx);
    REGISTER_API(AddSessionToUnlink, ctx);
    REGISTER_API(SessionRegisterCtxCreate, ctx);
    REGISTER_API(SessionRegisterCtxFree, ctx);
    REGISTER_API(SessionRegisterSetMaxIdle, ctx);
    REGISTER_API(FreeFlatExecution, ctx);
    REGISTER_API(GetReader, ctx);
    REGISTER_API(StreamReaderCtxCreate, ctx);
    REGISTER_API(StreamReaderCtxFree, ctx);
    REGISTER_API(KeysReaderCtxCreate, ctx);
    REGISTER_API(KeysReaderCtxFree, ctx);
    REGISTER_API(StreamReaderTriggerArgsCreate, ctx);
    REGISTER_API(StreamReaderTriggerArgsFree, ctx);
    REGISTER_API(KeysReaderTriggerArgsCreate, ctx);
    REGISTER_API(KeysReaderTriggerArgsSetHookCommands, ctx);
    REGISTER_API(KeysReaderTriggerArgsFree, ctx);
    REGISTER_API(KeysReaderSetAvoidEvents, ctx);
    REGISTER_API(CommandCtxGetShallowCopy, ctx);
    REGISTER_API(CommandCtxFree, ctx);
    REGISTER_API(CommandCtxOverrideReply, ctx);
    REGISTER_API(CommandCtxGetCommand, ctx);
    REGISTER_API(CommandCtxGet, ctx);
    REGISTER_API(CommandReaderTriggerArgsCreate, ctx);
    REGISTER_API(CommandReaderTriggerArgsCreateHook, ctx);
    REGISTER_API(CommandReaderTriggerArgsFree, ctx);
    REGISTER_API(GetCommandReaderTriggerCtx, ctx);
    REGISTER_API(CommandReaderTriggerCtxGetShallowCopy, ctx);
    REGISTER_API(CommandReaderTriggerCtxNext, ctx);
    REGISTER_API(CommandReaderTriggerCtxFree, ctx);

    REGISTER_API(GetExecution, ctx);
    REGISTER_API(GetFepById, ctx);
    REGISTER_API(DumpRegistration, ctx);
    REGISTER_API(GetRunFlags, ctx);
    REGISTER_API(GetFep, ctx);
    REGISTER_API(IsDone, ctx);
    REGISTER_API(GetRecordsLen, ctx);
    REGISTER_API(GetRecord, ctx);
    REGISTER_API(GetErrorsLen, ctx);
    REGISTER_API(GetError, ctx);
    REGISTER_API(AddOnDoneCallback, ctx);
    REGISTER_API(AddOnRunningCallback, ctx);
    REGISTER_API(AddOnHoldingCallback, ctx);
    REGISTER_API(DropExecution, ctx);
    REGISTER_API(AbortExecution, ctx);
    REGISTER_API(GetId, ctx);
    REGISTER_API(FepGetId, ctx);

    REGISTER_API(GetDummyRecord, ctx);
    REGISTER_API(RecordCreate, ctx);
    REGISTER_API(RecordTypeCreate, ctx);
    REGISTER_API(FreeRecord, ctx);
    REGISTER_API(RecordGetType, ctx);
    REGISTER_API(AsyncRecordCreate, ctx);
    REGISTER_API(AsyncRecordContinue, ctx);
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
    REGISTER_API(ErrorRecordCreate, ctx);
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
    REGISTER_API(GetNullRecord, ctx);
    REGISTER_API(RecordSendReply, ctx);

    REGISTER_API(GetTotalDuration, ctx);
    REGISTER_API(GetReadDuration, ctx);

    REGISTER_API(GetExecutionFromCtx, ctx);
    REGISTER_API(SetError, ctx);
    REGISTER_API(GetRedisModuleCtx, ctx);
    REGISTER_API(GetFlatExecutionPrivateData, ctx);
    REGISTER_API(GetPrivateData, ctx);
    REGISTER_API(SetPrivateData, ctx);
    REGISTER_API(RegisterExecutionOnStartCallback, ctx);
    REGISTER_API(RegisterExecutionOnUnpausedCallback, ctx);
    REGISTER_API(RegisterFlatExecutionOnRegisteredCallback, ctx);
    REGISTER_API(RegisterFlatExecutionOnUnregisteredCallback, ctx);
    REGISTER_API(SetFlatExecutionOnStartCallback, ctx);
    REGISTER_API(SetFlatExecutionOnUnpausedCallback, ctx);
    REGISTER_API(SetFlatExecutionOnRegisteredCallback, ctx);
    REGISTER_API(SetFlatExecutionOnUnregisteredCallback, ctx);

    REGISTER_API(DropLocalyOnDone, ctx);

    REGISTER_API(GetMyHashTag, ctx);

    REGISTER_API(WorkerDataCreate, ctx);
    REGISTER_API(ExecutionThreadPoolCreate, ctx);
    REGISTER_API(ExecutionThreadPoolDefine, ctx);
    REGISTER_API(WorkerDataFree, ctx);
    REGISTER_API(WorkerDataGetShallowCopy, ctx);
    REGISTER_API(GetCompiledOs, ctx);
    REGISTER_API(ReturnResultsAndErrors, ctx);

    REGISTER_API(GetShardUUID, ctx);
    REGISTER_API(LockHanlderRegister, ctx);
    REGISTER_API(LockHanlderAcquire, ctx);
    REGISTER_API(LockHanlderRelease, ctx);
    REGISTER_API(ExecuteCommand, ctx);

    REGISTER_API(GetListRecordType, ctx);
    REGISTER_API(GetStringRecordType, ctx);
    REGISTER_API(GetErrorRecordType, ctx);
    REGISTER_API(GetLongRecordType, ctx);
    REGISTER_API(GetDoubleRecordType, ctx);
    REGISTER_API(GetKeyRecordType, ctx);
    REGISTER_API(GetKeysHandlerRecordType, ctx);
    REGISTER_API(GetHashSetRecordType, ctx);
    REGISTER_API(GetNullRecordType, ctx);

    REGISTER_API(GetConfig, ctx);
    REGISTER_API(ProfileEnabled, ctx);

    REGISTER_API(RegisterPlugin, ctx);
    REGISTER_API(PluginSetInfoCallback, ctx);
    REGISTER_API(PluginSetUnlinkSessionCallback, ctx);
    REGISTER_API(PluginSetSerializeSessionCallback, ctx);
    REGISTER_API(PluginSetDeserializeSessionCallback, ctx);
    REGISTER_API(PluginSetSetCurrSessionCallback, ctx);
    REGISTER_API(PutUsedSession, ctx);

    REGISTER_API(ExecutionPlanIsLocal, ctx);
    REGISTER_API(GetVersion, ctx);
    REGISTER_API(GetVersionStr, ctx);
    REGISTER_API(IsCrdt, ctx);
    REGISTER_API(IsEnterprise, ctx);
    REGISTER_API(GetShardIdentifier, ctx);
    REGISTER_API(ASprintf, ctx);
    REGISTER_API(ArrToStr, ctx);

    REGISTER_API(IsClusterMode, ctx);
    REGISTER_API(GetNodeIdByKey, ctx);
    REGISTER_API(ClusterIsMyId, ctx);
    REGISTER_API(ClusterIsInitialized, ctx);
    REGISTER_API(AddLockStateHandler, ctx);
    REGISTER_API(SetAbortCallback, ctx);
    REGISTER_API(AddConfigHooks, ctx);
    REGISTER_API(RegisterLoadingEvent, ctx);

    REGISTER_API(KeysReaderSetReadRecordCallback, ctx);
    REGISTER_API(KeysReaderTriggerArgsSetReadRecordCallback, ctx);
    REGISTER_API(KeysReaderRegisterReadRecordCallback, ctx);

    REGISTER_API(GetRedisVersion, ctx);

    return REDISMODULE_OK;
}

static void RG_OnDropExecutionMsgReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = RedisGears_GetExecution(payload);
	if(!ep){
		RedisModule_Log(staticCtx, "notice", "got msg to drop an unexists execution : %s", payload);
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

static int RedisGears_LoadSinglePlugin(RedisModuleCtx *ctx, char* plugin) {
    char* pluginFile = NULL;
    const char* moduleDataDir = getenv("modulesdatadir");
    if(moduleDataDir){
        // modulesdatadir env var exists, we are running on redis enterprise and we need to run on modules directory
        rg_asprintf(&pluginFile, "%s/%s/%d/deps/%s/plugin/%s.so", moduleDataDir, REDISGEARS_MODULE_NAME, REDISGEARS_MODULE_VERSION, plugin, plugin);
    }else{
        pluginFile = RG_STRDUP(plugin);
    }

    void* handler = dlopen(pluginFile, RTLD_NOW|RTLD_GLOBAL);
    RG_FREE(pluginFile);

    if (handler == NULL) {
        RedisModule_Log(staticCtx, "warning", "Failed loading gears plugin %s, error='%s'", plugin, dlerror());
        return REDISMODULE_ERR;
    }
    int (*onload)(void *) = dlsym(handler,"RedisGears_OnLoad");
    if (onload == NULL) {
        dlclose(handler);
        RedisModule_Log(staticCtx, "warning",
            "Gears plugin %s does not export RedisGears_OnLoad() symbol", plugin);
        return REDISMODULE_ERR;
    }
    if (onload(ctx) == REDISMODULE_ERR) {
        dlclose(handler);
        RedisModule_Log(staticCtx, "warning",
            "Plugin %s initialization failed", plugin);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static int RedisGears_InitializePlugins(RedisModuleCtx *ctx) {
    char** plugins = GearsConfig_GetPlugins();
    if (array_len(plugins) == 0 && IsEnterprise()) {
        /* On enterprise, if no plugin was specified, we will load the default Python plugin. */
        RedisModule_Log(staticCtx, "notice", "No plugin was specified, automatically loading the python plugin.");
        if (RedisGears_LoadSinglePlugin(ctx, "gears_python") != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }
    } else {
        for(size_t i = 0 ; i < array_len(plugins) ; ++i){
            if (RedisGears_LoadSinglePlugin(ctx, plugins[i]) != REDISMODULE_OK) {
                return REDISMODULE_ERR;
            }
        }
    }
    return REDISMODULE_OK;
}

static int Command_DumpPlugins(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_ReplyWithArray(ctx, Gears_dictSize(plugins));
    Gears_dictIterator* iter = Gears_dictGetIterator(plugins);
    Gears_dictEntry *curr = NULL;
    while((curr = Gears_dictNext(iter))){
        Plugin* p = Gears_dictGetVal(curr);
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithCString(ctx, p->name);
        RedisModule_ReplyWithLongLong(ctx, p->version);
    }
    Gears_dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

static void RedisGears_InfoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
    if (RedisModule_InfoAddSection(ctx, NULL) == REDISMODULE_OK) {
        // default section
        RedisModule_InfoAddFieldULongLong(ctx, "nexecutions", ExecutionPlan_NExecutions());
        RedisModule_InfoAddFieldULongLong(ctx, "nregistrations", ExecutionPlan_NRegistrations());
    }

    if (RedisModule_InfoAddSection(ctx, "regisrations") == REDISMODULE_OK) {
        ExecutionPlan_InfoRegistrations(ctx, for_crash_report);
    }

    if (RedisModule_InfoAddSection(ctx, "plugins") == REDISMODULE_OK) {
        Gears_dictIterator* iter = Gears_dictGetIterator(plugins);
        Gears_dictEntry *curr = NULL;
        while((curr = Gears_dictNext(iter))){
            Plugin* p = Gears_dictGetVal(curr);
            RedisModule_InfoBeginDictField(ctx, p->name);
            RedisModule_InfoAddFieldULongLong(ctx, "version", p->version);
            RedisModule_InfoEndDictField(ctx);
        }
        Gears_dictReleaseIterator(iter);
    }

    Gears_dictIterator* iter = Gears_dictGetIterator(plugins);
    Gears_dictEntry *curr = NULL;
    while((curr = Gears_dictNext(iter))){
        Plugin* p = Gears_dictGetVal(curr);
        if (p->infoFunc) {
            p->infoFunc(ctx, for_crash_report);
        }
    }
    Gears_dictReleaseIterator(iter);
}

static bool isInitiated = false;

void RedisGears_OnShutDown(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data){
#ifdef VALGRIND
    ExecutionPlan_Clean();
#endif
}

int RedisGears_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)){
        staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
    }else{
        staticCtx = RedisModule_GetThreadSafeContext(NULL);
    }

	RedisModule_Log(staticCtx, "notice", "RedisGears version %s, git_sha=%s, compiled_os=%s",
	        REDISGEARS_VERSION_STR,
			REDISGEARS_GIT_SHA,
			REDISGEARS_OS_VERSION);

    GearsGetRedisVersion();
    RedisModule_Log(staticCtx, "notice", "Redis version found by RedisGears : %d.%d.%d - %s",
                    currVesion.redisMajorVersion, currVesion.redisMinorVersion, currVesion.redisPatchVersion,
	                IsEnterprise() ? (gearsIsCrdt ? "enterprise-crdt" : "enterprise") : "oss");
    if (IsEnterprise()) {
        RedisModule_Log(staticCtx, "notice", "Redis Enterprise version found by RedisGears : %d.%d.%d-%d",
                        gearsRlecMajorVersion, gearsRlecMinorVersion, gearsRlecPatchVersion, gearsRlecBuild);
    }

    if(GearsCheckSupportedVersion() != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Redis version is to old, please upgrade to redis %d.%d.%d and above.", supportedVersion.redisMajorVersion,
                                                                                                                supportedVersion.redisMinorVersion,
                                                                                                                supportedVersion.redisPatchVersion);
        return REDISMODULE_ERR;
    }

    if(LockHandler_Initialize() != REDISMODULE_OK){
	    RedisModule_Log(staticCtx, "warning", "could not initialize lock handler");
        return REDISMODULE_ERR;
    }

    if(RedisGears_RegisterApi(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "could not register RedisGears api");
        return REDISMODULE_ERR;
    }

    if(GearsConfig_Init(ctx, argv, argc) != REDISMODULE_OK){
    	RedisModule_Log(staticCtx, "warning", "could not initialize gears config");
    	return REDISMODULE_ERR;
    }

    Record_Initialize();

    if(CommandHook_Init(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "could not initialize command hooker");
        return REDISMODULE_ERR;
    }

    if(KeysReader_Initialize(ctx) != REDISMODULE_OK){
    	RedisModule_Log(staticCtx, "warning", "could not initialize default keys reader.");
		return REDISMODULE_ERR;
    }

    if(CommandReader_Initialize(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "could not initialize default keys reader.");
        return REDISMODULE_ERR;
    }

    if (RMAPI_FUNC_SUPPORTED(RedisModule_SubscribeToServerEvent)) {
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, RedisGears_OnShutDown);
    }

    Mgmt_Init();

    Cluster_Init();

    RGM_RegisterReader(KeysReader);
    RGM_RegisterReader(StreamReader);
    RGM_RegisterReader(CommandReader);
    RGM_RegisterReader(ShardIDReader);
    RGM_RegisterMap(GetValueMapper, NULL);
    ExecutionPlan_Initialize();

    Cluster_RegisterMsgReceiverM(RG_OnDropExecutionMsgReceived);
    Cluster_RegisterMsgReceiverM(RG_NetworkTest);

    if(RedisGears_CreateGearsDataType(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "failed create RedisGear DataType");
        return REDISMODULE_ERR;
    }

    RedisModule_RegisterInfoFunc(ctx, RedisGears_InfoFunc);

    if(RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, RedisGears_OnLoadingEvent) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Could not subscribe to loaded events");
        return REDISMODULE_ERR;
    }

    if(Command_Init() != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "could not initialize commands.");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.refreshcluster", Cluster_RefreshCluster, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.clusterset", Cluster_ClusterSet, "readonly", 0, 0, -1) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_CLUSTER_SET_FROM_SHARD_COMMAND, Cluster_ClusterSetFromShard, "readonly", 0, 0, -1) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "RG_CLUSTER_SET_FROM_SHARD_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.infocluster", Cluster_GetClusterInfo, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.getclusterinfo");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.hello", Cluster_RedisGearsHello, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.getclusterinfo");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_MSG_COMMAND, Cluster_OnMsgArrive, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "RG_INNER_MSG_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_REGISTER_COMMAND, ExecutionPlan_InnerRegister, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "RG_INNER_REGISTER_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.dumpexecutions", ExecutionPlan_ExecutionsDump, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.dumpexecutions");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.dumpregistrations", ExecutionPlan_DumpRegistrations, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.dumpregistrations");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, RG_INNER_UNREGISTER_COMMAND, ExecutionPlan_InnerUnregisterExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "RG_INNER_UNREGISTER_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.unregister", ExecutionPlan_UnregisterExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.unregister");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.getexecution", Command_ExecutionGet, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.getexecution");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.getresults", Command_GetResults, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.getresults");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.getresultsblocking", Command_GetResultsBlocking, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.getresultsblocking");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.dropexecution", Command_DropExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.dropexecution");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.abortexecution", Command_AbortExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.abortexecution");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pauseregistrations", Command_PauseOrUnpausedRegistrations, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pauseregistrations");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.unpauseregistrations", Command_PauseOrUnpausedRegistrations, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.unpauseregistrations");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.clearregistrationsstats", Command_FlushRegistrationsStats, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.clearregistrationsstats");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.networktest", Command_NetworkTest, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.networktest");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.dumpplugins", Command_DumpPlugins, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.dumpplugins");
        return REDISMODULE_ERR;
    }

    plugins = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    if (RedisGears_InitializePlugins(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "RedisGears plugin initialization failed");
        return REDISMODULE_ERR;
    }

    isInitiated = true;
    return REDISMODULE_OK;
}
