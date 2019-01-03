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
#include "redisdl.h"
#include "globals.h"
#include <stdbool.h>

#define EXECUTION_PLAN_FREE_MSG 100

#define REGISTER_API(name, registerApiCallback) \
    if(registerApiCallback("RedisGears_" #name, RG_ ## name)){\
        printf("could not register RedisGears_" #name "\r\n");\
        return false;\
    }

static int RG_RegisterReader(char* name, RedisGears_ReaderCallback reader){
    return ReadersMgmt_Add(name, reader, NULL);
}

static int RG_RegisterForEach(char* name, RedisGears_ForEachCallback writer, ArgType* type){
    return ForEachsMgmt_Add(name, writer, type);
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

static FlatExecutionPlan* RG_CreateCtx(char* readerName){
    FlatExecutionPlan* fep = FlatExecutionPlan_New();
    FlatExecutionPlan_SetReader(fep, readerName);
    return fep;
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

static int RG_Register(FlatExecutionPlan* fep, char* key){
    return FlatExecutionPlan_Register(fep, key);
}

static ExecutionPlan* RG_Run(FlatExecutionPlan* fep, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData){
	return FlatExecutionPlan_Run(fep, NULL, arg, callback, privateData);
}

static void RG_FreeFlatExecution(FlatExecutionPlan* fep){
    FlatExecutionPlan_Free(fep);
}

static bool RG_RegisterExecutionDoneCallback(ExecutionPlan* ep, RedisGears_OnExecutionDoneCallback callback){
	if(ep->isDone){
		return false;
	}
	ep->callback = callback;
	return true;
}

static bool RG_IsDone(ExecutionPlan* ep){
	return ep->isDone;
}

static const char* RG_GetId(ExecutionPlan* ep){
    return ep->idStr;
}

static long long RG_GetRecordsLen(ExecutionPlan* ep){
	assert(ep->isDone);
	return array_len(ep->results);
}

static void* RG_GetPrivateData(ExecutionPlan* ep){
	return ep->privateData;
}

static void RG_SetPrivateData(ExecutionPlan* ep, void* privateData, FreePrivateData freeCallback){
	ep->privateData = privateData;
	ep->freeCallback = freeCallback;
}

static Record* RG_GetRecord(ExecutionPlan* ep, long long i){
	assert(ep && ep->isDone);
	assert(i >= 0 && i < array_len(ep->results));
	return ep->results[i];
}

static void RG_DropExecution(ExecutionPlan* ep){
    if(Cluster_IsClusterMode()){
        Cluster_SendMsgM(NULL, RG_OnDropExecutionMsgReceived, ep->idStr, strlen(ep->idStr));
    }
    ExecutionPlan_Free(ep);
}

static ExecutionPlan* RG_GetExecution(const char* id){
	ExecutionPlan* ep =	ExecutionPlan_FindByStrId(id);
	return ep;
}

static ArgType* RG_CreateType(char* name, ArgFree free, ArgDuplicate dup, ArgSerialize serialize, ArgDeserialize deserialize){
    ArgType* ret = RG_ALLOC(sizeof(*ret));
    *ret = (ArgType){
        .type = RG_STRDUP(name),
        .free = free,
        .dup = dup,
        .serialize = serialize,
        .deserialize = deserialize,
    };
    return ret;
}

static void RG_BWWriteLong(BufferWriter* bw, long val){
    BufferWriter_WriteLong(bw, val);
}

static void RG_BWWriteString(BufferWriter* bw, char* str){
    BufferWriter_WriteString(bw, str);
}

static void RG_BWWriteBuffer(BufferWriter* bw, char* buff, size_t len){
    BufferWriter_WriteBuff(bw, buff, len);
}

static long RG_BRReadLong(BufferReader* br){
    return BufferReader_ReadLong(br);
}

static char* RG_BRReadString(BufferReader* br){
    return BufferReader_ReadString(br);
}

static char* RG_BRReadBuffer(BufferReader* br, size_t* len){
    return BufferReader_ReadBuff(br, len);
}

static long long RG_GetTotalDuration(ExecutionPlan* ep){
    return FlatExecutionPlan_GetExecutionDuration(ep);
}

static long long RG_GetReadDuration(ExecutionPlan* ep){
	return FlatExecutionPlan_GetReadDuration(ep);
}


static bool RedisGears_RegisterApi(int (*registerApiCallback)(const char *funcname, void *funcptr)){
    REGISTER_API(CreateType, registerApiCallback);
    REGISTER_API(BWWriteLong, registerApiCallback);
    REGISTER_API(BWWriteString, registerApiCallback);
    REGISTER_API(BWWriteBuffer, registerApiCallback);
    REGISTER_API(BRReadLong, registerApiCallback);
    REGISTER_API(BRReadString, registerApiCallback);
    REGISTER_API(BRReadBuffer, registerApiCallback);

    REGISTER_API(RegisterReader, registerApiCallback);
    REGISTER_API(RegisterForEach, registerApiCallback);
    REGISTER_API(RegisterMap, registerApiCallback);
    REGISTER_API(RegisterAccumulator, registerApiCallback);
    REGISTER_API(RegisterAccumulatorByKey, registerApiCallback);
    REGISTER_API(RegisterFilter, registerApiCallback);
    REGISTER_API(RegisterGroupByExtractor, registerApiCallback);
    REGISTER_API(RegisterReducer, registerApiCallback);
    REGISTER_API(CreateCtx, registerApiCallback);
    REGISTER_API(Map, registerApiCallback);
    REGISTER_API(Accumulate, registerApiCallback);
    REGISTER_API(AccumulateBy, registerApiCallback);
    REGISTER_API(FlatMap, registerApiCallback);
    REGISTER_API(Filter, registerApiCallback);
    REGISTER_API(GroupBy, registerApiCallback);
    REGISTER_API(Collect, registerApiCallback);
    REGISTER_API(Repartition, registerApiCallback);
    REGISTER_API(ForEach, registerApiCallback);
    REGISTER_API(Limit, registerApiCallback);
    REGISTER_API(Run, registerApiCallback);
    REGISTER_API(Register, registerApiCallback);
    REGISTER_API(FreeFlatExecution, registerApiCallback);

    REGISTER_API(GetExecution, registerApiCallback);
    REGISTER_API(IsDone, registerApiCallback);
    REGISTER_API(GetRecordsLen, registerApiCallback);
    REGISTER_API(GetRecord, registerApiCallback);
    REGISTER_API(RegisterExecutionDoneCallback, registerApiCallback);
    REGISTER_API(GetPrivateData, registerApiCallback);
	REGISTER_API(SetPrivateData, registerApiCallback);
	REGISTER_API(DropExecution, registerApiCallback);
	REGISTER_API(GetId, registerApiCallback);

    REGISTER_API(FreeRecord, registerApiCallback);
    REGISTER_API(RecordGetType, registerApiCallback);
    REGISTER_API(KeyRecordCreate, registerApiCallback);
    REGISTER_API(KeyRecordSetKey, registerApiCallback);
    REGISTER_API(KeyRecordSetVal, registerApiCallback);
    REGISTER_API(KeyRecordGetVal, registerApiCallback);
    REGISTER_API(KeyRecordGetKey, registerApiCallback);
    REGISTER_API(ListRecordCreate, registerApiCallback);
    REGISTER_API(ListRecordLen, registerApiCallback);
    REGISTER_API(ListRecordAdd, registerApiCallback);
    REGISTER_API(ListRecordGet, registerApiCallback);
    REGISTER_API(ListRecordPop, registerApiCallback);
    REGISTER_API(StringRecordCreate, registerApiCallback);
    REGISTER_API(StringRecordGet, registerApiCallback);
    REGISTER_API(StringRecordSet, registerApiCallback);
    REGISTER_API(DoubleRecordCreate, registerApiCallback);
    REGISTER_API(DoubleRecordGet, registerApiCallback);
    REGISTER_API(DoubleRecordSet, registerApiCallback);
    REGISTER_API(LongRecordCreate, registerApiCallback);
    REGISTER_API(LongRecordGet, registerApiCallback);
    REGISTER_API(LongRecordSet, registerApiCallback);
    REGISTER_API(KeyHandlerRecordCreate, registerApiCallback);
    REGISTER_API(KeyHandlerRecordGet, registerApiCallback);
    REGISTER_API(HashSetRecordCreate, registerApiCallback);
    REGISTER_API(HashSetRecordSet, registerApiCallback);
    REGISTER_API(HashSetRecordGet, registerApiCallback);
    REGISTER_API(HashSetRecordGetAllKeys, registerApiCallback);
    REGISTER_API(HashSetRecordFreeKeysArray, registerApiCallback);

    REGISTER_API(GetTotalDuration, registerApiCallback);
    REGISTER_API(GetReadDuration, registerApiCallback);

    return true;
}

static void RG_OnDropExecutionMsgReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	ExecutionPlan* ep = RedisGears_GetExecution(payload);
	if(!ep){
		printf("warning: execution not found %s !!!\r\n", payload);
		return;
	}
	ExecutionPlan_Free(ep);
}

bool apiRegistered = false;

int RedisModule_RegisterApi(int (*registerApiCallback)(const char *funcname, void *funcptr)) {
	if(!RedisGears_RegisterApi(registerApiCallback)){
		return REDISMODULE_ERR;
	}
	apiRegistered = true;
	return REDISMODULE_OK;
}

int moduleRegisterApi(const char *funcname, void *funcptr);

int RedisGears_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(!apiRegistered){
        if(!RedisGears_RegisterApi(moduleRegisterApi)){
            RedisModule_Log(ctx, "warning", "could not register RedisGears api");
            return REDISMODULE_ERR;
        }
    }

    if(!RedisGears_Initialize()){
        RedisModule_Log(ctx, "warning", "could not initialize RedisGears api");
        return REDISMODULE_ERR;
    }

    if(!RediDL_Initialize()){
        RedisModule_Log(ctx, "warning", "could not initialize RediDL api, running without DL support");
    }else{
        RedisModule_Log(ctx, "notice", "redisdl loaded successfully");
        globals.redisDLLoaded = true;
    }

    Mgmt_Init();

    Cluster_Init();

    RSM_RegisterReader(KeysReader);
    RSM_RegisterReader(StreamReader);
    RSM_RegisterForEach(KeyRecordWriter, NULL);
    RSM_RegisterMap(GetValueMapper, NULL);
    RSM_RegisterFilter(Example_Filter, NULL);
    RSM_RegisterGroupByExtractor(KeyRecordStrValueExtractor, NULL);
    RSM_RegisterReducer(CountReducer, NULL);

    ExecutionPlan_Initialize(1);

#ifdef WITHPYTHON
    RedisGearsPy_Init(ctx);
#endif

    Cluster_RegisterMsgReceiverM(RG_OnDropExecutionMsgReceived);

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

    if (RedisModule_CreateCommand(ctx, RG_INNER_MSG_COMMAND, Cluster_OnMsgArrive, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "RG_INNER_MSG_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.dumpexecutions", ExecutionPlan_ExecutionsDump, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.dumpexecutions");
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

    if (RedisModule_CreateCommand(ctx, "rg.reexecute", Command_ReExecute, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.reexecute");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}



