/*
 * module.c
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

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
#ifdef WITHPYTHON
#include "redisgears_python.h"
#endif
#include "record.h"
#include "commands.h"
#include "redisdl.h"
#include "globals.h"
#include <stdbool.h>

#define EXECUTION_PLAN_FREE_MSG 100

#define REGISTER_API(name, ctx) \
    if(RedisModule_ExportSharedAPI(ctx, "RedisGears_" #name, RG_ ## name)){\
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


static bool RedisGears_RegisterApi(RedisModuleCtx* ctx){
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
    REGISTER_API(RegisterFilter, ctx);
    REGISTER_API(RegisterGroupByExtractor, ctx);
    REGISTER_API(RegisterReducer, ctx);
    REGISTER_API(CreateCtx, ctx);
    REGISTER_API(Map, ctx);
    REGISTER_API(Accumulate, ctx);
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

    REGISTER_API(GetExecution, ctx);
    REGISTER_API(IsDone, ctx);
    REGISTER_API(GetRecordsLen, ctx);
    REGISTER_API(GetRecord, ctx);
    REGISTER_API(RegisterExecutionDoneCallback, ctx);
    REGISTER_API(GetPrivateData, ctx);
	REGISTER_API(SetPrivateData, ctx);
	REGISTER_API(DropExecution, ctx);
	REGISTER_API(GetId, ctx);

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
    REGISTER_API(HashSetRecordFreeKeysArray, ctx);

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

int RedisGears_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(!RedisGears_RegisterApi(ctx)){
        RedisModule_Log(ctx, "warning", "could not register RedisGears api");
        return REDISMODULE_ERR;
    }

    if(!RedisGears_Initialize(ctx)){
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

    ExecutionPlan_Initialize(ctx, 1);

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



