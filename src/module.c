/*
 * module.c
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

#include "redistar.h"
#include "redismodule.h"
#include "version.h"
#include "mgmt.h"
#include "execution_plan.h"
#include "example.h"
#include "cluster.h"
#include "utils/arr_rm_alloc.h"
#include "utils/buffer.h"
#include "redistar_memory.h"
#ifdef WITHPYTHON
#include "redistar_python.h"
#endif
#include "record.h"
#include "commands.h"
#include <stdbool.h>

#define EXECUTION_PLAN_FREE_MSG 6

#define REGISTER_API(name, registerApiCallback) \
    if(registerApiCallback("RediStar_" #name, RS_ ## name)){\
        printf("could not register RediStar_" #name "\r\n");\
        return false;\
    }

static int RS_RegisterReader(char* name, RediStar_ReaderCallback reader, ArgType* type){
    return ReadersMgmt_Add(name, reader, type);
}

static int RS_RegisterWriter(char* name, RediStar_WriterCallback writer, ArgType* type){
    return WritersMgmt_Add(name, writer, type);
}

static int RS_RegisterMap(char* name, RediStar_MapCallback map, ArgType* type){
    return MapsMgmt_Add(name, map, type);
}

static int RS_RegisterFilter(char* name, RediStar_FilterCallback filter, ArgType* type){
    return FiltersMgmt_Add(name, filter, type);
}

static int RS_RegisterGroupByExtractor(char* name, RediStar_ExtractorCallback extractor, ArgType* type){
    return ExtractorsMgmt_Add(name, extractor, type);
}

static int RS_RegisterReducer(char* name, RediStar_ReducerCallback reducer, ArgType* type){
    return ReducersMgmt_Add(name, reducer, type);
}

static RediStarCtx* RS_CreateCtx(char* name, char* readerName, void* arg){
    RediStarCtx* res = RS_ALLOC(sizeof(*res));
    res->fep = FlatExecutionPlan_New(name);
    res->ep = NULL;
    FlatExecutionPlan_SetReader(res->fep, readerName, arg);
    return res;
}

static int RS_Map(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_AddMapStep(ctx->fep, name, arg);
    return 1;
}

static int RS_Filter(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_AddFilterStep(ctx->fep, name, arg);
    return 1;
}

static int RS_GroupBy(RediStarCtx* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg){
    FlatExecutionPlan_AddGroupByStep(ctx->fep, extraxtorName, extractorArg, reducerName, reducerArg);
    return 1;
}

static int RS_Collect(RediStarCtx* ctx){
	FlatExecutionPlan_AddCollectStep(ctx->fep);
	return 1;
}

static int RS_Write(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_SetWriter(ctx->fep, name, arg);
    ctx->ep = FlatExecutionPlan_Run(ctx->fep, NULL, NULL);
    RS_FREE(ctx);
    return 1;
}

static int RS_Run(RediStarCtx* ctx, RediStar_OnExecutionDoneCallback callback, void* privateData){
	ctx->ep = FlatExecutionPlan_Run(ctx->fep, callback, privateData);
	return 1;
}

static bool RS_RegisterExecutionDoneCallback(RediStarCtx* ctx, RediStar_OnExecutionDoneCallback callback){
	if(ctx->ep->isDone){
		return false;
	}
	ctx->ep->callback = callback;
	return true;
}

static void RS_FreeCtx(RediStarCtx* ctx){
	RS_FREE(ctx);
}

static bool RS_IsDone(RediStarCtx* ctx){
	return ctx->ep && ctx->ep->isDone;
}

static long long RS_GetRecordsLen(RediStarCtx* ctx){
	assert(ctx->ep && ctx->ep->isDone);
	return array_len(ctx->ep->results);
}

static void* RS_GetPrivateData(RediStarCtx* ctx){
	return ctx->ep->privateData;
}

static void RS_SetPrivateData(RediStarCtx* ctx, void* privateData, FreePrivateData freeCallback){
	ctx->ep->privateData = privateData;
	ctx->ep->freeCallback = freeCallback;
}

static Record* RS_GetRecord(RediStarCtx* ctx, long long i){
	assert(ctx->ep && ctx->ep->isDone);
	assert(i >= 0 && i < array_len(ctx->ep->results));
	return ctx->ep->results[i];
}

static void RS_DropExecution(RediStarCtx* starCtx, RedisModuleCtx* ctx){
	if(Cluster_IsClusterMode()){
		RedisModule_SendClusterMessage(ctx, NULL, EXECUTION_PLAN_FREE_MSG, starCtx->fep->id, EXECUTION_PLAN_ID_LEN);
	}
	ExecutionPlan_Free(starCtx->ep, ctx);
}

static RediStarCtx* RS_GetCtxById(const char* id){
	ExecutionPlan* ep =	ExecutionPlan_FindById(id);
	if(!ep){
		return NULL;
	}
	RediStarCtx* res = RS_ALLOC(sizeof(*res));
	res->fep = ep->fep;
	res->ep = ep;
	return res;
}

static RediStarCtx* RS_GetCtxByName(const char* name){
	ExecutionPlan* ep =	ExecutionPlan_FindByName(name);
	if(!ep){
		return NULL;
	}
	RediStarCtx* res = RS_ALLOC(sizeof(*res));
	res->fep = ep->fep;
	res->ep = ep;
	return res;
}

static ArgType* RS_CreateType(char* name, ArgFree free, ArgSerialize serialize, ArgDeserialize deserialize){
    ArgType* ret = RS_ALLOC(sizeof(*ret));
    *ret = (ArgType){
        .type = RS_STRDUP(name),
        .free = free,
        .serialize = serialize,
        .deserialize = deserialize,
    };
    return ret;
}

static void RS_BWWriteLong(BufferWriter* bw, long val){
    BufferWriter_WriteLong(bw, val);
}

static void RS_BWWriteString(BufferWriter* bw, char* str){
    BufferWriter_WriteString(bw, str);
}

static void RS_BWWriteBuffer(BufferWriter* bw, char* buff, size_t len){
    BufferWriter_WriteBuff(bw, buff, len);
}

static long RS_BRReadLong(BufferReader* br){
    return BufferReader_ReadLong(br);
}

static char* RS_BRReadString(BufferReader* br){
    return BufferReader_ReadString(br);
}

static char* RS_BRReadBuffer(BufferReader* br, size_t* len){
    return BufferReader_ReadBuff(br, len);
}


static bool RediStar_RegisterApi(int (*registerApiCallback)(const char *funcname, void *funcptr)){
    REGISTER_API(CreateType, registerApiCallback);
    REGISTER_API(BWWriteLong, registerApiCallback);
    REGISTER_API(BWWriteString, registerApiCallback);
    REGISTER_API(BWWriteBuffer, registerApiCallback);
    REGISTER_API(BRReadLong, registerApiCallback);
    REGISTER_API(BRReadString, registerApiCallback);
    REGISTER_API(BRReadBuffer, registerApiCallback);

    REGISTER_API(RegisterReader, registerApiCallback);
    REGISTER_API(RegisterWriter, registerApiCallback);
    REGISTER_API(RegisterMap, registerApiCallback);
    REGISTER_API(RegisterFilter, registerApiCallback);
    REGISTER_API(RegisterGroupByExtractor, registerApiCallback);
    REGISTER_API(RegisterReducer, registerApiCallback);
    REGISTER_API(CreateCtx, registerApiCallback);
    REGISTER_API(Map, registerApiCallback);
    REGISTER_API(Filter, registerApiCallback);
    REGISTER_API(GroupBy, registerApiCallback);
    REGISTER_API(Collect, registerApiCallback);
    REGISTER_API(Write, registerApiCallback);
    REGISTER_API(Run, registerApiCallback);

    REGISTER_API(GetCtxByName, registerApiCallback);
    REGISTER_API(GetCtxById, registerApiCallback);
    REGISTER_API(FreeCtx, registerApiCallback);
    REGISTER_API(IsDone, registerApiCallback);
    REGISTER_API(GetRecordsLen, registerApiCallback);
    REGISTER_API(GetRecord, registerApiCallback);
    REGISTER_API(RegisterExecutionDoneCallback, registerApiCallback);
    REGISTER_API(GetPrivateData, registerApiCallback);
	REGISTER_API(SetPrivateData, registerApiCallback);
	REGISTER_API(DropExecution, registerApiCallback);

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

    return true;
}

static void RS_OnDropExecutionMsgReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	RediStarCtx* starCtx = RediStar_GetCtxById(payload);
	if(!starCtx){
		// todo: write warning
		return;
	}
	ExecutionPlan_Free(starCtx->ep, ctx);
	RediStar_FreeCtx(starCtx);
}

ArgType* GetKeysReaderArgType();
ArgType* GetKeysWriterArgType();

bool apiRegistered = false;

int RedisModule_RegisterApi(int (*registerApiCallback)(const char *funcname, void *funcptr)) {
	if(!RediStar_RegisterApi(registerApiCallback)){
		return REDISMODULE_ERR;
	}
	apiRegistered = true;
	return REDISMODULE_OK;
}

int moduleRegisterApi(const char *funcname, void *funcptr);

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "RediStar", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(!apiRegistered){
        if(!RediStar_RegisterApi(moduleRegisterApi)){
            RedisModule_Log(ctx, "warning", "could not register RediStar api\r\n");
            return REDISMODULE_ERR;
        }
    }

    if(!RediStar_Initialize()){
        RedisModule_Log(ctx, "warning", "could not initialize RediStar api\r\n");
        return REDISMODULE_ERR;
    }

    Mgmt_Init();

    RSM_RegisterReader(KeysReader, GetKeysReaderArgType());
    RSM_RegisterWriter(ReplyWriter, GetKeysWriterArgType());
    RSM_RegisterGroupByExtractor(KeyRecordStrValueExtractor, NULL);
    RSM_RegisterReducer(CountReducer, NULL);

    ExecutionPlan_Initialize(ctx, 1);

#ifdef WITHPYTHON
    RediStarPy_Init(ctx);
#endif

    RedisModule_RegisterClusterMessageReceiver(ctx, EXECUTION_PLAN_FREE_MSG, RS_OnDropExecutionMsgReceived);

    if (RedisModule_CreateCommand(ctx, "rs.example", Example_CommandCallback, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command example");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rs.refreshcluster", Command_RefreshCluster, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rs.dumpexecutions", ExecutionPlan_ExecutionsDump, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.getresults", Command_GetResults, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.getresultsblocking", Command_GetResultsBlocking, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.dropexecution", Command_DropExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    return REDISMODULE_OK;
}



