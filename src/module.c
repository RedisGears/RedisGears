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
#include <stdbool.h>

#define EXECUTION_PLAN_FREE_MSG 6

int moduleRegisterApi(const char *funcname, void *funcptr);

#define REGISTER_API(name) \
    if(moduleRegisterApi("RediStar_" #name, RS_ ## name)){\
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

static bool RS_RegisterExecutionDoneCallback(RediStarCtx* ctx, RediStar_OnExecutionDoneCallback callback, void* privateData){
	if(ctx->ep->isDone){
		return false;
	}
	ctx->ep->callback = callback;
	ctx->ep->privateData = privateData;
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

static Record* RS_GetRecord(RediStarCtx* ctx, long long i){
	assert(ctx->ep && ctx->ep->isDone);
	assert(i >= 0 && i < array_len(ctx->ep->results));
	return ctx->ep->results[i];
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


static bool RediStar_RegisterApi(){
    REGISTER_API(CreateType);
    REGISTER_API(BWWriteLong);
    REGISTER_API(BWWriteString);
    REGISTER_API(BWWriteBuffer);
    REGISTER_API(BRReadLong);
    REGISTER_API(BRReadString);
    REGISTER_API(BRReadBuffer);

    REGISTER_API(RegisterReader);
    REGISTER_API(RegisterWriter);
    REGISTER_API(RegisterMap);
    REGISTER_API(RegisterFilter);
    REGISTER_API(RegisterGroupByExtractor);
    REGISTER_API(RegisterReducer);
    REGISTER_API(CreateCtx);
    REGISTER_API(Map);
    REGISTER_API(Filter);
    REGISTER_API(GroupBy);
    REGISTER_API(Collect);
    REGISTER_API(Write);
    REGISTER_API(Run);

    REGISTER_API(GetCtxByName);
    REGISTER_API(FreeCtx);
    REGISTER_API(IsDone);
    REGISTER_API(GetRecordsLen);
    REGISTER_API(GetRecord);
    REGISTER_API(RegisterExecutionDoneCallback)

    REGISTER_API(FreeRecord);
    REGISTER_API(RecordGetType);
    REGISTER_API(KeyRecordCreate);
    REGISTER_API(KeyRecordSetKey);
    REGISTER_API(KeyRecordSetVal);
    REGISTER_API(KeyRecordGetVal);
    REGISTER_API(KeyRecordGetKey);
    REGISTER_API(ListRecordCreate);
    REGISTER_API(ListRecordLen);
    REGISTER_API(ListRecordAdd);
    REGISTER_API(ListRecordGet);
    REGISTER_API(StringRecordCreate);
    REGISTER_API(StringRecordGet);
    REGISTER_API(StringRecordSet);
    REGISTER_API(DoubleRecordCreate);
    REGISTER_API(DoubleRecordGet);
    REGISTER_API(DoubleRecordSet);
    REGISTER_API(LongRecordCreate);
    REGISTER_API(LongRecordGet);
    REGISTER_API(LongRecordSet);
    REGISTER_API(KeyHandlerRecordCreate);
    REGISTER_API(KeyHandlerRecordGet);

    return true;
}

ArgType* GetKeysReaderArgType();
ArgType* GetKeysWriterArgType();

/* this cluster refresh is a hack for now, we should come up with a better solution!! */

static int RS_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    Cluster_Refresh();
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static void RS_ReturnResult(RedisModuleCtx* rctx, Record* record){
    size_t listLen;
    char* str;
#ifdef WITHPYTHON
    PyObject* obj;
#endif
    switch(RediStar_RecordGetType(record)){
    case STRING_RECORD:
        str = RediStar_StringRecordGet(record);
        RedisModule_ReplyWithStringBuffer(rctx, str, strlen(str));
        break;
    case LONG_RECORD:
        RedisModule_ReplyWithLongLong(rctx, RediStar_LongRecordGet(record));
        break;
    case DOUBLE_RECORD:
        RedisModule_ReplyWithDouble(rctx, RediStar_DoubleRecordGet(record));
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_ReplyWithStringBuffer(rctx, "KEY HANDLER RECORD", strlen("KEY HANDLER RECORD"));
        break;
    case KEY_RECORD:
        RedisModule_ReplyWithArray(rctx, 2);
        size_t keyLen;
        char* key = RediStar_KeyRecordGetKey(record, &keyLen);
        RedisModule_ReplyWithStringBuffer(rctx, key, keyLen);
        RS_ReturnResult(rctx, RediStar_KeyRecordGetVal(record));
        break;
    case LIST_RECORD:
        listLen = RediStar_ListRecordLen(record);
        RedisModule_ReplyWithArray(rctx, listLen);
        for(int i = 0 ; i < listLen ; ++i){
        	RS_ReturnResult(rctx, RediStar_ListRecordGet(record, i));
        }
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        obj = RS_PyObjRecordGet(record);
        if(PyObject_TypeCheck(obj, &PyBaseString_Type)) {
            str = PyString_AsString(obj);
            RedisModule_ReplyWithStringBuffer(rctx, str, strlen(str));
        }else{
            RedisModule_ReplyWithStringBuffer(rctx, "PY RECORD", strlen("PY RECORD"));
        }
        break;
#endif
    default:
        assert(false);
    }
}

static void RS_ReturnResults(RediStarCtx* starCtx, RedisModuleCtx *ctx){
	long long len = RS_GetRecordsLen(starCtx);
	RedisModule_ReplyWithArray(ctx, len);
	for(long long i = 0 ; i < len ; ++i){
		Record* r = RS_GetRecord(starCtx, i);
		RS_ReturnResult(ctx, r);
	}
}

static void RS_ExecutionDone(RediStarCtx* starCtx, void *privateData){
	RedisModuleBlockedClient* bc = privateData;
	RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc);
	RedisModule_ThreadSafeContextLock(rctx);
	RS_ReturnResults(starCtx, rctx);
	RedisModule_ThreadSafeContextUnlock(rctx);
}

int RS_GetResults(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* name = RedisModule_StringPtrLen(argv[1], NULL);
	RediStarCtx* starCtx = RS_GetCtxByName(name);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RS_IsDone(starCtx)){
		RedisModule_ReplyWithError(ctx, "execution is still running");
		return REDISMODULE_OK;
	}

	RS_ReturnResults(starCtx, ctx);

	RS_FreeCtx(starCtx);
	return REDISMODULE_OK;
}

static int RS_GetResultsBlocking(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* name = RedisModule_StringPtrLen(argv[1], NULL);
	RediStarCtx* starCtx = RS_GetCtxByName(name);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
	if(RS_RegisterExecutionDoneCallback(starCtx, RS_ExecutionDone, bc)){
		return REDISMODULE_OK;
	}
	RedisModule_AbortBlock(bc);
	RS_ReturnResults(starCtx, ctx);

	RS_FreeCtx(starCtx);
	return REDISMODULE_OK;
}

static void ExecutionPlan_OnReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
	RediStarCtx* starCtx = RS_GetCtxById(payload);
	if(!starCtx){
		// todo: write warning
		return;
	}
	ExecutionPlan_Free(starCtx->ep, ctx);
	RS_FreeCtx(starCtx);
}

static int RS_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* name = RedisModule_StringPtrLen(argv[1], NULL);
	RediStarCtx* starCtx = RS_GetCtxByName(name);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(Cluster_IsClusterMode()){
		RedisModule_SendClusterMessage(ctx, NULL, EXECUTION_PLAN_FREE_MSG, starCtx->fep->id, EXECUTION_PLAN_ID_LEN);
	}
	ExecutionPlan_Free(starCtx->ep, ctx);

	RS_FreeCtx(starCtx);

	RedisModule_ReplyWithSimpleString(ctx, "OK");

	return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "RediStar", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(!RediStar_RegisterApi()){
        RedisModule_Log(ctx, "warning", "could not register RediStar api\r\n");
        return REDISMODULE_ERR;
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

    RedisModule_RegisterClusterMessageReceiver(ctx, EXECUTION_PLAN_FREE_MSG, ExecutionPlan_OnReceived);

    if (RedisModule_CreateCommand(ctx, "rs.example", Example_CommandCallback, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command example");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rs.refreshcluster", RS_RefreshCluster, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rs.dumpexecutions", ExecutionPlan_ExecutionsDump, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.getresults", RS_GetResults, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.getresultsblocking", RS_GetResultsBlocking, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rs.dropexecution", RS_DropExecution, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rs.refreshcluster");
		return REDISMODULE_ERR;
	}

    return REDISMODULE_OK;
}



