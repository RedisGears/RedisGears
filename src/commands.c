#include "commands.h"
#include "utils/arr_rm_alloc.h"
#include "cluster.h"
#include "record.h"
#include <Python.h>

/* this cluster refresh is a hack for now, we should come up with a better solution!! */
int Command_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    Cluster_Refresh();
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static void Command_ReturnResult(RedisModuleCtx* rctx, Record* record){
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
        Command_ReturnResult(rctx, RediStar_KeyRecordGetVal(record));
        break;
    case LIST_RECORD:
        listLen = RediStar_ListRecordLen(record);
        RedisModule_ReplyWithArray(rctx, listLen);
        for(int i = 0 ; i < listLen ; ++i){
        	Command_ReturnResult(rctx, RediStar_ListRecordGet(record, i));
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

static void Command_ReturnResults(ExecutionPlan* starCtx, RedisModuleCtx *ctx){
	long long len = RediStar_GetRecordsLen(starCtx);
	RedisModule_ReplyWithArray(ctx, len);
	for(long long i = 0 ; i < len ; ++i){
		Record* r = RediStar_GetRecord(starCtx, i);
		Command_ReturnResult(ctx, r);
	}
}

static void Command_ExecutionDone(ExecutionPlan* starCtx, void *privateData){
	RedisModuleBlockedClient** bc = privateData;
	for(size_t i = 0 ; i < array_len(bc) ; ++i){
		RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc[i]);
		Command_ReturnResults(starCtx, rctx);
		RedisModule_UnblockClient(bc[i], NULL);
		RedisModule_FreeThreadSafeContext(rctx);
	}
}

int Command_GetResults(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* starCtx = RediStar_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RediStar_IsDone(starCtx)){
		RedisModule_ReplyWithError(ctx, "execution is still running");
		return REDISMODULE_OK;
	}

	Command_ReturnResults(starCtx, ctx);
	return REDISMODULE_OK;
}

static void Command_FreePrivateData(void* privateData){
	RedisModuleBlockedClient **blockClients = privateData;
	array_free(blockClients);
}

int Command_GetResultsBlocking(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* starCtx = RediStar_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
	if(RediStar_RegisterExecutionDoneCallback(starCtx, Command_ExecutionDone)){
		RedisModuleBlockedClient **blockClients = RediStar_GetPrivateData(starCtx);
		if(!blockClients){
			blockClients = array_new(RedisModuleBlockedClient*, 10);
			RediStar_SetPrivateData(starCtx, blockClients, Command_FreePrivateData);
		}
		blockClients = array_append(blockClients, bc);
		return REDISMODULE_OK;
	}
	RedisModule_AbortBlock(bc);
	Command_ReturnResults(starCtx, ctx);
	return REDISMODULE_OK;
}

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* starCtx = RediStar_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RediStar_IsDone(starCtx)){
		RedisModule_ReplyWithError(ctx, "can not drop a running execution");
		return REDISMODULE_OK;
	}

	RediStar_DropExecution(starCtx, ctx);

	RedisModule_ReplyWithSimpleString(ctx, "OK");

	return REDISMODULE_OK;
}


