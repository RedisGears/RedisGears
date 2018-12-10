#include "commands.h"
#include "utils/arr_rm_alloc.h"
#include "cluster.h"
#include "record.h"
#ifdef WITHPYTHON
#include <Python.h>
#endif

static void Command_ReturnResult(RedisModuleCtx* rctx, Record* record){
    size_t listLen;
    char* str;
#ifdef WITHPYTHON
    PyObject* obj;
#endif
    switch(RedisGears_RecordGetType(record)){
    case STRING_RECORD:
        str = RedisGears_StringRecordGet(record, &listLen);
        RedisModule_ReplyWithStringBuffer(rctx, str, listLen);
        break;
    case LONG_RECORD:
        RedisModule_ReplyWithLongLong(rctx, RedisGears_LongRecordGet(record));
        break;
    case DOUBLE_RECORD:
        RedisModule_ReplyWithDouble(rctx, RedisGears_DoubleRecordGet(record));
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_ReplyWithStringBuffer(rctx, "KEY HANDLER RECORD", strlen("KEY HANDLER RECORD"));
        break;
    case KEY_RECORD:
        RedisModule_ReplyWithArray(rctx, 2);
        size_t keyLen;
        char* key = RedisGears_KeyRecordGetKey(record, &keyLen);
        RedisModule_ReplyWithStringBuffer(rctx, key, keyLen);
        Command_ReturnResult(rctx, RedisGears_KeyRecordGetVal(record));
        break;
    case LIST_RECORD:
        listLen = RedisGears_ListRecordLen(record);
        RedisModule_ReplyWithArray(rctx, listLen);
        for(int i = 0 ; i < listLen ; ++i){
        	Command_ReturnResult(rctx, RedisGears_ListRecordGet(record, i));
        }
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        obj = RG_PyObjRecordGet(record);
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

void Command_ReturnResults(ExecutionPlan* starCtx, RedisModuleCtx *ctx){
	long long len = RedisGears_GetRecordsLen(starCtx);
	RedisModule_ReplyWithArray(ctx, len);
	for(long long i = 0 ; i < len ; ++i){
		Record* r = RedisGears_GetRecord(starCtx, i);
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
	ExecutionPlan* starCtx = RedisGears_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RedisGears_IsDone(starCtx)){
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
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* starCtx = RedisGears_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
	if(RedisGears_RegisterExecutionDoneCallback(starCtx, Command_ExecutionDone)){
		RedisModuleBlockedClient **blockClients = RedisGears_GetPrivateData(starCtx);
		if(!blockClients){
			blockClients = array_new(RedisModuleBlockedClient*, 10);
			RedisGears_SetPrivateData(starCtx, blockClients, Command_FreePrivateData);
		}
		blockClients = array_append(blockClients, bc);
		return REDISMODULE_OK;
	}
	RedisModule_AbortBlock(bc);
	Command_ReturnResults(starCtx, ctx);
	return REDISMODULE_OK;
}

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* starCtx = RedisGears_GetExecution(id);

	if(!starCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RedisGears_IsDone(starCtx)){
		RedisModule_ReplyWithError(ctx, "can not drop a running execution");
		return REDISMODULE_OK;
	}

	RedisGears_DropExecution(starCtx);

	RedisModule_ReplyWithSimpleString(ctx, "OK");

	return REDISMODULE_OK;
}

int Command_ReExecute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    return REDISMODULE_OK;
    // todo : implement by opening redis key
//    if(argc != 3){
//        return RedisModule_WrongArity(ctx);
//    }
//
//    const char* name = RedisModule_StringPtrLen(argv[1], NULL);
//    char* arg = RG_STRDUP(RedisModule_StringPtrLen(argv[2], NULL));;
//    FlatExecutionPlan* starCtx = RedisGears_GetFlatExecution(name);
//
//    if(!starCtx){
//        RedisModule_ReplyWithError(ctx, "flat execution plan does not exits");
//        return REDISMODULE_OK;
//    }
//
//    ExecutionPlan* ep = RedisGears_Run(starCtx, arg, NULL, NULL);
//    const char* id = RedisGears_GetId(ep);
//    RedisModule_ReplyWithStringBuffer(ctx, id, strlen(id));
//    return REDISMODULE_OK;
}


