#include "commands.h"
#include "utils/arr_rm_alloc.h"
#include "cluster.h"
#include "record.h"
#ifdef WITHPYTHON
#include <Python.h>
#include <object.h>
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
        if(PyList_Check(obj)){
			listLen = PyList_Size(obj);
			Record* rgl = RedisGears_ListRecordCreate(listLen);
			for(int i = 0 ; i < listLen ; ++i){
				Record* temp = RG_PyObjRecordCreate();
				PyObject* pItem = PyList_GetItem(obj, i);
				RG_PyObjRecordSet(temp, pItem);
				Py_INCREF(pItem);
				RedisGears_ListRecordAdd(rgl, temp);
			}			
			Command_ReturnResult(rctx, rgl);
			RedisGears_FreeRecord(rgl);
        }else if(PyLong_Check(obj)) {
			RedisModule_ReplyWithLongLong(rctx, PyLong_AsLongLong(obj));
		}else if(PyFloat_Check(obj)){
		    double d = PyFloat_AsDouble(obj);
		    RedisModuleString* str = RedisModule_CreateStringPrintf(NULL, "%lf", d);
		    RedisModule_ReplyWithString(rctx, str);
		    RedisModule_FreeString(NULL, str);
		}else if(PyUnicode_Check(obj)) {
		    size_t len;
		    str = (char*)PyUnicode_AsUTF8AndSize(obj, &len);
            RedisModule_ReplyWithStringBuffer(rctx, (char*)str, len);
        }else{
            RedisModule_ReplyWithStringBuffer(rctx, "PY RECORD", strlen("PY RECORD"));
        }
        break;
#endif
    case ERROR_RECORD:
        str = RedisGears_StringRecordGet(record, &listLen);
        RedisModule_ReplyWithError(rctx, str);
        break;
    default:
        assert(false);
    }
}

void Command_ReturnResults(ExecutionPlan* gearsCtx, RedisModuleCtx *ctx){
	long long len = RedisGears_GetRecordsLen(gearsCtx);
	RedisModule_ReplyWithArray(ctx, len);
	for(long long i = 0 ; i < len ; ++i){
		Record* r = RedisGears_GetRecord(gearsCtx, i);
		Command_ReturnResult(ctx, r);
	}
}

void Command_ReturnErrors(ExecutionPlan* gearsCtx, RedisModuleCtx *ctx){
	long long len = RedisGears_GetErrorsLen(gearsCtx);
	RedisModule_ReplyWithArray(ctx, len);
	for(long long i = 0 ; i < len ; ++i){
		Record* error = RedisGears_GetError(gearsCtx, i);
		size_t errorStrLen;
		char* errorStr = RedisGears_StringRecordGet(error, &errorStrLen);
		RedisModule_ReplyWithStringBuffer(ctx, errorStr, errorStrLen);
	}
}

void Command_ReturnResultsAndErrors(ExecutionPlan* gearsCtx, RedisModuleCtx *ctx){
	RedisModule_ReplyWithArray(ctx, 2);
	Command_ReturnResults(gearsCtx, ctx);
	Command_ReturnErrors(gearsCtx, ctx);
}

static void Command_ExecutionDone(ExecutionPlan* gearsCtx, void *privateData){
	RedisModuleBlockedClient** bc = privateData;
	for(size_t i = 0 ; i < array_len(bc) ; ++i){
		RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc[i]);
		Command_ReturnResultsAndErrors(gearsCtx, rctx);
		RedisModule_UnblockClient(bc[i], NULL);
		RedisModule_FreeThreadSafeContext(rctx);
	}
}

int Command_GetResults(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc < 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* gearsCtx = RedisGears_GetExecution(id);

	if(!gearsCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exist");
		return REDISMODULE_OK;
	}

	if(!RedisGears_IsDone(gearsCtx)){
		RedisModule_ReplyWithError(ctx, "execution is still running");
		return REDISMODULE_OK;
	}

	Command_ReturnResultsAndErrors(gearsCtx, ctx);
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
	ExecutionPlan* gearsCtx = RedisGears_GetExecution(id);

	if(!gearsCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
	if(RedisGears_RegisterExecutionDoneCallback(gearsCtx, Command_ExecutionDone)){
		RedisModuleBlockedClient **blockClients = RedisGears_GetPrivateData(gearsCtx);
		if(!blockClients){
			blockClients = array_new(RedisModuleBlockedClient*, 10);
			RedisGears_SetPrivateData(gearsCtx, blockClients, Command_FreePrivateData);
		}
		blockClients = array_append(blockClients, bc);
		return REDISMODULE_OK;
	}
	RedisModule_AbortBlock(bc);
	Command_ReturnResultsAndErrors(gearsCtx, ctx);
	return REDISMODULE_OK;
}

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* gearsCtx = RedisGears_GetExecution(id);

	if(!gearsCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exits");
		return REDISMODULE_OK;
	}

	if(!RedisGears_IsDone(gearsCtx)){
		RedisModule_ReplyWithError(ctx, "can not drop a running execution");
		return REDISMODULE_OK;
	}

	RedisGears_DropExecution(gearsCtx);

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
//    FlatExecutionPlan* gearsCtx = RedisGears_GetFlatExecution(name);
//
//    if(!gearsCtx){
//        RedisModule_ReplyWithError(ctx, "flat execution plan does not exits");
//        return REDISMODULE_OK;
//    }
//
//    ExecutionPlan* ep = RedisGears_Run(gearsCtx, arg, NULL, NULL);
//    const char* id = RedisGears_GetId(ep);
//    RedisModule_ReplyWithStringBuffer(ctx, id, strlen(id));
//    return REDISMODULE_OK;
}


