#include "commands.h"
#include "utils/arr_rm_alloc.h"
#include "cluster.h"
#include "record.h"
#include "execution_plan.h"
#include "lock_handler.h"
#ifdef WITHPYTHON
#include <Python.h>
#include <object.h>
#endif

static ExecutionThreadPool* mgmtPool;
static WorkerData* mgmtWorker;

#define STRING_TYPE_VERSION 1

static void Command_ReturnResult(RedisModuleCtx* rctx, Record* record){
    RG_RecordSendReply(record, rctx);
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
	RedisModuleBlockedClient* bc = privateData;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResultsAndErrors(gearsCtx, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(rctx);
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

int Command_GetResultsBlocking(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* gearsCtx = RedisGears_GetExecution(id);

	if(!gearsCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exist");
		return REDISMODULE_OK;
	}

	RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 1000000);
	if(!RedisGears_AddOnDoneCallback(gearsCtx, Command_ExecutionDone, bc)){
	    RedisModule_AbortBlock(bc);
        Command_ReturnResultsAndErrors(gearsCtx, ctx);
	}
	return REDISMODULE_OK;
}

Record* Command_AbortExecutionMap(ExecutionCtx* rctx, Record *data, void* arg){
    const char* executionId = arg;
    RedisGears_FreeRecord(data);
    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);

    while(true){
        LockHandler_Acquire(ctx);
        ExecutionPlan* gearsCtx = RedisGears_GetExecution(executionId);

        if(!gearsCtx){
            RedisGears_SetError(rctx, RG_STRDUP("execution does not exist"));
            LockHandler_Release(ctx);
            return NULL;
        }

        if(RedisGears_IsDone(gearsCtx)){
            LockHandler_Release(ctx);
            return RedisGears_StringRecordCreate(RG_STRDUP("OK"), strlen("OK"));
        }

        if(gearsCtx->isPaused){
            // abort the execution and execute its Done Actions
            gearsCtx->status = ABORTED;
            EPStatus_DoneAction(gearsCtx);
            LockHandler_Release(ctx);
            return RedisGears_StringRecordCreate(RG_STRDUP("OK"), strlen("OK"));
        }
#ifdef WITHPYTHON
        // execution is running
        unsigned long threadID = (unsigned long)gearsCtx->executionPD;
        LockHandler_Release(ctx);

        RedisGearsPy_ForceStop(threadID);
        usleep(1000);
#else
        LockHandler_Release(ctx);
#endif
    }
    RedisModule_Assert(false);
    return NULL;
}

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	const char* id = RedisModule_StringPtrLen(argv[1], NULL);
	ExecutionPlan* gearsCtx = RedisGears_GetExecution(id);

	if(!gearsCtx){
		RedisModule_ReplyWithError(ctx, "execution plan does not exist");
		return REDISMODULE_OK;
	}

	if(!RedisGears_IsDone(gearsCtx)){
		RedisModule_ReplyWithError(ctx, "can not drop a not yet finished execution, abort it first.");
		return REDISMODULE_OK;
	}

	RedisGears_DropExecution(gearsCtx);

	RedisModule_ReplyWithSimpleString(ctx, "OK");

	return REDISMODULE_OK;
}

static void Command_AbortDone(ExecutionPlan* gearsCtx, void *privateData){
    RedisModuleBlockedClient* bc = privateData;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc);
    if(RedisGears_GetErrorsLen(gearsCtx) > 0){
        Record* error = RedisGears_GetError(gearsCtx, 0);
        RedisModule_ReplyWithError(rctx, RedisGears_StringRecordGet(error, NULL));
    }else{
        RedisModule_ReplyWithSimpleString(rctx, "OK");
    }
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(rctx);

    RedisGears_DropExecution(gearsCtx);
}

int Command_AbortExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);

    const char* id = RedisModule_StringPtrLen(argv[1], NULL);

    char* err = NULL;
    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader);
    RGM_Map(fep, Command_AbortExecutionMap, RG_STRDUP(id));
    RGM_Collect(fep);
    ExecutionPlan* ep = RedisGears_Run(fep, ExecutionModeAsync, NULL, Command_AbortDone, bc, mgmtWorker, &err);
    if(!ep){
        RedisModule_AbortBlock(bc);
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
    }

    RedisGears_FreeFlatExecution(fep);

    return REDISMODULE_OK;
}

static void Command_StringFree(void* arg){
    RG_FREE(arg);
}

static void* Command_StringDup(void* arg){
    return RG_STRDUP(arg);
}

static int Command_StringSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    RedisGears_BWWriteString(bw, arg);
    return REDISMODULE_OK;
}

static void* Command_StringDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > STRING_TYPE_VERSION){
        return NULL;
    }
    const char* id = RedisGears_BRReadString(br);
    return RG_STRDUP(id);
}

static char* Command_StringToString(void* arg){
    return RG_STRDUP(arg);
}

int Command_Init(){
    mgmtPool = ExecutionPlan_CreateThreadPool("MgmtPool", 1);
    mgmtWorker = ExecutionPlan_CreateWorker(mgmtPool);
    ArgType* stringType = RedisGears_CreateType("StringType",
                                                STRING_TYPE_VERSION,
                                                Command_StringFree,
                                                Command_StringDup,
                                                Command_StringSerialize,
                                                Command_StringDeserialize,
                                                Command_StringToString);
    RGM_RegisterMap(Command_AbortExecutionMap, stringType)
    return REDISMODULE_OK;
}


