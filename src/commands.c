#include "commands.h"
#include "cluster.h"
#include "record.h"
#include "execution_plan.h"
#include "lock_handler.h"
#include "mgmt.h"

#include <unistd.h>

static ExecutionThreadPool* mgmtPool;
static WorkerData* mgmtWorker;

#define STRING_TYPE_VERSION 1

void Command_ReturnResult(RedisModuleCtx* rctx, Record* record){
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

        LockHandler_Release(ctx);
        if(gearsCtx->abort){
            gearsCtx->abort(gearsCtx->abortPD);
        }
        usleep(1000);
    }
    RedisModule_Assert(false);
    return NULL;
}

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(argc != 2){
		return RedisModule_WrongArity(ctx);
	}

	VERIFY_CLUSTER_INITIALIZE(ctx);

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

    VERIFY_CLUSTER_INITIALIZE(ctx);

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);

    const char* id = RedisModule_StringPtrLen(argv[1], NULL);

    char* err = NULL;
    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, &err);
    if(!fep){
        if(!err){
            err = RG_STRDUP("Failed creating abort Flat Execution Plan");
        }
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
        return REDISMODULE_OK;
    }
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

static int Command_StringSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
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

static Record* Command_CallReplyToRecord(RedisModuleCallReply *rep){
    if(rep == NULL){
        return RedisGears_StringRecordCreate(RG_STRDUP("NULL reply"), strlen("NULL reply"));
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_UNKNOWN){
        return RedisGears_StringRecordCreate(RG_STRDUP("Unknow reply"), strlen("Unknow reply"));
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL){
        return RedisGears_StringRecordCreate(RG_STRDUP("NULL reply type"), strlen("NULL reply type"));
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_STRING ||
            RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char *err = RedisModule_CallReplyStringPtr(rep, &len);
        char* errCStr = RG_ALLOC(len + 1);
        memcpy(errCStr, err, len);
        errCStr[len] = '\0';
        return RedisGears_StringRecordCreate(errCStr, len);
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER){
        long long val = RedisModule_CallReplyInteger(rep);
        return RedisGears_LongRecordCreate(val);
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY){
        size_t len = RedisModule_CallReplyLength(rep);
        Record* listRecord = RedisGears_ListRecordCreate(RedisModule_CallReplyLength(rep));
        for(size_t i = 0 ; i < len ; ++i){
            RedisModuleCallReply* innerRep = RedisModule_CallReplyArrayElement(rep, i);
            Record* innerRecord = Command_CallReplyToRecord(innerRep);
            RedisGears_ListRecordAdd(listRecord, innerRecord);
        }

        return listRecord;
    }

    RedisModule_Assert(false);
    return NULL;
}

Record* Command_MirrorMapper(ExecutionCtx* rctx, Record *data, void* arg){
    return data;
}

Record* Command_SingleShardGetter(ExecutionCtx* rctx, Record *data, void* arg){
    const char* executionId = arg;
    RedisGears_FreeRecord(data);
    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);

    RedisModule_ThreadSafeContextLock(ctx);

    RedisModuleCallReply *rep = RedisModule_Call(ctx, "rg.getexecution", "cc", executionId, "SHARD");

    RedisModule_ThreadSafeContextUnlock(ctx);

    if(!rep){
        RedisGears_SetError(rctx, RG_STRDUP("Got NULL reply"));
        return NULL;
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_UNKNOWN){
        RedisModule_FreeCallReply(rep);
        RedisGears_SetError(rctx, RG_STRDUP("Unknow reply type"));
        return NULL;
    }

    if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char *err = RedisModule_CallReplyStringPtr(rep, &len);
        char* errCStr = RG_ALLOC(len + 1);
        memcpy(errCStr, err, len);
        errCStr[len] = '\0';
        RedisGears_SetError(rctx, errCStr);
        RedisModule_FreeCallReply(rep);
        return NULL;
    }

    Record* ret = Command_CallReplyToRecord(rep);
    RedisModule_FreeCallReply(rep);
    return ret;
}

static void onDoneResultsOnly(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResults(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

int Command_ExecutionGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2 || argc > 3){
        return RedisModule_WrongArity(ctx);
    }

    const char* id = RedisModule_StringPtrLen(argv[1], NULL);
    bool bClusterPlan = Cluster_IsClusterMode();

    ExecutionPlan* ep = RedisGears_GetExecution(id);

    if(!ep){
        RedisModule_ReplyWithError(ctx, "execution plan does not exist");
        return REDISMODULE_OK;
    }

    if(argc == 3){
        const char *subcommand = RedisModule_StringPtrLen(argv[2], NULL);
        if(!strcasecmp(subcommand, "shard")){
            bClusterPlan = false;
        }else if(!strcasecmp(subcommand, "cluster")){
            if(!bClusterPlan){
                RedisModule_ReplyWithError(ctx, "no cluster detected - use `RG.GETEXECUTION <id> [SHARD]` instead");
                return REDISMODULE_OK;
            }
        }else{
            RedisModule_ReplyWithError(ctx, "unknown subcommand");
            return REDISMODULE_OK;
        }
    }

    if(bClusterPlan){
        char* err = NULL;
        const char *eid = RedisModule_StringPtrLen(argv[1], NULL);
        FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, &err);
        if(!fep){
            if(!err){
                err = RG_STRDUP("Failed creating flat execution plan");
            }
            RedisModule_ReplyWithError(ctx, err);
            RG_FREE(err);
            return REDISMODULE_OK;
        }
        RGM_Map(fep, Command_SingleShardGetter, RG_STRDUP(eid));
        RGM_Collect(fep);
        RGM_FlatMap(fep, Command_MirrorMapper, NULL);
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        ExecutionPlan* ep = RGM_Run(fep, ExecutionModeAsync, NULL, onDoneResultsOnly, bc, &err);
        RedisGears_FreeFlatExecution(fep);
        if(!ep){
            RedisModule_AbortBlock(bc);
            if(!err){
                err = RG_STRDUP("Failed creating execution plan");
            }
            RedisModule_ReplyWithError(ctx, err);
            RG_FREE(err);
            return REDISMODULE_OK;
        }

    }else{
        RedisModule_ReplyWithArray(ctx, 1);
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithStringBuffer(ctx, "shard_id", strlen("shard_id"));
        char myId[REDISMODULE_NODE_ID_LEN];
        if(Cluster_IsClusterMode()){
            memcpy(myId, Cluster_GetMyId(), REDISMODULE_NODE_ID_LEN);
        }else{
            memset(myId, '0', REDISMODULE_NODE_ID_LEN);
        }
        RedisModule_ReplyWithStringBuffer(ctx, myId, REDISMODULE_NODE_ID_LEN);
        RedisModule_ReplyWithStringBuffer(ctx, "execution_plan", strlen("execution_plan"));
        RedisModule_ReplyWithArray(ctx, 16);
        RedisModule_ReplyWithStringBuffer(ctx, "status", strlen("status"));
        RedisModule_ReplyWithStringBuffer(ctx, statusesNames[ep->status], strlen(statusesNames[ep->status]));
        RedisModule_ReplyWithStringBuffer(ctx, "shards_received", strlen("shards_received"));
        RedisModule_ReplyWithLongLong(ctx, ep->totalShardsRecieved);
        RedisModule_ReplyWithStringBuffer(ctx, "shards_completed", strlen("shards_completed"));
        RedisModule_ReplyWithLongLong(ctx, ep->totalShardsCompleted);
        RedisModule_ReplyWithStringBuffer(ctx, "results", strlen("results"));
        // TODO: once results and errors are linked lists we can provide more insight here
        if(RedisGears_IsDone(ep)){
            RedisModule_ReplyWithLongLong(ctx, RedisGears_GetRecordsLen(ep));
        }else{
            RedisModule_ReplyWithLongLong(ctx, -1);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "errors", strlen("errors"));
        if(RedisGears_IsDone(ep)){
            long long errorsLen = RedisGears_GetErrorsLen(ep);
            RedisModule_ReplyWithArray(ctx,errorsLen);
            for(long long i = 0; i < errorsLen; i++){
                Record* error = RedisGears_GetError(ep, i);
                size_t errorStrLen;
                char* errorStr = RedisGears_StringRecordGet(error, &errorStrLen);
                RedisModule_ReplyWithStringBuffer(ctx, errorStr, errorStrLen);
            }
        }else{
            RedisModule_ReplyWithArray(ctx, 0);
        }

        RedisModule_ReplyWithStringBuffer(ctx, "total_duration", strlen("total_duration"));
        RedisModule_ReplyWithLongLong(ctx, DURATION2MS(FlatExecutionPlan_GetExecutionDuration(ep)));
        RedisModule_ReplyWithStringBuffer(ctx, "read_duration", strlen("read_duration"));
        RedisModule_ReplyWithLongLong(ctx, DURATION2MS(FlatExecutionPlan_GetReadDuration(ep)));

        uint32_t fstepsLen = array_len(ep->fep->steps);
        RedisModule_ReplyWithStringBuffer(ctx, "steps", strlen("steps"));
        RedisModule_ReplyWithArray(ctx, fstepsLen);
        for(size_t i = 0; i < fstepsLen; i++){
            ExecutionStep *step = ep->steps[i];
            FlatExecutionStep fstep = ep->fep->steps[fstepsLen - i - 1];
            RedisModule_ReplyWithArray(ctx, 8);
            RedisModule_ReplyWithStringBuffer(ctx, "type", strlen("type"));
            RedisModule_ReplyWithStringBuffer(ctx, stepsNames[step->type], strlen(stepsNames[step->type]));
            RedisModule_ReplyWithStringBuffer(ctx, "duration", strlen("duration"));
            RedisModule_ReplyWithLongLong(ctx, DURATION2MS(step->executionDuration));
            RedisModule_ReplyWithStringBuffer(ctx, "name", strlen("name"));
            RedisModule_ReplyWithStringBuffer(ctx, fstep.bStep.stepName, strlen(fstep.bStep.stepName));
            RedisModule_ReplyWithStringBuffer(ctx, "arg", strlen("arg"));
            ExecutionStepArg arg = fstep.bStep.arg;
            if(arg.stepArg){
                ArgType* type = arg.type;
                if(type && type->tostring){
                    char* argCstr = type->tostring(arg.stepArg);
                    RedisModule_ReplyWithStringBuffer(ctx, argCstr, strlen(argCstr));
                    RG_FREE(argCstr);
                }else{
                    RedisModule_ReplyWithStringBuffer(ctx, "", strlen(""));
                }
            }else{
                RedisModule_ReplyWithStringBuffer(ctx, "", strlen(""));
            }
        }
    }
    return REDISMODULE_OK;
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
    RGM_RegisterMap(Command_AbortExecutionMap, stringType);
    RGM_RegisterMap(Command_SingleShardGetter, stringType);
    RGM_RegisterMap(Command_MirrorMapper, NULL);
    return REDISMODULE_OK;
}


