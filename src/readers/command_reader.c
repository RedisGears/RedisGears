#include "command_reader.h"
#include "redisgears_memory.h"
#include "execution_plan.h"
#include "record.h"

typedef struct CommandReaderTriggerArgs{
    char* trigger;
}CommandReaderTriggerArgs;

CommandReaderTriggerArgs* CommandReaderTriggerArgs_Create(const char* trigger){
    CommandReaderTriggerArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->trigger = RG_STRDUP(trigger);
    return ret;
}

void CommandReaderTriggerArgs_Free(CommandReaderTriggerArgs* crtArgs){
    RG_FREE(crtArgs->trigger);
    RG_FREE(crtArgs);
}

typedef struct CommandReaderTriggerCtx{
    size_t refCount;
    CommandReaderTriggerArgs* args;
    ExecutionMode mode;
    FlatExecutionPlan* fep;
    size_t numTriggered;
    size_t numSuccess;
    size_t numFailures;
    size_t numAborted;
    char* lastError;
    Gears_dict* pendingExections;
    WorkerData* wd;
}CommandReaderTriggerCtx;

Gears_dict* CommandRegistrations = NULL;

static CommandReaderTriggerCtx* CommandReaderTriggerCtx_Create(FlatExecutionPlan* fep, ExecutionMode mode, CommandReaderTriggerArgs* args){
    CommandReaderTriggerCtx* ret = RG_ALLOC(sizeof(*ret));
    *ret = (CommandReaderTriggerCtx){
            .fep = fep,
            .mode = mode,
            .args = args,
            .refCount=1,
            .numTriggered = 0,
            .numSuccess = 0,
            .numFailures = 0,
            .numAborted = 0,
            .lastError = NULL,
            .pendingExections = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
            .wd = RedisGears_WorkerDataCreate(NULL),
    };
    return ret;
}

static CommandReaderTriggerCtx* CommandReaderTriggerCtx_GetShallowCopy(CommandReaderTriggerCtx* crtCtx){
    ++crtCtx->refCount;
    return crtCtx;
}

static void CommandReaderTriggerCtx_Free(CommandReaderTriggerCtx* crtCtx){
    if((--crtCtx->refCount) == 0){
        if(crtCtx->lastError){
            RG_FREE(crtCtx->lastError);
        }
        CommandReaderTriggerArgs_Free(crtCtx->args);
        FlatExecutionPlan_Free(crtCtx->fep);
        Gears_dictRelease(crtCtx->pendingExections);
        RedisGears_WorkerDataFree(crtCtx->wd);
        RG_FREE(crtCtx);
    }
}

typedef struct CommandReaderArgs{
    Record* argv; // list record contains all the arguments given to the command (including the command itself)
}CommandReaderArgs;

typedef struct CommandReaderCtx{
    CommandReaderArgs* args;
}CommandReaderCtx;

CommandReaderArgs* CommandReaderArgs_Create(RedisModuleString** argv, size_t argc){
    CommandReaderArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->argv = RedisGears_ListRecordCreate(argc);
    for(size_t i = 0 ; i < argc ; ++i){
        const char* arg = RedisModule_StringPtrLen(argv[i], NULL);
        Record* strRecord = RedisGears_StringRecordCreate(RG_STRDUP(arg), strlen(arg));
        RedisGears_ListRecordAdd(ret->argv, strRecord);
    }
    return ret;
}

static CommandReaderArgs* CommandReaderArgs_CreateFromRecord(Record* argv){
    CommandReaderArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->argv = argv;
    return ret;
}

void  CommandReaderArgs_Free(CommandReaderArgs* crArgs){
    if(crArgs->argv){
        RedisGears_FreeRecord(crArgs->argv);
    }
    RG_FREE(crArgs);
}

static Record* CommandReader_Next(ExecutionCtx* rctx, void* ctx){
    CommandReaderCtx* readerCtx = ctx;
    if(!readerCtx->args->argv){
        return NULL;
    }
    Record* next = readerCtx->args->argv;
    readerCtx->args->argv = NULL;
    return next;
}

static void CommandReader_Free(void* ctx){
    CommandReaderCtx* readerCtx = ctx;
    CommandReaderArgs_Free(readerCtx->args);
    RG_FREE(readerCtx);
}

static void CommandReader_Reset(void* ctx, void * arg){
    CommandReaderCtx* readerCtx = ctx;
    CommandReaderArgs_Free(readerCtx->args);
    readerCtx->args = arg;
}

static void CommandReader_Serialize(void* ctx, Gears_BufferWriter* bw){
    CommandReaderCtx* readerCtx = ctx;
    char* err = NULL;
    int res = RG_SerializeRecord(bw, readerCtx->args->argv, &err);
    RedisModule_Assert(res == REDISMODULE_OK);
}

static void CommandReader_Deserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){
    CommandReaderCtx* readerCtx = ctx;
    Record* argv = RG_DeserializeRecord(br);
    readerCtx->args = CommandReaderArgs_CreateFromRecord(argv);
}

static Reader* CommandReader_Create(void* arg){
    CommandReaderCtx* readerCtx = RG_ALLOC(sizeof(*readerCtx));
    readerCtx->args = arg;
    Reader* reader = RG_ALLOC(sizeof(*reader));
    *reader = (Reader){
        .ctx = readerCtx,
        .next = CommandReader_Next,
        .free = CommandReader_Free,
        .reset = CommandReader_Reset,
        .serialize = CommandReader_Serialize,
        .deserialize = CommandReader_Deserialize,
    };
    return reader;
}

static void CommandReader_InnerRegister(FlatExecutionPlan* fep, ExecutionMode mode, CommandReaderTriggerArgs* crtArgs){
    CommandReaderTriggerCtx* crtCtx = CommandReaderTriggerCtx_Create(fep, mode, crtArgs);
    Gears_dictAdd(CommandRegistrations, crtArgs->trigger, crtCtx);
}

static int CommandReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    if(!CommandRegistrations){
        CommandRegistrations = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    }
    CommandReaderTriggerArgs* crtArgs = args;
    CommandReaderTriggerCtx* crtCtx = Gears_dictFetchValue(CommandRegistrations, crtArgs->trigger);
    if(crtCtx){
        *err = RG_STRDUP("trigger already registered");
        return REDISMODULE_ERR;
    }

    CommandReader_InnerRegister(fep, mode, crtArgs);
    return REDISMODULE_OK;
}

static CommandReaderTriggerCtx* CommandReader_FindByFep(FlatExecutionPlan* fep){
    Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
    Gears_dictEntry *entry = NULL;
    CommandReaderTriggerCtx* crtCtx = NULL;
    while((entry = Gears_dictNext(iter))){
        CommandReaderTriggerCtx* tempCrtCtx = Gears_dictGetVal(entry);
        if(tempCrtCtx->fep == fep){
            crtCtx = tempCrtCtx;
            break;
        }
    }
    Gears_dictReleaseIterator(iter);
    return crtCtx;
}

static void CommandReader_UnregisterTrigger(FlatExecutionPlan* fep, bool abortPending){
    CommandReaderTriggerCtx* crtCtx = CommandReader_FindByFep(fep);

    if(crtCtx){
        if(abortPending){
            ExecutionPlan** abortEpArray = array_new(ExecutionPlan*, 10);
            Gears_dictIterator *iter = Gears_dictGetIterator(crtCtx->pendingExections);
            Gears_dictEntry *entry = NULL;
            while((entry = Gears_dictNext(iter))){
                char* idStr = Gears_dictGetKey(entry);
                ExecutionPlan* ep = RedisGears_GetExecution(idStr);
                if(!ep){
                    RedisModule_Log(NULL, "warning", "Failed finding pending execution to abort on unregister.");
                    continue;
                }

                // we can not abort right now cause aborting might cause values to be deleted
                // from localPendingExecutions and it will mess up with the iterator
                // so we must collect all the exeuctions first and then abort one by one
                abortEpArray = array_append(abortEpArray, ep);
            }
            Gears_dictReleaseIterator(iter);
            Gears_dictEmpty(crtCtx->pendingExections, NULL);

            for(size_t i = 0 ; i < array_len(abortEpArray) ; ++i){
                // we can not free while iterating so we add to the abortEpArray and free after
                if(RedisGears_AbortExecution(abortEpArray[i]) != REDISMODULE_OK){
                    RedisModule_Log(NULL, "warning", "Failed aborting execution on unregister.");
                }
            }

            array_free(abortEpArray);
        }
        Gears_dictDelete(CommandRegistrations, crtCtx->args->trigger);
        CommandReaderTriggerCtx_Free(crtCtx);
    }
}

static void CommandReader_SerializeArgs(void* var, Gears_BufferWriter* bw){
    CommandReaderTriggerArgs* crtArgs = var;
    RedisGears_BWWriteString(bw, crtArgs->trigger);
}

static void* CommandReader_DeserializeArgs(Gears_BufferReader* br){
    const char* command = RedisGears_BRReadString(br);
    return CommandReaderTriggerArgs_Create(command);
}

static void CommandReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    CommandReaderTriggerCtx* crtCtx = CommandReader_FindByFep(fep);
    RedisModule_Assert(crtCtx);
    RedisModule_ReplyWithArray(ctx, 14);
    RedisModule_ReplyWithStringBuffer(ctx, "mode", strlen("mode"));
    if(crtCtx->mode == ExecutionModeSync){
        RedisModule_ReplyWithStringBuffer(ctx, "sync", strlen("sync"));
    } else if(crtCtx->mode == ExecutionModeAsync){
        RedisModule_ReplyWithStringBuffer(ctx, "async", strlen("async"));
    } else if(crtCtx->mode == ExecutionModeAsyncLocal){
        RedisModule_ReplyWithStringBuffer(ctx, "async_local", strlen("async_local"));
    } else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "numTriggered", strlen("numTriggered"));
    RedisModule_ReplyWithLongLong(ctx, crtCtx->numTriggered);
    RedisModule_ReplyWithStringBuffer(ctx, "numSuccess", strlen("numSuccess"));
    RedisModule_ReplyWithLongLong(ctx, crtCtx->numSuccess);
    RedisModule_ReplyWithStringBuffer(ctx, "numFailures", strlen("numFailures"));
    RedisModule_ReplyWithLongLong(ctx, crtCtx->numFailures);
    RedisModule_ReplyWithStringBuffer(ctx, "numAborted", strlen("numAborted"));
    RedisModule_ReplyWithLongLong(ctx, crtCtx->numAborted);
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(crtCtx->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, crtCtx->lastError, strlen(crtCtx->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "trigger", strlen("trigger"));
    RedisModule_ReplyWithStringBuffer(ctx, crtCtx->args->trigger, strlen(crtCtx->args->trigger));
}

static void CommandReader_RdbSave(RedisModuleIO *rdb){
    if(!CommandRegistrations){
        RedisModule_SaveSigned(rdb, 0);
        return;
    }
    RedisModule_SaveSigned(rdb, Gears_dictSize(CommandRegistrations));
    Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
    Gears_dictEntry *entry = NULL;
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    while((entry = Gears_dictNext(iter))){
        CommandReaderTriggerCtx* crtCtx = Gears_dictGetVal(entry);
        RedisModule_SaveSigned(rdb, crtCtx->mode);

        int res = FlatExecutionPlan_Serialize(&bw, crtCtx->fep, NULL);
        RedisModule_Assert(res == REDISMODULE_OK); // fep already registered, must be serializable.

        CommandReader_SerializeArgs(crtCtx->args, &bw);

        RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

        Gears_BufferClear(buff);
    }
    Gears_dictReleaseIterator(iter);
    Gears_BufferFree(buff);
}

static void CommandReader_RdbLoad(RedisModuleIO *rdb, int encver){
    long numRegistrations = RedisModule_LoadSigned(rdb);
    for(size_t i = 0 ; i < numRegistrations ; ++i){
        ExecutionMode mode = RedisModule_LoadSigned(rdb);

        size_t len;
        char* data = RedisModule_LoadStringBuffer(rdb, &len);

        Gears_Buffer buff = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, &buff);

        char* err = NULL;
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br, &err, encver);
        if(!fep){
            RedisModule_Log(NULL, "warning", "Could not deserialize flat execution, error='%s'", err);
            RedisModule_Assert(false);
        }

        CommandReaderTriggerArgs* crtArgs = CommandReader_DeserializeArgs(&br);
        RedisModule_Free(data);

        int ret = CommandReader_RegisrterTrigger(fep, mode, crtArgs, &err);
        if(ret != REDISMODULE_OK){
            RedisModule_Log(NULL, "warning", "Could not register on rdbload execution, error='%s'", err);
            RedisModule_Assert(false);
        }

        FlatExecutionPlan_AddToRegisterDict(fep);
    }
}

static void CommandReader_Clear(){
    if(!CommandRegistrations){
        return;
    }
    Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        CommandReaderTriggerCtx* crtCtx = Gears_dictGetVal(entry);
        FlatExecutionPlan_RemoveFromRegisterDict(crtCtx->fep);
        CommandReaderTriggerCtx_Free(crtCtx);
    }
    Gears_dictReleaseIterator(iter);

    Gears_dictEmpty(CommandRegistrations, NULL);
}

static void CommandReader_FreeArgs(void* args){
    CommandReaderTriggerArgs_Free(args);
}

RedisGears_ReaderCallbacks CommandReader = {
        .create = CommandReader_Create,
        .registerTrigger = CommandReader_RegisrterTrigger,
        .unregisterTrigger = CommandReader_UnregisterTrigger,
        .serializeTriggerArgs = CommandReader_SerializeArgs,
        .deserializeTriggerArgs = CommandReader_DeserializeArgs,
        .freeTriggerArgs = CommandReader_FreeArgs,
        .dumpRegistratioData = CommandReader_DumpRegistrationData,
        .rdbSave = CommandReader_RdbSave,
        .rdbLoad = CommandReader_RdbLoad,
        .clear = CommandReader_Clear,
};

typedef enum RctxType{
    RctxType_BlockedClient, RctxType_Context
}RctxType;

typedef struct CommandPD{
    union{
        RedisModuleCtx* ctx;
        RedisModuleBlockedClient* bc;
    }rctx;
    RctxType rctxType;
    CommandReaderTriggerCtx* crtCtx;
}CommandPD;

static CommandPD* CommandPD_Create(RedisModuleCtx* ctx, CommandReaderTriggerCtx* crtCtx){
    CommandPD* pd = RG_ALLOC(sizeof(*pd));
    pd->crtCtx = CommandReaderTriggerCtx_GetShallowCopy(crtCtx);
    if(pd->crtCtx->mode == ExecutionModeSync){
        pd->rctx.ctx = ctx;
        pd->rctxType = RctxType_Context;
    }else{
        pd->rctx.bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        pd->rctxType = RctxType_BlockedClient;
    }
    return pd;
}

static void CommandPD_Free(CommandPD* pd){
    CommandReaderTriggerCtx_Free(pd->crtCtx);
    RG_FREE(pd);
}

static void CommandReader_OnDone(ExecutionPlan* ep, void* privateData){
    RedisModuleCtx* rctx;
    CommandPD* pd = privateData;
    CommandReaderTriggerCtx* crtCtx = pd->crtCtx;
    if(pd->rctxType == RctxType_BlockedClient){
        rctx = RedisModule_GetThreadSafeContext(pd->rctx.bc);
    }else{
        rctx = pd->rctx.ctx;
    }

    long long errorsLen = RedisGears_GetErrorsLen(ep);

    if(EPIsFlagOn(ep, EFIsLocal) && crtCtx->mode != ExecutionModeSync){
        Gears_dictDelete(crtCtx->pendingExections, ep->idStr);
    }

    if(errorsLen > 0){
        ++crtCtx->numFailures;
        Record* r = RedisGears_GetError(ep, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        if(crtCtx->lastError){
            RG_FREE(crtCtx->lastError);
        }
        crtCtx->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));
        RedisModule_ReplyWithError(rctx, crtCtx->lastError);
    } else if(ep->status == ABORTED){
        ++crtCtx->numAborted;
        Command_ReturnResults(ep, rctx);
    } else {
        ++crtCtx->numSuccess;
        Command_ReturnResults(ep, rctx);
    }

    if(pd->rctxType == RctxType_BlockedClient){
        RedisModule_UnblockClient(pd->rctx.bc, NULL);
        RedisModule_FreeThreadSafeContext(rctx);
    }

    RedisGears_DropExecution(ep);

    CommandPD_Free(pd);
}

static int CommandReader_Trigger(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }
    if(!CommandRegistrations){
        RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
        return REDISMODULE_OK;
    }

    const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
    CommandReaderTriggerCtx* crtCtx = Gears_dictFetchValue(CommandRegistrations, subCommand);

    if(!crtCtx){
        RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
        return REDISMODULE_OK;
    }

    CommandPD* pd = CommandPD_Create(ctx, crtCtx);

    char* err = NULL;
    CommandReaderArgs* args = CommandReaderArgs_Create(argv + 1, argc - 1);
    ExecutionPlan* ep = RedisGears_Run(crtCtx->fep, crtCtx->mode, args, CommandReader_OnDone, pd, crtCtx->wd, &err);
    if(!ep){
        // error accurred
        ++crtCtx->numAborted;
        if(pd->rctxType == RctxType_BlockedClient){
            RedisModule_AbortBlock(pd->rctx.bc);
        }
        char* msg;
        rg_asprintf(&msg, "ERR Could not trigger execution, %s", err);
        if(err){
            RG_FREE(err);
        }
        RedisModule_ReplyWithError(ctx, msg);
        RG_FREE(msg);
        CommandReaderArgs_Free(args);
        CommandPD_Free(pd);
    }else{
        ++crtCtx->numTriggered;
        if(EPIsFlagOn(ep, EFIsLocal) && crtCtx->mode != ExecutionModeSync){
            Gears_dictAdd(crtCtx->pendingExections, ep->idStr, NULL);
        }
    }
    return REDISMODULE_OK;
}

int CommandReader_Initialize(RedisModuleCtx* ctx){
    // this command is considered readonly but it might actaully right data to redis
    // using rm_call. In this case the effect of the execution is replicated
    // and not the execution itself.
    if (RedisModule_CreateCommand(ctx, "rg.trigger", CommandReader_Trigger, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.command");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
