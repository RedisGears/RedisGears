#include "command_reader.h"
#include "redisgears_memory.h"
#include "execution_plan.h"
#include "record.h"
#include "lock_handler.h"
#include "version.h"
#include <string.h>

#define COMMAND_FLAG_MOVEABLEKEYS (1 << 0)
#define COMMAND_FLAG_NOSCRIPT (1 << 1)

typedef struct CommandReaderTriggerInfo{
    int arity;
    int firstKey;
    int lastKey;
    int jump;
    int commandFlags;
}CommandReaderTriggerInfo;

typedef struct CommandReaderTriggerArgs{
    char* trigger;
    CommandReaderTriggerInfo* info;
    char* keyPrefix; // if not NULL, fire if any of the keys start with this prefix.
}CommandReaderTriggerArgs;

CommandReaderTriggerInfo* CommandReaderTriggerArgs_CreateInfo(const char* trigger){
    CommandReaderTriggerInfo* res = NULL;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);

    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "COMMAND", "cc", "INFO", trigger);
    LockHandler_Release(ctx);

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_UNKNOWN){
        // command was blocked ... someone must have override the 'COMMAND' command :)
        goto done;
    }

    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    RedisModuleCallReply *cReply = RedisModule_CallReplyArrayElement(reply, 0);
    if(RedisModule_CallReplyType(cReply) == REDISMODULE_REPLY_NULL){
        goto done;
    }

    res = RG_ALLOC(sizeof(*res));

    RedisModuleCallReply *arityReply = RedisModule_CallReplyArrayElement(cReply, 1);
    RedisModule_Assert(RedisModule_CallReplyType(arityReply) == REDISMODULE_REPLY_INTEGER);
    res->arity = RedisModule_CallReplyInteger(arityReply);

    RedisModuleCallReply *flagsReply = RedisModule_CallReplyArrayElement(cReply, 2);
    RedisModule_Assert(RedisModule_CallReplyType(flagsReply) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(flagsReply) ; ++i){
        RedisModuleCallReply *flagReply = RedisModule_CallReplyArrayElement(flagsReply, i);
        RedisModule_Assert(RedisModule_CallReplyType(flagReply) == REDISMODULE_REPLY_STRING);
        size_t flagReplyLen;
        const char* flagStr = RedisModule_CallReplyStringPtr(flagReply, &flagReplyLen);
        char flagCStr[flagReplyLen + 1];
        memcpy(flagCStr, flagStr, flagReplyLen);
        flagCStr[flagReplyLen] = '\0';
        if(strcasecmp(flagCStr, "movablekeys") == 0){
            res->commandFlags |= COMMAND_FLAG_MOVEABLEKEYS;
        }
        if(strcasecmp(flagCStr, "noscript") == 0){
            res->commandFlags |= COMMAND_FLAG_NOSCRIPT;
        }
    }

    RedisModuleCallReply *firstKeyReply = RedisModule_CallReplyArrayElement(cReply, 3);
    RedisModule_Assert(RedisModule_CallReplyType(firstKeyReply) == REDISMODULE_REPLY_INTEGER);
    res->firstKey = RedisModule_CallReplyInteger(firstKeyReply);

    RedisModuleCallReply *lastKeyReply = RedisModule_CallReplyArrayElement(cReply, 4);
    RedisModule_Assert(RedisModule_CallReplyType(lastKeyReply) == REDISMODULE_REPLY_INTEGER);
    res->lastKey = RedisModule_CallReplyInteger(lastKeyReply);

    RedisModuleCallReply *jumpReply = RedisModule_CallReplyArrayElement(cReply, 5);
    RedisModule_Assert(RedisModule_CallReplyType(jumpReply) == REDISMODULE_REPLY_INTEGER);
    res->jump = RedisModule_CallReplyInteger(jumpReply);

done:
    RedisModule_FreeCallReply(reply);
    RedisModule_FreeThreadSafeContext(ctx);
    return res;
}

CommandReaderTriggerArgs* CommandReaderTriggerArgs_Create(const char* trigger, const char* keyPrefix){
    CommandReaderTriggerArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->trigger = RG_STRDUP(trigger);
    ret->keyPrefix = keyPrefix ? RG_STRDUP(keyPrefix) : NULL;
    ret->info = CommandReaderTriggerArgs_CreateInfo(trigger);
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
    Gears_listNode* listNode;
    bool currentlyRunning;
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
            .wd = RedisGears_WorkerDataCreate(fep->executionThreadPool),
            .listNode = NULL,
            .currentlyRunning = false,
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

static int CommandReader_Serialize(ExecutionCtx* ectx, void* ctx, Gears_BufferWriter* bw){
    CommandReaderCtx* readerCtx = ctx;
    RG_SerializeRecord(ectx, bw, readerCtx->args->argv);
    return REDISMODULE_OK;
}

static int CommandReader_Deserialize(ExecutionCtx* ectx, void* ctx, Gears_BufferReader* br){
    CommandReaderCtx* readerCtx = ctx;
    Record* argv = RG_DeserializeRecord(ectx, br);
    readerCtx->args = CommandReaderArgs_CreateFromRecord(argv);
    return REDISMODULE_OK;
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
    Gears_dictEntry *existing;
    Gears_dictEntry *entry = Gears_dictAddRaw(CommandRegistrations, crtArgs->trigger, &existing);
    if(entry){
        Gears_dictGetVal(entry) = Gears_listCreate();
    }else{
        entry = existing;
    }
    CommandReaderTriggerCtx* crtCtx = CommandReaderTriggerCtx_Create(fep, mode, crtArgs);
    Gears_listAddNodeHead(Gears_dictGetVal(entry), crtCtx);
    crtCtx->listNode = Gears_listLast(((Gears_list*)Gears_dictGetVal(entry)));
}

static int CommandReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    if(!CommandRegistrations){
        CommandRegistrations = Gears_dictCreate(&Gears_dictTypeHeapStringsCaseInsensitive, NULL);
    }
    CommandReaderTriggerArgs* crtArgs = args;

    if((crtArgs->info) && (crtArgs->info->commandFlags & COMMAND_FLAG_NOSCRIPT)){
        *err = RG_STRDUP("Can not override a command which are not allowed inside a script");
        return REDISMODULE_ERR;
    }

    if((crtArgs->info) && (crtArgs->info->commandFlags & COMMAND_FLAG_MOVEABLEKEYS) && (crtArgs->keyPrefix)){
        // we can not override a command by key prefix and moveable keys
        *err = RG_STRDUP("Can not override a command with moveable keys by key prefix");
        return REDISMODULE_ERR;
    }

    if(crtArgs->keyPrefix){
        if(!crtArgs->info){
            *err = RG_STRDUP("Can not override a new command by key prefix");
            return REDISMODULE_ERR;
        }
        if(crtArgs->info->firstKey <= 0){
            // should not really happened
            *err = RG_STRDUP("Can not override a command by key prefix with none positive first key");
            return REDISMODULE_ERR;
        }

        if(crtArgs->info->jump <= 0){
            // should not really happened
            *err = RG_STRDUP("Can not override a command by key prefix with none positive jump");
            return REDISMODULE_ERR;
        }
    }

    CommandReader_InnerRegister(fep, mode, crtArgs);
    return REDISMODULE_OK;
}

static CommandReaderTriggerCtx* CommandReader_FindByFep(FlatExecutionPlan* fep){
    Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
    Gears_dictEntry *entry = NULL;
    CommandReaderTriggerCtx* crtCtx = NULL;
    while((entry = Gears_dictNext(iter))){
        Gears_list* tempCrtCtxs = Gears_dictGetVal(entry);
        Gears_listIter* iter = Gears_listGetIterator(tempCrtCtxs, AL_START_HEAD);
        Gears_listNode* node = NULL;
        while((node = Gears_listNext(iter))){
            CommandReaderTriggerCtx* tempCrtCtx = Gears_listNodeValue(node);
            if(tempCrtCtx->fep == fep){
                crtCtx = tempCrtCtx;
                break;
            }
        }
        Gears_listReleaseIterator(iter);
        if(crtCtx){
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
        RedisModule_Assert(crtCtx->listNode);
        Gears_list* l = Gears_dictFetchValue(CommandRegistrations, crtCtx->args->trigger);
        Gears_listDelNode(l, crtCtx->listNode);
        crtCtx->listNode = NULL;
        if(Gears_listLength(l) == 0){
            Gears_listRelease(l);
            Gears_dictDelete(CommandRegistrations, crtCtx->args->trigger);
        }
        CommandReaderTriggerCtx_Free(crtCtx);
    }
}

static void CommandReader_SerializeArgs(void* var, Gears_BufferWriter* bw){
    CommandReaderTriggerArgs* crtArgs = var;
    RedisGears_BWWriteString(bw, crtArgs->trigger);
    if(crtArgs->keyPrefix){
        RedisGears_BWWriteLong(bw, 1);
        RedisGears_BWWriteString(bw, crtArgs->keyPrefix);
    }else{
        RedisGears_BWWriteLong(bw, 0);
    }
}

static void* CommandReader_DeserializeArgs(Gears_BufferReader* br, int encver){
    const char* command = RedisGears_BRReadString(br);
    const char* keyPrefix = NULL;
    if(RedisGears_BRReadLong(br)){
        keyPrefix = RedisGears_BRReadString(br);
    }
    return CommandReaderTriggerArgs_Create(command, keyPrefix);
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
        Gears_list* l = Gears_dictGetVal(entry);
        RedisModule_SaveSigned(rdb, Gears_listLength(l));
        Gears_listIter* listIter = Gears_listGetIterator(l, AL_START_HEAD);
        Gears_listNode* node = NULL;
        while((node = Gears_listNext(listIter))){
            CommandReaderTriggerCtx* crtCtx = Gears_listNodeValue(node);
            RedisModule_SaveSigned(rdb, crtCtx->mode);

            int res = FlatExecutionPlan_Serialize(&bw, crtCtx->fep, NULL);
            RedisModule_Assert(res == REDISMODULE_OK); // fep already registered, must be serializable.

            CommandReader_SerializeArgs(crtCtx->args, &bw);

            RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

            Gears_BufferClear(buff);
        }
    }
    Gears_dictReleaseIterator(iter);
    Gears_BufferFree(buff);
}

static int CommandReader_RdbLoad(RedisModuleIO *rdb, int encver){
    long numTriggerts = RedisModule_LoadSigned(rdb);
    for(size_t i = 0 ; i < numTriggerts ; ++i){
        long numRegistrations = 1;
        if(encver >= VERSION_WITH_MULTI_REGISTRATION_PER_TRIGGER){
            numRegistrations = RedisModule_LoadSigned(rdb);
        }
        for(size_t j = 0 ; j < numRegistrations ; ++j){
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
                RedisModule_Free(data);
                return REDISMODULE_ERR;
            }

            CommandReaderTriggerArgs* crtArgs = CommandReader_DeserializeArgs(&br, encver);
            RedisModule_Free(data);
            if(!crtArgs){
                RedisModule_Log(NULL, "warning", "Could not deserialize flat execution args");
                FlatExecutionPlan_Free(fep);
                return REDISMODULE_ERR;
            }


            int ret = CommandReader_RegisrterTrigger(fep, mode, crtArgs, &err);
            if(ret != REDISMODULE_OK){
                RedisModule_Log(NULL, "warning", "Could not register on rdbload execution, error='%s'", err);
                CommandReaderTriggerArgs_Free(crtArgs);
                FlatExecutionPlan_Free(fep);
                return REDISMODULE_ERR;
            }

            FlatExecutionPlan_AddToRegisterDict(fep);
        }
    }
    return REDISMODULE_OK;
}

static void CommandReader_Clear(){
    if(!CommandRegistrations){
        return;
    }
    Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        Gears_list* l = Gears_dictGetVal(entry);
        Gears_listIter* listIter = Gears_listGetIterator(l, AL_START_HEAD);
        Gears_listNode* node = NULL;
        while((node = Gears_listNext(listIter))){
            CommandReaderTriggerCtx* crtCtx = Gears_listNodeValue(node);
            FlatExecutionPlan_RemoveFromRegisterDict(crtCtx->fep);
            CommandReaderTriggerCtx_Free(crtCtx);
        }
        Gears_listReleaseIterator(listIter);
        Gears_listRelease(l);
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

static void CommandReader_ReturnResults(ExecutionPlan* gearsCtx, RedisModuleCtx *ctx){
    long long len = RedisGears_GetRecordsLen(gearsCtx);
    if(len == 0){
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    }
    if(len > 1){
        RedisModule_ReplyWithArray(ctx, len);
    }
    for(long long i = 0 ; i < len ; ++i){
        Record* r = RedisGears_GetRecord(gearsCtx, i);
        Command_ReturnResult(ctx, r);
    }
}

static void CommandReader_Reply(ExecutionPlan* ep, RedisModuleCtx* ctx){
    long long errorsLen = RedisGears_GetErrorsLen(ep);
    if(errorsLen > 0){
        Record* r = RedisGears_GetError(ep, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        const char* lastError = RedisGears_StringRecordGet(r, NULL);
        RedisModule_ReplyWithError(ctx, lastError);
    }else{
        CommandReader_ReturnResults(ep, ctx);
    }
}

static void CommandReader_OnDoneReply(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient* bc = privateData;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(bc);
    CommandReader_Reply(ep, ctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(ctx);
    RedisGears_DropExecution(ep);
}

static void CommandReader_OnDone(ExecutionPlan* ep, void* privateData){
    CommandReaderTriggerCtx* crtCtx = privateData;

    long long errorsLen = RedisGears_GetErrorsLen(ep);

    Gears_dictDelete(crtCtx->pendingExections, ep->idStr);

    if(errorsLen > 0){
        ++crtCtx->numFailures;
        Record* r = RedisGears_GetError(ep, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        if(crtCtx->lastError){
            RG_FREE(crtCtx->lastError);
        }
        crtCtx->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));
    } else if(ep->status == ABORTED){
        ++crtCtx->numAborted;
    } else {
        ++crtCtx->numSuccess;
    }

    CommandReaderTriggerCtx_Free(crtCtx);
}

CommandReaderTriggerCtx* currTCtx = NULL;

static int CommandReader_Trigger(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }
    if(!CommandRegistrations){
        RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
        return REDISMODULE_OK;
    }

    CommandReaderTriggerCtx* crtCtx = currTCtx;

    if(!crtCtx){
        const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
        Gears_list* l = Gears_dictFetchValue(CommandRegistrations, subCommand);
        if(!l){
            RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
            return REDISMODULE_OK;
        }

        Gears_listIter* iter = Gears_listGetIterator(l, AL_START_HEAD);
        Gears_listNode* node = NULL;
        for(node = Gears_listNext(iter) ; node ; node = Gears_listNext(iter), crtCtx = NULL){
            crtCtx = Gears_listNodeValue(node);
            if(crtCtx->currentlyRunning){
                continue;
            }
            if(!crtCtx->args->info){
                // found the first command lets break
                break;
            }
        }
        Gears_listReleaseIterator(iter);
        if(!crtCtx){
            RedisModule_ReplyWithError(ctx, "ERR subcommand not found, notice that for command override you should not use RG.TRIGGER (just send the command as is).");
            return REDISMODULE_OK;
        }
    }

    crtCtx->currentlyRunning = true;

    char* err = NULL;
    CommandReaderArgs* args = CommandReaderArgs_Create(argv + 1, argc - 1);
    ExecutionPlan* ep = RedisGears_Run(crtCtx->fep, crtCtx->mode, args, CommandReader_OnDone,
                                       CommandReaderTriggerCtx_GetShallowCopy(crtCtx), crtCtx->wd, &err);

    ++crtCtx->numTriggered;

    if(!ep){
        // error accurred
        ++crtCtx->numAborted;
        char* msg;
        rg_asprintf(&msg, "ERR Could not trigger execution, %s", err);
        if(err){
            RG_FREE(err);
        }
        RedisModule_ReplyWithError(ctx, msg);
        RG_FREE(msg);
        CommandReaderArgs_Free(args);
        return REDISMODULE_OK;
    }else if(EPIsFlagOn(ep, EFDone)){
        CommandReader_Reply(ep, ctx);
        RedisGears_DropExecution(ep);
    } else {
        if(EPIsFlagOn(ep, EFIsLocal)){
            Gears_dictAdd(crtCtx->pendingExections, ep->idStr, NULL);
        }
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        RedisGears_AddOnDoneCallback(ep, CommandReader_OnDoneReply, bc);
    }

    crtCtx->currentlyRunning = false;

    crtCtx = NULL;

    return REDISMODULE_OK;
}

#define GEARS_OVERRIDE_COMMAND "rg.trigger"
RedisModuleString* GearsOverrideCommand = NULL;

void CommandReader_CommandFilter(RedisModuleCommandFilterCtx *filter){
    if(!CommandRegistrations){
        return;
    }

    const RedisModuleString* cmd = RedisModule_CommandFilterArgGet(filter, 0);
    const char* cmdCStr = RedisModule_StringPtrLen(cmd, NULL);
    Gears_list* l = Gears_dictFetchValue(CommandRegistrations, cmdCStr);
    if(!l){
        // command not found
        return;
    }

    currTCtx = NULL;
    Gears_listIter* iter = Gears_listGetIterator(l, AL_START_HEAD);
    Gears_listNode* node = NULL;
    for(node = Gears_listNext(iter); node ; node = Gears_listNext(iter), currTCtx = NULL){

        currTCtx = Gears_listNodeValue(node);

        if(currTCtx->currentlyRunning){
            continue;
        }

        CommandReaderTriggerArgs* triggerArgs = currTCtx->args;

        const char* keyPrefix = triggerArgs->keyPrefix;
        if(!keyPrefix){
            break;
        }

        CommandReaderTriggerInfo* info = triggerArgs->info;
        RedisModule_Assert(info);
        RedisModule_Assert(!(info->commandFlags & COMMAND_FLAG_MOVEABLEKEYS));

        size_t nArgs = RedisModule_CommandFilterArgsCount(filter);
        int first = info->firstKey;
        int last = info->lastKey;
        int jump = info->jump;

        RedisModule_Assert(first > 0);
        RedisModule_Assert(jump > 0);

        if(last < 0){
            last = nArgs + last;
        }

        if(first > last){
            // Could not find any key, must be a command arity error, we will let Redis handle it.
            continue;
        }

        // check command information to decide if we need to override it
        bool keyFound = false;
        for(size_t i = first ; i <= last ; i+=jump){
            const RedisModuleString* key = RedisModule_CommandFilterArgGet(filter, i);
            const char* keyCStr = RedisModule_StringPtrLen(key, NULL);
            if(strncmp(keyPrefix, keyCStr, strlen(triggerArgs->keyPrefix)) == 0){
                keyFound = true;
                break;
            }
        }

        if(keyFound){
            break;
        }
    }

    Gears_listReleaseIterator(iter);

    if(currTCtx){
        RedisModule_RetainString(NULL, GearsOverrideCommand);
        RedisModule_CommandFilterArgInsert(filter, 0, GearsOverrideCommand);
    }
}

int CommandReader_Initialize(RedisModuleCtx* ctx){
    GearsOverrideCommand = RedisModule_CreateString(NULL, GEARS_OVERRIDE_COMMAND, strlen(GEARS_OVERRIDE_COMMAND));
    RedisModuleCommandFilter *cmdFilter = RedisModule_RegisterCommandFilter(ctx, CommandReader_CommandFilter, REDISMODULE_CMDFILTER_NOSELF);

    // this command is considered readonly but it might actaully write data to redis
    // using rm_call. In this case the effect of the execution is replicated
    // and not the execution itself.
    if (RedisModule_CreateCommand(ctx, GEARS_OVERRIDE_COMMAND, CommandReader_Trigger, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.command");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
