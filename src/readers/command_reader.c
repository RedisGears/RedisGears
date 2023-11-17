#include "command_reader.h"
#include "redisgears_memory.h"
#include "execution_plan.h"
#include "record.h"
#include "lock_handler.h"
#include "version.h"
#include "cluster.h"
#include "command_hook.h"
#include "readers_common.h"
#include "mgmt.h"

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

typedef enum{
    TriggerType_Trigger, TriggerType_Hook
}TriggerType;

typedef struct CommandReaderTriggerArgs{
    union{
        struct{
            char* hook;
            CommandReaderTriggerInfo info;
            char* keyPrefix; // if not NULL, fire if any of the keys start with this prefix.
        }hookData;
        char* trigger;
    };
    TriggerType triggerType;
    int inOrder; // it true, fire the events in the order they arrive,
                 // i.e, do not start the next event before the last one finished.
}CommandReaderTriggerArgs;

typedef struct CommandReaderTriggerCtx{
    size_t refCount;
    CommandReaderTriggerArgs* args;
    ExecutionMode mode;
    FlatExecutionPlan* fep;
    size_t numTriggered;
    size_t numSuccess;
    size_t numFailures;
    size_t numAborted;
    size_t lastRunDuration;
    size_t totalRunDuration;
    char* lastError;
    Gears_dict* pendingExections;
    Gears_listNode* listNode;
    WorkerData* wd;
}CommandReaderTriggerCtx;

typedef struct CommandReaderArgs{
    Record* argv; // list record contains all the arguments given to the command (including the command itself)
    CommandReaderTriggerCtx* crtCtx;
}CommandReaderArgs;

typedef struct CommandReaderCtx{
    CommandReaderArgs* args;
}CommandReaderCtx;

static CommandReaderTriggerCtx* CommandReader_FindByFep(FlatExecutionPlan* fep);
static void CommandReader_Free(void* ctx);

int CommandReaderTriggerArgs_CreateInfo(CommandReaderTriggerArgs* crtArgs){
    int ret = REDISMODULE_OK;
    CommandReaderTriggerInfo* info = &(crtArgs->hookData.info);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);

    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "COMMAND", "cc", "INFO", crtArgs->hookData.hook);
    LockHandler_Release(ctx);

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_UNKNOWN){
        // command was blocked ... someone must have override the 'COMMAND' command :)
        ret = REDISMODULE_ERR;
        goto done;
    }

    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    RedisModuleCallReply *cReply = RedisModule_CallReplyArrayElement(reply, 0);
    if(RedisModule_CallReplyType(cReply) == REDISMODULE_REPLY_NULL){
        ret = REDISMODULE_ERR;
        goto done;
    }

    RedisModuleCallReply *arityReply = RedisModule_CallReplyArrayElement(cReply, 1);
    RedisModule_Assert(RedisModule_CallReplyType(arityReply) == REDISMODULE_REPLY_INTEGER);
    info->arity = RedisModule_CallReplyInteger(arityReply);

    RedisModuleCallReply *flagsReply = RedisModule_CallReplyArrayElement(cReply, 2);
    RedisModule_Assert(RedisModule_CallReplyType(flagsReply) == REDISMODULE_REPLY_ARRAY);
    info->commandFlags = 0;
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(flagsReply) ; ++i){
        RedisModuleCallReply *flagReply = RedisModule_CallReplyArrayElement(flagsReply, i);
        RedisModule_Assert(RedisModule_CallReplyType(flagReply) == REDISMODULE_REPLY_STRING);
        size_t flagReplyLen;
        const char* flagStr = RedisModule_CallReplyStringPtr(flagReply, &flagReplyLen);
        char flagCStr[flagReplyLen + 1];
        memcpy(flagCStr, flagStr, flagReplyLen);
        flagCStr[flagReplyLen] = '\0';
        if(strcasecmp(flagCStr, "movablekeys") == 0){
            info->commandFlags |= COMMAND_FLAG_MOVEABLEKEYS;
        }
        if(strcasecmp(flagCStr, "noscript") == 0){
            info->commandFlags |= COMMAND_FLAG_NOSCRIPT;
        }
    }

    RedisModuleCallReply *firstKeyReply = RedisModule_CallReplyArrayElement(cReply, 3);
    RedisModule_Assert(RedisModule_CallReplyType(firstKeyReply) == REDISMODULE_REPLY_INTEGER);
    info->firstKey = RedisModule_CallReplyInteger(firstKeyReply);

    RedisModuleCallReply *lastKeyReply = RedisModule_CallReplyArrayElement(cReply, 4);
    RedisModule_Assert(RedisModule_CallReplyType(lastKeyReply) == REDISMODULE_REPLY_INTEGER);
    info->lastKey = RedisModule_CallReplyInteger(lastKeyReply);

    RedisModuleCallReply *jumpReply = RedisModule_CallReplyArrayElement(cReply, 5);
    RedisModule_Assert(RedisModule_CallReplyType(jumpReply) == REDISMODULE_REPLY_INTEGER);
    info->jump = RedisModule_CallReplyInteger(jumpReply);

done:
    RedisModule_FreeCallReply(reply);
    RedisModule_FreeThreadSafeContext(ctx);
    return ret;
}

CommandReaderTriggerArgs* CommandReaderTriggerArgs_CreateTrigger(const char* trigger, int inOrder){
    CommandReaderTriggerArgs* ret = RG_CALLOC(1, sizeof(*ret));
    ret->triggerType = TriggerType_Trigger;
    ret->trigger = RG_STRDUP(trigger);
    ret->inOrder = inOrder;
    return ret;
}

CommandReaderTriggerArgs* CommandReaderTriggerArgs_CreateHook(const char* hook, const char* keyPrefix, int inOrder){
    CommandReaderTriggerArgs* ret = RG_CALLOC(1, sizeof(*ret));
    ret->triggerType = TriggerType_Hook;
    ret->hookData.hook = RG_STRDUP(hook);
    ret->hookData.keyPrefix = keyPrefix ? RG_STRDUP(keyPrefix) : NULL;
    ret->inOrder = inOrder;
    return ret;
}

void CommandReaderTriggerArgs_Free(CommandReaderTriggerArgs* crtArgs){
    switch(crtArgs->triggerType){
    case TriggerType_Trigger:
        RG_FREE(crtArgs->trigger);
        break;
    case TriggerType_Hook:
        RG_FREE(crtArgs->hookData.hook);
        if(crtArgs->hookData.keyPrefix){
            RG_FREE(crtArgs->hookData.keyPrefix);
        }
        break;
    default:
        RedisModule_Assert(false);
    }

    RG_FREE(crtArgs);
}

Gears_dict* CommandRegistrations = NULL;
Gears_dict* HookRegistrations = NULL;

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
            .lastRunDuration = 0,
            .totalRunDuration = 0,
            .lastError = NULL,
            .pendingExections = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
            .listNode = NULL,
            .wd = args->inOrder? RedisGears_WorkerDataCreate(fep->executionThreadPool) : NULL,
    };
    return ret;
}

CommandReaderTriggerCtx* CommandReaderTriggerCtx_Get(ExecutionCtx* eCtx){
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(eCtx);
    ExecutionStep* reader = ep->steps[array_len(ep->steps) - 1];
    RedisModule_Assert(reader->type == READER);
    Reader* r = reader->reader.r;
    // compare one of the reader functions to make sure its the CommandReader
    // its a hack but its faster then compare strings
    if(r->free != CommandReader_Free){
        return NULL;
    }

    CommandReaderCtx* readerCtx = reader->reader.r->ctx;
    return readerCtx->args->crtCtx;
}

CommandReaderTriggerCtx* CommandReaderTriggerCtx_GetShallowCopy(CommandReaderTriggerCtx* crtCtx){
    __atomic_add_fetch(&crtCtx->refCount, 1, __ATOMIC_SEQ_CST);
    return crtCtx;
}

static void CommandReaderTriggerCtx_ResetStats(CommandReaderTriggerCtx* crtCtx){
    resetStats(crtCtx);
}

void CommandReaderTriggerCtx_Free(CommandReaderTriggerCtx* crtCtx){
    if(__atomic_sub_fetch(&crtCtx->refCount, 1, __ATOMIC_SEQ_CST) > 0){
        return;
    }
    if(crtCtx->lastError){
        RG_FREE(crtCtx->lastError);
    }
    CommandReaderTriggerArgs_Free(crtCtx->args);
    Gears_dictRelease(crtCtx->pendingExections);
    FlatExecutionPlan_Free(crtCtx->fep);
    if (crtCtx->wd) {
        RedisGears_WorkerDataFree(crtCtx->wd);
    }
    RG_FREE(crtCtx);
}

CommandReaderArgs* CommandReaderArgs_Create(RedisModuleString** argv, size_t argc, CommandReaderTriggerCtx* crtCtx){
    CommandReaderArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->argv = RedisGears_ListRecordCreate(argc);
    for(size_t i = 0 ; i < argc ; ++i){
        const char* arg = RedisModule_StringPtrLen(argv[i], NULL);
        Record* strRecord = RedisGears_StringRecordCreate(RG_STRDUP(arg), strlen(arg));
        RedisGears_ListRecordAdd(ret->argv, strRecord);
    }
    ret->crtCtx = CommandReaderTriggerCtx_GetShallowCopy(crtCtx);
    return ret;
}

static CommandReaderArgs* CommandReaderArgs_CreateFromRecord(Record* argv){
    CommandReaderArgs* ret = RG_ALLOC(sizeof(*ret));
    ret->argv = argv;
    return ret;
}

void  CommandReaderArgs_Free(CommandReaderArgs* crArgs){
    if(crArgs->crtCtx){
        CommandReaderTriggerCtx_Free(crArgs->crtCtx);
    }
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
    if(readerCtx->args){
        CommandReaderArgs_Free(readerCtx->args);
    }

    RG_FREE(readerCtx);
}

static void CommandReader_Reset(void* ctx, void * arg){
    CommandReaderCtx* readerCtx = ctx;
    if(readerCtx->args){
        CommandReaderArgs_Free(readerCtx->args);
    }

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
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    FlatExecutionPlan* fep = RedisGears_GetFep(ep);
    readerCtx->args->crtCtx = CommandReader_FindByFep(fep);
    if(readerCtx->args->crtCtx){
        readerCtx->args->crtCtx = CommandReaderTriggerCtx_GetShallowCopy(readerCtx->args->crtCtx);
    }
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

static int CommandReader_InnerRegister(FlatExecutionPlan* fep, ExecutionMode mode, CommandReaderTriggerArgs* crtArgs, char** err){
    CommandReaderTriggerCtx* crtCtx = NULL;
    switch(crtArgs->triggerType){
    case TriggerType_Trigger:
        if(!CommandRegistrations){
            CommandRegistrations = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
        }
        crtCtx = CommandReaderTriggerCtx_Create(fep, mode, crtArgs);
        Gears_dictAdd(CommandRegistrations, crtArgs->trigger, crtCtx);
        break;
    case TriggerType_Hook:
        if(!HookRegistrations){
            HookRegistrations = Gears_dictCreate(&Gears_dictTypeHeapStringsCaseInsensitive, NULL);
        }
        Gears_dictEntry *existing;
        Gears_dictEntry *entry = Gears_dictAddRaw(HookRegistrations, crtArgs->trigger, &existing);
        if(entry){
            Gears_dictGetVal(entry) = Gears_listCreate();
        }else{
            entry = existing;
            Gears_list* list = Gears_dictGetVal(entry);
            CommandReaderTriggerCtx* next = Gears_listNodeValue(Gears_listFirst(list));
            if(next->mode != ExecutionModeSync){
                *err = RG_STRDUP("Can not override a none sync registration");
                return REDISMODULE_ERR;
            }
        }
        crtCtx = CommandReaderTriggerCtx_Create(fep, mode, crtArgs);
        Gears_listAddNodeHead(Gears_dictGetVal(entry), crtCtx);
        crtCtx->listNode = Gears_listFirst(((Gears_list*)Gears_dictGetVal(entry)));
        break;
    default:
        RedisModule_Assert(false);
    }
    return REDISMODULE_OK;
}

static int CommandReader_VerifyRegister(SessionRegistrationCtx *srctx, FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    CommandReaderTriggerArgs* crtArgs = args;
    switch(crtArgs->triggerType){
    case TriggerType_Trigger:
        if(CommandRegistrations){
            CommandReaderTriggerCtx* crtCtx = Gears_dictFetchValue(CommandRegistrations, crtArgs->trigger);
            if(crtCtx) {
                // verify that this registration is not going to be removed, if it does we will allow it.
                if (srctx) {
                    int found = 0;
                    for (size_t i = 0 ; i < array_len(srctx->idsToUnregister) ; ++i) {
                        if (strcmp(srctx->idsToUnregister[i], crtCtx->fep->idStr) == 0) {
                            found = 1;
                        }
                    }
                    if (!found) {
                        *err = RG_STRDUP("trigger already registered");
                        return REDISMODULE_ERR;
                    }
                }
            }
        }
        // verify that we do not have another registration on this session with this same trigger
        if (srctx) {
            for (size_t i = 0 ; i < array_len(srctx->registrationsData) ; ++i) {
                RegistrationData *rd = srctx->registrationsData + i;
                RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(rd->fep->reader->reader);
                if (callbacks->verifyRegister == CommandReader_VerifyRegister) {
                    // another command reader found, check its arguments.
                    CommandReaderTriggerArgs* crtArgs2 = rd->args;
                    if (crtArgs2->triggerType == TriggerType_Trigger){
                        if (strcmp(crtArgs2->trigger, crtArgs->trigger) == 0) {
                            *err = RG_STRDUP("trigger already registered in this session");
                            return REDISMODULE_ERR;
                        }
                    }
                }
            }
        }
        break;
    case TriggerType_Hook:
        if(CommandReaderTriggerArgs_CreateInfo(crtArgs) != REDISMODULE_OK){
            *err = RG_STRDUP("Can not override an unexisting command");
            return REDISMODULE_ERR;
        }

        if(crtArgs->hookData.info.commandFlags & COMMAND_FLAG_NOSCRIPT){
            *err = RG_STRDUP("Can not override a command which are not allowed inside a script");
            return REDISMODULE_ERR;
        }

        if((crtArgs->hookData.info.commandFlags & COMMAND_FLAG_MOVEABLEKEYS) && (crtArgs->hookData.keyPrefix)){
            // we can not override a command by key prefix and moveable keys
            *err = RG_STRDUP("Can not override a command with moveable keys by key prefix");
            return REDISMODULE_ERR;
        }

        if(crtArgs->hookData.keyPrefix){
            if(crtArgs->hookData.info.firstKey <= 0){
                // should not really happened
                *err = RG_STRDUP("Can not override a command by key prefix with none positive first key");
                return REDISMODULE_ERR;
            }

            if(crtArgs->hookData.info.jump <= 0){
                // should not really happened
                *err = RG_STRDUP("Can not override a command by key prefix with none positive jump");
                return REDISMODULE_ERR;
            }
        }
        if (HookRegistrations) {
            Gears_list* list = Gears_dictFetchValue(HookRegistrations, crtArgs->trigger);
            if (list) {
                CommandReaderTriggerCtx* next = Gears_listNodeValue(Gears_listFirst(list));
                if(next->mode != ExecutionModeSync){
                    *err = RG_STRDUP("Can not override a none sync registration");
                    return REDISMODULE_ERR;
                }
            }
        }
        // lets also verify that we do not have another none sync registration that hook this command
        if (srctx) {
            for (size_t i = 0 ; i < array_len(srctx->registrationsData) ; ++i) {
                RegistrationData *rd = srctx->registrationsData + i;
                RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(rd->fep->reader->reader);
                if (callbacks->verifyRegister == CommandReader_VerifyRegister) {
                    // another command reader found, check its arguments.
                    CommandReaderTriggerArgs* crtArgs2 = rd->args;
                    if (crtArgs2->triggerType == TriggerType_Hook){
                        if (strcmp(crtArgs2->hookData.hook, crtArgs->hookData.hook) == 0) {
                            if (rd->mode != ExecutionModeSync){
                                *err = RG_STRDUP("Can not override a none sync registration which already created on this session");
                                return REDISMODULE_ERR;
                            }
                        }
                    }
                }
            }
        }
        break;
    default:
        RedisModule_Assert(false);
    }
    return REDISMODULE_OK;
}

static int CommandReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    if (CommandReader_VerifyRegister(NULL, fep, mode, args, err) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    CommandReaderTriggerArgs* crtArgs = args;
    return CommandReader_InnerRegister(fep, mode, crtArgs, err);
}

static CommandReaderTriggerCtx* CommandReader_FindByFep(FlatExecutionPlan* fep){
    CommandReaderTriggerCtx* crtCtx = NULL;

    // search on trigger registrations
    if(CommandRegistrations){
        Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            CommandReaderTriggerCtx* tempCrtCtx = Gears_dictGetVal(entry);
            if(tempCrtCtx->fep == fep){
                crtCtx = tempCrtCtx;
                break;
            }
            if(crtCtx){
                break;
            }
        }
        Gears_dictReleaseIterator(iter);
    }

    if(crtCtx){
        return crtCtx;
    }

    if(HookRegistrations){
        // search on hook registrations
        Gears_dictIterator *iter = Gears_dictGetIterator(HookRegistrations);
        Gears_dictEntry *entry = NULL;
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
    }

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
                    RedisModule_Log(staticCtx, "warning", "Failed finding pending execution to abort on unregister.");
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
                    RedisModule_Log(staticCtx, "warning", "Failed aborting execution on unregister.");
                }
            }

            array_free(abortEpArray);
        }
        switch(crtCtx->args->triggerType){
        case TriggerType_Trigger:
            Gears_dictDelete(CommandRegistrations, crtCtx->args->trigger);
            break;
        case TriggerType_Hook:
            RedisModule_Assert(crtCtx->listNode);
            Gears_list* l = Gears_dictFetchValue(HookRegistrations, crtCtx->args->hookData.hook);
            Gears_listDelNode(l, crtCtx->listNode);
            crtCtx->listNode = NULL;
            if(Gears_listLength(l) == 0){
                Gears_listRelease(l);
                Gears_dictDelete(HookRegistrations, crtCtx->args->hookData.hook);
            }
            break;
        default:
            RedisModule_Assert(false);
        }

        CommandReaderTriggerCtx_Free(crtCtx);
    }
}

static void CommandReader_SerializeArgs(void* var, Gears_BufferWriter* bw){
    CommandReaderTriggerArgs* crtArgs = var;
    RedisGears_BWWriteLong(bw, crtArgs->triggerType);
    RedisGears_BWWriteLong(bw, crtArgs->inOrder);
    switch(crtArgs->triggerType){
    case TriggerType_Trigger:
        RedisGears_BWWriteString(bw, crtArgs->trigger);
        break;
    case TriggerType_Hook:
        RedisGears_BWWriteString(bw, crtArgs->hookData.hook);
        if(crtArgs->hookData.keyPrefix){
            RedisGears_BWWriteLong(bw, 1);
            RedisGears_BWWriteString(bw, crtArgs->hookData.keyPrefix);
        }else{
            RedisGears_BWWriteLong(bw, 0);
        }
        RedisGears_BWWriteBuffer(bw, (char*)(&(crtArgs->hookData.info)), sizeof(CommandReaderTriggerInfo));
        break;
    default:
        RedisModule_Assert(false);
    }
}

static void* CommandReader_DeserializeArgs(Gears_BufferReader* br, int encver){
    const char* trigger;
    const char* hook;
    const char* keyPrefix;
    int inOrder = 0;
    CommandReaderTriggerArgs* crtArgs = NULL;
    TriggerType triggerType = TriggerType_Trigger;
    if(encver >= VERSION_WITH_COMMAND_HOOKS){
        triggerType = RedisGears_BRReadLong(br);
    }
    if (encver >= VERSION_WITH_COMMAND_READER_IN_ORDER) {
        inOrder = RedisGears_BRReadLong(br);
    }
    switch(triggerType){
    case TriggerType_Trigger:
        trigger = RedisGears_BRReadString(br);
        crtArgs = CommandReaderTriggerArgs_CreateTrigger(trigger, inOrder);
        break;
    case TriggerType_Hook:
        hook = RedisGears_BRReadString(br);
        keyPrefix = NULL;
        if(RedisGears_BRReadLong(br)){
            keyPrefix = RedisGears_BRReadString(br);
        }
        crtArgs = CommandReaderTriggerArgs_CreateHook(hook, keyPrefix, inOrder);
        size_t len;
        crtArgs->hookData.info = *((CommandReaderTriggerInfo*)RedisGears_BRReadBuffer(br, &len));
        RedisModule_Assert(len == sizeof(CommandReaderTriggerInfo));
        break;
    default:
        RedisModule_Assert(false);
    }
    return crtArgs;
}

static void CommandReader_DumpRegistrationInfo(FlatExecutionPlan* fep, RedisModuleInfoCtx *ctx, int for_crash_report) {
    CommandReaderTriggerCtx* crtCtx = CommandReader_FindByFep(fep);

    if(crtCtx->mode == ExecutionModeSync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "sync");
    } else if(crtCtx->mode == ExecutionModeAsync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async");
    } else if(crtCtx->mode == ExecutionModeAsyncLocal){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async_local");
    } else{
        RedisModule_InfoAddFieldCString(ctx, "mode", "unknown");
    }

    RedisModule_InfoAddFieldULongLong(ctx, "numTriggered", crtCtx->numTriggered);
    RedisModule_InfoAddFieldULongLong(ctx, "numSuccess", crtCtx->numSuccess);
    RedisModule_InfoAddFieldULongLong(ctx, "numFailures", crtCtx->numFailures);
    RedisModule_InfoAddFieldULongLong(ctx, "numAborted", crtCtx->numAborted);
    RedisModule_InfoAddFieldULongLong(ctx, "lastRunDurationMS", DURATION2MS(crtCtx->lastRunDuration));
    RedisModule_InfoAddFieldULongLong(ctx, "totalRunDurationMS", totalDurationMS(crtCtx));
    RedisModule_InfoAddFieldDouble(ctx, "avgRunDurationMS", avgDurationMS(crtCtx));
    RedisModule_InfoAddFieldCString(ctx, "lastError", crtCtx->lastError ? crtCtx->lastError : "None");
    RedisModule_InfoAddFieldCString(ctx, "trigger", crtCtx->args->trigger);
    RedisModule_InfoAddFieldULongLong(ctx, "inorder", crtCtx->args->inOrder);
}

static void CommandReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    CommandReaderTriggerCtx* crtCtx = CommandReader_FindByFep(fep);
    RedisModule_Assert(crtCtx);
    RedisModule_ReplyWithArray(ctx, 20);
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
    RedisModule_ReplyWithStringBuffer(ctx, "lastRunDurationMS", strlen("lastRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, DURATION2MS(crtCtx->lastRunDuration));
    RedisModule_ReplyWithStringBuffer(ctx, "totalRunDurationMS", strlen("totalRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, totalDurationMS(crtCtx));
    RedisModule_ReplyWithStringBuffer(ctx, "avgRunDurationMS", strlen("avgRunDurationMS"));
    RedisModule_ReplyWithDouble(ctx, avgDurationMS(crtCtx));
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(crtCtx->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, crtCtx->lastError, strlen(crtCtx->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithStringBuffer(ctx, "trigger", strlen("trigger"));
    RedisModule_ReplyWithStringBuffer(ctx, crtCtx->args->trigger, strlen(crtCtx->args->trigger));
    RedisModule_ReplyWithStringBuffer(ctx, "inorder", strlen("inorder"));
    RedisModule_ReplyWithLongLong(ctx, crtCtx->args->inOrder);
}

static void CommandReader_RdbSaveSingleRegistration(RedisModuleIO *rdb, Gears_Buffer* buff, CommandReaderTriggerCtx* crtCtx){
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);

    RedisModule_SaveSigned(rdb, crtCtx->mode);

    char* err = NULL;
    int res = FlatExecutionPlan_Serialize(&bw, crtCtx->fep, &err);
    if(res != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Failed serializing fep, err='%s'", err);
        RedisModule_Assert(false); // fep already registered, must be serializable.
    }


    CommandReader_SerializeArgs(crtCtx->args, &bw);

    RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);

    Gears_BufferClear(buff);
}

static void CommandReader_RdbSave(RedisModuleIO *rdb){
    Gears_Buffer* buff = Gears_BufferCreate();

    // trigger registrations
    if(CommandRegistrations){
        RedisModule_SaveSigned(rdb, Gears_dictSize(CommandRegistrations));
        Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            CommandReaderTriggerCtx* crtCtx = Gears_dictGetVal(entry);
            CommandReader_RdbSaveSingleRegistration(rdb, buff, crtCtx);
        }
        Gears_dictReleaseIterator(iter);
    }else{
        RedisModule_SaveSigned(rdb, 0);
    }

    // hook registrations
    if(HookRegistrations){
        RedisModule_SaveSigned(rdb, Gears_dictSize(HookRegistrations));
        Gears_dictIterator *iter = Gears_dictGetIterator(HookRegistrations);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            Gears_list* l = Gears_dictGetVal(entry);
            RedisModule_SaveSigned(rdb, Gears_listLength(l));
            Gears_listIter* listIter = Gears_listGetIterator(l, AL_START_HEAD);
            Gears_listNode* node = NULL;
            while((node = Gears_listNext(listIter))){
                CommandReaderTriggerCtx* crtCtx = Gears_listNodeValue(node);
                CommandReader_RdbSaveSingleRegistration(rdb, buff, crtCtx);
            }
            Gears_listReleaseIterator(listIter);
        }
        Gears_dictReleaseIterator(iter);
    }else{
        RedisModule_SaveSigned(rdb, 0);
    }

    Gears_BufferFree(buff);
}

static int CommandReader_RdbLoadSingleRegistration(RedisModuleIO *rdb, int encver){
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
        RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution, error='%s'", err);
        RedisModule_Free(data);
        return REDISMODULE_ERR;
    }

    CommandReaderTriggerArgs* crtArgs = CommandReader_DeserializeArgs(&br, encver);
    RedisModule_Free(data);
    if(!crtArgs){
        RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution args");
        FlatExecutionPlan_Free(fep);
        return REDISMODULE_ERR;
    }


    int ret = CommandReader_RegisrterTrigger(fep, mode, crtArgs, &err);
    if(ret != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Could not register on rdbload execution, error='%s'", err);
        CommandReaderTriggerArgs_Free(crtArgs);
        FlatExecutionPlan_Free(fep);
        return REDISMODULE_ERR;
    }

    FlatExecutionPlan_AddToRegisterDict(fep);

    return REDISMODULE_OK;
}

static int CommandReader_RdbLoad(RedisModuleIO *rdb, int encver){
    // load trigger registrations
    long numTriggerts = RedisModule_LoadSigned(rdb);
    for(size_t i = 0 ; i < numTriggerts ; ++i){
        if(CommandReader_RdbLoadSingleRegistration(rdb, encver) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }

    if(encver >= VERSION_WITH_COMMAND_HOOKS){
        // load hook registrations
        long numHooks = RedisModule_LoadSigned(rdb);
        for(size_t i = 0 ; i < numHooks ; ++i){
            long numRegistrations = RedisModule_LoadSigned(rdb);;
            for(size_t j = 0 ; j < numRegistrations ; ++j){
                if(CommandReader_RdbLoadSingleRegistration(rdb, encver) != REDISMODULE_OK){
                    return REDISMODULE_ERR;
                }
            }
        }
    }
    return REDISMODULE_OK;
}

static void CommandReader_ClearStats(){
    // clear triggers
    if(CommandRegistrations){
        Gears_dictIterator *iter = Gears_dictGetIterator(CommandRegistrations);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            CommandReaderTriggerCtx* crtCtx = Gears_dictGetVal(entry);
            CommandReaderTriggerCtx_ResetStats(crtCtx);
        }
        Gears_dictReleaseIterator(iter);
    }

    // clear hooks
    if(HookRegistrations){
        Gears_dictIterator *iter = Gears_dictGetIterator(HookRegistrations);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            Gears_list* l = Gears_dictGetVal(entry);
            Gears_listIter* listIter = Gears_listGetIterator(l, AL_START_HEAD);
            Gears_listNode* node = NULL;
            while((node = Gears_listNext(listIter))){
                CommandReaderTriggerCtx* crtCtx = Gears_listNodeValue(node);
                CommandReaderTriggerCtx_ResetStats(crtCtx);
            }
            Gears_listReleaseIterator(listIter);
        }
        Gears_dictReleaseIterator(iter);
    }
}

static void CommandReader_Clear(){
    // clear triggers
    if(CommandRegistrations){
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

    // clear hooks
    if(HookRegistrations){
        Gears_dictIterator *iter = Gears_dictGetIterator(HookRegistrations);
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
        Gears_dictEmpty(HookRegistrations, NULL);
    }


}

static void CommandReader_FreeArgs(void* args){
    CommandReaderTriggerArgs_Free(args);
}

RedisGears_ReaderCallbacks CommandReader = {
        .create = CommandReader_Create,
        .verifyRegister = CommandReader_VerifyRegister,
        .registerTrigger = CommandReader_RegisrterTrigger,
        .unregisterTrigger = CommandReader_UnregisterTrigger,
        .serializeTriggerArgs = CommandReader_SerializeArgs,
        .deserializeTriggerArgs = CommandReader_DeserializeArgs,
        .freeTriggerArgs = CommandReader_FreeArgs,
        .dumpRegistratioData = CommandReader_DumpRegistrationData,
        .dumpRegistratioInfo = CommandReader_DumpRegistrationInfo,
        .rdbSave = CommandReader_RdbSave,
        .rdbLoad = CommandReader_RdbLoad,
        .clear = CommandReader_Clear,
        .clearStats = CommandReader_ClearStats,
};

typedef enum RctxType{
    RctxType_BlockedClient, RctxType_Context
}RctxType;

static void CommandReader_ReturnResults(ExecutionPlan* gearsCtx, RedisModuleCtx *ctx){
    Command_ReturnResults(gearsCtx, ctx);
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

static void CommandReader_HookReply(ExecutionPlan* ep, RedisModuleCtx* ctx){
    long long errorsLen = RedisGears_GetErrorsLen(ep);
    long long resLen = RedisGears_GetRecordsLen(ep);
    if(errorsLen > 0){
        Record* r = RedisGears_GetError(ep, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        const char* lastError = RedisGears_StringRecordGet(r, NULL);
        RedisModule_ReplyWithError(ctx, lastError);
    }else if(resLen != 1){
        RedisModule_ReplyWithError(ctx, "Command hook must return exactly one result");
    }else{
        Record* r = RedisGears_GetRecord(ep, 0);
        Command_ReturnResult(ctx, r);
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

static void CommandReader_OnDoneHookReply(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient* bc = privateData;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(bc);
    CommandReader_HookReply(ep, ctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(ctx);
    RedisGears_DropExecution(ep);
}

static void CommandReader_OnDone(ExecutionPlan* ep, void* privateData){
    CommandReaderTriggerCtx* crtCtx = privateData;

    long long errorsLen = RedisGears_GetErrorsLen(ep);
    crtCtx->lastRunDuration = FlatExecutionPlan_GetExecutionDuration(ep);
    crtCtx->totalRunDuration += crtCtx->lastRunDuration;

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

static CommandReaderTriggerCtx* currTCtx = NULL;
static bool noOverride = false;

static void CommandReader_ExectionRunningCallback(ExecutionPlan* ep, void* privateData) {
    // privateData is the blocked client
    RedisModule_BlockedClientMeasureTimeStart(privateData);
}

static void CommandReader_ExectionHoldingCallback(ExecutionPlan* ep, void* privateData) {
    // privateData is the blocked client
    RedisModule_BlockedClientMeasureTimeEnd(privateData);
}

static int CommandReader_Trigger(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    /* Handle getkeys-api introspection */
    if (RedisModule_IsKeysPositionRequest(ctx)) {
        if (currTCtx) {
            // Only on command hook we want to check for keys
            if (RMAPI_FUNC_SUPPORTED(RedisModule_GetCommandKeys)) {
                CommandHook_DeclareKeys(ctx, argv + 1, argc - 1);
            } else {
                // fallback to calculate key ourself to support old redis versions.
                CommandReaderTriggerArgs* triggerArgs = currTCtx->args;

                CommandReaderTriggerInfo* info = &(triggerArgs->hookData.info);
                CommandHook_DeclareKeysLegacy(ctx, argc - 1, info->firstKey, info->lastKey, info->jump);
            }
        }
        return REDISMODULE_OK;
    }

    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    void (*replyCallback)(ExecutionPlan*, RedisModuleCtx*);
    void (*onDoneCallback)(ExecutionPlan* ep, void* privateData);

    CommandReaderTriggerCtx* crtCtx = currTCtx;
    currTCtx = NULL;

    if(!crtCtx){
        replyCallback = CommandReader_Reply;
        onDoneCallback = CommandReader_OnDoneReply;
        if(!CommandRegistrations){
            RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
            return REDISMODULE_OK;
        }

        const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
        crtCtx = Gears_dictFetchValue(CommandRegistrations, subCommand);
        if(!crtCtx){
            RedisModule_ReplyWithError(ctx, "ERR subcommand not found");
            return REDISMODULE_OK;
        }
    }else{
        // command hooked, we will just call the original command if this is a replication connection.
        int ctxFlags = RedisModule_GetContextFlags(ctx);
        if((ctxFlags & REDISMODULE_CTX_FLAGS_REPLICATED) ||
                (ctxFlags & REDISMODULE_CTX_FLAGS_LOADING)){
            const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
            noOverride = true;
            RedisModuleCallReply* rep = RedisModule_Call(ctx, subCommand, "!v", argv + 2, argc - 2);
            noOverride = false;
            RedisModule_ReplyWithCallReply(ctx, rep);
            RedisModule_FreeCallReply(rep);
            return REDISMODULE_OK;
        }

        replyCallback = CommandReader_HookReply;
        onDoneCallback = CommandReader_OnDoneHookReply;
    }

    int runFlags = 0;
    int ctxFlags = RedisModule_GetContextFlags(ctx);
    if((ctxFlags & REDISMODULE_CTX_FLAGS_MULTI) ||
       (ctxFlags & REDISMODULE_CTX_FLAGS_LUA) ||
       (ctxFlags & REDISMODULE_CTX_FLAGS_DENY_BLOCKING)){
        if(crtCtx->mode != ExecutionModeSync){
            RedisModule_ReplyWithError(ctx, "ERR can not run a non-sync execution inside a MULTI/LUA (blocking is not allowed) or on loading.");
            return REDISMODULE_OK;
        }
        runFlags |= RFNoAsync;
    }

    if(crtCtx->mode == ExecutionModeAsync){
        VERIFY_CLUSTER_INITIALIZE(ctx);
    }

    char* err = NULL;
    CommandReaderArgs* args = CommandReaderArgs_Create(argv + 1, argc - 1, crtCtx);
    ExecutionPlan* ep = RedisGears_RunWithFlags(crtCtx->fep, crtCtx->mode, args, CommandReader_OnDone,
                                       CommandReaderTriggerCtx_GetShallowCopy(crtCtx), crtCtx->wd, &err, runFlags);

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
        replyCallback(ep, ctx);
        RedisGears_DropExecution(ep);
    } else {
        RedisModule_Assert(!(ctxFlags & REDISMODULE_CTX_FLAGS_MULTI) &&
                           !(ctxFlags & REDISMODULE_CTX_FLAGS_LUA) &&
                           !(ctxFlags & REDISMODULE_CTX_FLAGS_DENY_BLOCKING));
        if(EPIsFlagOn(ep, EFIsLocal)){
            Gears_dictAdd(crtCtx->pendingExections, ep->idStr, NULL);
        }
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        RedisGears_AddOnDoneCallback(ep, onDoneCallback, bc);
        if (crtCtx->mode == ExecutionModeAsync || crtCtx->mode == ExecutionModeAsyncLocal) {
            // on async executions we will set running and holding callbacks to update slowlog stats
            // but we do it only if we have the relevant api from Redis
            if (RedisModule_BlockedClientMeasureTimeStart && RedisModule_BlockedClientMeasureTimeStart) {
                RedisGears_AddOnRunningCallback(ep, CommandReader_ExectionRunningCallback, bc);
                RedisGears_AddOnHoldingCallback(ep, CommandReader_ExectionHoldingCallback, bc);
            }
        }
    }

    return REDISMODULE_OK;
}

#define GEARS_OVERRIDE_COMMAND "rg.trigger"
static RedisModuleString* GearsOverrideCommand = NULL;
static Gears_dict* startNodes = NULL;
static Gears_listNode noOveride;

void CommandReader_CommandFilter(RedisModuleCommandFilterCtx *filter){
    if(!HookRegistrations){
        return;
    }

    if(noOverride){
        return;
    }

    const RedisModuleString* cmd = RedisModule_CommandFilterArgGet(filter, 0);
    const char* cmdCStr = RedisModule_StringPtrLen(cmd, NULL);
    Gears_listNode* node = Gears_dictFetchValue(startNodes, cmdCStr);

    if (node == &noOveride) {
        return;
    }

    if(!node){
        Gears_list* l = Gears_dictFetchValue(HookRegistrations, cmdCStr);
        if(!l){
            // command not found
            return;
        }


        node = Gears_listFirst(l);
    }

    currTCtx = NULL;

    for(; node ; node = Gears_listNextNode(node), currTCtx = NULL){

        currTCtx = Gears_listNodeValue(node);

        CommandReaderTriggerArgs* triggerArgs = currTCtx->args;

        RedisModule_Assert(triggerArgs->triggerType == TriggerType_Hook);

        const char* keyPrefix = triggerArgs->hookData.keyPrefix;
        if(!keyPrefix){
            // found hook
            break;
        }

        CommandReaderTriggerInfo* info = &(triggerArgs->hookData.info);
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
            if(strncmp(keyPrefix, keyCStr, strlen(keyPrefix)) == 0){
                keyFound = true;
                break;
            }
        }

        if(keyFound){
            break;
        }
    }

    if(currTCtx){
        RedisModule_RetainString(NULL, GearsOverrideCommand);
        RedisModule_CommandFilterArgInsert(filter, 0, GearsOverrideCommand);
    }
}

int CommandReader_Initialize(RedisModuleCtx* ctx){
    startNodes = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    GearsOverrideCommand = RedisModule_CreateString(NULL, GEARS_OVERRIDE_COMMAND, strlen(GEARS_OVERRIDE_COMMAND));
    RedisModuleCommandFilter *cmdFilter = RedisModule_RegisterCommandFilter(ctx, CommandReader_CommandFilter, 0);

    // this command is considered readonly but it might actually write data to Redis
    // using rm_call. In this case the effect of the execution is replicated
    // and not the execution itself.
    if (RedisModule_CreateCommand(ctx, GEARS_OVERRIDE_COMMAND, CommandReader_Trigger, "getkeys-api readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "GEARS_OVERRIDE_COMMAND);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.triggeronkey", CommandReader_Trigger, "readonly", 2, 2, 1) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.triggeronkey");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

RedisModuleCallReply* CommandReaderTriggerCtx_CallNext(CommandReaderTriggerCtx* crtCtx, RedisModuleString** argv, size_t argc){
    if(!crtCtx->listNode){
        return NULL;
    }
    Gears_listNode* oldStartNode = Gears_dictFetchValue(startNodes, crtCtx->args->hookData.hook);
    Gears_listNode* startNode = Gears_listNextNode(crtCtx->listNode);
    if (startNode) {
        Gears_dictReplace(startNodes, crtCtx->args->hookData.hook, startNode);
    } else {
        Gears_dictReplace(startNodes, crtCtx->args->hookData.hook, &noOveride);
    }
    RedisModuleCallReply *rep = RedisModule_Call(staticCtx, crtCtx->args->hookData.hook, "!v", argv, argc);
    Gears_dictReplace(startNodes, crtCtx->args->hookData.hook, oldStartNode);
    return rep;
}
