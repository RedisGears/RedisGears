#include "command_hook.h"

#include "module.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "utils/dict.h"
#include "utils/adlist.h"

#include <errno.h>

static RedisModuleCommandFilter *cmdFilter = NULL;
static Gears_dict* HookRegistrations = NULL;

#define COMMAND_FLAG_MOVEABLEKEYS (1 << 0)
#define COMMAND_FLAG_NOSCRIPT (1 << 1)
#define COMMAND_FLAG_READONLY (1 << 2)
#define COMMAND_FLAG_DENYOOM (1 << 3)
#define COMMAND_FLAG_WRITE (1 << 4)

typedef struct CommandInfo{
    int arity;
    int firstKey;
    int lastKey;
    int jump;
    int commandFlags;
}CommandInfo;

typedef struct CommandHookCtx{
    char* cmd;
    char* keyPrefix;
    size_t prefixLen;
    HookCallback callback;
    void* pd;
    CommandInfo info;
    Gears_listNode* listNode;
}CommandHookCtx;

static CommandInfo CommandHook_CreateInfo(const char* cmd, char** err){
    CommandInfo info;

    LockHandler_Acquire(staticCtx);
    RedisModuleCallReply *reply = RedisModule_Call(staticCtx, "COMMAND", "cc", "INFO", cmd);
    LockHandler_Release(staticCtx);

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_UNKNOWN){
        // command was blocked ... someone must have override the 'COMMAND' command :)
        *err = RG_STRDUP("bad reply on COMMAND command");
        goto done;
    }

    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    RedisModuleCallReply *cReply = RedisModule_CallReplyArrayElement(reply, 0);
    if(RedisModule_CallReplyType(cReply) == REDISMODULE_REPLY_NULL){
        *err = RG_STRDUP("bad reply on COMMAND command");
        goto done;
    }

    RedisModuleCallReply *arityReply = RedisModule_CallReplyArrayElement(cReply, 1);
    RedisModule_Assert(RedisModule_CallReplyType(arityReply) == REDISMODULE_REPLY_INTEGER);
    info.arity = RedisModule_CallReplyInteger(arityReply);

    RedisModuleCallReply *flagsReply = RedisModule_CallReplyArrayElement(cReply, 2);
    RedisModule_Assert(RedisModule_CallReplyType(flagsReply) == REDISMODULE_REPLY_ARRAY);
    info.commandFlags = 0;
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(flagsReply) ; ++i){
        RedisModuleCallReply *flagReply = RedisModule_CallReplyArrayElement(flagsReply, i);
        RedisModule_Assert(RedisModule_CallReplyType(flagReply) == REDISMODULE_REPLY_STRING);
        size_t flagReplyLen;
        const char* flagStr = RedisModule_CallReplyStringPtr(flagReply, &flagReplyLen);
        char flagCStr[flagReplyLen + 1];
        memcpy(flagCStr, flagStr, flagReplyLen);
        flagCStr[flagReplyLen] = '\0';
        if(strcasecmp(flagCStr, "movablekeys") == 0){
            info.commandFlags |= COMMAND_FLAG_MOVEABLEKEYS;
        }
        if(strcasecmp(flagCStr, "noscript") == 0){
            info.commandFlags |= COMMAND_FLAG_NOSCRIPT;
        }
        if(strcasecmp(flagCStr, "readonly") == 0){
            info.commandFlags |= COMMAND_FLAG_READONLY;
        }
        if(strcasecmp(flagCStr, "denyoom") == 0){
            info.commandFlags |= COMMAND_FLAG_DENYOOM;
        }
        if(strcasecmp(flagCStr, "write") == 0){
            info.commandFlags |= COMMAND_FLAG_WRITE;
        }
    }

    RedisModuleCallReply *firstKeyReply = RedisModule_CallReplyArrayElement(cReply, 3);
    RedisModule_Assert(RedisModule_CallReplyType(firstKeyReply) == REDISMODULE_REPLY_INTEGER);
    info.firstKey = RedisModule_CallReplyInteger(firstKeyReply);

    RedisModuleCallReply *lastKeyReply = RedisModule_CallReplyArrayElement(cReply, 4);
    RedisModule_Assert(RedisModule_CallReplyType(lastKeyReply) == REDISMODULE_REPLY_INTEGER);
    info.lastKey = RedisModule_CallReplyInteger(lastKeyReply);

    RedisModuleCallReply *jumpReply = RedisModule_CallReplyArrayElement(cReply, 5);
    RedisModule_Assert(RedisModule_CallReplyType(jumpReply) == REDISMODULE_REPLY_INTEGER);
    info.jump = RedisModule_CallReplyInteger(jumpReply);

done:
    RedisModule_FreeCallReply(reply);
    return info;
}

static CommandHookCtx* currHook;
static RedisModuleString* GearsHookCommand = NULL;

static bool noFilter = false;

static void CommandHook_Filter(RedisModuleCommandFilterCtx *filter){
    if(noFilter){
        return;
    }
    if(!HookRegistrations){
        return;
    }

    const RedisModuleString* cmd = RedisModule_CommandFilterArgGet(filter, 0);
    const char* cmdCStr = RedisModule_StringPtrLen(cmd, NULL);
    Gears_list* l = Gears_dictFetchValue(HookRegistrations, cmdCStr);
    if(!l){
        return;
    }
    Gears_listNode* node = Gears_listFirst(l);

    currHook = NULL;

    for(; node ; node = Gears_listNextNode(node), currHook = NULL){

        currHook = Gears_listNodeValue(node);


        const char* keyPrefix = currHook->keyPrefix;
        size_t prefixLen = currHook->prefixLen;
        if(!keyPrefix){
            // found hook
            break;
        }

        RedisModule_Assert(prefixLen > 0);

        if(keyPrefix[0] == '*'){
            // found hook
            break;
        }

        CommandInfo* info = &(currHook->info);
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
            if(IsKeyMatch(keyPrefix, keyCStr, prefixLen)){
                keyFound = true;
                break;
            }
        }

        if(keyFound){
            break;
        }
    }

    if(currHook){
        RedisModule_RetainString(NULL, GearsHookCommand);
        RedisModule_CommandFilterArgInsert(filter, 0, GearsHookCommand);
    }
}

void* CommandHook_Unhook(CommandHookCtx* hook){
    Gears_list* l = Gears_dictFetchValue(HookRegistrations, hook->cmd);
    Gears_listDelNode(l, hook->listNode);

    if(Gears_listLength(l) == 0){
        Gears_dictDelete(HookRegistrations, hook->cmd);
        Gears_listRelease(l);
    }

    if(Gears_dictSize(HookRegistrations) == 0){
        if(RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)){
            // if we have detach ctx it is safe to unregister our filter and reregister it later when needed
            RedisModule_UnregisterCommandFilter(staticCtx, cmdFilter);
            cmdFilter = NULL;
        }
    }

    RG_FREE(hook->cmd);
    if(hook->keyPrefix){
        RG_FREE(hook->keyPrefix);
    }

    void* ret = hook->pd;

    RG_FREE(hook);

    return ret;
}

CommandHookCtx* CommandHook_Hook(const char* cmd, const char* keyPrefix, HookCallback callback, void* pd, char** err){
    CommandInfo info = CommandHook_CreateInfo(cmd, err);
    if(*err){
        return NULL;
    }

    if(info.commandFlags & COMMAND_FLAG_NOSCRIPT){
        *err = RG_STRDUP("Can not hook a command which are not allowed inside a script");
        return NULL;
    }

    if((info.commandFlags & COMMAND_FLAG_MOVEABLEKEYS) && keyPrefix){
        // we can not override a command by key prefix and moveable keys
        *err = RG_STRDUP("Can not hook a command with moveable keys by key prefix");
        return NULL;
    }

    if(keyPrefix){
        if(info.firstKey <= 0){
            // should not really happened
            *err = RG_STRDUP("Can not hook a command by key prefix with none positive first key");
            return NULL;
        }

        if(info.jump <= 0){
            // should not really happened
            *err = RG_STRDUP("Can not override a command by key prefix with none positive jump");
            return NULL;
        }
    }

    if(keyPrefix && strlen(keyPrefix) == 0){
        // should not really happened
        *err = RG_STRDUP("Empty perfix given to command hooker");
        return NULL;
    }

    if(!cmdFilter){
        cmdFilter = RedisModule_RegisterCommandFilter(staticCtx, CommandHook_Filter, REDISMODULE_CMDFILTER_NOSELF);
    }

    CommandHookCtx* hook = RG_ALLOC(sizeof(*hook));
    *hook = (CommandHookCtx){
        .cmd = RG_STRDUP(cmd),
        .keyPrefix = keyPrefix? RG_STRDUP(keyPrefix) : NULL,
        .callback = callback,
        .pd = pd,
        .info = info,
    };
    hook->prefixLen = keyPrefix? strlen(keyPrefix) : 0;

    Gears_list* l = Gears_dictFetchValue(HookRegistrations, cmd);
    if(!l){
        l = Gears_listCreate();
        Gears_dictAdd(HookRegistrations, hook->cmd, l);
    }
    Gears_listAddNodeTail(l, hook);
    hook->listNode = Gears_listLast(l);
    return hook;
}

#define GEARS_HOOK_COMMAND "RG.INNERHOOK"

int CommandHook_HookCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    // we need to protect ourself from recursive hooks, unfortunatly we can not trust redis here
    noFilter = true;

    CommandHookCtx* hook = currHook;
    currHook = NULL;

    RedisModule_Assert(hook);

    int ctxFlags = RedisModule_GetContextFlags(ctx);
    if((ctxFlags & REDISMODULE_CTX_FLAGS_REPLICATED) ||
            (ctxFlags & REDISMODULE_CTX_FLAGS_LOADING)){
        // do not hook replication stream or on loading
        const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
        RedisModuleCallReply* rep = RedisModule_Call(ctx, subCommand, "!v", argv + 2, argc - 2);
        if(rep){
            RedisModule_ReplyWithCallReply(ctx, rep);
            RedisModule_FreeCallReply(rep);
        }else{
            if(errno){
                RedisModule_ReplyWithError(ctx, strerror(errno));
            }else{
                RedisModule_ReplyWithError(ctx, "error happened running the command");
            }
        }
        noFilter = false;
        return REDISMODULE_OK;
    }

    if(hook->info.commandFlags & COMMAND_FLAG_DENYOOM && RedisModule_GetUsedMemoryRatio){
        float memoryRetio = RedisModule_GetUsedMemoryRatio();
        if(memoryRetio > 1){
            // we are our of memory and should deny the command
            RedisModule_ReplyWithError(ctx, "OOM command not allowed when used memory > 'maxmemory'");
            noFilter = false;
            return REDISMODULE_OK;
        }
    }

    int ret = hook->callback(ctx, argv + 1, argc - 1, hook->pd);

    noFilter = false;
    return ret;
}

int CommandHook_Init(RedisModuleCtx* ctx){
    HookRegistrations = Gears_dictCreate(&Gears_dictTypeHeapStringsCaseInsensitive, NULL);
    GearsHookCommand = RedisModule_CreateString(NULL, GEARS_HOOK_COMMAND, strlen(GEARS_HOOK_COMMAND));
    if (RedisModule_CreateCommand(ctx, GEARS_HOOK_COMMAND, CommandHook_HookCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command "GEARS_HOOK_COMMAND);
        return REDISMODULE_ERR;
    }

    if(!RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)){
        // if we do not have a detach ctx we have to register our command filter now and keep it
        cmdFilter = RedisModule_RegisterCommandFilter(ctx, CommandHook_Filter, REDISMODULE_CMDFILTER_NOSELF);
    }

    return REDISMODULE_OK;
}


