#include "keys_reader.h"
#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "execution_plan.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "record.h"
#include "config.h"
#include "mgmt.h"
#include "version.h"
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include "../command_hook.h"
#include "readers_common.h"

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10

static Record* (*KeysReader_ScanNextKeyFunc)(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx);

typedef struct KeysReaderRegisterData{
    long long refCount;
    FlatExecutionPlan* fep;
    KeysReaderTriggerArgs* args;
    ExecutionMode mode;
    char* lastError;
    unsigned long long numTriggered;
    unsigned long long numSuccess;
    unsigned long long numFailures;
    unsigned long long numAborted;
    long long lastRunDuration;
    long long totalRunDuration;
    Gears_dict* localPendingExecutions;
    Gears_list* localDoneExecutions;
    WorkerData* wd;
    CommandHookCtx** hooks;
}KeysReaderRegisterData;

Gears_list* keysReaderRegistration = NULL;

RedisModuleDict *keysDict = NULL;

static Record* KeysReader_Next(ExecutionCtx* ectx, void* ctx);

typedef struct CommandCtx{
    size_t refCount;
    RedisModuleString** args;
    Record* overrideReply;
    RedisModuleCallReply* realReply;
    char* errnostr;
    RedisModuleCtx* clientCtx;
    RedisModuleBlockedClient* bc;
}CommandCtx;

static bool ignoreKeysEvents = false;
static CommandCtx* currCmdCtx = NULL;

typedef struct KeysReaderCtx{
    char* match;
    size_t matchLen;
    char* event;
    long long cursorIndex;
    RedisModuleScanCursor* cursor;
    bool isDone;
    Record** pendingRecords;
    bool readValue;
    bool noScan;
    char* readRecordStr;
    RedisGears_KeysReaderReadRecordCallback readRecord;
    CommandCtx* cmdCtx;
}KeysReaderCtx;

typedef struct KeysReaderTriggerArgs{
    char* prefix;
    char** eventTypes;
    int* keyTypes;
    char** hookCommands;
    bool readValue;
    char* readRecordStr;
    RedisGears_KeysReaderReadRecordCallback readRecord;
}KeysReaderTriggerArgs;

static CommandCtx* KeyReader_CommandCtxCreate(RedisModuleCtx* clientCtx, RedisModuleString** argv, size_t argc){
    CommandCtx* cmdCtx = RG_ALLOC(sizeof(*cmdCtx));
    cmdCtx->refCount = 1;
    cmdCtx->overrideReply = NULL;
    cmdCtx->realReply = NULL;
    cmdCtx->errnostr = NULL;
    cmdCtx->clientCtx = clientCtx;
    cmdCtx->bc = NULL;
    cmdCtx->args = array_new(RedisModuleString*, argc);

    for(size_t i = 0 ; i < argc ; ++i){
        RedisModule_RetainString(NULL, argv[i]);
        cmdCtx->args = array_append(cmdCtx->args, argv[i]);
    }

    return cmdCtx;
}

CommandCtx* KeyReader_CommandCtxGetShallowCopy(CommandCtx* cmdCtx){
    __atomic_add_fetch(&cmdCtx->refCount, 1, __ATOMIC_RELAXED);
    return cmdCtx;
}

void KeyReader_CommandCtxFree(CommandCtx* cmdCtx){
    if(__atomic_sub_fetch(&cmdCtx->refCount, 1, __ATOMIC_RELAXED) > 0){
        return;
    }

    RedisModuleCtx* clientCtx;
    if(cmdCtx->bc){
        clientCtx = RedisModule_GetThreadSafeContext(cmdCtx->bc);
    }else{
        clientCtx = cmdCtx->clientCtx;
    }

    if(cmdCtx->overrideReply){
        RedisGears_RecordSendReply(cmdCtx->overrideReply, clientCtx);
    }else{
        if(cmdCtx->realReply){
            RedisModule_ReplyWithCallReply(clientCtx, cmdCtx->realReply);
        }else if(cmdCtx->errnostr){
            RedisModule_ReplyWithError(clientCtx, cmdCtx->errnostr);
        }else{
            RedisModule_Log(staticCtx, "warning", "Failed getting results from command execution");
            RedisModule_ReplyWithError(clientCtx, "Failed getting results from command execution");
        }
    }

    for(size_t i = 0 ; i < array_len(cmdCtx->args) ; ++i){
        RedisModule_FreeString(clientCtx, cmdCtx->args[i]);
    }

    array_free(cmdCtx->args);

    if(cmdCtx->overrideReply){
        RedisGears_FreeRecord(cmdCtx->overrideReply);
    }

    if(cmdCtx->realReply){
        RedisModule_FreeCallReply(cmdCtx->realReply);
    }

    if(cmdCtx->errnostr){
        RG_FREE(cmdCtx->errnostr);
    }

    if(cmdCtx->bc){
        RedisModule_UnblockClient(cmdCtx->bc, NULL);
        RedisModule_FreeThreadSafeContext(clientCtx);
    }

    RG_FREE(cmdCtx);
}

int KeyReader_CommandCtxOverrideReply(CommandCtx* cmdCtx, Record* r, char** err){
    void* expected = NULL;
    __atomic_compare_exchange(&cmdCtx->overrideReply, &expected, &r, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    if(cmdCtx->overrideReply != r){
        *err = RG_STRDUP("Can only override reply once");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

RedisModuleString** KeyReader_CommandCtxGetCommand(CommandCtx* cmdCtx, size_t* len){
    *len = array_len(cmdCtx->args);
    return cmdCtx->args;
}

int KeyReader_SetAvoidEvents(int avoidEvents){
    int oldVal = ignoreKeysEvents;
    ignoreKeysEvents = avoidEvents;
    return oldVal;
}

CommandCtx* KeyReader_CommandCtxGet(ExecutionCtx* eCtx){
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(eCtx);
    ExecutionStep* reader = ep->steps[array_len(ep->steps) - 1];
    RedisModule_Assert(reader->type == READER);
    Reader* r = reader->reader.r;
    // compare one of the reader functions to make sure its the CommandReader
    // its a hack but its faster then compare strings
    if(r->free != KeysReaderCtx_Free){
        return NULL;
    }

    KeysReaderCtx* readerCtx = reader->reader.r->ctx;
    return readerCtx->cmdCtx;
}

void KeysReaderTriggerArgs_Free(KeysReaderTriggerArgs* args){
    RG_FREE(args->prefix);
    if(args->eventTypes){
        array_free_ex(args->eventTypes, RG_FREE(*(char**)ptr));
    }
    if(args->hookCommands){
        array_free_ex(args->hookCommands, RG_FREE(*(char**)ptr));
    }
    if(args->keyTypes){
        array_free(args->keyTypes);
    }
    if(args->readRecordStr){
        RG_FREE(args->readRecordStr);
    }
    RG_FREE(args);
}

static void KeysReaderRegisterData_Free(KeysReaderRegisterData* rData){
    if((--rData->refCount) == 0){

        Gears_listNode* n = NULL;

        // if we free registration there must not be any pending executions.
        // either all executions was finished or aborted
        RedisModule_Assert(Gears_dictSize(rData->localPendingExecutions) == 0);

        while((n = Gears_listFirst(rData->localDoneExecutions))){
            char* epIdStr = Gears_listNodeValue(n);
            Gears_listDelNode(rData->localDoneExecutions, n);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            RG_FREE(epIdStr);
            if(!ep){
                RedisModule_Log(staticCtx, "info", "Failed finding done execution to drop on unregister. Execution was probably already dropped.");
                continue;
            }
            // all the executions here are done, will just drop it.
            RedisGears_DropExecution(ep);
        }

        if(rData->hooks){
            for(size_t i = 0 ; i < array_len(rData->hooks) ; ++i){
                CommandHook_Unhook(rData->hooks[i]);
            }
            array_free(rData->hooks);
        }

        Gears_dictRelease(rData->localPendingExecutions);
        Gears_listRelease(rData->localDoneExecutions);

        if(rData->lastError){
            RG_FREE(rData->lastError);
        }
        KeysReaderTriggerArgs_Free(rData->args);
        FlatExecutionPlan_Free(rData->fep);
        RedisGears_WorkerDataFree(rData->wd);
        RG_FREE(rData);
    }
}

static KeysReaderRegisterData* KeysReaderRegisterData_Create(FlatExecutionPlan* fep, void* args, ExecutionMode mode, CommandHookCtx** hooks){
    KeysReaderRegisterData* rData = RG_ALLOC(sizeof(*rData));
    *rData = (KeysReaderRegisterData){
        .refCount = 1,
        .fep = fep,
        .args = args,
        .mode = mode,
        .lastError = NULL,
        .numTriggered = 0,
        .numSuccess = 0,
        .numFailures = 0,
        .numAborted = 0,
        .lastRunDuration = 0,
        .totalRunDuration = 0,
        .localPendingExecutions = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL),
        .localDoneExecutions = Gears_listCreate(),
        .wd = RedisGears_WorkerDataCreate(fep->executionThreadPool),
        .hooks = hooks,
    };
    return rData;
}

static KeysReaderRegisterData* KeysReaderRegisterData_GetShallowCopy(KeysReaderRegisterData* rData){
    ++rData->refCount;
    return rData;
}

KeysReaderCtx* KeysReaderCtx_Create(const char* match, bool readValue, const char* event, bool noScan){
#define PENDING_KEYS_INIT_CAP 10
    size_t matchLen = match? strlen(match) : 0;
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = match? RG_STRDUP(match) : NULL,
        .matchLen = matchLen,
        .event = event ? RG_STRDUP(event) : NULL,
        .cursor = RedisModule_ScanCursorCreate(),
        .isDone = false,
        .readValue = readValue,
        .noScan = noScan,
        .pendingRecords = array_new(Record*, PENDING_KEYS_INIT_CAP),
        .readRecordStr = NULL,
        .readRecord = NULL,
        .cursorIndex = 0,
        .cmdCtx = NULL,
    };
    return krctx;
}

static KeysReaderCtx* KeysReaderCtx_CreateWithCmdCtx(const char* match, bool readValue, const char* event, bool noScan, CommandCtx* cmdCtx){
    KeysReaderCtx* readerCtx = KeysReaderCtx_Create(match, readValue, event, noScan);
    if(cmdCtx){
        readerCtx->cmdCtx = KeyReader_CommandCtxGetShallowCopy(cmdCtx);
    }
    return readerCtx;
}

int KeysReaderCtx_SetReadRecordCallback(KeysReaderCtx* krCtx, const char* readRecordCallback){
    krCtx->readRecord = KeysReaderReadRecordsMgmt_Get(readRecordCallback);
    if(!krCtx->readRecord){
        return REDISMODULE_ERR;
    }
    krCtx->readRecordStr = RG_STRDUP(readRecordCallback);
    return REDISMODULE_OK;
}

void KeysReaderCtx_Free(void* ctx){
    KeysReaderCtx* krctx = ctx;
    if(krctx->cmdCtx){
        KeyReader_CommandCtxFree(krctx->cmdCtx);
        krctx->cmdCtx = NULL;
    }
    if(krctx->match){
        RG_FREE(krctx->match);
    }
    if(krctx->event){
        RG_FREE(krctx->event);
    }
    if(krctx->readRecordStr){
        RG_FREE(krctx->readRecordStr);
    }
    RedisModule_ScanCursorDestroy(krctx->cursor);
    for(size_t i = 0 ; i < array_len(krctx->pendingRecords) ; ++i){
        RedisGears_FreeRecord(krctx->pendingRecords[i]);
    }
    array_free(krctx->pendingRecords);

    RG_FREE(krctx);
}

static int RG_KeysReaderCtxSerialize(ExecutionCtx* ectx, void* ctx, Gears_BufferWriter* bw){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    RedisGears_BWWriteString(bw, krctx->match);
    if(krctx->event){
        RedisGears_BWWriteLong(bw, 1); // even exists
        RedisGears_BWWriteString(bw, krctx->event);
    }else{
        RedisGears_BWWriteLong(bw, 0); // even does not exists
    }
    RedisGears_BWWriteLong(bw, krctx->readValue);
    RedisGears_BWWriteLong(bw, krctx->noScan);
    if(krctx->readRecord){
        // readRecord callback exists
        RedisGears_BWWriteLong(bw, 1);
        RedisGears_BWWriteString(bw, krctx->readRecordStr);
    }else{
        RedisGears_BWWriteLong(bw, 0);
    }
    return REDISMODULE_OK;
}

static int RG_KeysReaderCtxDeserialize(ExecutionCtx* ectx, void* ctx, Gears_BufferReader* br){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    krctx->match = RG_STRDUP(RedisGears_BRReadString(br));
    krctx->matchLen = strlen(krctx->match);
    if(RedisGears_BRReadLong(br)){
        krctx->event = RG_STRDUP(RedisGears_BRReadString(br));
        // having an even means this execution was triggered by a registration and the fact
        // that we deserialize it means that the event did not occurred on this shard,
        // there is no need to supply any data to the execution so lets mark ourself as done
        krctx->isDone = true;
    }
    krctx->readValue = RedisGears_BRReadLong(br);
    krctx->noScan = RedisGears_BRReadLong(br);
    krctx->readRecordStr = NULL;
    krctx->readRecord = NULL;
    if(RedisGears_BRReadLong(br)){
        const char* readRecordCallbackName = RedisGears_BRReadString(br);
        krctx->readRecord = KeysReaderReadRecordsMgmt_Get(readRecordCallbackName);
        RedisModule_Assert(krctx->readRecord);
    }
    return REDISMODULE_OK;
}

static Record* GetStringValueRecord(RedisModuleKey* handler, RedisModuleCtx* ctx, const char* keyStr){
    size_t len;
    char* val = NULL;
    RedisModuleCallReply *r = NULL;
    if(!gearsIsCrdt){
        val = RedisModule_StringDMA(handler, &len, REDISMODULE_READ);
    } else {
        // on crdt the string llapi is not supported so we need to use rm_call
        r = RedisModule_Call(ctx, "GET", "c", keyStr);
        val = (char*)RedisModule_CallReplyStringPtr(r, &len);
    }
    char* strVal = RG_ALLOC(len + 1);
    memcpy(strVal, val, len);
    strVal[len] = '\0';

    Record* strRecord = RedisGears_StringRecordCreate(strVal, len);

    if(r){
        RedisModule_FreeCallReply(r);
    }

    return strRecord;
}

static Record* ValueToHashSetMapper(const char* keyStr, RedisModuleCtx* ctx){
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGETALL", "c", keyStr);
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t len = RedisModule_CallReplyLength(reply);
    RedisModule_Assert(len % 2 == 0);
    Record *hashSetRecord = RedisGears_HashSetRecordCreate();
    for(int i = 0 ; i < len ; i+=2){
        RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(reply, i);
        RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(reply, i + 1);
        size_t keyStrLen;
        const char* keyStr = RedisModule_CallReplyStringPtr(keyReply, &keyStrLen);
        char keyCStr[keyStrLen + 1];
        memcpy(keyCStr, keyStr, keyStrLen);
        keyCStr[keyStrLen] = '\0';
        size_t valStrLen;
        const char* valStr = RedisModule_CallReplyStringPtr(valReply, &valStrLen);
        char* valCStr = RG_ALLOC(valStrLen + 1);
        memcpy(valCStr, valStr, valStrLen);
        valCStr[valStrLen] = '\0';
        Record* valRecord = RedisGears_StringRecordCreate(valCStr, valStrLen);
        RedisGears_HashSetRecordSet(hashSetRecord, keyCStr, valRecord);
    }
    RedisModule_FreeCallReply(reply);
    return hashSetRecord;
}

static Record* ValueToListMapper(const char* keyStr, RedisModuleCtx* ctx){
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "lrange", "cll", keyStr, 0, -1);
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t len = RedisModule_CallReplyLength(reply);
    Record *listRecord = RedisGears_ListRecordCreate(10);
    for(int i = 0 ; i < len ; ++i){
        RedisModuleCallReply *r = RedisModule_CallReplyArrayElement(reply, i);
        RedisModule_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING);
        RedisModuleString* key = RedisModule_CreateStringFromCallReply(r);
        size_t vaLen;
        const char* val = RedisModule_StringPtrLen(key, &vaLen);
        char* str = RG_ALLOC(vaLen + 1);
        memcpy(str, val, vaLen);
        str[vaLen] = '\0';
        Record* strRecord = RedisGears_StringRecordCreate(str, vaLen);
        RedisModule_FreeString(ctx, key);
        RedisGears_ListRecordAdd(listRecord, strRecord);
    }
    RedisModule_FreeCallReply(reply);
    return listRecord;
}

static Record* GetValueRecord(RedisModuleCtx* rctx, const char* keyStr, RedisModuleKey* handler){
    switch(RedisModule_KeyType(handler)){
    case REDISMODULE_KEYTYPE_STRING:
        return GetStringValueRecord(handler, rctx, keyStr);
        break;
    case REDISMODULE_KEYTYPE_LIST:
        return ValueToListMapper(keyStr, rctx);
        break;
    case REDISMODULE_KEYTYPE_HASH:
        return ValueToHashSetMapper(keyStr, rctx);
        break;
    default:
        // we do not want how to parse this type, we will return a key record with no value
        return NULL;
    }
}

static Record* GetTypeRecord(RedisModuleKey* handler){
    char* typeStr = "unknown";
    switch(RedisModule_KeyType(handler)){
    case REDISMODULE_KEYTYPE_STRING:
        typeStr = "string";
        break;
    case REDISMODULE_KEYTYPE_LIST:
        typeStr = "list";
        break;
    case REDISMODULE_KEYTYPE_HASH:
        typeStr = "hash";
        break;
    case REDISMODULE_KEYTYPE_SET:
        typeStr = "set";
        break;
    case REDISMODULE_KEYTYPE_ZSET:
        typeStr = "zset";
        break;
    case REDISMODULE_KEYTYPE_MODULE:
        typeStr = "module";
        break;
    default:
        break;
    }
    return RedisGears_StringRecordCreate(RG_STRDUP(typeStr), strlen(typeStr));
}

static Record* KeysReader_ReadKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx, RedisModuleString* key, RedisModuleKey* keyPtr){
    if(readerCtx->readRecord){
        Record* res = readerCtx->readRecord(rctx, key, keyPtr, readerCtx->readValue, readerCtx->event);
        if(res){
            return res;
        }
        // fall back to usual read
    }
    size_t keyLen;
    const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);
    Record* record = RedisGears_HashSetRecordCreate();

    char* keyCStr = RG_ALLOC(keyLen + 1);
    memcpy(keyCStr, keyStr, keyLen);
    keyCStr[keyLen] = '\0';

    Record* keyRecord = RedisGears_StringRecordCreate(keyCStr, keyLen);
    RedisGears_HashSetRecordSet(record, "key", keyRecord);

    if(readerCtx->readValue){
        int oldAvoidEvents = KeyReader_SetAvoidEvents(1);
        RedisModuleKey *keyHandler = keyPtr;
        if (!keyHandler) {
            keyHandler = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
        }
        KeyReader_SetAvoidEvents(oldAvoidEvents);
        if(keyHandler){
            Record* keyType = GetTypeRecord(keyHandler);
            Record* val = GetValueRecord(rctx, keyCStr, keyHandler);
            RedisGears_HashSetRecordSet(record, "value", val);
            RedisGears_HashSetRecordSet(record, "type", keyType);
            if (!keyPtr) {
                // we open the key so we also need to close it
                RedisModule_CloseKey(keyHandler);
            }
        }else{
            RedisGears_HashSetRecordSet(record, "value", NULL);
            RedisGears_HashSetRecordSet(record, "type", RedisGears_StringRecordCreate(RG_STRDUP("empty"), strlen("empty")));
        }
    }

    if(readerCtx->event){
        Record* eventRecord = RedisGears_StringRecordCreate(RG_STRDUP(readerCtx->event), strlen(readerCtx->event));
        RedisGears_HashSetRecordSet(record, "event", eventRecord);
    }else{
        RedisGears_HashSetRecordSet(record, "event", NULL);
    }

    return record;
}

/* Glob-style pattern matching. */
static int GearsStringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase)
{
    if(patternLen == 1 && pattern[0] == '*'){
        return 1;
    }

    while(patternLen && stringLen) {
        switch(pattern[0]) {
        case '*':
            while (patternLen && pattern[1] == '*') {
                pattern++;
                patternLen--;
            }
            if (patternLen == 1)
                return 1; /* match */
            while(stringLen) {
                if (GearsStringmatchlen(pattern+1, patternLen-1,
                            string, stringLen, nocase))
                    return 1; /* match */
                string++;
                stringLen--;
            }
            return 0; /* no match */
            break;
        case '?':
            string++;
            stringLen--;
            break;
        case '[':
        {
            int not, match;

            pattern++;
            patternLen--;
            not = pattern[0] == '^';
            if (not) {
                pattern++;
                patternLen--;
            }
            match = 0;
            while(1) {
                if (pattern[0] == '\\' && patternLen >= 2) {
                    pattern++;
                    patternLen--;
                    if (pattern[0] == string[0])
                        match = 1;
                } else if (pattern[0] == ']') {
                    break;
                } else if (patternLen == 0) {
                    pattern--;
                    patternLen++;
                    break;
                } else if (patternLen >= 3 && pattern[1] == '-') {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];
                    if (start > end) {
                        int t = start;
                        start = end;
                        end = t;
                    }
                    if (nocase) {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }
                    pattern += 2;
                    patternLen -= 2;
                    if (c >= start && c <= end)
                        match = 1;
                } else {
                    if (!nocase) {
                        if (pattern[0] == string[0])
                            match = 1;
                    } else {
                        if (tolower((int)pattern[0]) == tolower((int)string[0]))
                            match = 1;
                    }
                }
                pattern++;
                patternLen--;
            }
            if (not)
                match = !match;
            if (!match)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        }
        case '\\':
            if (patternLen >= 2) {
                pattern++;
                patternLen--;
            }
            /* fall through */
        default:
            if (!nocase) {
                if (pattern[0] != string[0])
                    return 0; /* no match */
            } else {
                if (tolower((int)pattern[0]) != tolower((int)string[0]))
                    return 0; /* no match */
            }
            string++;
            stringLen--;
            break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0) {
            while(*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringLen == 0)
        return 1;
    return 0;
}

static void KeysReader_ScanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key, void *privdata){
    KeysReaderCtx* readerCtx = privdata;
    size_t len;
    const char* keyStr = RedisModule_StringPtrLen(keyname, &len);
    if(GearsStringmatchlen(readerCtx->match, readerCtx->matchLen, keyStr, len, 0)){
        Record* record = KeysReader_ReadKey(ctx, readerCtx, keyname, key);
        readerCtx->pendingRecords = array_append(readerCtx->pendingRecords, record);
    }
}

static Record* KeysReader_ScanAPINextKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx){
    if(array_len(readerCtx->pendingRecords) > 0){
        return array_pop(readerCtx->pendingRecords);
    }
    if(readerCtx->isDone){
        return NULL;
    }

    while(!readerCtx->isDone){
        LockHandler_Acquire(rctx);
        readerCtx->isDone = !RedisModule_Scan(rctx, readerCtx->cursor, KeysReader_ScanCallback, readerCtx);
        LockHandler_Release(rctx);

        if(array_len(readerCtx->pendingRecords) > 0){
            return array_pop(readerCtx->pendingRecords);
        }
    }
    return NULL;
}

static Record* KeysReader_ScanNextKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx){
    if(array_len(readerCtx->pendingRecords) > 0){
        return array_pop(readerCtx->pendingRecords);
    }
    if(readerCtx->isDone){
        return NULL;
    }
    LockHandler_Acquire(rctx);
    while(true){
        RedisModuleCallReply *reply = RedisModule_Call(rctx, "SCAN", "lcccc", readerCtx->cursorIndex, "COUNT", "10000", "MATCH", readerCtx->match);
        if (reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
            if(reply) RedisModule_FreeCallReply(reply);
            LockHandler_Release(rctx);
            return NULL;
        }

        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

        RedisModule_Assert(RedisModule_CallReplyLength(reply) == 2);

        RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

        RedisModule_Assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

        RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
        RedisModule_StringToLongLong(cursorStr, &readerCtx->cursorIndex);
        RedisModule_FreeString(rctx, cursorStr);

        if(readerCtx->cursorIndex == 0){
            readerCtx->isDone = true;
        }

        RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
        if(RedisModule_CallReplyLength(keysReply) < 1){
            RedisModule_FreeCallReply(reply);
            if(readerCtx->isDone){
                LockHandler_Release(rctx);
                return NULL;
            }
            continue;
        }
        for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
            RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
            RedisModule_Assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
            RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
            Record* record = KeysReader_ReadKey(rctx, readerCtx, key, NULL);
            if(record == NULL){
                continue;
            }
            RedisModule_FreeString(rctx, key);
            readerCtx->pendingRecords = array_append(readerCtx->pendingRecords, record);
        }
        RedisModule_FreeCallReply(reply);
        LockHandler_Release(rctx);
        return array_pop(readerCtx->pendingRecords);
    }
    return NULL;
}

static Record* KeysReader_Next(ExecutionCtx* ectx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    Record* record = NULL;
    if(!readerCtx->noScan){
        record = KeysReader_ScanNextKeyFunc(RedisGears_GetRedisModuleCtx(ectx), readerCtx);
    }else{
        if(readerCtx->isDone){
            return NULL;
        }
        RedisModuleString* key = RedisModule_CreateString(NULL, readerCtx->match, strlen(readerCtx->match));
        RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
        LockHandler_Acquire(rctx);
        record = KeysReader_ReadKey(rctx, readerCtx, key, NULL);
        LockHandler_Release(rctx);
        RedisModule_FreeString(NULL, key);
        readerCtx->isDone = true;
    }
    return record;
}

static void KeysReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    KeysReaderRegisterData* rData = privateData;

    // free the related command ctx if exists
    ExecutionStep* reader = ctx->steps[array_len(ctx->steps) - 1];
    KeysReaderCtx* readerCtx = reader->reader.r->ctx;
    if(readerCtx->cmdCtx){
        KeyReader_CommandCtxFree(readerCtx->cmdCtx);
        readerCtx->cmdCtx = NULL;
    }

    if(EPIsFlagOn(ctx, EFIsLocal)){
        Gears_dictDelete(rData->localPendingExecutions, ctx->idStr);

        char* epIdStr = RG_STRDUP(ctx->idStr);
        // Add the execution id to the localDoneExecutions list
        Gears_listAddNodeTail(rData->localDoneExecutions, epIdStr);
        if(GearsConfig_GetMaxExecutionsPerRegistration() > 0 && Gears_listLength(rData->localDoneExecutions) > GearsConfig_GetMaxExecutionsPerRegistration()){
            Gears_listNode *head = Gears_listFirst(rData->localDoneExecutions);
            epIdStr = Gears_listNodeValue(head);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            if(ep){
                RedisModule_Assert(EPIsFlagOn(ep, EFDone));
                RedisGears_DropExecution(ep);
            }
            RG_FREE(epIdStr);
            Gears_listDelNode(rData->localDoneExecutions, head);
        }
    }

    rData->lastRunDuration = FlatExecutionPlan_GetExecutionDuration(ctx);
    rData->totalRunDuration += rData->lastRunDuration;

    long long errorsLen = RedisGears_GetErrorsLen(ctx);

    if(errorsLen > 0){
        ++rData->numFailures;
        Record* r = RedisGears_GetError(ctx, 0);
        RedisModule_Assert(RedisGears_RecordGetType(r) == errorRecordType);
        if(rData->lastError){
            RG_FREE(rData->lastError);
        }
        rData->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));
    } else if(ctx->status == ABORTED){
        ++rData->numAborted;
    } else {
        ++rData->numSuccess;
    }

    KeysReaderRegisterData_Free(rData);
}

static int KeysReader_IsKeyMatch(const char* prefix, const char* key){
    size_t len = strlen(prefix);
    const char* data = prefix;
    int isPrefix = prefix[len - 1] == '*';
    if(isPrefix){
        --len;
    }
    if(isPrefix){
        return strncmp(data, key, len) == 0;
    }else{
        return strcmp(data, key) == 0;
    }
}

static int KeysReader_ShouldFire(RedisModuleCtx *ctx, KeysReaderTriggerArgs* args, RedisModuleString* key, const char* event){
    if(args->eventTypes){
        bool evenFound = false;
        for(size_t i = 0 ; i < array_len(args->eventTypes) ; i++){
            if(strcasecmp(args->eventTypes[i], event) == 0){
                evenFound = true;
                break;
            }
        }
        if(!evenFound){
            return 0;
        }
    }
    if(args->hookCommands){
        if(!currCmdCtx){
            // no command ctx -> no command cause the notification
            // but we were asked to filter by command so it for sure does not pass the filter
            return 0;
        }
        bool commandFound = false;
        const char* currCmd = RedisModule_StringPtrLen(currCmdCtx->args[0], NULL);
        for(size_t i = 0 ; i < array_len(args->hookCommands) ; i++){
            if(strcasecmp(args->hookCommands[i], currCmd) == 0){
                commandFound = true;
                break;
            }
        }
        if(!commandFound){
            return 0;
        }
    }
    if(args->keyTypes){
        int oldAvoidEvents = KeyReader_SetAvoidEvents(1);
        RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
        KeyReader_SetAvoidEvents(oldAvoidEvents);
        int type = RedisModule_KeyType(kp);
        RedisModule_CloseKey(kp);
        bool typeFound = false;
        for(size_t i = 0 ; i < array_len(args->keyTypes) ; i++){
            if(type == args->keyTypes[i]){
                typeFound = true;
                break;
            }
        }
        if(!typeFound){
            return 0;
        }
    }
    const char* keyCStr = RedisModule_StringPtrLen(key, NULL);
    return KeysReader_IsKeyMatch(args->prefix, keyCStr);
}

static int KeysReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    if(ignoreKeysEvents){
        return REDISMODULE_OK;
    }
    int flags = RedisModule_GetContextFlags(ctx);
    if(!(flags & REDISMODULE_CTX_FLAGS_MASTER)){
        // we are not executing registrations on slave
        return REDISMODULE_OK;
    }
    if(flags & REDISMODULE_CTX_FLAGS_LOADING){
        // we are not executing registrations on slave
        return REDISMODULE_OK;
    }
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    const char* keyCStr = RedisModule_StringPtrLen(key, NULL);
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(KeysReader_ShouldFire(ctx, rData->args, key, event)){
            ++rData->numTriggered;
            RedisGears_OnExecutionDoneCallback callback = NULL;
            void* privateData = NULL;
            callback = KeysReader_ExecutionDone;
            privateData = KeysReaderRegisterData_GetShallowCopy(rData);
            char* err = NULL;
            CommandCtx* cmdCtx = NULL;
            if(rData->args->hookCommands){
                // we were asked to hook the command so we must have the cmdCtx
                RedisModule_Assert(currCmdCtx);
                cmdCtx = currCmdCtx;
            }
            KeysReaderCtx* arg = KeysReaderCtx_CreateWithCmdCtx(keyCStr, rData->args->readValue, event, true, cmdCtx);
            if(rData->args->readRecord){
                arg->readRecordStr = RG_STRDUP(rData->args->readRecordStr);
                arg->readRecord = rData->args->readRecord;
            }
            ExecutionPlan* ep = RedisGears_Run(rData->fep, rData->mode, arg, callback, privateData, rData->wd, &err);
            if(!ep){
                ++rData->numAborted;
                RedisModule_Log(staticCtx, "warning", "could not execute flat execution on trigger, %s", err);
                if(err){
                    RG_FREE(err);
                }
                RedisGears_KeysReaderCtxFree(arg);
                continue;
            }
            RedisModule_Assert(rData->mode != ExecutionModeSync || EPIsFlagOn(ep, EFDone));
            if(EPIsFlagOn(ep, EFIsLocal) && EPIsFlagOff(ep, EFDone)){
                // execution is local
                // If execution is SYNC it will be added to localDoneExecutions on done
                // Otherwise, save it to the registration pending execution list.
                // currently we are not save global executions and those will not be listed
                // in the registration execution list nor will be drop on unregister.
                // todo: handle none local executions
                Gears_dictAdd(rData->localPendingExecutions, ep->idStr, NULL);
            }

            if(EPIsFlagOff(ep, EFDone)){
                // execution is not done, if he client is not yet blocked we need to block it.
                // the client will be release when the command ctx will be released by all its owners.
                if(cmdCtx){
                    // check if we are allow to block
                    int ctxFlags = RedisModule_GetContextFlags(currCmdCtx->clientCtx);
                    if((ctxFlags & REDISMODULE_CTX_FLAGS_MULTI) ||
                       (ctxFlags & REDISMODULE_CTX_FLAGS_LUA) ||
                       (ctxFlags & REDISMODULE_CTX_FLAGS_DENY_BLOCKING)){
                        // we are not allow to block, we will free the cmdCtx
                        KeyReader_CommandCtxFree(cmdCtx);
                        arg->cmdCtx = NULL;
                    } else{
                        if(!currCmdCtx->bc){
                            currCmdCtx->bc = RedisModule_BlockClient(currCmdCtx->clientCtx, NULL, NULL, NULL, 0);
                        }
                    }
                }

            }
        }
    }
    Gears_listReleaseIterator(iter);
    return REDISMODULE_OK;
}

#define FindRegistrationDataFlagPop 0x01

static KeysReaderRegisterData* KeysReader_FindRegistrationData(FlatExecutionPlan* fep, int flags){
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(rData->fep == fep){
            if(flags & FindRegistrationDataFlagPop){
                Gears_listDelNode(keysReaderRegistration, node);
            }
            Gears_listReleaseIterator(iter);
            return rData;
        }
    }
    Gears_listReleaseIterator(iter);
    return NULL;
}

static void KeysReader_SerializeArgs(void* var, Gears_BufferWriter* bw){
    KeysReaderTriggerArgs* args = var;
    RedisGears_BWWriteString(bw, args->prefix);
    if(args->eventTypes){
        RedisGears_BWWriteLong(bw, 1); // eventTypes exists
        RedisGears_BWWriteLong(bw, array_len(args->eventTypes));
        for(size_t i = 0 ; i < array_len(args->eventTypes) ; ++i){
            RedisGears_BWWriteString(bw, args->eventTypes[i]);
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // eventTypes does not exist
    }

    if(args->hookCommands){
        RedisGears_BWWriteLong(bw, 1); // hookCommands exists
        RedisGears_BWWriteLong(bw, array_len(args->hookCommands));
        for(size_t i = 0 ; i < array_len(args->hookCommands) ; ++i){
            RedisGears_BWWriteString(bw, args->hookCommands[i]);
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // hookCommands does not exist
    }

    if(args->keyTypes){
        RedisGears_BWWriteLong(bw, 1); // keyTypes exists
        RedisGears_BWWriteLong(bw, array_len(args->keyTypes));
        for(size_t i = 0 ; i < array_len(args->keyTypes) ; ++i){
            RedisGears_BWWriteLong(bw, args->keyTypes[i]);
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // keyTypes does not exist
    }

    RedisGears_BWWriteLong(bw, args->readValue);

    if(args->readRecord){
        RedisGears_BWWriteLong(bw, 1);
        RedisGears_BWWriteString(bw, args->readRecordStr);
    }else{
        RedisGears_BWWriteLong(bw, 0);
    }
}

static void* KeysReader_DeserializeArgs(Gears_BufferReader* br, int encver){
    char* regex = RedisGears_BRReadString(br);
    char** eventTypes = NULL;
    char** hookCommands = NULL;
    int* keyTypes = NULL;
    if(RedisGears_BRReadLong(br)){
        eventTypes = array_new(char*, 10);
        size_t len = RedisGears_BRReadLong(br);
        for(size_t i = 0 ; i < len ; ++i){
            eventTypes = array_append(eventTypes, RG_STRDUP(RedisGears_BRReadString(br)));
        }
    }

    int hookCommandsExists;
    if(encver >= VERSION_WITH_KEYS_READER_READ_CALLBACK){
        hookCommandsExists = RedisGears_BRReadLong(br);
    }else{
        hookCommandsExists = 0;
    }

    if(hookCommandsExists){
        hookCommands = array_new(char*, 10);
        size_t len = RedisGears_BRReadLong(br);
        for(size_t i = 0 ; i < len ; ++i){
            hookCommands = array_append(hookCommands, RG_STRDUP(RedisGears_BRReadString(br)));
        }
    }

    if(RedisGears_BRReadLong(br)){
        keyTypes = array_new(int, 10);
        size_t len = RedisGears_BRReadLong(br);
        for(size_t i = 0 ; i < len ; ++i){
            keyTypes = array_append(keyTypes, RedisGears_BRReadLong(br));
        }
    }

    bool readValue = RedisGears_BRReadLong(br);
    KeysReaderTriggerArgs* ret = KeysReaderTriggerArgs_Create(regex, eventTypes, keyTypes, readValue);
    if(hookCommands){
        KeysReaderTriggerArgs_SetTriggerHookCommands(ret, hookCommands);
    }
    if(encver >= VERSION_WITH_KEYS_READER_READ_CALLBACK){
        if(RedisGears_BRReadLong(br)){
            const char* readRecordCallback = RedisGears_BRReadString(br);
            if(KeysReaderTriggerArgs_SetReadRecordCallback(ret, readRecordCallback) != REDISMODULE_OK){
                RedisModule_Log(staticCtx, "warning", "Failed loading readRecordCallback");
                KeysReaderTriggerArgs_Free(ret);
                ret = NULL;
            }
        }
    }
    return ret;
}

static void KeysReader_UnregisterTrigger(FlatExecutionPlan* fep, bool abortPending){
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, FindRegistrationDataFlagPop);
    RedisModule_Assert(rData);

    if(abortPending){
        // unregister require aborting all pending executions
        ExecutionPlan** abortEpArray = array_new(ExecutionPlan*, 10);
        Gears_dictIterator *iter = Gears_dictGetIterator(rData->localPendingExecutions);
        Gears_dictEntry* entry = NULL;
        while((entry = Gears_dictNext(iter))){
            char* epIdStr = Gears_dictGetKey(entry);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
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

        for(size_t i = 0 ; i < array_len(abortEpArray) ; ++i){
            // we can not free while iterating so we add to the epArr and free after
            if(RedisGears_AbortExecution(abortEpArray[i]) != REDISMODULE_OK){
                RedisModule_Log(staticCtx, "warning", "Failed aborting execution on unregister.");
            }
        }

        array_free(abortEpArray);
    }

    KeysReaderRegisterData_Free(rData);
}

static const char* KeysReader_GetKeyTypeStr(int keyType){
    switch(keyType){
    case REDISMODULE_KEYTYPE_STRING:
        return "string";
    case REDISMODULE_KEYTYPE_LIST:
        return "list";
    case REDISMODULE_KEYTYPE_HASH:
        return "hash";
    case REDISMODULE_KEYTYPE_SET:
         return "set";
    case REDISMODULE_KEYTYPE_ZSET:
        return "zset";
    case REDISMODULE_KEYTYPE_MODULE:
        return "module";
    default:
        RedisModule_Assert(false);
        return NULL;
    }
}

static char* KeysReader_keyTypeToStr(int keyType){
    return RG_STRDUP(KeysReader_GetKeyTypeStr(keyType));
}

static char* KeysReader_StrToStr(void* str){
    return RG_STRDUP((char*)str);
}

static void KeysReader_DumpRegistrationInfo(FlatExecutionPlan* fep, RedisModuleInfoCtx *ctx, int for_crash_report) {
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, 0);

    if(rData->mode == ExecutionModeSync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "sync");
    } else if(rData->mode == ExecutionModeAsync){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async");
    } else if(rData->mode == ExecutionModeAsyncLocal){
        RedisModule_InfoAddFieldCString(ctx, "mode", "async_local");
    } else{
        RedisModule_InfoAddFieldCString(ctx, "mode", "unknown");
    }

    RedisModule_InfoAddFieldULongLong(ctx, "numTriggered", rData->numTriggered);
    RedisModule_InfoAddFieldULongLong(ctx, "numSuccess", rData->numSuccess);
    RedisModule_InfoAddFieldULongLong(ctx, "numFailures", rData->numFailures);
    RedisModule_InfoAddFieldULongLong(ctx, "numAborted", rData->numAborted);
    RedisModule_InfoAddFieldULongLong(ctx, "lastRunDurationMS", DURATION2MS(rData->lastRunDuration));
    RedisModule_InfoAddFieldULongLong(ctx, "totalRunDurationMS", totalDurationMS(rData));
    RedisModule_InfoAddFieldDouble(ctx, "avgRunDurationMS", avgDurationMS(rData));
    RedisModule_InfoAddFieldCString(ctx, "lastError", rData->lastError ? rData->lastError : "None");
    RedisModule_InfoAddFieldCString(ctx, "regex", rData->args->prefix);

    char* eventTypesStr = rData->args->eventTypes? ArrToStr((void**)rData->args->eventTypes, array_len(rData->args->eventTypes), KeysReader_StrToStr, '|') : NULL;
    RedisModule_InfoAddFieldCString(ctx, "eventTypes", eventTypesStr? eventTypesStr : "None");
    RG_FREE(eventTypesStr);

    char* keyTypesStr = rData->args->keyTypes? IntArrToStr(rData->args->keyTypes, array_len(rData->args->keyTypes), KeysReader_keyTypeToStr, '|') : NULL;
    RedisModule_InfoAddFieldCString(ctx, "keyTypes", keyTypesStr? keyTypesStr : "None");
    RG_FREE(keyTypesStr);

    char* hookCommandsStr = rData->args->hookCommands? ArrToStr((void**)rData->args->hookCommands, array_len(rData->args->hookCommands), KeysReader_StrToStr, '|') : NULL;
    RedisModule_InfoAddFieldCString(ctx, "hookCommands", hookCommandsStr? hookCommandsStr : "None");
    RG_FREE(hookCommandsStr);
}

static void KeysReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, 0);
    RedisModule_Assert(rData);
    RedisModule_ReplyWithArray(ctx, 20);
    RedisModule_ReplyWithStringBuffer(ctx, "mode", strlen("mode"));
    if(rData->mode == ExecutionModeSync){
        RedisModule_ReplyWithStringBuffer(ctx, "sync", strlen("sync"));
    } else if(rData->mode == ExecutionModeAsync){
        RedisModule_ReplyWithStringBuffer(ctx, "async", strlen("async"));
    } else if(rData->mode == ExecutionModeAsyncLocal){
        RedisModule_ReplyWithStringBuffer(ctx, "async_local", strlen("async_local"));
    } else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "numTriggered", strlen("numTriggered"));
    RedisModule_ReplyWithLongLong(ctx, rData->numTriggered);
    RedisModule_ReplyWithStringBuffer(ctx, "numSuccess", strlen("numSuccess"));
    RedisModule_ReplyWithLongLong(ctx, rData->numSuccess);
    RedisModule_ReplyWithStringBuffer(ctx, "numFailures", strlen("numFailures"));
    RedisModule_ReplyWithLongLong(ctx, rData->numFailures);
    RedisModule_ReplyWithStringBuffer(ctx, "numAborted", strlen("numAborted"));
    RedisModule_ReplyWithLongLong(ctx, rData->numAborted);
    RedisModule_ReplyWithStringBuffer(ctx, "lastRunDurationMS", strlen("lastRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, DURATION2MS(rData->lastRunDuration));
    RedisModule_ReplyWithStringBuffer(ctx, "totalRunDurationMS", strlen("totalRunDurationMS"));
    RedisModule_ReplyWithLongLong(ctx, totalDurationMS(rData));
    RedisModule_ReplyWithStringBuffer(ctx, "avgRunDurationMS", strlen("avgRunDurationMS"));
    RedisModule_ReplyWithDouble(ctx, avgDurationMS(rData));
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(rData->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, rData->lastError, strlen(rData->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 8);
    RedisModule_ReplyWithStringBuffer(ctx, "regex", strlen("regex"));
    RedisModule_ReplyWithStringBuffer(ctx, rData->args->prefix, strlen(rData->args->prefix));
    RedisModule_ReplyWithStringBuffer(ctx, "eventTypes", strlen("eventTypes"));
    if(rData->args->eventTypes){
        RedisModule_ReplyWithArray(ctx, array_len(rData->args->eventTypes));
        for(size_t i = 0 ; i < array_len(rData->args->eventTypes) ; ++i){
            RedisModule_ReplyWithStringBuffer(ctx, rData->args->eventTypes[i], strlen(rData->args->eventTypes[i]));
        }
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "keyTypes", strlen("keyTypes"));
    if(rData->args->keyTypes){
        RedisModule_ReplyWithArray(ctx, array_len(rData->args->keyTypes));
        for(size_t i = 0 ; i < array_len(rData->args->keyTypes) ; ++i){
            const char* keyType = KeysReader_GetKeyTypeStr(rData->args->keyTypes[i]);
            RedisModule_ReplyWithStringBuffer(ctx, keyType, strlen(keyType));
        }
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "hookCommands", strlen("hookCommands"));
    if(rData->args->hookCommands){
        RedisModule_ReplyWithArray(ctx, array_len(rData->args->hookCommands));
        for(size_t i = 0 ; i < array_len(rData->args->hookCommands) ; ++i){
            RedisModule_ReplyWithStringBuffer(ctx, rData->args->hookCommands[i], strlen(rData->args->hookCommands[i]));
        }
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
}

static void KeysReader_RegisterKeySpaceEvent(){
    if(!keysReaderRegistration){
        keysReaderRegistration = Gears_listCreate();
        int event = REDISMODULE_NOTIFY_ALL | REDISMODULE_NOTIFY_KEY_MISS;
        if(currVesion.redisMajorVersion >= 6 && IsEnterprise()){
            // we get the trimmed notification on enterprise only from redis v6 and above
            event |= REDISMODULE_NOTIFY_TRIMMED;
        }
        if(RedisModule_SubscribeToKeyspaceEvents(staticCtx, event, KeysReader_OnKeyTouched) != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "Failed register on key space notification of KeysReader");
        }
    }
}

static int KeysReader_CommandHook(RedisModuleCtx* ctx, RedisModuleString** argv, size_t argc, void* pd){
    currCmdCtx = KeyReader_CommandCtxCreate(ctx, argv, argc);

    // call the command
    const char* subCommand = RedisModule_StringPtrLen(argv[0], NULL);
    currCmdCtx->realReply = RedisModule_Call(staticCtx, subCommand, "!v", argv + 1, argc - 1);
    if(!currCmdCtx->realReply){
        if(errno){
            currCmdCtx->errnostr = RG_STRDUP(strerror(errno));
        }
    }

    KeyReader_CommandCtxFree(currCmdCtx);
    currCmdCtx = NULL;

    return REDISMODULE_OK;
}

static int KeysReader_VerifyRegister(SessionRegistrationCtx *srctx, FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    KeysReaderTriggerArgs* readerArgs = args;
    if(readerArgs->hookCommands){
        for(size_t i = 0 ; i < array_len(readerArgs->hookCommands) ; ++i){
            if (CommandHook_VerifyHook(readerArgs->hookCommands[i], readerArgs->prefix, err) != REDISMODULE_OK) {
                return REDISMODULE_ERR;
            }
        }
    }
    return REDISMODULE_OK;
}

static int KeysReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    KeysReaderTriggerArgs* readerArgs = args;

    CommandHookCtx** hooks = NULL;

    if(readerArgs->hookCommands){
        hooks = array_new(CommandHookCtx*, 10);
        for(size_t i = 0 ; i < array_len(readerArgs->hookCommands) ; ++i){
            CommandHookCtx* hookCtx = CommandHook_Hook(readerArgs->hookCommands[i], readerArgs->prefix, KeysReader_CommandHook, NULL, err);
            if(!hookCtx){
                for(size_t i = 0 ; i < array_len(hooks) ; ++i){
                    CommandHook_Unhook(hooks[i]);
                }
                array_free(hooks);
                return REDISMODULE_ERR;
            }
            hooks = array_append(hooks, hookCtx);
        }
    }

    KeysReader_RegisterKeySpaceEvent();

    KeysReaderRegisterData* rData = KeysReaderRegisterData_Create(fep, args, mode, hooks);

    Gears_listAddNodeTail(keysReaderRegistration, rData);
    return REDISMODULE_OK;
}

int KeysReader_Initialize(RedisModuleCtx* ctx){
    RedisVersion vertionWithScanAPI = {
            .redisMajorVersion = 6,
            .redisMinorVersion = 0,
            .redisPatchVersion = 6,
    };

    if ((GearsCompareVersions(currVesion, vertionWithScanAPI)) >= 0 && (RedisModule_Scan != NULL)) {
        KeysReader_ScanNextKeyFunc = KeysReader_ScanAPINextKey;
    }else{
        KeysReader_ScanNextKeyFunc = KeysReader_ScanNextKey;
    }
	return REDISMODULE_OK;
}

KeysReaderTriggerArgs* KeysReaderTriggerArgs_Create(const char* prefix, char** eventTypes, int* keyTypes, bool readValue){
    KeysReaderTriggerArgs* ret = RG_ALLOC(sizeof(*ret));
    *ret = (KeysReaderTriggerArgs){
        .prefix = RG_STRDUP(prefix),
        .eventTypes = eventTypes,
        .keyTypes = keyTypes,
        .readValue = readValue,
        .readRecordStr = NULL,
        .readRecord = NULL,
        .hookCommands = NULL,
    };
    return ret;
}

void KeysReaderTriggerArgs_SetTriggerHookCommands(KeysReaderTriggerArgs* krta, char** hookCommands){
    krta->hookCommands = hookCommands;
}

int KeysReaderTriggerArgs_SetReadRecordCallback(KeysReaderTriggerArgs* krta, const char* readRecordCallback){
    krta->readRecord = KeysReaderReadRecordsMgmt_Get(readRecordCallback);
    if(!krta->readRecord){
        return REDISMODULE_ERR;
    }
    krta->readRecordStr = RG_STRDUP(readRecordCallback);
    return REDISMODULE_OK;
}

static Reader* KeysReader_Create(void* arg){
    KeysReaderCtx* ctx = arg;
    if(!ctx){
        ctx = KeysReaderCtx_Create(NULL, true, NULL, true);
    }
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .next = KeysReader_Next,
        .free = KeysReaderCtx_Free,
        .serialize = RG_KeysReaderCtxSerialize,
        .deserialize = RG_KeysReaderCtxDeserialize,
    };
    return r;
}

static void GenericKeysReader_RdbSave(RedisModuleIO *rdb, bool (*shouldClear)(FlatExecutionPlan*)){
    if(!keysReaderRegistration){
        RedisModule_SaveUnsigned(rdb, 0); // done
        return;
    }

    Gears_Buffer* buf = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buf);

    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(!shouldClear(rData->fep)){
            continue;
        }
        RedisModule_SaveUnsigned(rdb, 1); // has more

        // serialize args
        char* err = NULL;
        int res = FlatExecutionPlan_Serialize(&bw, rData->fep, &err);
        if(res != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "Failed serializing fep, err='%s'", err);
            RedisModule_Assert(false); // fep already registered, must be serializable.
        }

        KeysReader_SerializeArgs(rData->args, &bw);

        RedisModule_SaveStringBuffer(rdb, buf->buff, buf->size);

        RedisModule_SaveUnsigned(rdb, rData->mode);

        Gears_BufferClear(buf);

    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);

    Gears_BufferFree(buf);
}

static bool KeysReader_ShouldContinue(FlatExecutionPlan* fep){
    return strcmp(fep->reader->reader, "KeysReader") == 0;
}

static void KeysReader_RdbSave(RedisModuleIO *rdb){
    GenericKeysReader_RdbSave(rdb, KeysReader_ShouldContinue);
}

static int KeysReader_RdbLoad(RedisModuleIO *rdb, int encver){
    while(RedisModule_LoadUnsigned(rdb)){
        size_t len;
        char* data = RedisModule_LoadStringBuffer(rdb, &len);

        Gears_Buffer buf = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, &buf);

        char* err = NULL;
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br, &err, encver);
        if(!fep){
            RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution, error='%s'", err);
            RedisModule_Free(data);
            return REDISMODULE_ERR;
        }

        void* args = KeysReader_DeserializeArgs(&br, encver);
        RedisModule_Free(data);

        if(!args){
            RedisModule_Log(staticCtx, "warning", "Could not deserialize flat execution args");
            FlatExecutionPlan_Free(fep);
            return REDISMODULE_ERR;
        }

        int mode = RedisModule_LoadUnsigned(rdb);
        int ret = KeysReader_RegisrterTrigger(fep, mode, args, &err);
        if(ret != REDISMODULE_OK){
            RedisModule_Log(staticCtx, "warning", "Could not register flat execution, error='%s'", err);
            KeysReaderTriggerArgs_Free(args);
            FlatExecutionPlan_Free(fep);
            return REDISMODULE_ERR;
        }

        FlatExecutionPlan_AddToRegisterDict(fep);
    }
    return REDISMODULE_OK;
}

static void GenricKeysReader_ClearStats(bool (*shouldClear)(FlatExecutionPlan*)){
    if(!keysReaderRegistration){
        return;
    }
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(!shouldClear(rData->fep)){
            continue;
        }
        resetStats(rData);
    }
    Gears_listReleaseIterator(iter);
}

static void GenricKeysReader_Clear(bool (*shouldClear)(FlatExecutionPlan*)){
    if(!keysReaderRegistration){
        return;
    }
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(!shouldClear(rData->fep)){
            continue;
        }
        FlatExecutionPlan_RemoveFromRegisterDict(rData->fep);
        KeysReaderRegisterData_Free(rData);
        Gears_listDelNode(keysReaderRegistration, node);
    }
    Gears_listReleaseIterator(iter);
}

static void KeysReader_ClearStats(){
    GenricKeysReader_ClearStats(KeysReader_ShouldContinue);
}

static void KeysReader_Clear(){
    GenricKeysReader_Clear(KeysReader_ShouldContinue);
}

static void KeysReader_FreeArgs(void* args){
    KeysReaderTriggerArgs_Free(args);
}

RedisGears_ReaderCallbacks KeysReader = {
        .create = KeysReader_Create,
        .verifyRegister = KeysReader_VerifyRegister,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
        .serializeTriggerArgs = KeysReader_SerializeArgs,
        .deserializeTriggerArgs = KeysReader_DeserializeArgs,
        .freeTriggerArgs = KeysReader_FreeArgs,
        .dumpRegistratioData = KeysReader_DumpRegistrationData,
        .dumpRegistratioInfo = KeysReader_DumpRegistrationInfo,
        .rdbSave = KeysReader_RdbSave,
        .rdbLoad = KeysReader_RdbLoad,
        .clear = KeysReader_Clear,
        .clearStats = KeysReader_ClearStats,
};
