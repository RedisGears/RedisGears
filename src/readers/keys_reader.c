#include "keys_reader.h"
#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "execution_plan.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "globals.h"
#include "lock_handler.h"
#include "record.h"
#include "config.h"

#include <assert.h>

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10

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
    Gears_list* localPendingExecutions;
    Gears_list* localDoneExecutions;
    WorkerData* wd;
}KeysReaderRegisterData;

Gears_list* keysReaderRegistration = NULL;

RedisModuleDict *keysDict = NULL;

static Record* KeysReader_Next(ExecutionCtx* ectx, void* ctx);

typedef struct KeysReaderCtx{
    size_t matchLen;
    char* match;
    long long cursorIndex;
    bool isDone;
    Record** pendingRecords;
    bool readValue;
    bool isPrefix;
}KeysReaderCtx;

typedef struct KeysReaderTriggerArgs{
    char* regex;
    char** eventTypes;
    int* keyTypes;
}KeysReaderTriggerArgs;

void KeysReaderTriggerArgs_Free(KeysReaderTriggerArgs* args){
    RG_FREE(args->regex);
    if(args->eventTypes){
        array_free_ex(args->eventTypes, RG_FREE(*(char**)ptr));
    }
    if(args->keyTypes){
        array_free(args->keyTypes);
    }
    RG_FREE(args);
}

static void KeysReaderRegisterData_Free(KeysReaderRegisterData* rData){
    if((--rData->refCount) == 0){

        Gears_listNode* n = NULL;

        // if we free registration there must not be any pending executions.
        // either all executions was finished or aborted
        assert(Gears_listLength(rData->localPendingExecutions) == 0);

        while((n = Gears_listFirst(rData->localDoneExecutions))){
            char* epIdStr = Gears_listNodeValue(n);
            Gears_listDelNode(rData->localDoneExecutions, n);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            RG_FREE(epIdStr);
            if(!ep){
                RedisModule_Log(NULL, "info", "Failed finding done execution to drop on unregister. Execution was probably already dropped.");
                continue;
            }
            // all the executions here are done, will just drop it.
            RedisGears_DropExecution(ep);
        }

        Gears_listRelease(rData->localPendingExecutions);
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

static KeysReaderRegisterData* KeysReaderRegisterData_Create(FlatExecutionPlan* fep, void* args, ExecutionMode mode){
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
        .localPendingExecutions = Gears_listCreate(),
        .localDoneExecutions = Gears_listCreate(),
        .wd = RedisGears_WorkerDataCreate(NULL),
    };
    return rData;
}

static KeysReaderRegisterData* KeysReaderRegisterData_GetShallowCopy(KeysReaderRegisterData* rData){
    ++rData->refCount;
    return rData;
}

static bool KeysReaderCtx_AnalizeArg(char* match, size_t* matchLen){
    bool isPrefix = false;
    if(match){
        *matchLen = strlen(match);
        if(match[*matchLen - 1] == '*'){
            isPrefix = true;
        }
    }
    return isPrefix;
}

static void KeysReaderCtx_Reset(void* ctx, void* arg){
    KeysReaderCtx* krctx = ctx;
    while(array_len(krctx->pendingRecords) > 0){
        RedisGears_FreeRecord(array_pop(krctx->pendingRecords));
    }
    if(krctx->match){
        RG_FREE(krctx->match);
    }

    krctx->cursorIndex = 0;
    krctx->isDone = false;

    krctx->isPrefix = KeysReaderCtx_AnalizeArg(arg, &krctx->matchLen);
    krctx->match = arg;
}

static KeysReaderCtx* KeysReaderCtx_Create(char* match, bool readValue){
#define PENDING_KEYS_INIT_CAP 10
    size_t matchLen = 0;
    bool isPrefix = KeysReaderCtx_AnalizeArg(match, &matchLen);
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .matchLen = matchLen,
        .match = match,
        .cursorIndex = 0,
        .isDone = false,
        .readValue = readValue,
        .isPrefix = isPrefix,
        .pendingRecords = array_new(Record*, PENDING_KEYS_INIT_CAP),
    };
    return krctx;
}

static void RG_KeysReaderCtxSerialize(void* ctx, Gears_BufferWriter* bw){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    RedisGears_BWWriteString(bw, krctx->match);
}

static void RG_KeysReaderCtxDeserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    char* match = RedisGears_BRReadString(br);
    krctx->match = RG_STRDUP(match);
    krctx->matchLen = strlen(krctx->match);
    if(match[krctx->matchLen - 1] == '*'){
        krctx->isPrefix = true;
    }
}

static void KeysReader_Free(void* ctx){
    KeysReaderCtx* krctx = ctx;
    if(krctx->match){
        RG_FREE(krctx->match);
    }
    for(size_t i = 0 ; i < array_len(krctx->pendingRecords) ; ++i){
        RedisGears_FreeRecord(krctx->pendingRecords[i]);
    }
    array_free(krctx->pendingRecords);
    RG_FREE(krctx);
}

static Record* ValueToStringMapper(Record *record, RedisModuleKey* handler){
    size_t len;
    char* val = RedisModule_StringDMA(handler, &len, REDISMODULE_READ);
    char* strVal = RG_ALLOC(len + 1);
    memcpy(strVal, val, len);
    strVal[len] = '\0';

    Record* strRecord = RedisGears_StringRecordCreate(strVal, len);

    RedisGears_KeyRecordSetVal(record, strRecord);
    return record;
}

static Record* ValueToHashSetMapper(Record *record, RedisModuleCtx* ctx){
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGETALL", "c", RedisGears_KeyRecordGetKey(record, NULL));
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t len = RedisModule_CallReplyLength(reply);
    assert(len % 2 == 0);
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
    RedisGears_KeyRecordSetVal(record, hashSetRecord);
    RedisModule_FreeCallReply(reply);
    return record;
}

static Record* ValueToListMapper(Record *record, RedisModuleCtx* ctx){
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "lrange", "cll", RedisGears_KeyRecordGetKey(record, NULL), 0, -1);
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
    size_t len = RedisModule_CallReplyLength(reply);
    Record *listRecord = RedisGears_ListRecordCreate(10);
    for(int i = 0 ; i < len ; ++i){
        RedisModuleCallReply *r = RedisModule_CallReplyArrayElement(reply, i);
        assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING);
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
    RedisGears_KeyRecordSetVal(record, listRecord);
    return record;
}

static Record* ValueToRecordMapper(RedisModuleCtx* rctx, Record* record, RedisModuleKey* handler){
    switch(RedisModule_KeyType(handler)){
    case REDISMODULE_KEYTYPE_STRING:
        return ValueToStringMapper(record, handler);
        break;
    case REDISMODULE_KEYTYPE_LIST:
        return ValueToListMapper(record, rctx);
        break;
    case REDISMODULE_KEYTYPE_HASH:
        return ValueToHashSetMapper(record, rctx);
        break;
    default:
        // we do not want how to parse this type, we will return a key record with no value
        return record;
    }
}

static Record* KeysReader_ReadKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx, RedisModuleString* key){
    size_t keyLen;
    const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);
    Record* record = NULL;

    char* keyCStr = RG_ALLOC(keyLen + 1);
    memcpy(keyCStr, keyStr, keyLen);
    keyCStr[keyLen] = '\0';

    RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
    if(!keyHandler){
        if(readerCtx->readValue){
            // if we reached here and the key does not exists
            // lets return a dictionary with only the key indicating that the key was deleted.
            Record* keyRecord = RedisGears_StringRecordCreate(keyCStr, keyLen);
            record = RedisGears_HashSetRecordCreate();
            RedisGears_HashSetRecordSet(record, "key", keyRecord);
            return record;
        }
        RG_FREE(keyCStr);
        return NULL;
    }

    if(!readerCtx->readValue){
        record = RedisGears_StringRecordCreate(keyCStr, keyLen);
    }else{
        record = RedisGears_KeyRecordCreate();

        RedisGears_KeyRecordSetKey(record, keyCStr, keyLen);

        ValueToRecordMapper(rctx, record, keyHandler);
    }

    RedisModule_CloseKey(keyHandler);

    return record;
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

        assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

        assert(RedisModule_CallReplyLength(reply) == 2);

        RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

        assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

        RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
        RedisModule_StringToLongLong(cursorStr, &readerCtx->cursorIndex);
        RedisModule_FreeString(rctx, cursorStr);

        if(readerCtx->cursorIndex == 0){
            readerCtx->isDone = true;
        }

        RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
        assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
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
            assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
            RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
            Record* record = KeysReader_ReadKey(rctx, readerCtx, key);
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
    if(readerCtx->isPrefix){
        record = KeysReader_ScanNextKey(RedisGears_GetRedisModuleCtx(ectx), readerCtx);
    }else{
        if(readerCtx->isDone){
            return NULL;
        }
        if(!readerCtx->readValue){
            record = RedisGears_StringRecordCreate(RG_STRDUP(readerCtx->match), readerCtx->matchLen);
        }else{
            RedisModuleString* key = RedisModule_CreateString(NULL, readerCtx->match, readerCtx->matchLen);
            RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
            LockHandler_Acquire(rctx);
            record = KeysReader_ReadKey(rctx, readerCtx, key);
            LockHandler_Release(rctx);
            RedisModule_FreeString(NULL, key);
        }
        readerCtx->isDone = true;
    }
    return record;
}

static void KeysReader_ExecutionDone(ExecutionPlan* ctx, void* privateData){
    KeysReaderRegisterData* rData = privateData;

    if(EPIsFlagOn(ctx, EFIsLocal)){
        Gears_listNode *head = Gears_listFirst(rData->localPendingExecutions);
        char* epIdStr = NULL;
        while(head){
            epIdStr = Gears_listNodeValue(head);
            Gears_listDelNode(rData->localPendingExecutions, head);
            if(strcmp(epIdStr, ctx->idStr) != 0){
                RedisModule_Log(NULL, "warning", "Got an out of order execution on registration, ignoring execution.");
                RG_FREE(epIdStr);
                head = Gears_listFirst(rData->localPendingExecutions);
                continue;
            }
            // Found the execution id, we can stop iterating
            break;
        }
        if(!epIdStr){
            epIdStr = RG_STRDUP(ctx->idStr);
        }

        if(EPIsFlagOn(ctx, EFIsLocal)){
            // Add the execution id to the localDoneExecutions list
            Gears_listAddNodeTail(rData->localDoneExecutions, epIdStr);
            if(GearsConfig_GetMaxExecutionsPerRegistration() > 0 && Gears_listLength(rData->localDoneExecutions) > GearsConfig_GetMaxExecutionsPerRegistration()){
                Gears_listNode *head = Gears_listFirst(rData->localDoneExecutions);
                epIdStr = Gears_listNodeValue(head);
                ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
                if(ep){
                    assert(EPIsFlagOn(ep, EFDone));
                    RedisGears_DropExecution(ep);
                }
                RG_FREE(epIdStr);
                Gears_listDelNode(rData->localDoneExecutions, head);
            }
        }
    }

    long long errorsLen = RedisGears_GetErrorsLen(ctx);

    if(errorsLen > 0){
        ++rData->numFailures;
        Record* r = RedisGears_GetError(ctx, 0);
        assert(RedisGears_RecordGetType(r) == ERROR_RECORD);
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
            if(strcmp(args->eventTypes[i], event) == 0){
                evenFound = true;
                break;
            }
        }
        if(!evenFound){
            return 0;
        }
    }
    if(args->keyTypes){
        RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
        int type = RedisModule_KeyType(kp);
        RedisModule_CloseKey(kp);
        if(type != REDISMODULE_KEYTYPE_EMPTY){
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
    }
    const char* keyCStr = RedisModule_StringPtrLen(key, NULL);
    return KeysReader_IsKeyMatch(args->regex, keyCStr);
}

static int KeysReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
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
            ExecutionPlan* ep = RedisGears_Run(rData->fep, rData->mode, RG_STRDUP(keyCStr), callback, privateData, rData->wd, &err);
            if(!ep){
                ++rData->numAborted;
                RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger, %s", err);
                if(err){
                    RG_FREE(err);
                }
                continue;
            }
            if(EPIsFlagOn(ep, EFIsLocal) && rData->mode != ExecutionModeSync){
                // execution is local
                // If execution is SYNC it will be added to localDoneExecutions on done
                // Otherwise, save it to the registration pending execution list.
                // currently we are not save global executions and those will not be listed
                // in the registration execution list nor will be drop on unregister.
                // todo: handle none local executions
                char* idStr = RG_STRDUP(ep->idStr);
                Gears_listAddNodeTail(rData->localPendingExecutions, idStr);
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
    RedisGears_BWWriteString(bw, args->regex);
    if(args->eventTypes){
        RedisGears_BWWriteLong(bw, 1); // eventTypes exists
        RedisGears_BWWriteLong(bw, array_len(args->eventTypes));
        for(size_t i = 0 ; i < array_len(args->eventTypes) ; ++i){
            RedisGears_BWWriteString(bw, args->eventTypes[i]);
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // eventTypes does not exist
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
}

static void* KeysReader_DeserializeArgs(Gears_BufferReader* br){
    char* regex = RedisGears_BRReadString(br);
    char** eventTypes = NULL;
    int* keyTypes = NULL;
    if(RedisGears_BRReadLong(br)){
        eventTypes = array_new(char*, 10);
        size_t len = RedisGears_BRReadLong(br);
        for(size_t i = 0 ; i < len ; ++i){
            eventTypes = array_append(eventTypes, RG_STRDUP(RedisGears_BRReadString(br)));
        }
    }
    if(RedisGears_BRReadLong(br)){
        keyTypes = array_new(int, 10);
        size_t len = RedisGears_BRReadLong(br);
        for(size_t i = 0 ; i < len ; ++i){
            keyTypes = array_append(keyTypes, RedisGears_BRReadLong(br));
        }
    }
    return KeysReaderTriggerArgs_Create(regex, eventTypes, keyTypes);
}

static void KeysReader_UnregisterTrigger(FlatExecutionPlan* fep, bool abortPending){
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, FindRegistrationDataFlagPop);
    assert(rData);

    if(abortPending){
        // unregister require aborting all pending executions
        ExecutionPlan** abortEpArray = array_new(ExecutionPlan*, 10);

        Gears_listNode* n = NULL;
        Gears_listIter *iter = Gears_listGetIterator(rData->localPendingExecutions, AL_START_HEAD);
        while((n = Gears_listNext(iter))){
            char* epIdStr = Gears_listNodeValue(n);
            ExecutionPlan* ep = RedisGears_GetExecution(epIdStr);
            if(!ep){
                RedisModule_Log(NULL, "warning", "Failed finding pending execution to abort on unregister.");
                continue;
            }

            // we can not abort right now cause aborting might cause values to be deleted
            // from localPendingExecutions and it will mess up with the iterator
            // so we must collect all the exeuctions first and then abort one by one
            abortEpArray = array_append(abortEpArray, ep);
        }
        Gears_listReleaseIterator(iter);

        for(size_t i = 0 ; i < array_len(abortEpArray) ; ++i){
            // we can not free while iterating so we add to the epArr and free after
            if(RedisGears_AbortExecution(abortEpArray[i]) != REDISMODULE_OK){
                RedisModule_Log(NULL, "warning", "Failed aborting execution on unregister.");
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
        assert(false);
        return NULL;
    }
}

static void KeysReader_DumpRegistrationData(RedisModuleCtx* ctx, FlatExecutionPlan* fep){
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, 0);
    assert(rData);
    RedisModule_ReplyWithArray(ctx, 14);
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
    RedisModule_ReplyWithStringBuffer(ctx, "lastError", strlen("lastError"));
    if(rData->lastError){
        RedisModule_ReplyWithStringBuffer(ctx, rData->lastError, strlen(rData->lastError));
    }else{
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithStringBuffer(ctx, "args", strlen("args"));
    RedisModule_ReplyWithArray(ctx, 6);
    RedisModule_ReplyWithStringBuffer(ctx, "regex", strlen("regex"));
    RedisModule_ReplyWithStringBuffer(ctx, rData->args->regex, strlen(rData->args->regex));
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
}

static void KeysReader_RegisterKeySpaceEvent(){
    if(!keysReaderRegistration){
        RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
        keysReaderRegistration = Gears_listCreate();
        if(RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_ALL, KeysReader_OnKeyTouched) != REDISMODULE_OK){
            // todo : print warning
        }
        RedisModule_FreeThreadSafeContext(ctx);
    }
}

static int KeysReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err){
    KeysReader_RegisterKeySpaceEvent();

    KeysReaderRegisterData* rData = KeysReaderRegisterData_Create(fep, args, mode);

    Gears_listAddNodeTail(keysReaderRegistration, rData);
    return REDISMODULE_OK;
}

int KeysReader_Initialize(RedisModuleCtx* ctx){
	return REDISMODULE_OK;
}

KeysReaderTriggerArgs* KeysReaderTriggerArgs_Create(const char* regex, char** eventTypes, int* keyTypes){
    KeysReaderTriggerArgs* ret = RG_ALLOC(sizeof(*ret));
    *ret = (KeysReaderTriggerArgs){
        .regex = RG_STRDUP(regex),
        .eventTypes = eventTypes,
        .keyTypes = keyTypes,
    };
    return ret;
}

static Reader* KeysReader_Create(void* arg){
    KeysReaderCtx* ctx = KeysReaderCtx_Create(arg, true);
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .next = KeysReader_Next,
        .free = KeysReader_Free,
        .serialize = RG_KeysReaderCtxSerialize,
        .deserialize = RG_KeysReaderCtxDeserialize,
    };
    return r;
}

static Reader* KeysOnlyReader_Create(void* arg){
    KeysReaderCtx* ctx = KeysReaderCtx_Create(arg, false);
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .next = KeysReader_Next,
        .free = KeysReader_Free,
        .reset = KeysReaderCtx_Reset,
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
        int res = FlatExecutionPlan_Serialize(&bw, rData->fep, NULL);
        assert(res == REDISMODULE_OK); // fep already registered, must be serializable.

        KeysReader_SerializeArgs(rData->args, &bw);

        RedisModule_SaveStringBuffer(rdb, buf->buff, buf->size);

        RedisModule_SaveUnsigned(rdb, rData->mode);

        Gears_BufferClear(buf);

    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);

    Gears_BufferFree(buf);
}

static bool KeysOnlyReader_ShouldContinue(FlatExecutionPlan* fep){
    return strcmp(fep->reader->reader, "KeysOnlyReader") == 0;
}

static bool KeysReader_ShouldContinue(FlatExecutionPlan* fep){
    return strcmp(fep->reader->reader, "KeysReader") == 0;
}

static void KeysReader_RdbSave(RedisModuleIO *rdb){
    GenericKeysReader_RdbSave(rdb, KeysReader_ShouldContinue);
}

static void KeysOnlyReader_RdbSave(RedisModuleIO *rdb){
    GenericKeysReader_RdbSave(rdb, KeysOnlyReader_ShouldContinue);
}

static void KeysReader_RdbLoad(RedisModuleIO *rdb, int encver){
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
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br, &err);
        if(!fep){
            RedisModule_Log(NULL, "Could not deserialize flat execution, error='%s'", err);
            assert(false);
        }

        void* args = KeysReader_DeserializeArgs(&br);
        RedisModule_Free(data);

        int mode = RedisModule_LoadUnsigned(rdb);
        int ret = KeysReader_RegisrterTrigger(fep, mode, args, &err);
        if(ret != REDISMODULE_OK){
            RedisModule_Log(NULL, "Could not register flat execution, error='%s'", err);
            assert(false);
        }

        FlatExecutionPlan_AddToRegisterDict(fep);
    }
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

static void KeysReader_Clear(){
    GenricKeysReader_Clear(KeysReader_ShouldContinue);
}

static void KeysOnlyReader_Clear(){
    GenricKeysReader_Clear(KeysOnlyReader_ShouldContinue);
}

static void KeysReader_FreeArgs(void* args){
    KeysReaderTriggerArgs_Free(args);
}

RedisGears_ReaderCallbacks KeysReader = {
        .create = KeysReader_Create,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
        .serializeTriggerArgs = KeysReader_SerializeArgs,
        .deserializeTriggerArgs = KeysReader_DeserializeArgs,
        .freeTriggerArgs = KeysReader_FreeArgs,
        .dumpRegistratioData = KeysReader_DumpRegistrationData,
        .rdbSave = KeysReader_RdbSave,
        .rdbLoad = KeysReader_RdbLoad,
        .clear = KeysReader_Clear,
};

RedisGears_ReaderCallbacks KeysOnlyReader = {
        .create = KeysOnlyReader_Create,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
        .serializeTriggerArgs = KeysReader_SerializeArgs,
        .deserializeTriggerArgs = KeysReader_DeserializeArgs,
        .freeTriggerArgs = KeysReader_FreeArgs,
        .dumpRegistratioData = KeysReader_DumpRegistrationData,
        .rdbSave = KeysOnlyReader_RdbSave,
        .rdbLoad = KeysReader_RdbLoad,
        .clear = KeysOnlyReader_Clear,
};
