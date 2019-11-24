#include "keys_reader.h"
#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "execution_plan.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "redisearch_api.h"
#include "globals.h"
#include "lock_handler.h"
#include "record.h"

#include <assert.h>

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10

typedef struct KeysReaderRegisterData{
    long long refCount;
    FlatExecutionPlan* fep;
    char* args;
    ExecutionMode mode;
    char* lastError;
    unsigned long long numTriggered;
    unsigned long long numSuccess;
    unsigned long long numFailures;
    unsigned long long numAborted;
}KeysReaderRegisterData;

Gears_list* keysReaderRegistration = NULL;

IndexSpec* keyIdx = NULL;

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

static void KeysReaderRegisterData_Free(KeysReaderRegisterData* rData){
    if((--rData->refCount) == 0){
        if(rData->lastError){
            RG_FREE(rData->lastError);
        }
        RG_FREE(rData->args);
        FlatExecutionPlan_Free(rData->fep);
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

static void RG_KeysReaderCtxDeserialize(void* ctx, Gears_BufferReader* br){
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

    RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
    if(!keyHandler){
        return NULL;
    }

    char* keyCStr = RG_ALLOC(keyLen + 1);
    memcpy(keyCStr, keyStr, keyLen);
    keyCStr[keyLen] = '\0';

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

    long long errorsLen = RedisGears_GetErrorsLen(ctx);

    if(errorsLen > 0){
        ++rData->numFailures;
        Record* r = RedisGears_GetError(ctx, 0);
        assert(RedisGears_RecordGetType(r) == ERROR_RECORD);
        if(rData->lastError){
            RG_FREE(rData->lastError);
        }
        rData->lastError = RG_STRDUP(RedisGears_StringRecordGet(r, NULL));
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
        if(KeysReader_IsKeyMatch(rData->args, keyCStr)){
            ++rData->numTriggered;
            RedisGears_OnExecutionDoneCallback callback = NULL;
            void* privateData = NULL;
            callback = KeysReader_ExecutionDone;
            privateData = KeysReaderRegisterData_GetShallowCopy(rData);
            if(!RedisGears_Run(rData->fep, rData->mode, RG_STRDUP(keyCStr), callback, privateData)){
                ++rData->numAborted;
                RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
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

static void KeysReader_SerializeArgs(void* args, Gears_BufferWriter* bw){
    RedisGears_BWWriteString(bw, args);
}

static void* KeysReader_DeserializeArgs(Gears_BufferReader* br){
    char* args = RG_STRDUP(RedisGears_BRReadString(br));
    return args;
}

static void KeysReader_UnregisterTrigger(FlatExecutionPlan* fep){
    KeysReaderRegisterData* rData = KeysReader_FindRegistrationData(fep, FindRegistrationDataFlagPop);
    assert(rData);
    KeysReaderRegisterData_Free(rData);
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
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "regex", strlen("regex"));
    RedisModule_ReplyWithStringBuffer(ctx, rData->args, strlen(rData->args));
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

static int KeysReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args){
    KeysReader_RegisterKeySpaceEvent();

    KeysReaderRegisterData* rData = KeysReaderRegisterData_Create(fep, args, mode);

    Gears_listAddNodeTail(keysReaderRegistration, rData);
    return 1;
}

int KeysReader_Initialize(RedisModuleCtx* ctx){
	return REDISMODULE_OK;
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
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReaderRegisterData* rData = Gears_listNodeValue(node);
        if(!shouldClear(rData->fep)){
            continue;
        }
        RedisModule_SaveUnsigned(rdb, 1); // has more
        size_t len;
        const char* serializedFep = FlatExecutionPlan_Serialize(rData->fep, &len);
        assert(serializedFep); // fep already registered, must be serializable.
        RedisModule_SaveStringBuffer(rdb, serializedFep, len);
        RedisModule_SaveStringBuffer(rdb, rData->args, strlen(rData->args) + 1 /* for \0 */);
        RedisModule_SaveUnsigned(rdb, rData->mode);

    }
    RedisModule_SaveUnsigned(rdb, 0); // done
    Gears_listReleaseIterator(iter);
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
        FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(data, len);
        RedisModule_Free(data);
        char* args = RedisModule_LoadStringBuffer(rdb, NULL);
        int mode = RedisModule_LoadUnsigned(rdb);
        KeysReader_RegisrterTrigger(fep, mode, args);
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
    }
    Gears_listReleaseIterator(iter);
}

static void KeysReader_Clear(){
    GenricKeysReader_Clear(KeysReader_ShouldContinue);
}

static void KeysOnlyReader_Clear(){
    GenricKeysReader_Clear(KeysOnlyReader_ShouldContinue);
}

RedisGears_ReaderCallbacks KeysReader = {
        .create = KeysReader_Create,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
        .serializeTriggerArgs = KeysReader_SerializeArgs,
        .deserializeTriggerArgs = KeysReader_DeserializeArgs,
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
        .dumpRegistratioData = KeysReader_DumpRegistrationData,
        .rdbSave = KeysOnlyReader_RdbSave,
        .rdbLoad = KeysReader_RdbLoad,
        .clear = KeysOnlyReader_Clear,
};
