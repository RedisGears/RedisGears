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

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10

typedef struct KeysReadeRegisterData{
    FlatExecutionPlan* fep;
    void* args;
    ExecutionMode mode;
}KeysReadeRegisterData;

Gears_list* keysReaderRegistration = NULL;

IndexSpec* keyIdx = NULL;

RedisModuleDict *keysDict = NULL;

static Record* KeysReader_Next(ExecutionCtx* ectx, void* ctx);

static Record* (*KeysReader_NextCallback)(ExecutionCtx* rctx, void* ctx) = KeysReader_Next;

typedef struct KeysReaderCtx{
    size_t matchLen;
    char* match;
    long long cursorIndex;
    bool isDone;
    Record** pendingRecords;
    bool readValue;
    bool isPrefix;
    ResultsIterator* iter;
    RedisModuleDictIter* iter1;
}KeysReaderCtx;

static KeysReaderCtx* KeysReaderCtx_Create(char* match, bool readValue){
#define PENDING_KEYS_INIT_CAP 10
    bool isPrefix = false;
    size_t matchLen = 0;
    if(match){
        matchLen = strlen(match);
        if(match[matchLen - 1] == '*'){
            isPrefix = true;
        }
    }
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .matchLen = matchLen,
        .match = match,
        .cursorIndex = 0,
        .isDone = false,
        .readValue = readValue,
        .isPrefix = isPrefix,
        .pendingRecords = array_new(Record*, PENDING_KEYS_INIT_CAP),
		.iter = NULL,
		.iter1 = NULL,
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

    if(readerCtx->readValue){

        record = RedisGears_KeyRecordCreate();

        RedisGears_KeyRecordSetKey(record, keyCStr, keyLen);

        ValueToRecordMapper(rctx, record, keyHandler);
    }else{

        record = RedisGears_StringRecordCreate(keyCStr, keyLen);

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

static Record* KeysReader_RaxIndexNext(ExecutionCtx* ectx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
    if(!readerCtx->iter1){
        // todo support prefix in rax index
        readerCtx->iter1 = RedisModule_DictIteratorStartC(keysDict, "^", NULL, 0);
    }

    Record* r;
    const char* key = RedisModule_DictNextC(readerCtx->iter1, NULL, NULL);
    if(key == NULL){
      return NULL;
    }

    LockHandler_Acquire(rctx);
    RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, key, strlen(key));
    RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_READ);
    if(!keyHandler){
        // todo: handle this, currently its a poc for peformance check so its ok no to consider this
    }

    Record* record = RedisGears_KeyRecordCreate();
    RedisGears_KeyRecordSetKey(record, RG_STRDUP(key), strlen(key));
    ValueToRecordMapper(rctx, record, keyHandler);

    RedisModule_FreeString(rctx, keyRedisStr);
    RedisModule_CloseKey(keyHandler);
    LockHandler_Release(rctx);

    return record;
}

static Record* KeysReader_SearchIndexNext(ExecutionCtx* ectx, void* ctx){
	KeysReaderCtx* readerCtx = ctx;
	RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
	if(!readerCtx->iter){
		size_t matchLen = strlen(readerCtx->match);
		char match[matchLen + 1];
		memcpy(match, readerCtx->match, matchLen);
		match[matchLen] = '\0';
		int searchType = EXECT_SEARCH;
		if(match[matchLen - 1] == '*'){
			match[matchLen - 1] = '\0';
			searchType = PREFIX_SEARCH;
		}
		QueryNode* node = RediSearch_CreateTagNode(keyIdx, KEYS_NAME_FIELD, match, searchType);
		readerCtx->iter = RediSearch_GetResultsIterator(keyIdx, node);
	}
	const char* key = RediSearch_ResultsIteratorNext(keyIdx, readerCtx->iter);
	if(key == NULL){
		return NULL;
	}
	LockHandler_Acquire(rctx);
	RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, key, strlen(key));
	RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_READ);
	if(!keyHandler){
		// todo: handle this, currently its a poc for peformance check so its ok no to consider this
	}

	Record* record = RedisGears_KeyRecordCreate();
	RedisGears_KeyRecordSetKey(record, RG_STRDUP(key), strlen(key));
	ValueToRecordMapper(rctx, record, keyHandler);

	RedisModule_FreeString(rctx, keyRedisStr);
	RedisModule_CloseKey(keyHandler);
	LockHandler_Release(rctx);

	return record;
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
        RedisModuleString* key = RedisModule_CreateString(NULL, readerCtx->match, readerCtx->matchLen);
        RedisModuleCtx* rctx = RedisGears_GetRedisModuleCtx(ectx);
        LockHandler_Acquire(rctx);
        record = KeysReader_ReadKey(rctx, readerCtx, key);
        LockHandler_Release(rctx);
        RedisModule_FreeString(NULL, key);
        readerCtx->isDone = true;
    }
    return record;
}

static int KeysReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReadeRegisterData* rData = Gears_listNodeValue(node);
        char* keyStr = RG_STRDUP(RedisModule_StringPtrLen(key, NULL));
        if(!RedisGears_Run(rData->fep, rData->mode, keyStr, NULL, NULL)){
            RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
        }
    }
    Gears_listReleaseIterator(iter);
    return REDISMODULE_OK;
}

static void KeysReader_UnregisterTrigger(FlatExecutionPlan* fep){
    Gears_listIter *iter = Gears_listGetIterator(keysReaderRegistration, AL_START_HEAD);
    Gears_listNode* node = NULL;
    while((node = Gears_listNext(iter))){
        KeysReadeRegisterData* rData = Gears_listNodeValue(node);
        if(rData->fep == fep){
            Gears_listDelNode(keysReaderRegistration, node);
            Gears_listReleaseIterator(iter);
            RG_FREE(rData->args);
            RG_FREE(rData);
            return;
        }
    }
    assert(0);
}

static int KeysReader_RegisrterTrigger(FlatExecutionPlan* fep, ExecutionMode mode, void* args){
    RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
    if(!keysReaderRegistration){
        keysReaderRegistration = Gears_listCreate();
        if(RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_ALL, KeysReader_OnKeyTouched) != REDISMODULE_OK){
            // todo : print warning
        }
    }

    KeysReadeRegisterData* rData = RG_ALLOC(sizeof(*rData));
    *rData = (KeysReadeRegisterData){
        .fep = fep,
        .args = args,
        .mode = mode,
    };

    Gears_listAddNodeTail(keysReaderRegistration, rData);
    RedisModule_FreeThreadSafeContext(ctx);
    return 1;
}

static int KeysReader_IndexAllKeysInRax(RedisModuleCtx *rctx, RedisModuleString **argv, int argc){
    if(keysDict){
      RedisModule_ReplyWithError(rctx, "keys index already created");
      return REDISMODULE_OK;
    }

    keysDict = RedisModule_CreateDict(rctx);

    Reader* reader = KeysReader.create(RG_STRDUP("*"));
    Record* r = NULL;
    ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, NULL);
    while((r = reader->next(&ectx, reader->ctx))){
      const char* keyName = RedisGears_KeyRecordGetKey(r, NULL);
      RedisModule_DictSetC(keysDict, (char*)keyName, strlen(keyName), NULL);
      RedisGears_FreeRecord(r);
    }
    reader->free(reader->ctx);
    RG_FREE(reader);

    KeysReader_NextCallback = KeysReader_RaxIndexNext;

    RedisModule_ReplyWithSimpleString(rctx, "OK");

    return REDISMODULE_OK;
}

static int KeysReader_IndexAllKeysInRedisearch(RedisModuleCtx *rctx, RedisModuleString **argv, int argc){
	if(!globals.rediSearchLoaded){
		if(RediSearch_Initialize() != REDISMODULE_OK){
			RedisModule_ReplyWithError(rctx, "failed to initialize redisearch module");
			return REDISMODULE_OK;
		}
	}
	if(keyIdx){
		RedisModule_ReplyWithError(rctx, "keys index already created");
		return REDISMODULE_OK;
	}

	RediSearch_Field keyNameField = {
			.fieldName = KEYS_NAME_FIELD,
			.fieldType = INDEX_TYPE_TAG,
	};
	keyIdx = RediSearch_CreateIndexSpec(KEYS_SPEC_NAME, &keyNameField, 1);

	Reader* reader = KeysReader.create(RG_STRDUP("*"));
	Record* r = NULL;
	ExecutionCtx ectx = ExecutionCtx_Initialize(rctx, NULL);
	while((r = reader->next(&ectx, reader->ctx))){
		const char* keyName = RedisGears_KeyRecordGetKey(r, NULL);
		RediSearch_FieldVal val = {
				.fieldName = KEYS_NAME_FIELD,
				.val.str = keyName,
		};
		RediSearch_IndexSpecAddDocument(keyIdx, keyName, &val, 1);
		RedisGears_FreeRecord(r);
	}
	reader->free(reader->ctx);
	RG_FREE(reader);

	KeysReader_NextCallback = KeysReader_SearchIndexNext;

	RedisModule_ReplyWithSimpleString(rctx, "OK");

	return REDISMODULE_OK;
}

int KeysReader_Initialize(RedisModuleCtx* ctx){
	if (RedisModule_CreateCommand(ctx, "rg.redisearchkeysindex", KeysReader_IndexAllKeysInRedisearch, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.redisearchkeysindex");
		return REDISMODULE_ERR;
	}
	if (RedisModule_CreateCommand(ctx, "rg.raxkeysindex", KeysReader_IndexAllKeysInRax, "readonly", 0, 0, 0) != REDISMODULE_OK) {
	    RedisModule_Log(ctx, "warning", "could not register command rg.raxkeysindex");
        return REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}

static Reader* KeysReader_Create(void* arg){
    KeysReaderCtx* ctx = KeysReaderCtx_Create(arg, true);
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .next = KeysReader_NextCallback,
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
        .next = KeysReader_NextCallback,
        .free = KeysReader_Free,
        .serialize = RG_KeysReaderCtxSerialize,
        .deserialize = RG_KeysReaderCtxDeserialize,
    };
    return r;
}

RedisGears_ReaderCallbacks KeysReader = {
        .create = KeysReader_Create,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
};

RedisGears_ReaderCallbacks KeysOnlyReader = {
        .create = KeysOnlyReader_Create,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .unregisterTrigger = KeysReader_UnregisterTrigger,
};
