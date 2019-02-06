#include "keys_reader.h"
#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "redisearch_api.h"
#include "globals.h"

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10
list* keysReaderRegistration = NULL;

IndexSpec* keyIdx = NULL;

RedisModuleDict *keysDict = NULL;

static Record* KeysReader_Next(RedisModuleCtx* rctx, void* ctx);

static Record* (*KeysReader_NextCallback)(RedisModuleCtx* rctx, void* ctx) = KeysReader_Next;

typedef struct KeysReaderCtx{
    char* match;
    long long cursorIndex;
    bool isDone;
    Record** pendingRecords;
    ResultsIterator* iter;
    RedisModuleDictIter* iter1;
}KeysReaderCtx;

static KeysReaderCtx* RG_KeysReaderCtxCreate(char* match){
#define PENDING_KEYS_INIT_CAP 10
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = match,
        .cursorIndex = 0,
        .isDone = false,
        .pendingRecords = array_new(Record*, PENDING_KEYS_INIT_CAP),
		.iter = NULL,
		.iter1 = NULL,
    };
    return krctx;
}

static void RG_KeysReaderCtxSerialize(void* ctx, BufferWriter* bw){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    RedisGears_BWWriteString(bw, krctx->match);
}

static void RG_KeysReaderCtxDeserialize(void* ctx, BufferReader* br){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    char* match = RedisGears_BRReadString(br);
    krctx->match = RG_STRDUP(match);
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
        assert(false);
        return NULL;
    }
}

static enum RecordType KeysReader_KeyTypeToRecordType(int type){
    switch(type){
    case REDISMODULE_KEYTYPE_EMPTY:
        return NONE;
    case REDISMODULE_KEYTYPE_STRING:
        return STRING_RECORD;
    case REDISMODULE_KEYTYPE_LIST:
        return LIST_RECORD;
    case REDISMODULE_KEYTYPE_HASH:
        return HASH_SET_RECORD;
    case REDISMODULE_KEYTYPE_SET:
        return LIST_RECORD;
    case REDISMODULE_KEYTYPE_ZSET:
        return LIST_RECORD;
    case REDISMODULE_KEYTYPE_MODULE:
        return HASH_SET_RECORD;
    default:
        assert(false);
    }
    return NONE;
}

static Record* KeysReader_NextKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx){
    if(array_len(readerCtx->pendingRecords) > 0){
        return array_pop(readerCtx->pendingRecords);
    }
    if(readerCtx->isDone){
        return NULL;
    }
    RedisModule_ThreadSafeContextLock(rctx);
    RedisModuleCallReply *reply = RedisModule_Call(rctx, "SCAN", "lcccc", readerCtx->cursorIndex, "COUNT", "10000", "MATCH", readerCtx->match);
    if (reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        if(reply) RedisModule_FreeCallReply(reply);
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }

    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

    if (RedisModule_CallReplyLength(reply) < 1) {
        RedisModule_FreeCallReply(reply);
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }

    assert(RedisModule_CallReplyLength(reply) <= 2);

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
        RedisModule_ThreadSafeContextUnlock(rctx);
        return NULL;
    }
    for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
        RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
        assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
        RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
        RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
        if(!keyHandler){
            RedisModule_FreeString(rctx, key);
            continue;
        }
        size_t keyLen;
        const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);

        Record* record = RedisGears_KeyRecordCreate();

        char* keyCStr = RG_ALLOC(keyLen + 1);
        memcpy(keyCStr, keyStr, keyLen);
        keyCStr[keyLen] = '\0';

        RedisGears_KeyRecordSetKey(record, RedisGears_StringRecordCreate(keyCStr, keyLen));

        ValueToRecordMapper(rctx, record, keyHandler);

        readerCtx->pendingRecords = array_append(readerCtx->pendingRecords, record);

        RedisModule_FreeString(rctx, key);
        RedisModule_CloseKey(keyHandler);
    }
    RedisModule_FreeCallReply(reply);
    RedisModule_ThreadSafeContextUnlock(rctx);
    return array_pop(readerCtx->pendingRecords);
}

static Record* KeysReader_RaxIndexNext(RedisModuleCtx* rctx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    if(!readerCtx->iter1){
        // todo support prefix in rax index
        readerCtx->iter1 = RedisModule_DictIteratorStartC(keysDict, "^", NULL, 0);
    }

    const char* key = RedisModule_DictNextC(readerCtx->iter1, NULL, NULL);
    if(key == NULL){
      return NULL;
    }

    RedisModule_ThreadSafeContextLock(rctx);
    RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, key, strlen(key));
    RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_READ);
    if(!keyHandler){
        // todo: handle this, currently its a poc for peformance check so its ok no to consider this
    }

    Record* record = RedisGears_KeyRecordCreate();
    RedisGears_KeyRecordSetKey(record, RedisGears_StringRecordCreate(RG_STRDUP(key), strlen(key)));
    ValueToRecordMapper(rctx, record, keyHandler);

    RedisModule_FreeString(rctx, keyRedisStr);
    RedisModule_CloseKey(keyHandler);
    RedisModule_ThreadSafeContextUnlock(rctx);

    return record;
}

static Record* KeysReader_SearchIndexNext(RedisModuleCtx* rctx, void* ctx){
	KeysReaderCtx* readerCtx = ctx;
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
	RedisModule_ThreadSafeContextLock(rctx);
	RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, key, strlen(key));
	RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_READ);
	if(!keyHandler){
		// todo: handle this, currently its a poc for peformance check so its ok no to consider this
	}

	Record* record = RedisGears_KeyRecordCreate();
	RedisGears_KeyRecordSetKey(record, RedisGears_StringRecordCreate(RG_STRDUP(key), strlen(key)));
	ValueToRecordMapper(rctx, record, keyHandler);

	RedisModule_FreeString(rctx, keyRedisStr);
	RedisModule_CloseKey(keyHandler);
	RedisModule_ThreadSafeContextUnlock(rctx);

	return record;
}

static Record* KeysReader_Next(RedisModuleCtx* rctx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    Record* record = KeysReader_NextKey(rctx, readerCtx);
    return record;
}

static int KeysReader_OnKeyTouched(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    listIter *iter = listGetIterator(keysReaderRegistration, AL_START_HEAD);
    listNode* node = NULL;
    while((node = listNext(iter))){
        FlatExecutionPlan* fep = listNodeValue(node);
        char* keyStr = RG_STRDUP(RedisModule_StringPtrLen(key, NULL));
        if(!RedisGears_Run(fep, keyStr, NULL, NULL)){
            RedisModule_Log(ctx, "warning", "could not execute flat execution on trigger");
        }
    }
    listReleaseIterator(iter);
    return REDISMODULE_OK;
}

static void KeysReader_RegisrterTrigger(FlatExecutionPlan* fep, void* args){
    if(!keysReaderRegistration){
        keysReaderRegistration = listCreate();
        RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
        if(RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_STRING, KeysReader_OnKeyTouched) != REDISMODULE_OK){
            // todo : print warning
        }
        RedisModule_FreeThreadSafeContext(ctx);
    }
    listAddNodeHead(keysReaderRegistration, fep);
}

static int KeysReader_IndexAllKeysInRax(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(keysDict){
      RedisModule_ReplyWithError(ctx, "keys index already created");
      return REDISMODULE_OK;
    }

    keysDict = RedisModule_CreateDict(ctx);

    Reader* reader = KeysReader(RG_STRDUP("*"));
    Record* r = NULL;
    RedisModule_ThreadSafeContextUnlock(ctx);
    while((r = reader->next(ctx, reader->ctx))){
      const char* keyName = RedisGears_KeyRecordGetKey(r, NULL);
      RedisModule_DictSetC(keysDict, (char*)keyName, strlen(keyName), NULL);
      RedisGears_FreeRecord(r);
    }
    RedisModule_ThreadSafeContextLock(ctx);
    reader->free(reader->ctx);
    RG_FREE(reader);

    KeysReader_NextCallback = KeysReader_RaxIndexNext;

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

static int KeysReader_IndexAllKeysInRedisearch(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if(!globals.rediSearchLoaded){
		if(RediSearch_Initialize() != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "failed to initialize redisearch module");
			return REDISMODULE_OK;
		}
	}
	if(keyIdx){
		RedisModule_ReplyWithError(ctx, "keys index already created");
		return REDISMODULE_OK;
	}

	RediSearch_Field keyNameField = {
			.fieldName = KEYS_NAME_FIELD,
			.fieldType = INDEX_TYPE_TAG,
	};
	keyIdx = RediSearch_CreateIndexSpec(KEYS_SPEC_NAME, &keyNameField, 1);

	Reader* reader = KeysReader(RG_STRDUP("*"));
	Record* r = NULL;
	RedisModule_ThreadSafeContextUnlock(ctx);
	while((r = reader->next(ctx, reader->ctx))){
		const char* keyName = RedisGears_KeyRecordGetKey(r, NULL);
		RediSearch_FieldVal val = {
				.fieldName = KEYS_NAME_FIELD,
				.val.str = keyName,
		};
		RediSearch_IndexSpecAddDocument(keyIdx, keyName, &val, 1);
		RedisGears_FreeRecord(r);
	}
	RedisModule_ThreadSafeContextLock(ctx);
	reader->free(reader->ctx);
	RG_FREE(reader);

	KeysReader_NextCallback = KeysReader_SearchIndexNext;

	RedisModule_ReplyWithSimpleString(ctx, "OK");

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

Reader* KeysReader(void* arg){
    KeysReaderCtx* ctx = RG_KeysReaderCtxCreate(arg);
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .next = KeysReader_NextCallback,
        .free = KeysReader_Free,
        .serialize = RG_KeysReaderCtxSerialize,
        .deserialize = RG_KeysReaderCtxDeserialize,
    };
    return r;
}
