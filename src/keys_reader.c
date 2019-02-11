#include "keys_reader.h"
#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include "utils/thpool.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "redisearch_api.h"
#include "globals.h"
#include "lock_handler.h"
#include <pthread.h>

#define KEYS_NAME_FIELD "key_name"
#define KEYS_SPEC_NAME "keys_spec"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10
static list* keysReaderRegistration = NULL;

static IndexSpec* keyIdx = NULL;

static RedisModuleDict *keysDict = NULL;

static Record* KeysReader_Next(RedisModuleCtx* rctx, void* ctx);

static Record* (*KeysReader_NextCallback)(RedisModuleCtx* rctx, void* ctx) = KeysReader_Next;

typedef struct KeysReaderCtx{
    char* match;
    bool isDone;
    union{
        struct{
            pthread_mutex_t mutex;
            list* pendingRecords;
        };
        ResultsIterator* iter;
        RedisModuleDictIter* iter1;

    };
}KeysReaderCtx;

static KeysReaderCtx* RG_KeysReaderCtxCreate(char* match){
#define PENDING_KEYS_INIT_CAP 10
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = match,
        .isDone = false,
    };
    return krctx;
}

static void RG_KeysReaderCtxSerialize(void* ctx, BufferWriter* bw){
    KeysReaderCtx* krctx = (KeysReaderCtx*)ctx;
    RedisGears_BWWriteString(bw, krctx->match);
}

static void RG_KeysReaderCtxDeserialize(void* ctx, BufferReader* br){
    KeysReaderCtx** krctx = (KeysReaderCtx**)ctx;
    char* match = RedisGears_BRReadString(br);
    if(!(*krctx)){
        *krctx = RG_KeysReaderCtxCreate(RG_STRDUP(match));
    }else{
        (*krctx)->match = RG_STRDUP(match);
    }
}

static void KeysReader_Free(void* ctx){
    if(!ctx){
        return;
    }
    KeysReaderCtx* krctx = ctx;
    pthread_mutex_lock(&krctx->mutex);
    while(!krctx->isDone){
        pthread_mutex_unlock(&krctx->mutex);
        pthread_mutex_lock(&krctx->mutex);
    }
    if(krctx->match){
        RG_FREE(krctx->match);
    }
    if(krctx->pendingRecords){
        listIter *iter = listGetIterator(krctx->pendingRecords, AL_START_HEAD);
        listNode* node = NULL;
        while((node = listNext(iter))){
            Record* r = listNodeValue(node);
            if(r){
                RedisGears_FreeRecord(r);
            }
        }
        listReleaseIterator(iter);
        listRelease(krctx->pendingRecords);
        pthread_mutex_destroy(&krctx->mutex);
    }
    RG_FREE(krctx);
}

static Record* ValueToStringMapper(Record *record, RedisModuleString* key, RedisModuleCtx* ctx){
    size_t len;
    LockHandler_Acquire(ctx);
    RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
    char* val = RedisModule_StringDMA(kp, &len, REDISMODULE_READ);
    char* strVal = RG_ALLOC(len + 1);
    memcpy(strVal, val, len);
    strVal[len] = '\0';
    LockHandler_Release(ctx);

    Record* strRecord = RedisGears_StringRecordCreate(strVal, len);

    RedisGears_KeyRecordSetVal(record, strRecord);

    return record;
}

static Record* ValueToHashSetMapper(Record *record, RedisModuleCtx* ctx){
    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGETALL", "c", RedisGears_KeyRecordGetKey(record, NULL));
    LockHandler_Release(ctx);
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
    LockHandler_Acquire(ctx);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "lrange", "cll", RedisGears_KeyRecordGetKey(record, NULL), 0, -1);
    LockHandler_Release(ctx);
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

static Record* ValueToRecordMapper(RedisModuleCtx* rctx, Record* record, int type, RedisModuleString* key){
    switch(type){
    case REDISMODULE_KEYTYPE_STRING:
        return ValueToStringMapper(record, key, rctx);
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

static Record* KeysReader_RaxIndexNext(RedisModuleCtx* rctx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
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

    int type = RedisModule_KeyType(keyHandler);

    Record* record = RedisGears_KeyRecordCreate();
    RedisGears_KeyRecordSetKey(record, RG_STRDUP(key), strlen(key));
    ValueToRecordMapper(rctx, record, type, keyRedisStr);

    RedisModule_FreeString(rctx, keyRedisStr);
    RedisModule_CloseKey(keyHandler);
    LockHandler_Release(rctx);

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
	LockHandler_Acquire(rctx);
	RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, key, strlen(key));
	RedisModuleKey *keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_READ);
	if(!keyHandler){
		// todo: handle this, currently its a poc for peformance check so its ok no to consider this
	}

	int type = RedisModule_KeyType(keyHandler);

	Record* record = RedisGears_KeyRecordCreate();
	RedisGears_KeyRecordSetKey(record, RG_STRDUP(key), strlen(key));
	ValueToRecordMapper(rctx, record, type, keyRedisStr);

	RedisModule_FreeString(rctx, keyRedisStr);
	RedisModule_CloseKey(keyHandler);
	LockHandler_Release(rctx);

	return record;
}

typedef struct BatchReaderCtx{
    KeysReaderCtx* readerCtx;
    RedisModuleString** keys;
}BatchReaderCtx;

static void KeysReader_ReadKeyBatch(void* arg){
    BatchReaderCtx* brc = arg;
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    for(size_t i = 0 ; i < array_len(brc->keys) ; ++i){
        RedisModuleString* key = brc->keys[i];
        size_t keyLen;
        const char* keyStr = RedisModule_StringPtrLen(key, &keyLen);

        LockHandler_Acquire(ctx);
        RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
        int type = RedisModule_KeyType(kp);
        RedisModule_CloseKey(kp);
        LockHandler_Release(ctx);

        Record* record = RedisGears_KeyRecordCreate();

        char* keyCStr = RG_ALLOC(keyLen + 1);
        memcpy(keyCStr, keyStr, keyLen);
        keyCStr[keyLen] = '\0';

        RedisGears_KeyRecordSetKey(record, keyCStr, keyLen);

        ValueToRecordMapper(ctx, record, type, key);

        // get lock on pending records
        pthread_mutex_lock(&brc->readerCtx->mutex);
        listAddNodeTail(brc->readerCtx->pendingRecords, record);
        pthread_mutex_unlock(&brc->readerCtx->mutex);


        RedisModule_FreeString(ctx, key);
    }
    array_free(brc->keys);
    RG_FREE(brc);
    RedisModule_FreeThreadSafeContext(ctx);
}

static void* KeysReader_ScanKeys(void* arg){
    KeysReaderCtx* readerCtx = arg;
    long long cursorIndex = 0;
    threadpool tp = thpool_init(20);
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    do{
        LockHandler_Acquire(ctx);
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCAN", "lcccc", cursorIndex, "COUNT", "10000", "MATCH", readerCtx->match);
        LockHandler_Release(ctx);
        if (reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
            if(reply) RedisModule_FreeCallReply(reply);
            thpool_wait(tp);
            pthread_mutex_lock(&readerCtx->mutex);
            listAddNodeTail(readerCtx->pendingRecords, NULL); // indicating done
            readerCtx->isDone = true;
            pthread_mutex_unlock(&readerCtx->mutex);
            thpool_destroy(tp);
            return NULL;
        }

        assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

        if (RedisModule_CallReplyLength(reply) < 1) {
            RedisModule_FreeCallReply(reply);
            thpool_wait(tp);
            pthread_mutex_lock(&readerCtx->mutex);
            listAddNodeTail(readerCtx->pendingRecords, NULL); // indicating done
            readerCtx->isDone = true;
            pthread_mutex_unlock(&readerCtx->mutex);
            thpool_destroy(tp);
            return NULL;
        }

        assert(RedisModule_CallReplyLength(reply) <= 2);

        RedisModuleCallReply *cursorReply = RedisModule_CallReplyArrayElement(reply, 0);

        assert(RedisModule_CallReplyType(cursorReply) == REDISMODULE_REPLY_STRING);

        RedisModuleString *cursorStr = RedisModule_CreateStringFromCallReply(cursorReply);
        RedisModule_StringToLongLong(cursorStr, &cursorIndex);
        RedisModule_FreeString(ctx, cursorStr);

        RedisModuleCallReply *keysReply = RedisModule_CallReplyArrayElement(reply, 1);
        assert(RedisModule_CallReplyType(keysReply) == REDISMODULE_REPLY_ARRAY);
        if(RedisModule_CallReplyLength(keysReply) < 1){
            RedisModule_FreeCallReply(reply);
            thpool_wait(tp);
            pthread_mutex_lock(&readerCtx->mutex);
            listAddNodeTail(readerCtx->pendingRecords, NULL); // indicating done
            readerCtx->isDone = true;
            pthread_mutex_unlock(&readerCtx->mutex);
            thpool_destroy(tp);
            return NULL;
        }
        RedisModuleString** keys = array_new(RedisModuleString*, 10000);
        for(int i = 0 ; i < RedisModule_CallReplyLength(keysReply) ; ++i){
            RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(keysReply, i);
            assert(RedisModule_CallReplyType(keyReply) == REDISMODULE_REPLY_STRING);
            RedisModuleString* key = RedisModule_CreateStringFromCallReply(keyReply);
            keys = array_append(keys, key);
        }
        RedisModule_FreeCallReply(reply);
        BatchReaderCtx* brc = RG_ALLOC(sizeof(*brc));
        brc->keys = keys;
        brc->readerCtx = readerCtx;
        thpool_add_work(tp, KeysReader_ReadKeyBatch, brc);
    }while(cursorIndex != 0);

    thpool_wait(tp);
    pthread_mutex_lock(&readerCtx->mutex);
    listAddNodeTail(readerCtx->pendingRecords, NULL); // indicating done
    readerCtx->isDone = true;
    pthread_mutex_unlock(&readerCtx->mutex);
    thpool_destroy(tp);
    return NULL;
}

static Record* KeysReader_Next(RedisModuleCtx* rctx, void* ctx){
    KeysReaderCtx* readerCtx = ctx;
    if(!readerCtx->pendingRecords){
        pthread_t thread;
        readerCtx->pendingRecords = listCreate();
        if (pthread_mutex_init(&readerCtx->mutex, NULL) != 0){
            printf("\n mutex init failed \n");
            assert(false);
        }
        pthread_create(&thread, NULL, KeysReader_ScanKeys, readerCtx);
        pthread_detach(thread);
    }
    while(1){
        pthread_mutex_lock(&readerCtx->mutex);
        listNode* node = listFirst(readerCtx->pendingRecords);
        if(!node){
            bool isDone = readerCtx->isDone;
            pthread_mutex_unlock(&readerCtx->mutex);
            if(isDone){
                return NULL;
            }
            continue;
        }
        Record* record = listNodeValue(node);
        listDelNode(readerCtx->pendingRecords, node);
        pthread_mutex_unlock(&readerCtx->mutex);
        return record;
    }
    assert(false); //will never reach here!!
    return NULL;
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
    while((r = reader->next(ctx, reader->ctx))){
      const char* keyName = RedisGears_KeyRecordGetKey(r, NULL);
      RedisModule_DictSetC(keysDict, (char*)keyName, strlen(keyName), NULL);
      RedisGears_FreeRecord(r);
    }
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
	while((r = reader->next(ctx, reader->ctx))){
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
    KeysReaderCtx* ctx = NULL;
    if(arg){
        ctx = RG_KeysReaderCtxCreate(arg);
    }
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
