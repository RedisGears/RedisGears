#include "utils/arr_rm_alloc.h"
#include "utils/adlist.h"
#include <stdbool.h>
#include "redisgears.h"
#include "redisgears_memory.h"
#include "utils/dict.h"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10
static list* keysReaderRegistration = NULL;
static void* db;
static dict* keysDict;

typedef struct KeysReaderCtx{
    char* match;
    dictIterator *iter;
    Record** pendings;
}KeysReaderCtx;

static KeysReaderCtx* RG_KeysReaderCtxCreate(char* match){
#define PENDING_KEYS_INIT_CAP 10
    KeysReaderCtx* krctx = RG_ALLOC(sizeof(*krctx));
    *krctx = (KeysReaderCtx){
        .match = match,
        .iter = NULL,
        .pendings = NULL,
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
    if(krctx->iter){
        dictReleaseIterator(krctx->iter);
    }
    if(krctx->pendings){
        for(int i = 0 ; i < array_len(krctx->pendings) ; ++i){
            RedisGears_FreeRecord(krctx->pendings[i]);
        }
        array_free(krctx->pendings);
    }
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

typedef struct gearsObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:24; /* LRU time (relative to global lru_clock) or
                            * LFU data (least significant 8 bits frequency
                            * and most significant 16 bits access time). */
    int refcount;
    void *ptr;
} gobj;

typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} Gearzrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    char* min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} Gearzlexrangespec;

struct GearsModuleKey {
    RedisModuleCtx *ctx;
    void *db;
    void *key;      /* Key name object. */
    void *value;    /* Value object, or NULL if the key was not found. */
    void *iter;     /* Iterator. */
    int mode;       /* Opening mode. */

    /* Zset iterator. */
    uint32_t ztype;         /* REDISMODULE_ZSET_RANGE_* */
    Gearzrangespec zrs;         /* Score range. */
    Gearzlexrangespec zlrs;     /* Lex range. */
    uint32_t zstart;        /* Start pos for positional ranges. */
    uint32_t zend;          /* End pos for positional ranges. */
    void *zcurrent;         /* Zset iterator current node. */
    int zer;                /* Zset iterator end reached flag                             (true if end was reached). */
};


#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2
#define C_OK 0
#define C_ERR -1
void *hashTypeInitIterator(void *subject);
int hashTypeNext(void *hi);
void hashTypeCurrentObject(void *hi, int what, char **vstr, unsigned int *vlen, long long *vll);
void hashTypeReleaseIterator(void *hi);

static Record* ValueToHashSetMapper(Record *record, RedisModuleKey* handler){

    void* iter = hashTypeInitIterator(((struct GearsModuleKey*)handler)->value);
    Record *hashSetRecord = RedisGears_HashSetRecordCreate();
//    ((void**)hashSetRecord)[0] = ((gobj*)(((struct GearsModuleKey*)handler)->value))->ptr;
    while(hashTypeNext(iter) == C_OK){
        unsigned int keyLen;
        long long keyAsNumber;
        char* key;
        hashTypeCurrentObject(iter, OBJ_HASH_KEY, &key, &keyLen, &keyAsNumber);
        assert(key);
        unsigned int valueLen;
        long long valueAsNumber;
        char* value;
        hashTypeCurrentObject(iter, OBJ_HASH_VALUE, &value, &valueLen, &valueAsNumber);
        Record* valRecord = NULL;
        if(value){
            char* valStr = RG_ALLOC(sizeof(char) * (valueLen + 1));
            memcpy(valStr, value, valueLen);
            valStr[valueLen] = '\0';
            valRecord = RedisGears_StringRecordCreate(valStr, valueLen);
        }else{
            valRecord = RedisGears_LongRecordCreate(valueAsNumber);
        }
        char keyStr[sizeof(char) * (keyLen + 1)];
        memcpy(keyStr, key, keyLen);
        keyStr[keyLen] = '\0';
        RedisGears_HashSetRecordSet(hashSetRecord, keyStr, valRecord);
    }
    RedisGears_KeyRecordSetVal(record, hashSetRecord);
    hashTypeReleaseIterator(iter);
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
        return ValueToHashSetMapper(record, handler);
        break;
    default:
        assert(false);
        return NULL;
    }
}

static Record* KeysReader_NextKey(RedisModuleCtx* rctx, KeysReaderCtx* readerCtx){
#define BETCH_SIZE 1000
    if(readerCtx->pendings && array_len(readerCtx->pendings) > 0){
        return array_pop(readerCtx->pendings);
    }
    if(!readerCtx->iter){
        readerCtx->iter = dictGetSafeIterator(keysDict);
        readerCtx->pendings = array_new(Record*, BETCH_SIZE);
    }
    size_t currBetch = BETCH_SIZE;
    dictEntry *entry = NULL;
    while((entry = dictNext(readerCtx->iter))){
        const char* key = dictGetKey(entry);
        size_t keyLen = strlen(key);
        void* value = dictGetVal(entry);

        struct GearsModuleKey keyHandler = {
                .ctx = rctx,
                .db = db,
                .key = NULL,
                .value = value,
                .iter = NULL,
                .mode = REDISMODULE_READ | REDISMODULE_WRITE
        };

        Record* record = RedisGears_KeyRecordCreate();

        RedisGears_KeyRecordSetKey(record, RG_STRDUP(key), keyLen);

        ValueToRecordMapper(rctx, record, (RedisModuleKey*)&keyHandler);

        readerCtx->pendings = array_append(readerCtx->pendings, record);

        if(--currBetch <= 0){
            break;
        }
    }

    RedisModule_ThreadSafeContextUnlock(rctx);

    if(array_len(readerCtx->pendings) > 0){
        return array_pop(readerCtx->pendings);
    }
    return NULL;
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

Reader* KeysReader(void* arg){
    KeysReaderCtx* ctx = RG_KeysReaderCtxCreate(arg);
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .registerTrigger = KeysReader_RegisrterTrigger,
        .next = KeysReader_Next,
        .free = KeysReader_Free,
        .serialize = RG_KeysReaderCtxSerialize,
        .deserialize = RG_KeysReaderCtxDeserialize,
    };
    return r;
}

void KeysReader_Init(){
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleString* keyName = RedisModule_CreateString(ctx, "test", strlen("test"));
    RedisModuleKey* kp = RedisModule_OpenKey(ctx, keyName, REDISMODULE_WRITE);
    db = ((void**)kp)[1];
    keysDict = ((void**)db)[0];
    RedisModule_CloseKey(kp);
    RedisModule_FreeString(ctx, keyName);
    RedisModule_FreeThreadSafeContext(ctx);
}
