#include "record.h"

#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "execution_plan.h"

RecordType StopRecordType;

Record StopRecord;
Record WaitRecord;
Record DummyRecord;
Record NullRecord;

RecordType* listRecordType;
RecordType* stringRecordType;
RecordType* errorRecordType;
RecordType* longRecordType;
RecordType* doubleRecordType;
RecordType* keyRecordType;
RecordType* keysHandlerRecordType;
RecordType* hashSetRecordType;
RecordType* asyncRecordType;
RecordType* nullRecordType;

static RecordType** recordsTypes;

Record* RG_RecordCreate(RecordType* type){
    Record* ret = RG_ALLOC(type->size);
    ret->type = type;
    return ret;
}

static void StringRecord_Free(Record* base){
    StringRecord* record = (StringRecord*)base;
    RG_FREE(record->str);
}

static void DoubleRecord_Free(Record* base){}

static void LongRecord_Free(Record* base){}

static void ListRecord_Free(Record* base){
    ListRecord* record = (ListRecord*)base;
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(base) ; ++i){
        RG_FreeRecord(record->records[i]);
    }
    array_free(record->records);
}

static void KeyRecord_Free(Record* base){
    KeyRecord* record = (KeyRecord*)base;
    if(record->key){
        RG_FREE(record->key);
    }
    if(record->record){
        RG_FreeRecord(record->record);
    }
}

static void KeysHandlerRecord_Free(Record* base){
    KeysHandlerRecord* record = (KeysHandlerRecord*)base;
    RedisModule_CloseKey(record->keyHandler);
}

void RG_AsyncRecordContinueInternal(AsyncRecord* async, Record* r){
    if(async->originRecord){
        // if we have an original record and r is not NULL, i.e, true
        // we need to continue with the original record.
        if(r){
            if(RG_RecordGetType(r) == errorRecordType){
                RG_FreeRecord(async->originRecord);
            }else{
                RG_FreeRecord(r);
                r = async->originRecord;
            }
        }else{
            r = &DummyRecord;
            RG_FreeRecord(async->originRecord);
        }
    }
    if(async->overridePlaceHolder){
        if(r == &DummyRecord){
            // record was discarded
            r = RG_StringRecordCreate(RG_STRDUP("Discarded"), strlen("Discarded"));
        }
        *async->overridePlaceHolder = r;
        *(async->rptx) = &DummyRecord;
    }else{
        *(async->rptx) = r;
    }
    ExecutionPlan_PendingCtxFree(async->pctx);
    async->pctx = NULL;
}

static void AsyncRecord_Free(Record* base){
    AsyncRecord* async = (AsyncRecord*)base;
    if(async->pctx){
#define ERROR_MSG "Async record did not called continue"
        Record* error = RG_ErrorRecordCreate(RG_STRDUP(ERROR_MSG), strlen(ERROR_MSG));
        RG_AsyncRecordContinueInternal(async, error);
    }
}

static void HashSetRecord_Free(Record* base){
    HashSetRecord* record = (HashSetRecord*)base;
    Gears_dictIterator *iter;
    Gears_dictEntry *entry;
    iter = Gears_dictGetIterator(record->d);
    entry = NULL;
    while((entry = Gears_dictNext(iter))){
        Record* temp = Gears_dictGetVal(entry);
        RG_FreeRecord(temp);
    }
    Gears_dictReleaseIterator(iter);
    Gears_dictRelease(record->d);
}

static void NullRecord_Free(Record* base){}

static int StringRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    StringRecord* r = (StringRecord*)base;
    RedisGears_BWWriteBuffer(bw, r->str, r->len);
    return REDISMODULE_OK;
}

static int LongRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    LongRecord* r = (LongRecord*)base;
    RedisGears_BWWriteLong(bw, r->num);
    return REDISMODULE_OK;
}

static int DoubleRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    DoubleRecord* r = (DoubleRecord*)base;
    RedisGears_BWWriteLong(bw, (long)r->num);
    return REDISMODULE_OK;
}

static int ListRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    ListRecord* r = (ListRecord*)base;
    RedisGears_BWWriteLong(bw, RedisGears_ListRecordLen(base));
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(base) ; ++i){
        if(RG_SerializeRecord(ctx, bw, r->records[i]) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

static int KeyRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    KeyRecord* r = (KeyRecord*)base;
    RedisGears_BWWriteString(bw, r->key);
    if(r->record){
        RedisGears_BWWriteLong(bw, 1); // value exists
        if(RG_SerializeRecord(ctx, bw, r->record) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }else{
        RedisGears_BWWriteLong(bw, 0); // value missing
    }
    return REDISMODULE_OK;
}

static int KeysHandlerRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    // todo: what we can do here is to read the key and create a serializable record
    RedisModule_Assert(false && "can not serialize key handler record");
    return REDISMODULE_OK;
}

static int AsyncRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    RedisModule_Assert(false && "can not serialize async record");
    return REDISMODULE_OK;
}

static int HashSetRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    HashSetRecord* record = (HashSetRecord*)base;
    Gears_dictIterator *iter;
    Gears_dictEntry *entry;
    RedisGears_BWWriteLong(bw, Gears_dictSize(record->d));
    iter = Gears_dictGetIterator(record->d);
    entry = NULL;
    while((entry = Gears_dictNext(iter))){
        const char* k = Gears_dictGetKey(entry);
        Record* temp = Gears_dictGetVal(entry);
        RedisGears_BWWriteString(bw, k);
        if(RG_SerializeRecord(ctx, bw, temp) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }
    Gears_dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

static int NullRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base){
    return REDISMODULE_OK;
}

static Record* StringRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    size_t size;
    const char* temp = RedisGears_BRReadBuffer(br, &size);
    char* temp1 = RG_ALLOC(size);
    memcpy(temp1, temp, size);
    return RG_StringRecordCreate(temp1, size);
}

static Record* LongRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    return RG_LongRecordCreate(RedisGears_BRReadLong(br));
}

static Record* ErrorRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    size_t size;
    const char* temp = RedisGears_BRReadBuffer(br, &size);
    char* temp1 = RG_ALLOC(size + 1);
    memcpy(temp1, temp, size);
    temp1[size] = '\0';
    return RG_ErrorRecordCreate(temp1, size);
}

static Record* DoubleRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    return RG_DoubleRecordCreate((double)RedisGears_BRReadLong(br));
}

static Record* ListRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    size_t size = (size_t)RedisGears_BRReadLong(br);
    Record* r = RG_ListRecordCreate(size);
    for(size_t i = 0 ; i < size ; ++i){
        RG_ListRecordAdd(r, RG_DeserializeRecord(ctx, br));
    }
    return r;
}

static Record* KeyRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    Record* r = RedisGears_KeyRecordCreate();
    char* key = RG_STRDUP(RedisGears_BRReadString(br));
    RG_KeyRecordSetKey(r, key, strlen(key));
    bool isValExists = (bool)RedisGears_BRReadLong(br);
    if(isValExists){
        RedisGears_KeyRecordSetVal(r, RG_DeserializeRecord(ctx, br));
    }else{
        RedisGears_KeyRecordSetVal(r, NULL);
    }
    return r;
}

static Record* KeysHandlerRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    // todo: what we can do here is to read the key and create a serializable record
    RedisModule_Assert(false && "can not deserialize key handler record");
    return NULL;
}

static Record* AsyncRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    RedisModule_Assert(false && "can not deserialize async record");
    return NULL;
}

static Record* HashSetRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    Record* record = RedisGears_HashSetRecordCreate();
    size_t len = RedisGears_BRReadLong(br);
    for(size_t i = 0 ; i < len ; ++i){
        char* k = RedisGears_BRReadString(br);
        Record* r = RG_DeserializeRecord(ctx, br);
        RedisGears_HashSetRecordSet(record, k, r);
    }
    return record;
}

static Record* NullRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br){
    return &NullRecord;
}

static int StringRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    size_t listLen;
    char* str = RedisGears_StringRecordGet(r, &listLen);
    RedisModule_ReplyWithStringBuffer(rctx, str, listLen);
    return REDISMODULE_OK;
}

static int LongRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    RedisModule_ReplyWithLongLong(rctx, RedisGears_LongRecordGet(r));
    return REDISMODULE_OK;
}

static int DoubleRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    RedisModule_ReplyWithDouble(rctx, RedisGears_DoubleRecordGet(r));
    return REDISMODULE_OK;
}

static int ListRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    size_t listLen = RedisGears_ListRecordLen(r);
    RedisModule_ReplyWithArray(rctx, listLen);
    for(int i = 0 ; i < listLen ; ++i){
        RG_RecordSendReply(RedisGears_ListRecordGet(r, i), rctx);
    }
    return REDISMODULE_OK;
}

static int KeyRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    RedisModule_ReplyWithArray(rctx, 2);
    size_t keyLen;
    char* key = RedisGears_KeyRecordGetKey(r, &keyLen);
    RedisModule_ReplyWithStringBuffer(rctx, key, keyLen);
    RG_RecordSendReply(RedisGears_KeyRecordGetVal(r), rctx);
    return REDISMODULE_OK;
}

static int KeysHandlerRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    RedisModule_ReplyWithStringBuffer(rctx, "KEY HANDLER RECORD", strlen("KEY HANDLER RECORD"));
    return REDISMODULE_OK;
}

static int AsyncRecord_SendReply(Record* base, RedisModuleCtx* rctx){
    RedisModule_Assert(false); // can not reach here;
    return REDISMODULE_OK;
}

static int NullRecord_SendReply(Record* base, RedisModuleCtx* rctx){
    RedisModule_ReplyWithNull(rctx);
    return REDISMODULE_OK;
}

static int HashSetRecord_SendReply(Record* base, RedisModuleCtx* rctx){
    HashSetRecord* record = (HashSetRecord*)base;
    Gears_dictIterator *iter;
    Gears_dictEntry *entry;
    RedisModule_ReplyWithArray(rctx, Gears_dictSize(record->d));
    iter = Gears_dictGetIterator(record->d);
    entry = NULL;
    while((entry = Gears_dictNext(iter))){
        const char* k = Gears_dictGetKey(entry);
        Record* temp = Gears_dictGetVal(entry);
        RedisModule_ReplyWithArray(rctx, 2);
        RedisModule_ReplyWithCString(rctx, k);
        RG_RecordSendReply(temp, rctx);
    }
    Gears_dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

int RG_SerializeRecord(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* r){
    RedisGears_BWWriteLong(bw, r->type->id);
    return r->type->serialize(ctx, bw, r);
}

Record* RG_DeserializeRecord(ExecutionCtx* ctx, Gears_BufferReader* br){
    size_t typeId = RedisGears_BRReadLong(br);
    RedisModule_Assert(typeId >= 0 && typeId < array_len(recordsTypes));
    RecordType* type = recordsTypes[typeId];
    return type->deserialize(ctx, br);
}

int RG_RecordSendReply(Record* record, RedisModuleCtx* rctx){
    if(!record){
        RedisModule_ReplyWithNull(rctx);
        return REDISMODULE_OK;
    }
    if(!record->type->sendReply){
        RedisModule_ReplyWithCString(rctx, record->type->name);
        return REDISMODULE_OK;
    }
    return record->type->sendReply(record, rctx);
}

RecordType* RG_RecordTypeCreate(const char* name, size_t size,
                                RecordSendReply sendReply,
                                RecordSerialize serialize,
                                RecordDeserialize deserialize,
                                RecordFree free){
    RecordType* ret = RG_ALLOC(sizeof(RecordType));
    *ret = (RecordType){
            .name = RG_STRDUP(name),
            .size = size,
            .sendReply = sendReply,
            .serialize = serialize,
            .deserialize = deserialize,
            .free = free,
    };
    recordsTypes = array_append(recordsTypes, ret);
    ret->id = array_len(recordsTypes) - 1;
    return ret;
}

void Record_Initialize(){
    recordsTypes = array_new(RecordType*, 10);
    listRecordType = RG_RecordTypeCreate("ListRecord", sizeof(ListRecord),
                                         ListRecord_SendReply,
                                         ListRecord_Serialize,
                                         ListRecord_Deserialize,
                                         ListRecord_Free);

    stringRecordType = RG_RecordTypeCreate("StringRecord", sizeof(StringRecord),
                                           StringRecord_SendReply,
                                           StringRecord_Serialize,
                                           StringRecord_Deserialize,
                                           StringRecord_Free);

    errorRecordType = RG_RecordTypeCreate("ErrorRecord", sizeof(StringRecord),
                                          StringRecord_SendReply,
                                          StringRecord_Serialize,
                                          ErrorRecord_Deserialize,
                                          StringRecord_Free);

    longRecordType = RG_RecordTypeCreate("LongRecord", sizeof(LongRecord),
                                         LongRecord_SendReply,
                                         LongRecord_Serialize,
                                         LongRecord_Deserialize,
                                         LongRecord_Free);

    doubleRecordType = RG_RecordTypeCreate("DoubleRecord", sizeof(DoubleRecord),
                                           DoubleRecord_SendReply,
                                           DoubleRecord_Serialize,
                                           DoubleRecord_Deserialize,
                                           DoubleRecord_Free);

    keyRecordType = RG_RecordTypeCreate("KeyRecord", sizeof(KeyRecord),
                                        KeyRecord_SendReply,
                                        KeyRecord_Serialize,
                                        KeyRecord_Deserialize,
                                        KeyRecord_Free);

    keysHandlerRecordType = RG_RecordTypeCreate("KeysHandlerRecord", sizeof(KeysHandlerRecord),
                                                KeysHandlerRecord_SendReply,
                                                KeysHandlerRecord_Serialize,
                                                KeysHandlerRecord_Deserialize,
                                                KeysHandlerRecord_Free);

    hashSetRecordType = RG_RecordTypeCreate("HashSetRecord", sizeof(HashSetRecord),
                                            HashSetRecord_SendReply,
                                            HashSetRecord_Serialize,
                                            HashSetRecord_Deserialize,
                                            HashSetRecord_Free);

    asyncRecordType = RG_RecordTypeCreate("AsyncRecord", sizeof(AsyncRecord),
                                            AsyncRecord_SendReply,
                                            AsyncRecord_Serialize,
                                            AsyncRecord_Deserialize,
                                            AsyncRecord_Free);

    nullRecordType = RG_RecordTypeCreate("NullRecord", sizeof(NullRecord),
                                          NullRecord_SendReply,
                                          NullRecord_Serialize,
                                          NullRecord_Deserialize,
                                          NullRecord_Free);
    NullRecord.type = nullRecordType;
}

void RG_FreeRecord(Record* record){
    if(!record){
        return;
    }
    if(IS_SINGLETON(record)){
        return;
    }
    record->type->free(record);
    RG_FREE(record);
}

Record* RG_GetDummyRecord(){
    return &DummyRecord;
}

Record* RG_GetNullRecord(){
    return &NullRecord;
}

RecordType* RG_RecordGetType(Record* r){
    return r->type;
}
Record* RG_KeyRecordCreate(){
    KeyRecord* ret = (KeyRecord*)RG_RecordCreate(keyRecordType);
    ret->key = NULL;
    ret->len = 0;
    ret->record = NULL;
    return &ret->base;
}

void RG_KeyRecordSetKey(Record* base, char* key, size_t len){
    RedisModule_Assert(base->type == keyRecordType);
    KeyRecord* r = (KeyRecord*)base;
    r->key = key;
    r->len = len;
}
void RG_KeyRecordSetVal(Record* base, Record* val){
    RedisModule_Assert(base->type == keyRecordType);
    KeyRecord* r = (KeyRecord*)base;
    r->record = val;
}

Record* RG_KeyRecordGetVal(Record* base){
    RedisModule_Assert(base->type == keyRecordType);
    KeyRecord* r = (KeyRecord*)base;
    return r->record;
}
char* RG_KeyRecordGetKey(Record* base, size_t* len){
    RedisModule_Assert(base->type == keyRecordType);
    KeyRecord* r = (KeyRecord*)base;
    if(len){
        *len = r->len;
    }
    return r->key;
}
Record* RG_ListRecordCreate(size_t initSize){
    ListRecord* ret = (ListRecord*)RG_RecordCreate(listRecordType);
    ret->records = array_new(Record*, initSize);
    return &ret->base;
}

size_t RG_ListRecordLen(Record* base){
    RedisModule_Assert(base->type == listRecordType);
    ListRecord* r = (ListRecord*)base;
    return array_len(r->records);
}

void RG_ListRecordAdd(Record* base, Record* element){
    RedisModule_Assert(base->type == listRecordType);
    ListRecord* r = (ListRecord*)base;
    r->records = array_append(r->records, element);
}

Record* RG_ListRecordGet(Record* base, size_t index){
    RedisModule_Assert(base->type == listRecordType);
    RedisModule_Assert(RG_ListRecordLen(base) > index && index >= 0);
    ListRecord* r = (ListRecord*)base;
    return r->records[index];
}

Record* RG_ListRecordPop(Record* base){
    RedisModule_Assert(base->type == listRecordType);
    ListRecord* r = (ListRecord*)base;
    return array_pop(r->records);
}

Record* RG_StringRecordCreate(char* val, size_t len){
    StringRecord* ret = (StringRecord*)RG_RecordCreate(stringRecordType);
    ret->str = val;
    ret->len = len;
    return &ret->base;
}

char* RG_StringRecordGet(Record* base, size_t* len){
    RedisModule_Assert(base->type == stringRecordType || base->type == errorRecordType);
    StringRecord* r = (StringRecord*)base;
    if(len){
        *len = r->len;
    }
    return r->str;
}

void RG_StringRecordSet(Record* base, char* val, size_t len){
    RedisModule_Assert(base->type == stringRecordType || base->type == errorRecordType);
    StringRecord* r = (StringRecord*)base;
    r->str = val;
    r->len = len;
}

Record* RG_DoubleRecordCreate(double val){
    DoubleRecord* ret = (DoubleRecord*)RG_RecordCreate(doubleRecordType);
    ret->num = val;
    return &ret->base;
}

double RG_DoubleRecordGet(Record* base){
    RedisModule_Assert(base->type == doubleRecordType);
    DoubleRecord* r = (DoubleRecord*)base;
    return r->num;
}

void RG_DoubleRecordSet(Record* base, double val){
    RedisModule_Assert(base->type == doubleRecordType);
    DoubleRecord* r = (DoubleRecord*)base;
    r->num = val;
}

Record* RG_LongRecordCreate(long val){
    LongRecord* ret = (LongRecord*)RG_RecordCreate(longRecordType);
    ret->num = val;
    return &ret->base;
}
long RG_LongRecordGet(Record* base){
    RedisModule_Assert(base->type == longRecordType);
    LongRecord* r = (LongRecord*)base;
    return r->num;
}
void RG_LongRecordSet(Record* base, long val){
    RedisModule_Assert(base->type == longRecordType);
    LongRecord* r = (LongRecord*)base;
    r->num = val;
}

Record* RG_HashSetRecordCreate(){
    HashSetRecord* ret = (HashSetRecord*)RG_RecordCreate(hashSetRecordType);
    ret->d = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    return &ret->base;
}

int RG_HashSetRecordSet(Record* base, char* key, Record* val){
    RedisModule_Assert(base->type == hashSetRecordType);
    HashSetRecord* r = (HashSetRecord*)base;
    Record* oldVal = RG_HashSetRecordGet(base, key);
    if(oldVal){
        RG_FreeRecord(oldVal);
        Gears_dictDelete(r->d, key);
    }
    return Gears_dictAdd(r->d, key, val) == DICT_OK;
}

Record* RG_HashSetRecordGet(Record* base, char* key){
    RedisModule_Assert(base->type == hashSetRecordType);
    HashSetRecord* r = (HashSetRecord*)base;
    Gears_dictEntry *entry = Gears_dictFind(r->d, key);
    if(!entry){
        return 0;
    }
    return Gears_dictGetVal(entry);
}

char** RG_HashSetRecordGetAllKeys(Record* base){
    RedisModule_Assert(base->type == hashSetRecordType);
    HashSetRecord* r = (HashSetRecord*)base;
    Gears_dictIterator *iter = Gears_dictGetIterator(r->d);
    Gears_dictEntry *entry = NULL;
    char** ret = array_new(char*, Gears_dictSize(r->d));
    while((entry = Gears_dictNext(iter))){
        char* key = Gears_dictGetKey(entry);
        ret = array_append(ret, key);
    }
    Gears_dictReleaseIterator(iter);
    return ret;
}

Record* RG_AsyncRecordCreate(ExecutionCtx* ectx, char** err){
    if(!ectx->step){
        *err = RG_STRDUP("Can not create gearsFuture outside of step");
        return NULL;
    }

    if(ectx->asyncRecordCreated){
        *err = RG_STRDUP("Can not create async record twice on the same step");
        return NULL;
    }

    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    if(ep->mode == ExecutionModeSync){
        *err = RG_STRDUP("Can not create gearsFuture on sync execution");
        return NULL;
    }
    size_t maxSize;
    switch(ectx->step->type){
    case MAP:
    case FILTER:
    case FOREACH:
        maxSize = 1000;
        break;
    case ACCUMULATE:
    case ACCUMULATE_BY_KEY:
        maxSize = 1;
        break;
    default:
        *err = RG_STRDUP("Step does not support async");
        return NULL;
    }

    AsyncRecord* ret = (AsyncRecord*)RG_RecordCreate(asyncRecordType);
    if(!ectx->ep->pendingCtxs[ectx->step->stepId]){
        // todo: change max to match the step type
        ectx->ep->pendingCtxs[ectx->step->stepId] = ExecutionPlan_PendingCtxCreate(ectx->ep, ectx->step, maxSize);
    }
    ret->pctx = ExecutionPlan_PendingCtxGetShallowCopy(ectx->ep->pendingCtxs[ectx->step->stepId]);

    // place holder for the real value
    Gears_listAddNodeHead(ret->pctx->records, NULL);

    // save pointer to set the real value
    ret->rptx = (Record**)(&(Gears_listFirst(ret->pctx->records)->value));
    ret->overridePlaceHolder = ectx->actualPlaceHolder;
    ret->originRecord = ectx->originRecord;
    ectx->asyncRecordCreated = &(ret->base);
    return &ret->base;
}

void RG_AsyncRecordContinue(Record* asyncRecord, Record* r){
    AsyncRecord* async = (AsyncRecord*)asyncRecord;
    RG_AsyncRecordContinueInternal(async, r);
    RG_FreeRecord(asyncRecord);
}

Record* RG_KeyHandlerRecordCreate(RedisModuleKey* handler){
    KeysHandlerRecord* ret = (KeysHandlerRecord*)RG_RecordCreate(keysHandlerRecordType);
    ret->keyHandler = handler;
    return &ret->base;
}

RedisModuleKey* RG_KeyHandlerRecordGet(Record* base){
    RedisModule_Assert(base->type == keysHandlerRecordType);
    KeysHandlerRecord* r = (KeysHandlerRecord*)base;
    return r->keyHandler;
}

Record* RG_ErrorRecordCreate(char* val, size_t len){
    StringRecord* ret = (StringRecord*)RG_RecordCreate(errorRecordType);
    ret->str = val;
    ret->len = len;
    return &ret->base;
}
