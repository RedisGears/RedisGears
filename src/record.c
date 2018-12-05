#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "record.h"

#include "redisgears.h"
#include "redisgears_memory.h"
#ifdef WITHPYTHON
#include "redisgears_python.h"
#endif

typedef struct KeysHandlerRecord{
    RedisModuleKey *keyHandler;
}KeysHandlerRecord;

typedef struct LongRecord{
    long num;
}LongRecord;

typedef struct DoubleRecord{
    double num;
}DoubleRecord;

typedef struct StringRecord{
    size_t len;
    char* str;
}StringRecord;

typedef struct ListRecord{
    Record** records;
}ListRecord;

#ifdef WITHPYTHON
typedef struct PythonRecord{
    PyObject* obj;
}PythonRecord;
#endif

typedef struct KeyRecord{
    char* key;
    size_t len;
    Record* record;
}KeyRecord;

typedef struct HashSetRecord{
    dict* d;
}HashSetRecord;

typedef struct Record{
    union{
        KeysHandlerRecord keyHandlerRecord;
        LongRecord longRecord;
        StringRecord stringRecord;
        DoubleRecord doubleRecord;
        ListRecord listRecord;
        KeyRecord keyRecord;
        HashSetRecord hashSetRecord;
#ifdef WITHPYTHON
        PythonRecord pyRecord;
#endif
    };
    enum RecordType type;
}Record;


Record StopRecord = {
        .type = STOP_RECORD,
};


void RS_FreeRecord(Record* record){
    dictIterator *iter;
    dictEntry *entry;
    Record* temp;
    switch(record->type){
    case STRING_RECORD:
        RS_FREE(record->stringRecord.str);
        break;
    case LONG_RECORD:
    case DOUBLE_RECORD:
        break;
    case LIST_RECORD:
        for(size_t i = 0 ; i < RedisGears_ListRecordLen(record) ; ++i){
            RS_FreeRecord(record->listRecord.records[i]);
        }
        array_free(record->listRecord.records);
        break;
    case KEY_RECORD:
        if(record->keyRecord.key){
            RS_FREE(record->keyRecord.key);
        }
        if(record->keyRecord.record){
            RS_FreeRecord(record->keyRecord.record);
        }
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_CloseKey(record->keyHandlerRecord.keyHandler);
        break;
    case HASH_SET_RECORD:
        iter = dictGetIterator(record->hashSetRecord.d);
        entry = NULL;
        while((entry = dictNext(iter))){
            temp = dictGetVal(entry);
            RS_FreeRecord(temp);
        }
        dictReleaseIterator(iter);
        dictRelease(record->hashSetRecord.d);
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        Py_DECREF(record->pyRecord.obj);
        break;
#endif
    default:
        assert(false);
    }
    RS_FREE(record);
}

enum RecordType RS_RecordGetType(Record* r){
    return r->type;
}
Record* RS_KeyRecordCreate(){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = KEY_RECORD;
    ret->keyRecord.key = NULL;
    ret->keyRecord.len = 0;
    ret->keyRecord.record = NULL;
    return ret;
}

void RS_KeyRecordSetKey(Record* r, char* key, size_t len){
    assert(r->type == KEY_RECORD);
    r->keyRecord.key = key;
    r->keyRecord.len = len;
}
void RS_KeyRecordSetVal(Record* r, Record* val){
    assert(r->type == KEY_RECORD);
    r->keyRecord.record = val;
}

Record* RS_KeyRecordGetVal(Record* r){
    assert(r->type == KEY_RECORD);
    return r->keyRecord.record;
}
char* RS_KeyRecordGetKey(Record* r, size_t* len){
    assert(r->type == KEY_RECORD);
    if(len){
        *len = r->keyRecord.len;
    }
    return r->keyRecord.key;
}
Record* RS_ListRecordCreate(size_t initSize){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = LIST_RECORD;
    ret->listRecord.records = array_new(Record*, initSize);
    return ret;
}

size_t RS_ListRecordLen(Record* r){
    assert(r->type == LIST_RECORD);
    return array_len(r->listRecord.records);
}

void RS_ListRecordAdd(Record* r, Record* element){
    assert(r->type == LIST_RECORD);
    r->listRecord.records = array_append(r->listRecord.records, element);
}

Record* RS_ListRecordGet(Record* r, size_t index){
    assert(r->type == LIST_RECORD);
    assert(RS_ListRecordLen(r) > index && index >= 0);
    return r->listRecord.records[index];
}

Record* RS_ListRecordPop(Record* r){
    return array_pop(r->listRecord.records);
}

Record* RS_StringRecordCreate(char* val, size_t len){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = STRING_RECORD;
    ret->stringRecord.str = val;
    ret->stringRecord.len = len;
    return ret;
}

char* RS_StringRecordGet(Record* r, size_t* len){
    assert(r->type == STRING_RECORD);
    if(len){
        *len = r->stringRecord.len;
    }
    return r->stringRecord.str;
}

void RS_StringRecordSet(Record* r, char* val, size_t len){
    assert(r->type == STRING_RECORD);
    r->stringRecord.str = val;
    r->stringRecord.len = len;
}

Record* RS_DoubleRecordCreate(double val){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = DOUBLE_RECORD;
    ret->doubleRecord.num = val;
    return ret;
}

double RS_DoubleRecordGet(Record* r){
    assert(r->type == DOUBLE_RECORD);
    return r->doubleRecord.num;
}

void RS_DoubleRecordSet(Record* r, double val){
    assert(r->type == DOUBLE_RECORD);
    r->doubleRecord.num = val;
}

Record* RS_LongRecordCreate(long val){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = LONG_RECORD;
    ret->longRecord.num = val;
    return ret;
}
long RS_LongRecordGet(Record* r){
    assert(r->type == LONG_RECORD);
    return r->longRecord.num;
}
void RS_LongRecordSet(Record* r, long val){
    assert(r->type == LONG_RECORD);
    r->longRecord.num = val;
}

Record* RS_HashSetRecordCreate(){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = HASH_SET_RECORD;
    ret->hashSetRecord.d = dictCreate(&dictTypeHeapStrings, NULL);
    return ret;
}

int RS_HashSetRecordSet(Record* r, char* key, Record* val){
    assert(r->type == HASH_SET_RECORD);
    Record* oldVal = RS_HashSetRecordGet(r, key);
    if(oldVal){
        RS_FreeRecord(oldVal);
        dictDelete(r->hashSetRecord.d, key);
    }
    return dictAdd(r->hashSetRecord.d, key, val) == DICT_OK;
}

Record* RS_HashSetRecordGet(Record* r, char* key){
    assert(r->type == HASH_SET_RECORD);
    dictEntry *entry = dictFind(r->hashSetRecord.d, key);
    if(!entry){
        return 0;
    }
    return dictGetVal(entry);
}

char** RS_HashSetRecordGetAllKeys(Record* r, size_t* len){
    assert(r->type == HASH_SET_RECORD);
    dictIterator *iter = dictGetIterator(r->hashSetRecord.d);
    dictEntry *entry = NULL;
    char** ret = array_new(char*, dictSize(r->hashSetRecord.d));
    while((entry = dictNext(iter))){
        char* key = dictGetKey(entry);
        ret = array_append(ret, key);
    }
    *len = array_len(ret);
    dictReleaseIterator(iter);
    return ret;
}

void RS_HashSetRecordFreeKeysArray(char** keyArr){
    array_free(keyArr);
}

Record* RS_KeyHandlerRecordCreate(RedisModuleKey* handler){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = KEY_HANDLER_RECORD;
    ret->keyHandlerRecord.keyHandler = handler;
    return ret;
}

RedisModuleKey* RS_KeyHandlerRecordGet(Record* r){
    assert(r->type == KEY_HANDLER_RECORD);
    return r->keyHandlerRecord.keyHandler;
}

#ifdef WITHPYTHON
Record* RS_PyObjRecordCreate(){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = PY_RECORD;
    ret->pyRecord.obj = NULL;
    return ret;
}

PyObject* RS_PyObjRecordGet(Record* r){
    assert(r->type == PY_RECORD);
    return r->pyRecord.obj;
}

void RS_PyObjRecordSet(Record* r, PyObject* obj){
    assert(r->type == PY_RECORD);
    Py_INCREF(obj);
    r->pyRecord.obj = obj;
}
#endif

void RS_SerializeRecord(BufferWriter* bw, Record* r){
    RedisGears_BWWriteLong(bw, r->type);
    switch(r->type){
    case STRING_RECORD:
        RedisGears_BWWriteBuffer(bw, r->stringRecord.str, r->stringRecord.len);
        break;
    case LONG_RECORD:
        RedisGears_BWWriteLong(bw, r->longRecord.num);
        break;
    case DOUBLE_RECORD:
        RedisGears_BWWriteLong(bw, (long)r->doubleRecord.num);
        break;
    case LIST_RECORD:
        RedisGears_BWWriteLong(bw, RedisGears_ListRecordLen(r));
        for(size_t i = 0 ; i < RedisGears_ListRecordLen(r) ; ++i){
            RS_SerializeRecord(bw, r->listRecord.records[i]);
        }
        break;
    case KEY_RECORD:
        RedisGears_BWWriteString(bw, r->keyRecord.key);
        if(r->keyRecord.record){
            RedisGears_BWWriteLong(bw, 1); // value exists
            RS_SerializeRecord(bw, r->keyRecord.record);
        }else{
            RedisGears_BWWriteLong(bw, 0); // value missing
        }
        break;
    case KEY_HANDLER_RECORD:
        assert(false && "can not serialize key handler record");
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        RedisGearsPy_PyObjectSerialize(r->pyRecord.obj, bw);
        break;
#endif
    default:
        assert(false);
    }
}

Record* RS_DeserializeRecord(BufferReader* br){
    enum RecordType type = RedisGears_BRReadLong(br);
    Record* r;
    char* temp;
    char* temp1;
    size_t size;
    switch(type){
    case STRING_RECORD:
        temp = RedisGears_BRReadBuffer(br, &size);
        temp1 = RS_ALLOC(size);
        memcpy(temp1, temp, size);
        r = RS_StringRecordCreate(temp1, size);
        break;
    case LONG_RECORD:
        r = RS_LongRecordCreate(RedisGears_BRReadLong(br));
        break;
    case DOUBLE_RECORD:
        r = RS_DoubleRecordCreate((double)RedisGears_BRReadLong(br));
        break;
    case LIST_RECORD:
        size = (size_t)RedisGears_BRReadLong(br);
        r = RS_ListRecordCreate(size);
        for(size_t i = 0 ; i < size ; ++i){
            RS_ListRecordAdd(r, RS_DeserializeRecord(br));
        }
        break;
    case KEY_RECORD:
        r = RedisGears_KeyRecordCreate();
        char* key = RS_STRDUP(RedisGears_BRReadString(br));
        RS_KeyRecordSetKey(r, key, strlen(key));
        bool isValExists = (bool)RedisGears_BRReadLong(br);
        if(isValExists){
            RedisGears_KeyRecordSetVal(r, RS_DeserializeRecord(br));
        }else{
            RedisGears_KeyRecordSetVal(r, NULL);
        }
        break;
    case KEY_HANDLER_RECORD:
        assert(false && "can not deserialize key handler record");
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        r = RS_PyObjRecordCreate();
        PyObject* obj = RedisGearsPy_PyObjectDeserialize(br);
        r->pyRecord.obj = obj;
        break;
#endif
    default:
        assert(false);
    }
    return r;
}

