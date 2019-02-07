#include "utils/arr_rm_alloc.h"
#include "record.h"

#include "redisgears.h"
#include "redisgears_memory.h"
#ifdef WITHPYTHON
#include "redisgears_python.h"
#endif

#include <pthread.h>

char* RecordStrTypes[] = {
        "NONE",
        "KEY_HANDLER_RECORD",
        "LONG_RECORD",
        "DOUBLE_RECORD",
        "STRING_RECORD",
        "LIST_RECORD",
        "KEY_RECORD",
        "HASH_SET_RECORD",
        "STOP_RECORD",
#ifdef WITHPYTHON
        "PY_RECORD",
#endif
};

pthread_key_t _recordAllocatorKey;
pthread_key_t _recordDisposeKey;
pthread_key_t _recordFreeMemoryKey;


Record StopRecord = {
        .type = STOP_RECORD,
};

static inline Record* RG_DefaultAllocator(){
    return RG_ALLOC(sizeof(Record));
}

void RG_SetRecordAlocator(enum RecordAllocator allocator){
    switch(allocator){
    case DEFAULT:
        pthread_setspecific(_recordAllocatorKey, RG_DefaultAllocator);
        pthread_setspecific(_recordDisposeKey, RG_DisposeRecord);
        pthread_setspecific(_recordFreeMemoryKey, RG_FREE);
        break;
    case PYTHON:
        pthread_setspecific(_recordAllocatorKey, RedisGearsPy_AllocatePyRecord);
        pthread_setspecific(_recordDisposeKey, RedisGearsPy_DisposePyRecord);
        pthread_setspecific(_recordFreeMemoryKey, NULL); // python handles his own memory!!
        break;
    default:
        assert(false);
    }
}

int RG_RecordInit(){
    int err = pthread_key_create(&_recordAllocatorKey, NULL);
    err &= pthread_key_create(&_recordDisposeKey, NULL);
    err &= pthread_key_create(&_recordFreeMemoryKey, NULL);
    RG_SetRecordAlocator(DEFAULT);
    return !err;
}

static char* RG_RecordDefaultToStr(Record* r){
    char* typeStr = RG_STRDUP(RecordStrTypes[r->type]);
    return typeStr;
}

static inline Record* RecordAlloc(){
    Record_Alloc alloc = pthread_getspecific(_recordAllocatorKey);
    Record_Free free = pthread_getspecific(_recordFreeMemoryKey);
    Record_Dispose dispose = pthread_getspecific(_recordDisposeKey);
    Record* ret = alloc();
    ret->extractInt = NULL;
    ret->extractStr = NULL;
    ret->toStr = RG_RecordDefaultToStr;
    ret->free = free;
    ret->dispose = dispose;
    return ret;
}

static inline void RecordDispose(Record* r){
    r->dispose(r);
}

static inline void RecordFree(Record* r){
    if(r->free){
        r->free(r);
    }
}

void RG_DisposeRecord(Record* record){
    dictIterator *iter;
    dictEntry *entry;
    Record* temp;
    switch(record->type){
    case STRING_RECORD:
        RG_FREE(record->stringRecord.str);
        break;
    case LONG_RECORD:
    case DOUBLE_RECORD:
        break;
    case LIST_RECORD:
        for(size_t i = 0 ; i < RedisGears_ListRecordLen(record) ; ++i){
            RG_FreeRecord(record->listRecord.records[i]);
        }
        array_free(record->listRecord.records);
        break;
    case KEY_RECORD:
        if(record->keyRecord.key){
            RG_FreeRecord(record->keyRecord.key);
        }
        if(record->keyRecord.record){
            RG_FreeRecord(record->keyRecord.record);
        }
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_FreeString(NULL, record->keyHandlerRecord.key);
        break;
    case HASH_SET_RECORD:
        iter = dictGetIterator(record->hashSetRecord.d);
        entry = NULL;
        while((entry = dictNext(iter))){
            temp = dictGetVal(entry);
            RG_FreeRecord(temp);
        }
        dictReleaseIterator(iter);
        dictRelease(record->hashSetRecord.d);
        break;
    default:
        assert(false);
    }
    RecordFree(record);
}

void RG_FreeRecord(Record* record){
#ifdef WITHPYTHON
    if(record->type == PY_RECORD){
        if(record->pyRecord.obj){
            PyGILState_STATE state = PyGILState_Ensure();
            Py_DECREF(record->pyRecord.obj);
            PyGILState_Release(state);
        }
        RG_FREE(record);
        return;
    }
#endif
    RecordDispose(record);
}

enum RecordType RG_RecordGetType(Record* r){
    return r->type;
}

static char* RG_KeyRecordToStr(Record* r){
    char* valStr = r->keyRecord.record->toStr(r->keyRecord.record);
    char* keyStr = r->keyRecord.key->toStr(r->keyRecord.key);
    size_t len = strlen(valStr) + strlen(keyStr) + 20;
    char* ret = RG_ALLOC(len);
    snprintf(ret, len, "%s : %s", keyStr, valStr);
    RG_FREE(keyStr);
    RG_FREE(valStr);
    return ret;
}

static Record* RG_KeyRecordExtractStr(Record* r, const char* val){
    if(strcmp(val, "key") == 0){
        return r->keyRecord.key;
    }else if(strcmp(val, "value") == 0){
        return r->keyRecord.record;
    }
    return NULL;
}

Record* RG_KeyRecordCreate(){
    Record* ret = RecordAlloc();
    ret->type = KEY_RECORD;
    ret->keyRecord.key = NULL;
    ret->keyRecord.record = NULL;
    ret->toStr = RG_KeyRecordToStr;
    ret->extractStr = RG_KeyRecordExtractStr;
    return ret;
}

void RG_KeyRecordSetKey(Record* r, Record* key){
    assert(r->type == KEY_RECORD);
    assert(key->type == STRING_RECORD);
    r->keyRecord.key = key;
}
void RG_KeyRecordSetVal(Record* r, Record* val){
    assert(r->type == KEY_RECORD);
    r->keyRecord.record = val;
}

Record* RG_KeyRecordGetVal(Record* r){
    assert(r->type == KEY_RECORD);
    return r->keyRecord.record;
}
char* RG_KeyRecordGetKey(Record* r, size_t* len){
    assert(r->type == KEY_RECORD);
    if(len){
        *len = r->keyRecord.key->stringRecord.len;
    }
    return r->keyRecord.key->stringRecord.str;
}

static Record* RG_ListRecordExtractInt(Record* r, int val){
    size_t index = val & RG_ListRecordLen(r);
    return RG_ListRecordGet(r, index);
}

Record* RG_ListRecordCreate(size_t initSize){
    Record* ret = RecordAlloc();
    ret->type = LIST_RECORD;
    ret->listRecord.records = array_new(Record*, initSize);
    ret->extractInt = RG_ListRecordExtractInt;
    return ret;
}

size_t RG_ListRecordLen(Record* r){
    assert(r->type == LIST_RECORD);
    return array_len(r->listRecord.records);
}

void RG_ListRecordAdd(Record* r, Record* element){
    assert(r->type == LIST_RECORD);
    r->listRecord.records = array_append(r->listRecord.records, element);
}

Record* RG_ListRecordGet(Record* r, size_t index){
    assert(r->type == LIST_RECORD);
    assert(RG_ListRecordLen(r) > index && index >= 0);
    return r->listRecord.records[index];
}

Record* RG_ListRecordPop(Record* r){
    return array_pop(r->listRecord.records);
}

static char* RG_StringRecordToString(Record* r){
    return RG_STRDUP(r->stringRecord.str);
}

Record* RG_StringRecordCreate(char* val, size_t len){
    Record* ret = RecordAlloc();
    ret->type = STRING_RECORD;
    ret->stringRecord.str = val;
    ret->stringRecord.len = len;
    ret->toStr = RG_StringRecordToString;
    return ret;
}

char* RG_StringRecordGet(Record* r, size_t* len){
    assert(r->type == STRING_RECORD);
    if(len){
        *len = r->stringRecord.len;
    }
    return r->stringRecord.str;
}

void RG_StringRecordSet(Record* r, char* val, size_t len){
    assert(r->type == STRING_RECORD);
    r->stringRecord.str = val;
    r->stringRecord.len = len;
}

Record* RG_DoubleRecordCreate(double val){
    Record* ret = RecordAlloc();
    ret->type = DOUBLE_RECORD;
    ret->doubleRecord.num = val;
    return ret;
}

double RG_DoubleRecordGet(Record* r){
    assert(r->type == DOUBLE_RECORD);
    return r->doubleRecord.num;
}

void RG_DoubleRecordSet(Record* r, double val){
    assert(r->type == DOUBLE_RECORD);
    r->doubleRecord.num = val;
}

Record* RG_LongRecordCreate(long val){
    Record* ret = RecordAlloc();
    ret->type = LONG_RECORD;
    ret->longRecord.num = val;
    return ret;
}
long RG_LongRecordGet(Record* r){
    assert(r->type == LONG_RECORD);
    return r->longRecord.num;
}
void RG_LongRecordSet(Record* r, long val){
    assert(r->type == LONG_RECORD);
    r->longRecord.num = val;
}

Record* RG_HashSetRecordCreate(){
    Record* ret = RecordAlloc();
    ret->type = HASH_SET_RECORD;
    ret->hashSetRecord.d = dictCreate(&dictTypeHeapStrings, NULL);
    ret->extractStr = RG_HashSetRecordGet;
    return ret;
}

int RG_HashSetRecordSet(Record* r, const char* key, Record* val){
    assert(r->type == HASH_SET_RECORD);
    Record* oldVal = RG_HashSetRecordGet(r, key);
    if(oldVal){
        RG_FreeRecord(oldVal);
        dictDelete(r->hashSetRecord.d, key);
    }
    return dictAdd(r->hashSetRecord.d, (char*)key, val) == DICT_OK;
}

Record* RG_HashSetRecordGet(Record* r, const char* key){
    assert(r->type == HASH_SET_RECORD);
    dictEntry *entry = dictFind(r->hashSetRecord.d, key);
    if(!entry){
        return 0;
    }
    return dictGetVal(entry);
}

char** RG_HashSetRecordGetAllKeys(Record* r, size_t* len){
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

void RG_HashSetRecordFreeKeysArray(char** keyArr){
    array_free(keyArr);
}

Record* RG_KeyHandlerRecordCreate(RedisModuleString* key, enum RecordType rt){
    Record* ret = RecordAlloc();
    ret->type = KEY_HANDLER_RECORD;
    ret->keyHandlerRecord.key = key;
    ret->keyHandlerRecord.keyType = rt;
    return ret;
}

RedisModuleString* RG_KeyHandlerRecordGet(Record* r){
    assert(r->type == KEY_HANDLER_RECORD);
    return r->keyHandlerRecord.key;
}

enum RecordType RG_KeyHandlerRecordGetType(Record* r){
    assert(r->type == KEY_HANDLER_RECORD);
    return r->keyHandlerRecord.keyType;
}

#ifdef WITHPYTHON

static char* RG_PyObjRecordToStr(Record* r){
    PyObject* str = PyObject_Str(r->pyRecord.obj);
    char* cstr = RG_STRDUP(PyString_AsString(str));
    Py_DECREF(str);
    return cstr;
}

Record* RG_PyObjRecordCreate(){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = PY_RECORD;
    ret->pyRecord.obj = NULL;
    ret->toStr = RG_PyObjRecordToStr;
    return ret;
}

PyObject* RG_PyObjRecordGet(Record* r){
    assert(r->type == PY_RECORD);
    return r->pyRecord.obj;
}

void RG_PyObjRecordSet(Record* r, PyObject* obj){
    assert(r->type == PY_RECORD);
    r->pyRecord.obj = obj;
}
#endif

void RG_SerializeRecord(BufferWriter* bw, Record* r){
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
            RG_SerializeRecord(bw, r->listRecord.records[i]);
        }
        break;
    case KEY_RECORD:
        RG_SerializeRecord(bw, r->keyRecord.key);
        if(r->keyRecord.record){
            RedisGears_BWWriteLong(bw, 1); // value exists
            RG_SerializeRecord(bw, r->keyRecord.record);
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

Record* RG_DeserializeRecord(BufferReader* br){
    enum RecordType type = RedisGears_BRReadLong(br);
    Record* r;
    char* temp;
    char* temp1;
    size_t size;
    switch(type){
    case STRING_RECORD:
        temp = RedisGears_BRReadBuffer(br, &size);
        temp1 = RG_ALLOC(size);
        memcpy(temp1, temp, size);
        r = RG_StringRecordCreate(temp1, size);
        break;
    case LONG_RECORD:
        r = RG_LongRecordCreate(RedisGears_BRReadLong(br));
        break;
    case DOUBLE_RECORD:
        r = RG_DoubleRecordCreate((double)RedisGears_BRReadLong(br));
        break;
    case LIST_RECORD:
        size = (size_t)RedisGears_BRReadLong(br);
        r = RG_ListRecordCreate(size);
        for(size_t i = 0 ; i < size ; ++i){
            RG_ListRecordAdd(r, RG_DeserializeRecord(br));
        }
        break;
    case KEY_RECORD:
        r = RedisGears_KeyRecordCreate();
        RedisGears_KeyRecordSetKey(r, RG_DeserializeRecord(br));
        bool isValExists = (bool)RedisGears_BRReadLong(br);
        if(isValExists){
            RedisGears_KeyRecordSetVal(r, RG_DeserializeRecord(br));
        }else{
            RedisGears_KeyRecordSetVal(r, NULL);
        }
        break;
    case KEY_HANDLER_RECORD:
        assert(false && "can not deserialize key handler record");
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        r = RG_PyObjRecordCreate();
        PyObject* obj = RedisGearsPy_PyObjectDeserialize(br);
        r->pyRecord.obj = obj;
        break;
#endif
    default:
        assert(false);
    }
    return r;
}

char* RG_RecordToStr(Record* r){
    return r->toStr(r);
}

Record* RG_RecordExtractIntVal(Record* r, int val){
    if(!r->extractInt){
        return NULL;
    }
    return r->extractInt(r, val);
}

Record* RG_RecordExtractStrVal(Record* r, const char* val){
    if(!r->extractStr){
        return NULL;
    }
    return r->extractStr(r, val);
}

