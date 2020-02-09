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
    Gears_dict* d;
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


void RG_FreeRecord(Record* record){
    Gears_dictIterator *iter;
    Gears_dictEntry *entry;
    Record* temp;
    switch(record->type){
    case STRING_RECORD:
    case ERROR_RECORD:
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
            RG_FREE(record->keyRecord.key);
        }
        if(record->keyRecord.record){
            RG_FreeRecord(record->keyRecord.record);
        }
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_CloseKey(record->keyHandlerRecord.keyHandler);
        break;
    case HASH_SET_RECORD:
        iter = Gears_dictGetIterator(record->hashSetRecord.d);
        entry = NULL;
        while((entry = Gears_dictNext(iter))){
            temp = Gears_dictGetVal(entry);
            RG_FreeRecord(temp);
        }
        Gears_dictReleaseIterator(iter);
        Gears_dictRelease(record->hashSetRecord.d);
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
    	if(record->pyRecord.obj){
    	    RedisGearsPy_Lock();
    		if(record->pyRecord.obj != Py_None){
    		    Py_DECREF(record->pyRecord.obj);
    		}
    		RedisGearsPy_Unlock();
    	}
        break;
#endif
    default:
        assert(false);
    }
    RG_FREE(record);
}

int RG_RecordGetType(Record* r){
    return r->type;
}
Record* RG_KeyRecordCreate(){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = KEY_RECORD;
    ret->keyRecord.key = NULL;
    ret->keyRecord.len = 0;
    ret->keyRecord.record = NULL;
    return ret;
}

void RG_KeyRecordSetKey(Record* r, char* key, size_t len){
    assert(r->type == KEY_RECORD);
    r->keyRecord.key = key;
    r->keyRecord.len = len;
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
        *len = r->keyRecord.len;
    }
    return r->keyRecord.key;
}
Record* RG_ListRecordCreate(size_t initSize){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = LIST_RECORD;
    ret->listRecord.records = array_new(Record*, initSize);
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

Record* RG_StringRecordCreate(char* val, size_t len){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = STRING_RECORD;
    ret->stringRecord.str = val;
    ret->stringRecord.len = len;
    return ret;
}

char* RG_StringRecordGet(Record* r, size_t* len){
    assert(r->type == STRING_RECORD || r->type == ERROR_RECORD);
    if(len){
        *len = r->stringRecord.len;
    }
    return r->stringRecord.str;
}

void RG_StringRecordSet(Record* r, char* val, size_t len){
    assert(r->type == STRING_RECORD || r->type == ERROR_RECORD);
    r->stringRecord.str = val;
    r->stringRecord.len = len;
}

Record* RG_DoubleRecordCreate(double val){
    Record* ret = RG_ALLOC(sizeof(Record));
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
    Record* ret = RG_ALLOC(sizeof(Record));
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
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = HASH_SET_RECORD;
    ret->hashSetRecord.d = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    return ret;
}

int RG_HashSetRecordSet(Record* r, char* key, Record* val){
    assert(r->type == HASH_SET_RECORD);
    Record* oldVal = RG_HashSetRecordGet(r, key);
    if(oldVal){
        RG_FreeRecord(oldVal);
        Gears_dictDelete(r->hashSetRecord.d, key);
    }
    return Gears_dictAdd(r->hashSetRecord.d, key, val) == DICT_OK;
}

Record* RG_HashSetRecordGet(Record* r, char* key){
    assert(r->type == HASH_SET_RECORD);
    Gears_dictEntry *entry = Gears_dictFind(r->hashSetRecord.d, key);
    if(!entry){
        return 0;
    }
    return Gears_dictGetVal(entry);
}

char** RG_HashSetRecordGetAllKeys(Record* r, size_t* len){
    assert(r->type == HASH_SET_RECORD);
    Gears_dictIterator *iter = Gears_dictGetIterator(r->hashSetRecord.d);
    Gears_dictEntry *entry = NULL;
    char** ret = array_new(char*, Gears_dictSize(r->hashSetRecord.d));
    while((entry = Gears_dictNext(iter))){
        char* key = Gears_dictGetKey(entry);
        ret = array_append(ret, key);
    }
    *len = array_len(ret);
    Gears_dictReleaseIterator(iter);
    return ret;
}

void RG_HashSetRecordFreeKeysArray(char** keyArr){
    array_free(keyArr);
}

Record* RG_KeyHandlerRecordCreate(RedisModuleKey* handler){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = KEY_HANDLER_RECORD;
    ret->keyHandlerRecord.keyHandler = handler;
    return ret;
}

RedisModuleKey* RG_KeyHandlerRecordGet(Record* r){
    assert(r->type == KEY_HANDLER_RECORD);
    return r->keyHandlerRecord.keyHandler;
}

Record* RG_ErrorRecordCreate(char* val, size_t len){
    Record* ret = RG_StringRecordCreate(val, len);
    ret->type = ERROR_RECORD;
    return ret;
}

#ifdef WITHPYTHON
Record* RG_PyObjRecordCreate(){
    Record* ret = RG_ALLOC(sizeof(Record));
    ret->type = PY_RECORD;
    ret->pyRecord.obj = NULL;
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

void RG_SerializeRecord(Gears_BufferWriter* bw, Record* r){
    RedisGears_BWWriteLong(bw, r->type);
    switch(r->type){
    case STRING_RECORD:
    case ERROR_RECORD:
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
        RedisGears_BWWriteString(bw, r->keyRecord.key);
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

Record* RG_DeserializeRecord(Gears_BufferReader* br){
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
    case ERROR_RECORD:
        temp = RedisGears_BRReadBuffer(br, &size);
        temp1 = RG_ALLOC(size);
        memcpy(temp1, temp, size);
        r = RG_ErrorRecordCreate(temp1, size);
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
        char* key = RG_STRDUP(RedisGears_BRReadString(br));
        RG_KeyRecordSetKey(r, key, strlen(key));
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

