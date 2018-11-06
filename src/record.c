#include "redistar.h"
#include "redistar_memory.h"
#include "utils/arr_rm_alloc.h"
#include "record.h"
#ifdef WITHPYTHON
#include "redistar_python.h"
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

typedef struct Record{
    union{
        KeysHandlerRecord keyHandlerRecord;
        LongRecord longRecord;
        StringRecord stringRecord;
        DoubleRecord doubleRecord;
        ListRecord listRecord;
        KeyRecord keyRecord;
#ifdef WITHPYTHON
        PythonRecord pyRecord;
#endif
    };
    enum RecordType type;
}Record;


Record StopRecord = (Record){
        .type = STOP_RECORD,
};


void RS_FreeRecord(Record* record){
    switch(record->type){
    case STRING_RECORD:
        RS_FREE(record->stringRecord.str);
        break;
    case LONG_RECORD:
    case DOUBLE_RECORD:
        break;
    case LIST_RECORD:
        for(size_t i = 0 ; i < RediStar_ListRecordLen(record) ; ++i){
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
Record* RS_StringRecordCreate(char* val){
    Record* ret = RS_ALLOC(sizeof(Record));
    ret->type = STRING_RECORD;
    ret->stringRecord.str = val;
    return ret;
}

char* RS_StringRecordGet(Record* r){
    assert(r->type == STRING_RECORD);
    return r->stringRecord.str;
}

void RS_StringRecordSet(Record* r, char* val){
    assert(r->type == STRING_RECORD);
    r->stringRecord.str = val;
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
Record* RS_PyObjRecordCreare(){
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
    RediStar_BWWriteLong(bw, r->type);
    switch(r->type){
    case STRING_RECORD:
        RediStar_BWWriteString(bw, r->stringRecord.str);
        break;
    case LONG_RECORD:
        RediStar_BWWriteLong(bw, r->longRecord.num);
        break;
    case DOUBLE_RECORD:
        RediStar_BWWriteLong(bw, (long)r->doubleRecord.num);
        break;
    case LIST_RECORD:
        RediStar_BWWriteLong(bw, RediStar_ListRecordLen(r));
        for(size_t i = 0 ; i < RediStar_ListRecordLen(r) ; ++i){
            RS_SerializeRecord(bw, r->listRecord.records[i]);
        }
        break;
    case KEY_RECORD:
        RediStar_BWWriteString(bw, r->keyRecord.key);
        if(r->keyRecord.record){
            RediStar_BWWriteLong(bw, 1); // value exists
            RS_SerializeRecord(bw, r->keyRecord.record);
        }else{
            RediStar_BWWriteLong(bw, 0); // value missing
        }
        break;
    case KEY_HANDLER_RECORD:
        assert(false && "can not serialize key handler record");
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        RediStarPy_PyObjectSerialize(r->pyRecord.obj, bw);
        break;
#endif
    default:
        assert(false);
    }
}

Record* RS_DeserializeRecord(BufferReader* br){
    enum RecordType type = RediStar_BRReadLong(br);
    Record* r;
    char* temp;
    size_t size;
    switch(type){
    case STRING_RECORD:
        temp = RediStar_BRReadString(br);
        r = RS_StringRecordCreate(RS_STRDUP(temp));
        break;
    case LONG_RECORD:
        r = RS_LongRecordCreate(RediStar_BRReadLong(br));
        break;
    case DOUBLE_RECORD:
        r = RS_DoubleRecordCreate((double)RediStar_BRReadLong(br));
        break;
    case LIST_RECORD:
        size = (size_t)RediStar_BRReadLong(br);
        r = RS_ListRecordCreate(size);
        for(size_t i = 0 ; i < size ; ++i){
            RS_ListRecordAdd(r, RS_DeserializeRecord(br));
        }
        break;
    case KEY_RECORD:
        r = RediStar_KeyRecordCreate();
        char* key = RS_STRDUP(RediStar_BRReadString(br));
        RS_KeyRecordSetKey(r, key, strlen(key));
        bool isValExists = (bool)RediStar_BRReadLong(br);
        if(isValExists){
            RediStar_KeyRecordSetVal(r, RS_DeserializeRecord(br));
        }else{
            RediStar_KeyRecordSetVal(r, NULL);
        }
        break;
    case KEY_HANDLER_RECORD:
        assert(false && "can not deserialize key handler record");
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        r = RS_PyObjRecordCreare();
        PyObject* obj = RediStarPy_PyObjectDeserialize(br);
        r->pyRecord.obj = obj;
        break;
#endif
    default:
        assert(false);
    }
    return r;
}

