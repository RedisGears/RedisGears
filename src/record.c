#include "redistar.h"
#include "redistar_memory.h"
#include "utils/arr_rm_alloc.h"
#include <Python.h>

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

typedef struct PythonRecord{
    PyObject* obj;
}PythonRecord;

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
        PythonRecord pyRecord;
    };
    enum RecordType type;
}Record;

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
        RS_FREE(record->keyRecord.key);
        RS_FreeRecord(record->keyRecord.record);
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_CloseKey(record->keyHandlerRecord.keyHandler);
        break;
    case PY_RECORD:
        Py_DECREF(record->pyRecord.obj);
        break;
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
