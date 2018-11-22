/*
 * record.h
 *
 *  Created on: Oct 21, 2018
 *      Author: meir
 */

#ifndef SRC_RECORD_H_
#define SRC_RECORD_H_

#include "redistar.h"
#include "utils/buffer.h"
#ifdef WITHPYTHON
#include <Python.h>
#endif

enum AdditionalRecordTypes{
    STOP_RECORD = 8, // telling the execution plan to stop the execution.
#ifdef WITHPYTHON
    PY_RECORD
#endif
};

extern Record StopRecord;

void RS_FreeRecord(Record* record);
enum RecordType RS_RecordGetType(Record* r);

/** key record api **/
Record* RS_KeyRecordCreate();
void RS_KeyRecordSetKey(Record* r, char* key, size_t len);
void RS_KeyRecordSetVal(Record* r, Record* val);
Record* RS_KeyRecordGetVal(Record* r);
char* RS_KeyRecordGetKey(Record* r, size_t* len);

/** list record api **/
Record* RS_ListRecordCreate(size_t initial_size);
size_t RS_ListRecordLen(Record* r);
void RS_ListRecordAdd(Record* r, Record* element);
Record* RS_ListRecordGet(Record* r, size_t index);
Record* RS_ListRecordPop(Record* r);

/** string record api **/
Record* RS_StringRecordCreate(char* val, size_t len);
char* RS_StringRecordGet(Record* r, size_t* len);
void RS_StringRecordSet(Record* r, char* val, size_t len);

/** double record api **/
Record* RS_DoubleRecordCreate(double val);
double RS_DoubleRecordGet(Record* r);
void RS_DoubleRecordSet(Record* r, double val);

/** long record api **/
Record* RS_LongRecordCreate(long val);
long RS_LongRecordGet(Record* r);
void RS_LongRecordSet(Record* r, long val);

/** hash set record api **/
Record* RS_HashSetRecordCreate();
int RS_HashSetRecordSet(Record* r, char* key, Record* val);
Record* RS_HashSetRecordGet(Record* r, char* key);
char** RS_HashSetRecordGetAllKeys(Record* r, size_t* len);
void RS_HashSetRecordFreeKeysArray(char** keyArr);

/* todo: think if we can removed this!! */
Record* RS_KeyHandlerRecordCreate(RedisModuleKey* handler);
RedisModuleKey* RS_KeyHandlerRecordGet(Record* r);

void RS_SerializeRecord(BufferWriter* bw, Record* r);
Record* RS_DeserializeRecord(BufferReader* br);

#ifdef WITHPYTHON
Record* RS_PyObjRecordCreate();
PyObject* RS_PyObjRecordGet(Record* r);
void RS_PyObjRecordSet(Record* r, PyObject* obj);
#endif




#endif /* SRC_RECORD_H_ */
