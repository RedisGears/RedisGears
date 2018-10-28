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

void RS_FreeRecord(Record* record);
enum RecordType RS_RecordGetType(Record* r);
Record* RS_KeyRecordCreate();
void RS_KeyRecordSetKey(Record* r, char* key, size_t len);
void RS_KeyRecordSetVal(Record* r, Record* val);
Record* RS_KeyRecordGetVal(Record* r);
char* RS_KeyRecordGetKey(Record* r, size_t* len);
Record* RS_ListRecordCreate(size_t initial_size);
size_t RS_ListRecordLen(Record* r);
void RS_ListRecordAdd(Record* r, Record* element);
Record* RS_ListRecordGet(Record* r, size_t index);
Record* RS_StringRecordCreate(char* val);
char* RS_StringRecordGet(Record* r);
void RS_StringRecordSet(Record* r, char* val);
Record* RS_DoubleRecordCreate(double val);
double RS_DoubleRecordGet(Record* r);
void RS_DoubleRecordSet(Record* r, double val);
Record* RS_LongRecordCreate(long val);
long RS_LongRecordGet(Record* r);
void RS_LongRecordSet(Record* r, long val);
Record* RS_KeyHandlerRecordCreate(RedisModuleKey* handler);
RedisModuleKey* RS_KeyHandlerRecordGet(Record* r);
Record* RS_PyObjRecordCreare();

void RS_SerializeRecord(BufferWriter* bw, Record* r);
Record* RS_DeserializeRecord(BufferReader* br);

#ifdef WITHPYTHON
PyObject* RS_PyObjRecordGet(Record* r);
void RS_PyObjRecordSet(Record* r, PyObject* obj);
#endif




#endif /* SRC_RECORD_H_ */
