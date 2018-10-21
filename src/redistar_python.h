/*
 * redistar_python.h
 *
 *  Created on: Oct 20, 2018
 *      Author: meir
 */

#ifndef SRC_REDISTAR_PYTHON_H_
#define SRC_REDISTAR_PYTHON_H_

#include "redismodule.h"
#include "redistar.h"
#include <Python.h>

extern PyMethodDef EmbMethods[];

int RediStarPy_Init();
Record* RediStarPy_ToPyRecordMapper(Record *record, void* redisModuleCtx);
Record* RediStarPy_PyCallbackMapper(Record *record, void* arg);
bool RediStarPy_PyCallbackFilter(Record *record, void* arg);
char* RediStarPy_PyCallbackExtractor(Record *record, void* arg, size_t* len);
Record* RediStarPy_PyCallbackReducer(char* key, size_t keyLen, Record *records, void* arg);
int RediStarPy_Execut(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


#endif /* SRC_REDISTAR_PYTHON_H_ */
