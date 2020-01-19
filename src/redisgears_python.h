/*
 * redisgears_python.h
 *
 *  Created on: Oct 20, 2018
 *      Author: meir
 */

#ifndef SRC_REDISGEARG_PYTHON_H_
#define SRC_REDISGEARG_PYTHON_H_

#include <Python.h>
#include "redismodule.h"
#include "redisgears.h"

PyGILState_STATE PyGILState_Ensure(void);
void PyGILState_Release(PyGILState_STATE oldstate);

typedef struct PythonSubInterpreter PythonSubInterpreter;

typedef void (*DoneCallbackFunction)(ExecutionPlan*, void*); 

void RedisGearsPy_PyObjectSerialize(void* arg, Gears_BufferWriter* bw);
void* RedisGearsPy_PyObjectDeserialize(Gears_BufferReader* br);
int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RedisGearsPy_ExecuteWithCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, DoneCallbackFunction callback);
int RedisGearsPy_Init(RedisModuleCtx *ctx);
PyGILState_STATE RedisGearsPy_RestoreThread(PythonSubInterpreter* interpreter);
void RedisGearsPy_SaveThread(PyGILState_STATE oldstate);
bool RedisGearsPy_IsLockAcquired();

#endif /* SRC_REDISGEARG_PYTHON_H_ */
