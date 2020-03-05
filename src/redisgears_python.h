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

typedef struct PythonSessionCtx PythonSessionCtx;

typedef void (*DoneCallbackFunction)(ExecutionPlan*, void*); 

int RedisGearsPy_PyObjectSerialize(void* arg, Gears_BufferWriter* bw, char** err);
void* RedisGearsPy_PyObjectDeserialize(Gears_BufferReader* br);
int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RedisGearsPy_ExecuteWithCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, DoneCallbackFunction callback);
int RedisGearsPy_Init(RedisModuleCtx *ctx);
void RedisGearsPy_ForceStop(unsigned long threadID);
PythonSessionCtx* RedisGearsPy_Lock(PythonSessionCtx* currSession);
void RedisGearsPy_Unlock(PythonSessionCtx* prevSession);
bool RedisGearsPy_IsLockAcquired();
void RedisGearsPy_Clean();

#endif /* SRC_REDISGEARG_PYTHON_H_ */
