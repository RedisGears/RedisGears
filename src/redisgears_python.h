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

extern PyMethodDef EmbMethods[];

void RedisGearsPy_PyObjectSerialize(void* arg, BufferWriter* bw);
void* RedisGearsPy_PyObjectDeserialize(BufferReader* br);
int RedisGearsPy_Init(RedisModuleCtx *ctx);


#endif /* SRC_REDISGEARG_PYTHON_H_ */
