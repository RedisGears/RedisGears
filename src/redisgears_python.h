/*
 * redisgears_python.h
 *
 *  Created on: Oct 20, 2018
 *      Author: meir
 */

#ifndef SRC_REDISGEARS_PYTHON_H_
#define SRC_REDISGEARS_PYTHON_H_

#include "redismodule.h"
#include <Python.h>
#include "redisgears.h"

extern PyMethodDef EmbMethods[];

void RedisGearsPy_PyObjectSerialize(void* arg, BufferWriter* bw);
void* RedisGearsPy_PyObjectDeserialize(BufferReader* br);
int RedisGearsPy_Init(RedisModuleCtx *ctx);


#endif /* SRC_REDISGEARS_PYTHON_H_ */
