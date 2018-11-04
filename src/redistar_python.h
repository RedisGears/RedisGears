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

void RediStarPy_PyObjectSerialize(void* arg, BufferWriter* bw);
void* RediStarPy_PyObjectDeserialize(BufferReader* br);
int RediStarPy_Init(RedisModuleCtx *ctx);


#endif /* SRC_REDISTAR_PYTHON_H_ */
