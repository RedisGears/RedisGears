/*
 * example.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXAMPLE_H_
#define SRC_EXAMPLE_H_

#include "redisgears.h"
#include "redismodule.h"

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
bool Example_Filter(ExecutionCtx* rctx, Record *r, void* arg);


#endif /* SRC_EXAMPLE_H_ */
