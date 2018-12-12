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
Record* Example_Accumulate(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err);


#endif /* SRC_EXAMPLE_H_ */
