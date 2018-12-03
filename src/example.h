/*
 * example.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXAMPLE_H_
#define SRC_EXAMPLE_H_

#include "redismodule.h"
#include "redistar.h"

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
bool Example_Filter(RedisModuleCtx* rctx, Record *r, void* arg, char** err);


#endif /* SRC_EXAMPLE_H_ */
