/*
 * example.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXAMPLE_H_
#define SRC_EXAMPLE_H_

#include "redismodule.h"

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);



#endif /* SRC_EXAMPLE_H_ */
