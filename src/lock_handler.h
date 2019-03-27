/*
 * lock_handler.h
 *
 *  Created on: 7 Feb 2019
 *      Author: root
 */

#ifndef SRC_LOCK_HANDLER_H_
#define SRC_LOCK_HANDLER_H_

#include "redismodule.h"

int LockHandler_Initialize();
void LockHandler_Acquire(RedisModuleCtx* ctx);
void LockHandler_Release(RedisModuleCtx* ctx);

#endif /* SRC_LOCK_HANDLER_H_ */
