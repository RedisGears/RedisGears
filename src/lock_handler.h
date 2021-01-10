/*
 * lock_handler.h
 *
 *  Created on: 7 Feb 2019
 *      Author: root
 */

#ifndef SRC_LOCK_HANDLER_H_
#define SRC_LOCK_HANDLER_H_

#include "redismodule.h"
#include "redisgears.h"

int LockHandler_Initialize();
bool LockHandler_IsRedisGearsThread();
void LockHandler_Register();
void LockHandler_Acquire(RedisModuleCtx* ctx);
void LockHandler_Release(RedisModuleCtx* ctx);

void LockHandler_AddStateHanlder(SaveState save, RestoreState restore);

#endif /* SRC_LOCK_HANDLER_H_ */
