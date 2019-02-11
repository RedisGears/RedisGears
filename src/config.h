/*
 * config.h
 *
 *  Created on: 7 Jan 2019
 *      Author: root
 */

#ifndef SRC_CONFIG_H_
#define SRC_CONFIG_H_

#include "redismodule.h"

int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
const char* GearsCOnfig_GetPythonHomeDir();
long long GearsCOnfig_GetExecutionThreadAmount();

#endif /* SRC_CONFIG_H_ */
