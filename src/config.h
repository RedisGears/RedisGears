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
long long GearsConfig_GetMaxExecutions();
long long GearsConfig_GetMaxExecutionsPerRegistration();
long long GearsConfig_GetProfileExecutions();
long long GearsConfig_GetPythonAttemptTraceback();
long long GearsConfig_GetExecutionMaxIdleTime();
const char* GearsConfig_GetDependenciesUrl();
const char* GearsConfig_GetDependenciesSha256();
long long GearsConfig_CreateVenv();
const char* GearsConfig_GetExtraConfigVals(const char* key);

#endif /* SRC_CONFIG_H_ */
