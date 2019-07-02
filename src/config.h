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
const char* GearsConfig_GetPythonHomeDir();
long long GearsConfig_GetMaxExecutions();
long long GearsConfig_GetProfileExecutions();
long long consensusIdleStartInterval();
int GearsConfig_GetConsensusIdleStartInterval();
int GearsConfig_GetConsensusIdleEndInterval();
long long GearsConfig_GetConsensusShortPeriodicTasksInterval();
long long GearsConfig_GetConsensusLongPeriodicTasksInterval();
long long GearsConfig_GetClusterReconnectInterval();
long long GearsConfig_GetPythonAttemptTraceback();

#endif /* SRC_CONFIG_H_ */
