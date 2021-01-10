/*
 * config.h
 *
 *  Created on: 7 Jan 2019
 *      Author: root
 */

#ifndef SRC_CONFIG_H_
#define SRC_CONFIG_H_

#include "redismodule.h"
#include "redisgears.h"

void GearsConfig_AddHooks(BeforeConfigSet before, AfterConfigSet after, GetConfig getConfig);
int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc);
long long GearsConfig_GetMaxExecutions();
long long GearsConfig_GetMaxExecutionsPerRegistration();
long long GearsConfig_GetProfileExecutions();
long long GearsConfig_ForceDownloadDepsOnEnterprise();
long long GearsConfig_ExecutionThreads();
long long GearsConfig_ExecutionMaxIdleTime();
long long GearsConfig_SendMsgRetries();
const char* GearsConfig_GetExtraConfigVals(const char* key);
char** GearsConfig_GetPlugins();

#endif /* SRC_CONFIG_H_ */
