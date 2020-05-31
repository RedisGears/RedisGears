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
const char* GearsConfig_GetDependenciesUrl();
const char* GearsConfig_GetDependenciesSha256();
long long GearsConfig_CreateVenv();
long long GearsConfig_DownloadDeps();
long long GearsConfig_ForceDownloadDepsOnEnterprise();
long long GearsConfig_ExecutionThreads();
long long GearsConfig_ExecutionMaxIdleTime();
long long GearsConfig_SendMsgRetries();
long long GearsConfig_PythonInstallReqMaxIdleTime();
const char* GearsConfig_GetExtraConfigVals(const char* key);
const char* GearsConfig_GetPythonInstallationDir();
const char* GearsConfig_GetPluginsDirectory();

#endif /* SRC_CONFIG_H_ */
