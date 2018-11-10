/*
 * commands.h
 *
 *  Created on: 10 Nov 2018
 *      Author: root
 */

#ifndef SRC_COMMANDS_H_
#define SRC_COMMANDS_H_


#include "redistar.h"

int Command_DropExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Command_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Command_GetResults(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Command_GetResultsBlocking(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SRC_COMMANDS_H_ */
