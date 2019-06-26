/*
 * cluster.h
 *
 *  Created on: Nov 4, 2018
 *      Author: meir
 */

#ifndef SRC_CLUSTER_H_
#define SRC_CLUSTER_H_

#include "redismodule.h"
#include <stdbool.h>

#define RG_INNER_MSG_COMMAND "rg.innermsgcommand"

void Cluster_SendMsg(const char* id, char* function, char* msg, size_t len);
void Cluster_SendMsgToAllAndMyself(char* function, char* msg, size_t len);
void Cluster_SendMsgToMySelf(const char* function, char* msg, size_t len);
void Cluster_SendMsgToMySelfWithDelay(const char* function, char* msg, size_t len, long long delay);
#define Cluster_SendMsgM(id, function, msg, len) Cluster_SendMsg(id, #function, msg, len);
#define Cluster_SendMsgToAllAndMyselfM(function, msg, len) Cluster_SendMsgToAllAndMyself(#function, msg, len);
#define Cluster_SendMsgToMySelfM(function, msg, len) Cluster_SendMsgToMySelf(#function, msg, len);
#define Cluster_SendMsgToMySelfWithDelatM(function, msg, len, delay) Cluster_SendMsgToMySelfWithDelay(#function, msg, len, delay);
void Cluster_RegisterMsgReceiver(char* function, RedisModuleClusterMessageReceiver receiver);
#define Cluster_RegisterMsgReceiverM(function) Cluster_RegisterMsgReceiver(#function, function);
bool Cluster_IsClusterMode();
size_t Cluster_GetSize();
void Cluster_Init();
char* Cluster_GetMyId();
bool Cluster_IsMyId(const char* id);
char* Cluster_GetNodeIdByKey(char* key);
int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_RedisGearsHello(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


#endif /* SRC_CLUSTER_H_ */
