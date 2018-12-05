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

#define RS_INNER_MSG_COMMAND "rs.innermsgcommand"

void Cluster_SendMsg(char* id, char* function, char* msg, size_t len);
#define Cluster_SendMsgM(id, function, msg, len) Cluster_SendMsg(id, #function, msg, len);
void Cluster_RegisterMsgReceiver(char* function, RedisModuleClusterMessageReceiver receiver);
#define Cluster_RegisterMsgReceiverM(function) Cluster_RegisterMsgReceiver(#function, function);
bool Cluster_IsClusterMode();
size_t Cluster_GetSize();
void Cluster_Init();
char* Cluster_GetMyId();
bool Cluster_IsMyId(char* id);
char* Cluster_GetNodeIdByKey(char* key);
int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


#endif /* SRC_CLUSTER_H_ */
