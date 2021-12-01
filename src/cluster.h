/*
 * cluster.h
 *
 *  Created on: Nov 4, 2018
 *      Author: meir
 */

#pragma once

#define MAX_SLOT 16384
#define RG_INNER_MSG_COMMAND "RG.INNERMSGCOMMAND"
#define RG_CLUSTER_SET_FROM_SHARD_COMMAND "RG.CLUSTERSETFROMSHARD"

#define CLUSTER_ERROR "ERRCLUSTER"

void Cluster_SendMsg(const char* id, char* function, char* msg, size_t len);
#define Cluster_SendMsgM(id, function, msg, len) Cluster_SendMsg(id, #function, msg, len);
void Cluster_RegisterMsgReceiver(char* function, RedisModuleClusterMessageReceiver receiver);
#define Cluster_RegisterMsgReceiverM(function) Cluster_RegisterMsgReceiver(#function, function);
bool Cluster_IsClusterMode();
size_t Cluster_GetSize();
void Cluster_Init();
char* Cluster_GetMyId();
const char* Cluster_GetMyHashTag();
bool Cluster_IsMyId(const char* id);
bool Cluster_IsInitialized();
const char* Cluster_GetNodeIdByKey(const char* key);
int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_RedisGearsHello(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Cluster_ClusterSetFromShard(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

