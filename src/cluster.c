#include "cluster.h"
#include "redistar_memory.h"
#include <assert.h>

#define MAX_SLOT 16383

typedef struct Cluster{
    char* myId;
    bool isClusterMode;
    size_t idsLen;
    char** ids;
    char* slots[MAX_SLOT];
}Cluster;

Cluster* cluster;

static void Cluster_Free(RedisModuleCtx* ctx){
    RS_FREE(cluster->myId);
    RedisModule_FreeClusterNodesList(cluster->ids);
    for(size_t i = 0 ; i < MAX_SLOT ; ++i){
        RS_FREE(cluster->slots[i]);
    }
    RS_FREE(cluster);
    cluster = NULL;
}

bool Cluster_IsClusterMode(){
    return cluster && cluster->isClusterMode && cluster->idsLen > 1;
}

size_t Cluster_GetSize(){
    return cluster->idsLen;
}

void Cluster_Refresh(){
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    if(cluster){
        Cluster_Free(ctx);
    }

    cluster = RS_ALLOC(sizeof(*cluster));

    if(!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        cluster->isClusterMode = false;
        return;
    }

    cluster->isClusterMode = true;

    cluster->myId = RS_ALLOC(REDISMODULE_NODE_ID_LEN);
    memcpy(cluster->myId, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
    cluster->ids = RedisModule_GetClusterNodesList(ctx, &cluster->idsLen);

    RedisModuleCallReply *allSlotsRelpy = RedisModule_Call(ctx, "cluster", "c", "slots");
    assert(RedisModule_CallReplyType(allSlotsRelpy) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(allSlotsRelpy) ; ++i){
        RedisModuleCallReply *slotRangeRelpy = RedisModule_CallReplyArrayElement(allSlotsRelpy, i);

        RedisModuleCallReply *minslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 0);
        assert(RedisModule_CallReplyType(minslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long minslot = RedisModule_CallReplyInteger(minslotRelpy);

        RedisModuleCallReply *maxslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 1);
        assert(RedisModule_CallReplyType(maxslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long maxslot = RedisModule_CallReplyInteger(maxslotRelpy);

        RedisModuleCallReply *nodeDetailsRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 2);
        assert(RedisModule_CallReplyType(nodeDetailsRelpy) == REDISMODULE_REPLY_ARRAY);
        assert(RedisModule_CallReplyLength(nodeDetailsRelpy) == 3);
        RedisModuleCallReply *nodeidReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 2);
        size_t idLen;
        const char* id = RedisModule_CallReplyStringPtr(nodeidReply,&idLen);
        char nodeId[REDISMODULE_NODE_ID_LEN + 1];
        memcpy(nodeId, id, REDISMODULE_NODE_ID_LEN);
        nodeId[REDISMODULE_NODE_ID_LEN] = '\0';
        for(int i = minslot ; i <= maxslot ; ++i){
            cluster->slots[i] = RS_STRDUP(nodeId);
        }
    }
    RedisModule_FreeCallReply(allSlotsRelpy);
    RedisModule_FreeThreadSafeContext(ctx);
}

char* Cluster_GetMyId(){
    return cluster->myId;
}

char** Cluster_GetNodesList(size_t* len){
    *len = cluster->idsLen;
    return cluster->ids;
}

unsigned int keyHashSlot(char *key, int keylen);

char* Cluster_GetNodeIdByKey(char* key){
    unsigned int slot = keyHashSlot(key, strlen(key));
    return cluster->slots[slot];
}
