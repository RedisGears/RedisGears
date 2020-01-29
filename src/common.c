/*
 * common.c
 *
 *  Created on: Jan 29, 2020
 *      Author: root
 */

#include "common.h"
#include "cluster.h"
#include "redisgears_memory.h"
#include "redismodule.h"
#include <string.h>

static uint64_t idHashFunction(const void *key){
    return Gears_dictGenHashFunction(key, ID_LEN);
}

static int idKeyCompare(void *privdata, const void *key1, const void *key2){
    return memcmp(key1, key2, ID_LEN) == 0;
}

static void idKeyDestructor(void *privdata, void *key){
    RG_FREE(key);
}

static void* idKeyDup(void *privdata, const void *key){
    char* ret = RG_ALLOC(ID_LEN);
    memcpy(ret, key , ID_LEN);
    return ret;
}

Gears_dictType dictTypeHeapIds = {
        .hashFunction = idHashFunction,
        .keyDup = idKeyDup,
        .valDup = NULL,
        .keyCompare = idKeyCompare,
        .keyDestructor = idKeyDestructor,
        .valDestructor = NULL,
};

Gears_dictType* dictTypeHeapIdsPtr = &dictTypeHeapIds;

void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID){
    char generatedId[ID_LEN] = {0};
    if(!finalId){
        char noneClusterId[REDISMODULE_NODE_ID_LEN] = {0};
        char* id;
        if(Cluster_IsClusterMode()){
            id = Cluster_GetMyId();
        }else{
            memset(noneClusterId, '0', REDISMODULE_NODE_ID_LEN);
            id = noneClusterId;
        }
        memcpy(generatedId, id, REDISMODULE_NODE_ID_LEN);
        memcpy(generatedId + REDISMODULE_NODE_ID_LEN, lastID, sizeof(long long));
        finalId = generatedId;
        ++(*lastID);
    }
    memcpy(idBuf, finalId, ID_LEN);
    snprintf(idStrBuf, STR_ID_LEN, "%.*s-%lld", REDISMODULE_NODE_ID_LEN, idBuf, *(long long*)&idBuf[REDISMODULE_NODE_ID_LEN]);
}
