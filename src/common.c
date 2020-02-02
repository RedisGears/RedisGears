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
#include <stdarg.h>

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

int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg) {
  va_list args_copy;
  va_copy(args_copy, __arg);

  size_t needed = vsnprintf(NULL, 0, __fmt, __arg) + 1;
  *__ptr = RG_ALLOC(needed);

  int res = vsprintf(*__ptr, __fmt, args_copy);

  va_end(args_copy);

  return res;
}

int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...) {
  va_list ap;
  va_start(ap, __fmt);

  int res = rg_vasprintf(__ptr, __fmt, ap);

  va_end(ap);

  return res;
}
