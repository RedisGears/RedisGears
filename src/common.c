/*
 * common.c
 *
 *  Created on: Jan 29, 2020
 *      Author: root
 */

#include "cluster.h"
#include "redisgears_memory.h"
#include "redisgears.h"

#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <uuid/uuid.h>

RedisModuleCtx *staticCtx = NULL;

static char* shardUniqueId = NULL;

RedisVersion currVesion;

RedisVersion supportedVersion = {
        .redisMajorVersion = 6,
        .redisMinorVersion = 0,
        .redisPatchVersion = 0,
};

int gearsRlecMajorVersion;
int gearsRlecMinorVersion;
int gearsRlecPatchVersion;
int gearsRlecBuild;

bool gearsIsCrdt;

int GearsCompareVersions(RedisVersion v1, RedisVersion v2) {
    if (v1.redisMajorVersion < v2.redisMajorVersion) {
        return -1;
    } else if (v1.redisMajorVersion > v2.redisMajorVersion) {
        return 1;
    }

    if (v1.redisMinorVersion < v2.redisMinorVersion) {
        return -1;
    } else if (v1.redisMinorVersion > v2.redisMinorVersion) {
        return 1;
    }

    if (v1.redisPatchVersion < v2.redisPatchVersion) {
        return -1;
    } else if (v1.redisPatchVersion > v2.redisPatchVersion) {
        return 1;
    }

    return 0;
}

int GearsCheckSupportedVersion(){
    if (GearsCompareVersions(currVesion, supportedVersion) < 0) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

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
    }else{
        *lastID = MAX((long long)finalId[REDISMODULE_NODE_ID_LEN] + 1, *lastID);
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

char* IntArrToStr(int* arr, size_t len, char*(*toStr)(int), char sep) {
    char* res = array_new(char, 100);
    res = array_append(res, '[');
    if(len == 0){
        res = array_append(res, ']');
        res = array_append(res, '\0');
        char* ret = RG_STRDUP(res);
        array_free(res);
        return ret;
    }
    for(size_t i = 0 ; i < len ; ++i){
        char* elementStr = toStr(arr[i]);
        char* c = elementStr;
        while(*c){
            res = array_append(res, *c);
            ++c;
        }
        res = array_append(res, sep);
        RG_FREE(elementStr);
    }
    res[array_len(res) - 1] = ']';
    res = array_append(res, '\0');
    char* ret = RG_STRDUP(res);
    array_free(res);
    return ret;
}

char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*), char sep) {
    char* res = array_new(char, 100);
    res = array_append(res, '[');
    if(len == 0){
        res = array_append(res, ']');
        res = array_append(res, '\0');
        char* ret = RG_STRDUP(res);
        array_free(res);
        return ret;
    }
    for(size_t i = 0 ; i < len ; ++i){
        char* elementStr = toStr(arr[i]);
        char* c = elementStr;
        while(*c){
            res = array_append(res, *c);
            ++c;
        }
        res = array_append(res, sep);
        RG_FREE(elementStr);
    }
    res[array_len(res) - 1] = ']';
    res = array_append(res, '\0');
    char* ret = RG_STRDUP(res);
    array_free(res);
    return ret;
}

void GearsGetRedisVersion() {
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
    size_t len;
    const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

    int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &currVesion.redisMajorVersion,
                 &currVesion.redisMinorVersion, &currVesion.redisPatchVersion);

    RedisModule_Assert(n == 3);

    gearsRlecMajorVersion = -1;
    gearsRlecMinorVersion = -1;
    gearsRlecPatchVersion = -1;
    gearsRlecBuild = -1;
    char *enterpriseStr = strstr(replyStr, "rlec_version:");
    if (enterpriseStr) {
        n = sscanf(enterpriseStr, "rlec_version:%d.%d.%d-%d", &gearsRlecMajorVersion, &gearsRlecMinorVersion,
                   &gearsRlecPatchVersion, &gearsRlecBuild);
        if (n != 4) {
            RedisModule_Log(staticCtx, "warning", "Could not extract enterprise version");
        }
    }

    RedisModule_FreeCallReply(reply);

    gearsIsCrdt = true;
    reply = RedisModule_Call(ctx, "CRDT.CONFIG", "cc", "GET", "active-gc");
    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        gearsIsCrdt = false;
    }

    if(reply){
        RedisModule_FreeCallReply(reply);
    }

    RedisModule_FreeThreadSafeContext(ctx);
}

static const char *readRedisConfig(RedisModuleCtx *ctx, const char *item, RedisModuleCallReply **reply_p, size_t *len) {
    *reply_p = RedisModule_Call(ctx, "CONFIG", "cc", "GET", item);
    RedisModule_Assert(RedisModule_CallReplyType(*reply_p) == REDISMODULE_REPLY_ARRAY);
    RedisModuleCallReply *itemReply = RedisModule_CallReplyArrayElement(*reply_p, 1);
    RedisModule_Assert(RedisModule_CallReplyType(itemReply) == REDISMODULE_REPLY_STRING);
    
    const char *p = RedisModule_CallReplyStringPtr(itemReply, len);
    if (p && *p != '\0' && *p != '\n' && *p == '\r' && *p == ' ') {
        return p;
    }
    
    RedisModule_FreeCallReply(*reply_p);
    *reply_p = NULL;
    return NULL;    
}

const char* GetShardUniqueId() {
    if(!shardUniqueId){
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);

#if 0
        RedisModuleCallReply *reply;
        const char *filename;
        size_t len;
        char uuid[64];
        bool filename_found = true;
        
        filename = readRedisConfig(ctx, "logfile", &reply, &len);
        if (!filename) {
            RedisModule_Log(ctx, "notice", "log file is not available");

            filename = readRedisConfig(ctx, "dbfilename", &reply, &len);
            if (!filename) {
                RedisModule_Log(ctx, "notice", "db file is not available");
                
                filename_found = false;

                uuid_t binuuid;
                uuid_generate_random(binuuid);
                uuid_unparse_lower(binuuid, uuid);
                filename = uuid;
                len = strlen(uuid);
            }
        }
        if (filename_found) {
            RedisModule_Log(ctx, "notice", "Shard unique filename: %s", filename);
            const char* last = strrchr(filename, '/');
            if(last){
                len = len - (last - filename + 1);
                filename = last + 1;
            }
        }
    
        shardUniqueId = RG_ALLOC(len + 1);
        shardUniqueId[len] = '\0';
        memcpy(shardUniqueId, filename, len);
        RedisModule_Log(ctx, "notice", "shardUniqueId=%s", shardUniqueId);
        if (reply) {
            RedisModule_FreeCallReply(reply);
        }
#else
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "logfile");
        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
        RedisModuleCallReply *uuidReply = RedisModule_CallReplyArrayElement(reply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(uuidReply) == REDISMODULE_REPLY_STRING);
        size_t len;
        const char* logFileName = RedisModule_CallReplyStringPtr(uuidReply, &len);
        RedisModule_Log(staticCtx, "notice", "log file is %s", logFileName);
        const char* last = strrchr(logFileName, '/');
        if(last){
            len = len - (last - logFileName + 1);
            logFileName = last + 1;
        }
        shardUniqueId = RG_ALLOC(len + 1);
        shardUniqueId[len] = '\0';
        memcpy(shardUniqueId, logFileName, len);
        RedisModule_FreeCallReply(reply);
#endif
        RedisModule_FreeThreadSafeContext(ctx);
    }
    return shardUniqueId;
}

int ExecCommandVList(RedisModuleCtx *ctx, const char* logLevel, const char* __fmt, va_list __arg) {
    va_list ap;
    va_copy(ap, __arg);
    char* command;
    char* command_to_execute;

    rg_vasprintf(&command, __fmt, ap);
    rg_asprintf(&command_to_execute, "%s 2>&1", command);
    RedisModule_Log(staticCtx, logLevel, "Executing : %s", command_to_execute);
    FILE* f = popen(command_to_execute, "r");
    if (f == NULL) {
        RG_FREE(command);
        RG_FREE(command_to_execute);
        RedisModule_Log(staticCtx, "warning", "Failed to run command : %s", command_to_execute);
        exit(1);
    }

    /* Read the output a line at a time - output it. */
    char path[1035];
    while (fgets(path, sizeof(path), f) != NULL) {
        RedisModule_Log(staticCtx, logLevel, "%s", path);
    }

    /* close */
    /**
     * The returnvalue of the child process is
     * in the top 16 8 bits. You have to divide
     * the returned value of pclose by 256,
     * then you get the searched return value of the child process.
     */
    int exitCode = pclose(f)/256;

    if(exitCode != 0){
        RedisModule_Log(staticCtx, "warning", "Execution failed command : %s", command_to_execute);
    }

    RG_FREE(command);
    RG_FREE(command_to_execute);

    va_end(ap);

    return exitCode;
}

int ExecCommand(RedisModuleCtx *ctx, const char* __fmt, ...) {
    char* command;
    va_list ap;
    va_start(ap, __fmt);

    int exitCode = ExecCommandVList(ctx, "notice", __fmt, ap);

    va_end(ap);

    return exitCode;
}

int IsKeyMatch(const char* prefix, const char* key, size_t prefixLen){
    size_t len = prefixLen;
    const char* data = prefix;
    int isPrefix = prefix[len - 1] == '*';
    if(isPrefix){
        --len;
    }
    if(isPrefix){
        return strncmp(data, key, len) == 0;
    }else{
        return strcmp(data, key) == 0;
    }
}
