#include "redismodule.h"
#include "configuration_store.h"
#include "utils/dict.h"
#include "version.h"
#include "redisgears_memory.h"
#include "lock_handler.h"


#define RG_INNER_SET_CONFIGURATION_STORE_COMMAND "RG.INNERSETCONFIGURATIONSTORE"

typedef struct ConfigurationCtx{
    size_t refCount;
    char* val;
    size_t size;
}ConfigurationCtx;

static Gears_dict* configuration;

int ConfigurationStore_ConfigurationStoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 3){
        return RedisModule_WrongArity(ctx);
    }

    const char* key = RedisModule_StringPtrLen(argv[1], NULL);
    size_t dataLen;
    const char* data = RedisModule_StringPtrLen(argv[1], &dataLen);

    ConfigurationCtx* cStor = ConfigurationStore_CtxCreate(data, dataLen);

    ConfigurationStore_Set(key, cStor);

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

int ConfigurationStore_Initialize(RedisModuleCtx* ctx){
    configuration = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    if (RedisModule_CreateCommand(ctx, RG_INNER_SET_CONFIGURATION_STORE_COMMAND, ConfigurationStore_ConfigurationStoreCommand, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command "RG_INNER_SET_CONFIGURATION_STORE_COMMAND);
        return REDISMODULE_ERR;
    }


    return REDISMODULE_OK;
}

void ConfigurationStore_CtxFree(ConfigurationCtx* ctx){
    if(__atomic_sub_fetch(&ctx->refCount, 1, __ATOMIC_SEQ_CST) > 0){
        return;
    }
    RG_FREE(ctx->val);
    RG_FREE(ctx);
}

void ConfigurationStore_Set(const char* key, ConfigurationCtx* ctx){
    LockHandler_Acquire(staticCtx);
    if(!ctx){
        // set with NULL value means delete
        ConfigurationCtx* old = Gears_dictFetchValue(configuration, key);
        if(old){
            ConfigurationStore_CtxFree(old);
            Gears_dictDelete(configuration, key);
            LockHandler_Release(staticCtx);
        }
        return;
    }
    __atomic_add_fetch(&ctx->refCount, 1, __ATOMIC_SEQ_CST);
    Gears_dictEntry *existing;
    Gears_dictEntry *entry = Gears_dictAddRaw(configuration, (char*)key, &existing);
    if(!entry){
        entry = existing;
        ConfigurationCtx* old = Gears_dictGetVal(entry);
        ConfigurationStore_CtxFree(old);
    }
    Gears_dictSetVal(configuration, entry, ctx);

    // replicating to slave and aof
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_SelectDb(rctx, 0);
    RedisModule_Replicate(rctx, RG_INNER_SET_CONFIGURATION_STORE_COMMAND, "cb", key, ctx->val, ctx->size);
    RedisModule_FreeThreadSafeContext(rctx);

    LockHandler_Release(staticCtx);
}

ConfigurationCtx* ConfigurationStore_Get(const char* key){
    LockHandler_Acquire(staticCtx);
    ConfigurationCtx* res = Gears_dictFetchValue(configuration, key);
    if(res){
        __atomic_add_fetch(&res->refCount, 1, __ATOMIC_SEQ_CST);
    }
    LockHandler_Release(staticCtx);
    return res;
}

ConfigurationCtx* ConfigurationStore_CtxCreate(const char* val, size_t size){
    ConfigurationCtx* ret = RG_ALLOC(sizeof(*ret));
    ret->refCount = 1;
    ret->size = size;
    ret->val = RG_ALLOC(ret->size);
    memcpy(ret->val, val, size);
    return ret;
}

const char* ConfigurationStore_CtxGetVal(ConfigurationCtx* ctx, size_t* size){
    if(size){
        *size = ctx->size;
    }
    return ctx->val;
}

void ConfigurationStore_Save(RedisModuleIO *rdb, int when){
    if(when == REDISMODULE_AUX_BEFORE_RDB){
        return;
    }

    RedisModule_SaveUnsigned(rdb, Gears_dictSize(configuration));

    Gears_dictIterator *iter = Gears_dictGetIterator(configuration);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        const char* key = Gears_dictGetVal(entry);
        ConfigurationCtx* val = Gears_dictGetVal(entry);
        RedisModule_SaveStringBuffer(rdb, key, strlen(key) + 1); // +1 is not the \0
        RedisModule_SaveStringBuffer(rdb, val->val, val->size); // +1 is not the \0
    }

    Gears_dictReleaseIterator(iter);
}

int ConfigurationStore_Load(RedisModuleIO *rdb, int encver, int when){
    if(encver < VERSION_WITH_CONFIGURATION_STORE){
        return REDISMODULE_OK;
    }

    if(when == REDISMODULE_AUX_BEFORE_RDB){
        ConfigurationStore_Clear();
        return REDISMODULE_OK;
    }

    size_t len = RedisModule_LoadUnsigned(rdb);
    if(RedisModule_IsIOError(rdb)){
        return REDISMODULE_ERR;
    }

    for(size_t i = 0 ; i < len ; ++i){
        size_t keyLen;
        char * key = RedisModule_LoadStringBuffer(rdb, &keyLen);
        if(RedisModule_IsIOError(rdb)){
            goto error;
        }

        size_t valLen;
        char * val = RedisModule_LoadStringBuffer(rdb, &valLen);
        if(RedisModule_IsIOError(rdb)){
            RedisModule_Free(key);
            goto error;
        }

        ConfigurationCtx* ctx = ConfigurationStore_CtxCreate(val, valLen);
        ConfigurationStore_Set(key, ctx);

        RedisModule_Free(key);
        RedisModule_Free(val);
        ConfigurationStore_CtxFree(ctx);
    }

    return REDISMODULE_OK;

error:
    ConfigurationStore_Clear();
    return REDISMODULE_ERR;
}

void ConfigurationStore_Clear(){
    Gears_dictIterator *iter = Gears_dictGetIterator(configuration);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        ConfigurationCtx* val = Gears_dictGetVal(entry);
        ConfigurationStore_CtxFree(val);
    }

    Gears_dictReleaseIterator(iter);

    Gears_dictEmpty(configuration, NULL);
}

