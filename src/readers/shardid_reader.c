#include "shardid_reader.h"
#include "cluster.h"

typedef struct ShardIDReaderCtx{
    bool isDone;
}ShardIDReaderCtx;

static Record* ShardIDReader_Next(ExecutionCtx* rctx, void* ctx){
    ShardIDReaderCtx* sidCtx = ctx;
    if(sidCtx->isDone){
        return NULL;
    }
    const char* myId = Cluster_IsClusterMode() ? Cluster_GetMyId() : "1";
    sidCtx->isDone = true;
    return RedisGears_StringRecordCreate(RG_STRDUP(myId), strlen(myId));
}

static void ShardIDReader_free(void* ctx){
    RG_FREE(ctx);
}

static void ShardIDReader_reset(void* ctx, void * arg){
    ShardIDReaderCtx* sidCtx = ctx;
    sidCtx->isDone = false;
}

static void ShardIDReader_serialize(void* ctx, Gears_BufferWriter* bw){

}

static void ShardIDReader_deserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){

}

static Reader* ShardIDReader_Create(void* arg){
    ShardIDReaderCtx* ctx = RG_ALLOC(sizeof(*ctx));
    ctx->isDone = false;
    Reader* r = RG_ALLOC(sizeof(*r));
    *r = (Reader){
        .ctx = ctx,
        .next = ShardIDReader_Next,
        .reset = ShardIDReader_reset,
        .free = ShardIDReader_free,
        .serialize = ShardIDReader_serialize,
        .deserialize = ShardIDReader_deserialize,
    };
    return r;
}

RedisGears_ReaderCallbacks ShardIDReader = {
        .create = ShardIDReader_Create,
        .registerTrigger = NULL,
        .unregisterTrigger = NULL,
        .serializeTriggerArgs = NULL,
        .deserializeTriggerArgs = NULL,
        .freeTriggerArgs = NULL,
        .dumpRegistratioData = NULL,
        .rdbSave = NULL,
        .rdbLoad = NULL,
        .clear = NULL,
};
