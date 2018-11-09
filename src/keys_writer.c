#include "redistar.h"
#include <string.h>
#include <assert.h>
#include "redistar_memory.h"
#include "record.h"
#include "mgmt.h"
#include "redismodule.h"

typedef struct ResponseWriterCtx{
    size_t replySize;
}ResponseWriterCtx;

static void KeysWriter_Free(void* ctx){
    ResponseWriterCtx *writerCtx = ctx;
    RS_FREE(writerCtx);
}

static ArgType KeysWriterType = (ArgType){
    .type = "KeysWriterType",
    .free = KeysWriter_Free,
    .serialize = NULL,
    .deserialize = NULL,
};

static void KeysWriter_Start(RedisModuleCtx* rctx, void* ctx){

}

static void KeysWriter_Write(RedisModuleCtx* rctx, void* ctx, Record* record){

}

static void KeysWriter_Done(RedisModuleCtx* rctx, void* ctx){

}

ArgType* GetKeysWriterArgType(){
    return &KeysWriterType;
}

Writer* ReplyWriter(void* arg){
    ResponseWriterCtx* ctx = RS_ALLOC(sizeof(*ctx));
    *ctx = (ResponseWriterCtx){
        .replySize = 0,
    };
    Writer* ret = RS_ALLOC(sizeof(*ret));
    *ret = (Writer){
        .ctx = ctx,
        .Start = KeysWriter_Start,
        .Write = KeysWriter_Write,
        .Done = KeysWriter_Done,
    };
    return ret;
}
