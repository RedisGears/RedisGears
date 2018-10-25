#include "example.h"
#include "redismodule.h"
#include <assert.h>
#include "redistar.h"
#include <string.h>

int Example_CommandCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    KeysReaderCtx* readerCtx = RediStar_KeysReaderCtxCreate("*");
    RediStarCtx* rsctx = RSM_Load(KeysReader, ctx, readerCtx);
    RSM_Filter(rsctx, TypeFilter, (void*)REDISMODULE_KEYTYPE_STRING);
    RSM_Map(rsctx, ValueToRecordMapper, ctx);
    RSM_GroupBy(rsctx, KeyRecordStrValueExtractor, NULL, CountReducer, NULL);
    RSM_Write(rsctx, ReplyWriter, ctx);
    return REDISMODULE_OK;
}
