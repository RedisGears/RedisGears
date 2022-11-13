#include "redisai.h"

// RedisModule_Init is defined as a static function and so won't be exported as
// a symbol. Export a version under a slightly different name so that we can
// get access to it from Rust.

int Export_RedisAI_Init(RedisModuleCtx *ctx) {
    return RedisAI_Initialize(ctx);
}
