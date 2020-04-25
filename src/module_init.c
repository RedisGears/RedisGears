/*
 * module_init.c
 *
 *  Created on: 6 Dec 2018
 *      Author: root
 */
#include "redismodule.h"
#include "version.h"
#include <limits.h>
#include <stdlib.h>
#define __USE_GNU
#include <dlfcn.h>

int RedisGears_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void test(){

}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    Dl_info info;
    dladdr(test, &info);
    char resolved_path[PATH_MAX];
    realpath(info.dli_fname, resolved_path);

    void* handler = dlopen(resolved_path, RTLD_NOW|RTLD_GLOBAL);
    if(!handler){
        printf("failed loading symbols: %s\r\n", dlerror());
    }

    if (RedisModule_Init(ctx, REDISGEARS_MODULE_NAME, REDISGEARS_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return RedisGears_OnLoad(ctx, argv, argc);
}

