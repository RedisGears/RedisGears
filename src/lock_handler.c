/*
 * lock_handler.c
 *
 *  Created on: 7 Feb 2019
 *      Author: root
 */

#include "lock_handler.h"
#include "redisgears_memory.h"
#include "pthread.h"
#include <assert.h>

pthread_key_t _lockKey;

typedef struct LockHandlerCtx{
    int lockCounter;
}LockHandlerCtx;

int LockHandler_Initialize(){
    int err = pthread_key_create(&_lockKey, NULL);
    if(err){
        return REDISMODULE_ERR;
    }
    LockHandlerCtx* lh = RG_ALLOC(sizeof(*lh));
    lh->lockCounter = 1; // init is called from the main thread, the lock is always acquired
    pthread_setspecific(_lockKey, lh);
    return REDISMODULE_OK;
}

void LockHandler_Acquire(RedisModuleCtx* ctx){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    if(!lh){
        lh = RG_ALLOC(sizeof(*lh));
        lh->lockCounter = 0;
        pthread_setspecific(_lockKey, lh);
    }
    if(lh->lockCounter == 0){
        RedisModule_ThreadSafeContextLock(ctx);
    }
    ++lh->lockCounter;
}

void LockHandler_Realse(RedisModuleCtx* ctx){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    assert(lh);
    assert(lh->lockCounter > 0);
    if(--lh->lockCounter == 0){
        RedisModule_ThreadSafeContextUnlock(ctx);
    }
}



