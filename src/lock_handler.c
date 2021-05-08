/*
 * lock_handler.c
 *
 *  Created on: 7 Feb 2019
 *      Author: root
 */

#include "lock_handler.h"
#include "redisgears_memory.h"
#include "pthread.h"
#include "utils/arr_rm_alloc.h"
#include <assert.h>

static pthread_key_t _lockKey;

typedef struct LockHandlerCtx{
    int lockCounter;
}LockHandlerCtx;

typedef struct LockStateHandler{
    SaveState save;
    RestoreState restore;
}LockStateHandler;

LockStateHandler* statesHandlers;

static void LockHandler_Destructor(void *p) {
    RG_FREE(p);
}

int LockHandler_Initialize(){
    int err = pthread_key_create(&_lockKey, LockHandler_Destructor);
    if(err){
        return REDISMODULE_ERR;
    }
    LockHandlerCtx* lh = RG_ALLOC(sizeof(*lh));
    lh->lockCounter = 1; // init is called from the main thread, the lock is always acquired
    pthread_setspecific(_lockKey, lh);

    statesHandlers = array_new(LockStateHandler, 10);
    return REDISMODULE_OK;
}

bool LockHandler_IsRedisGearsThread(){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    return lh ? true : false;
}

void LockHandler_Register(){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    if(!lh){
        lh = RG_ALLOC(sizeof(*lh));
        lh->lockCounter = 0;
        pthread_setspecific(_lockKey, lh);
    }
}

void LockHandler_Acquire(RedisModuleCtx* ctx){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    if(!lh){
        lh = RG_ALLOC(sizeof(*lh));
        lh->lockCounter = 0;
        pthread_setspecific(_lockKey, lh);
    }
    if(lh->lockCounter == 0){
        // to avoid deadlocks, when we try to acquire the redis GIL we first check
        // if we hold the python GIL, if we do we first release it, then acquire the redis GIL
        // and then re-acquire the python GIL.
        void* states[array_len(statesHandlers)];
        for(size_t i = 0 ; i < array_len(statesHandlers) ; ++i){
            states[i] = statesHandlers[i].save();
        }
        RedisModule_ThreadSafeContextLock(ctx);
        for(size_t i = 0 ; i < array_len(statesHandlers) ; ++i){
            statesHandlers[i].restore(states[i]);
        }
    }
    ++lh->lockCounter;
}

void LockHandler_Release(RedisModuleCtx* ctx){
    LockHandlerCtx* lh = pthread_getspecific(_lockKey);
    RedisModule_Assert(lh);
    RedisModule_Assert(lh->lockCounter > 0);
    if(--lh->lockCounter == 0){
        RedisModule_ThreadSafeContextUnlock(ctx);
    }
}

void LockHandler_AddStateHanlder(SaveState save, RestoreState restore){
    LockStateHandler lsh = (LockStateHandler){
        .save = save, .restore = restore,
    };
    statesHandlers = array_append(statesHandlers, lsh);
}



