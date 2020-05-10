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
#ifdef WITHPYTHON
#include "redisgears_python.h"
#endif

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
#ifdef WITHPYTHON
        // to avoid deadlocks, when we try to acquire the redis GIL we first check
        // if we hold the python GIL, if we do we first release it, then acquire the redis GIL
        // and then re-acquire the python GIL.
        PyThreadState *_save;
        bool pythonLockAcquired = RedisGearsPy_IsLockAcquired();
        if(pythonLockAcquired){
            _save = PyEval_SaveThread();
        }
#endif
        RedisModule_ThreadSafeContextLock(ctx);
#ifdef WITHPYTHON
        if(pythonLockAcquired){
            PyEval_RestoreThread(_save);
        }
#endif
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



