#include "triggers.h"
#include "utils/adlist.h"
#include "execution_plan.h"
#include "redistar_memory.h"

#define ALL_KEY_REGISTRATION_INIT_SIZE 10
list* allKeysRegistration;


int Triggers_Init(){
    allKeysRegistration = listCreate();
    return 1;
}


static int Trigger_OnKeyArrive(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key){
    listIter *iter = listGetIterator(allKeysRegistration, AL_START_HEAD);
    listNode* node = NULL;
    while((node = listNext(iter))){
        FlatExecutionPlan* fep = listNodeValue(node);
        char* keyStr = RS_STRDUP(RedisModule_StringPtrLen(key, NULL));
        if(!RediStar_Run(fep, keyStr, NULL, NULL)){
            RedisModule_Log(ctx, "warning", "could not execution flat execution on trigger: %s", fep->name);
        }
    }
    return REDISMODULE_OK;
}

int Trigger_OnKeyArriveTrigger(RedisModuleCtx* rctx, FlatExecutionPlan* fep){
    if(RedisModule_SubscribeToKeyspaceEvents(rctx, REDISMODULE_NOTIFY_STRING, Trigger_OnKeyArrive) != REDISMODULE_OK){
        return 0;
    }
    listAddNodeHead(allKeysRegistration, fep);
    return 1;
}
