#include "distributed_registrations_dict.h"
#include "redismodule.h"

#include "utils/dict.h"
#include "distributed_data_type.h"
#include "utils/buffer.h"
#include "execution_plan.h"
#include "lock_handler.h"
#include "mgmt.h"
#include "redisgears_memory.h"

#define DISTRIBUTED_REGISTRATIONS_DICT_NAME "DistributedRegistrationsDict"

typedef struct DistributedRegistrationsDict{
    Gears_dict* registrations;
    DistributedDataType* ddt;
}DistributedRegistrationsDict;

typedef enum ExtraDataType{
    ADD, REMOVE,
}ExtraDataType;

typedef void (*OnOperationDoneCallback)(void* pd);

typedef struct ExtraData{
    union{
        struct{
            FlatExecutionPlan* fep;
            char* key;
        }add;
        struct{
            char* id;
        }remove;
    };
    ExtraDataType type;
    RedisModuleBlockedClient *bc;
}ExtraData;

DistributedRegistrationsDict drd;


static void DistributedRegistrationsDict_FreeExtraData(ExtraData* ed){
    switch(ed->type){
    case REMOVE:
        RG_FREE(ed->remove.id);
        break;
    case ADD:
        break;
    default:
        assert(false);
    }
    RG_FREE(ed);
}

static void DistributedRegistrationsDict_Serialize(void* pd, Gears_BufferWriter* bw){

}

static void DistributedRegistrationsDict_Deserialize(void* pd, Gears_BufferReader* br){

}

static void DistributedRegistrationsDict_Free(void* pd){

}

static void DistributedRegistrationsDict_RemoveRegistration(void* pd, const char* msg, size_t len, void* extraData){
    ExtraData* ed = extraData;
    if(!ed){
        ed = RG_CALLOC(1, sizeof(*ed));
        Gears_Buffer buff;
        buff.buff = (char*)msg;
        buff.size = len;
        buff.cap = len;
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, &buff);
        ed->remove.id = RG_STRDUP(Gears_BufferReaderReadString(&br));
        ed->type = REMOVE;
    }

    RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(ed->bc);
    LockHandler_Acquire(ctx);

    char realId[EXECUTION_PLAN_ID_LEN] = {0};
    if(!FlatExecutionPlan_StrIdToId(ed->remove.id, realId)){
        if(ed->bc){
            RedisModule_ReplyWithError(ctx, "could not parse the given Id");
        }
        goto end;
    }

    Gears_dict* registrations = pd;
    FlatExecutionPlan* fep = Gears_dictFetchValue(registrations, realId);

    if(!fep){
        if(ed->bc){
            RedisModule_ReplyWithError(ctx, "no such registration exists");
        }
        goto end;
    }

    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(fep->reader->reader);
    assert(callbacks);
    if(!callbacks->unregisterTrigger){
        if(ed->bc){
            RedisModule_ReplyWithError(ctx, "redear do not support unregister");
        }
        goto end;
    }

    callbacks->unregisterTrigger(fep);

    int res = Gears_dictDelete(registrations, fep->id);
    assert(res == DICT_OK);

    FlatExecutionPlan_Free(fep);

    if(ed->bc){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

end:
    if(ed->bc){
        RedisModule_UnblockClient(ed->bc, NULL);
    }
    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    DistributedRegistrationsDict_FreeExtraData(ed);
}

static void DistributedRegistrationsDict_AddRegistration(void* pd, const char* msg, size_t len, void* extraData){
    ExtraData* ed = extraData;
    if(!ed){
        ed = RG_CALLOC(1, sizeof(*ed));
        Gears_Buffer buff;
        buff.buff = (char*)msg;
        buff.size = len;
        buff.cap = len;
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, &buff);
        ed->add.fep = FlatExecutionPlan_Deserialize(&br);
        ed->add.key = RG_STRDUP(Gears_BufferReaderReadString(&br));
        ed->type = ADD;
    }

    Gears_dict* registrations = pd;

    RedisModuleCtx * ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);

    RedisGears_ReaderCallbacks* callbacks = ReadersMgmt_Get(ed->add.fep->reader->reader);
    assert(callbacks);
    assert(callbacks->registerTrigger);
    callbacks->registerTrigger(ed->add.fep, ed->add.key);

    // the registeredFepDict holds a weak pointer to the fep struct. If does not increase
    // the refcount and will be remove when the fep will be unregistered
    Gears_dictAdd(registrations, ed->add.fep->id, ed->add.fep);

    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    DistributedRegistrationsDict_FreeExtraData(ed);
}

static void DistributedRegistrationsDict_AddRegistrationAppliedOnCluster(void* pd, void* extraData){

}

static void DistributedRegistrationsDict_RemoveRegistrationAppliedOnCluster(void* pd, void* extraData){

}

void DistributedRegistrationsDict_Add(FlatExecutionPlan* fep, char* key){
    ExtraData* ed = RG_ALLOC(sizeof(*ed));
    ed->type = ADD;
    ed->add.fep = fep;
    ed->add.key = key;
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    FlatExecutionPlan_Serialize(fep, &bw);
    Gears_BufferWriterWriteString(&bw, key);

    DistributedDataType_Apply(drd.ddt, DistributedRegistrationsDict_AddRegistration,
                              buff->buff, buff->size, ed);

    Gears_BufferFree(buff);
}

void DistributedRegistrationsDict_Remove(const char* id, RedisModuleBlockedClient *bc){
    ExtraData* ed = RG_ALLOC(sizeof(*ed));
    ed->type = REMOVE;
    ed->remove.id= RG_STRDUP(id);
    ed->bc = bc;
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    Gears_BufferWriterWriteString(&bw, id);

    DistributedDataType_Apply(drd.ddt, DistributedRegistrationsDict_RemoveRegistration,
                              buff->buff, buff->size, ed);

    Gears_BufferFree(buff);
}

void DistributedRegistrationsDict_Dump(RedisModuleCtx *ctx){
    LockHandler_Acquire(ctx);

    Gears_dictIterator* iter = Gears_dictGetIterator(drd.registrations);
    Gears_dictEntry *curr = NULL;
    size_t numElements = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    while((curr = Gears_dictNext(iter))){
        FlatExecutionPlan* fep = Gears_dictGetVal(curr);
        RedisModule_ReplyWithArray(ctx, 6);
        RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
        RedisModule_ReplyWithStringBuffer(ctx, fep->idStr, strlen(fep->idStr));
        RedisModule_ReplyWithStringBuffer(ctx, "reader", strlen("reader"));
        RedisModule_ReplyWithStringBuffer(ctx, fep->reader->reader, strlen(fep->reader->reader));
        RedisModule_ReplyWithStringBuffer(ctx, "desc", strlen("desc"));
        if(fep->desc){
            RedisModule_ReplyWithStringBuffer(ctx, fep->desc, strlen(fep->desc));
        }else{
            RedisModule_ReplyWithNull(ctx);
        }
        ++numElements;
    }
    Gears_dictReleaseIterator(iter);

    RedisModule_ReplySetArrayLength(ctx, numElements);

    LockHandler_Release(ctx);
}

int DistributedRegistrationsDict_Init(){
    drd.registrations = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    drd.ddt = DistributedDataType_Create(DISTRIBUTED_REGISTRATIONS_DICT_NAME,
                                         drd.registrations,
                                         DistributedRegistrationsDict_Serialize,
                                         DistributedRegistrationsDict_Deserialize,
                                         DistributedRegistrationsDict_Free);

    DistributedDataType_Register(drd.ddt, DistributedRegistrationsDict_AddRegistration,
                                 DistributedRegistrationsDict_AddRegistrationAppliedOnCluster);

    DistributedDataType_Register(drd.ddt, DistributedRegistrationsDict_RemoveRegistration,
                                 DistributedRegistrationsDict_RemoveRegistrationAppliedOnCluster);

    return REDISMODULE_OK;
}



