#include "execution_plan.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include "record.h"
#include <assert.h>
#include <stdbool.h>
#include "redistar.h"
#include "redistar_memory.h"
#include "utils/dict.h"
#include "utils/adlist.h"
#include "utils/buffer.h"
#include <pthread.h>
#include <unistd.h>

#define NEW_EP_MSG_TYPE 1
#define NEW_RECORD_MSG_TYPE 2
#define DONE_SENDING_RECORDS_MSG_TYPE 3

// this is an hack so redis will not crash, we should try to
// avoid this as soon as possible.
static void* modulePointer;

typedef struct ExecutionPlansData{
    list* executionPlansToRun;
    dict* epDict;
    pthread_mutex_t mutex;
    pthread_t* workers;
}ExecutionPlansData;

ExecutionPlansData epData;

static long long lastId = 0;

static Record* ExecutionPlan_NextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err);
static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep);
static void FlatExecutionPlan_Free(FlatExecutionPlan* fep);
static void ExecutionPlan_Free(ExecutionPlan* ep, RedisModuleCtx *ctx);
static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader, void* readerArg);
static FlatExecutionWriter* FlatExecutionPlan_NewWriter(char* writer, void* writerArg);
static void ExecutionPlan_AddToRunList(ExecutionPlan* ep);

static void FlatExecutionPlan_SerializeReader(FlatExecutionReader* rfep, BufferWriter* bw){
    RediStar_BWWriteString(bw, rfep->reader);
    ArgType* type = ReadersMgmt_GetArgType(rfep->reader);
    if(type && type->serialize){
        // if we do not have a type or type do not have a serializer then we assume arg is NULL
        type->serialize(rfep->arg, bw);
    }
}

static void FlatExecutionPlan_SerializeWriter(FlatExecutionWriter* wfep, BufferWriter* bw){
    RediStar_BWWriteString(bw, wfep->writer);
    ArgType* type = WritersMgmt_GetArgType(wfep->writer);
    if(type && type->serialize){
        // if we do not have a type or type do not have a serializer then we assume arg is NULL
        type->serialize(wfep->arg, bw);
    }
}

static void FlatExecutionPlan_SerializeStep(FlatExecutionStep* step, BufferWriter* bw){
    RediStar_BWWriteLong(bw, step->type);
    RediStar_BWWriteString(bw, step->bStep.stepName);
    ArgType* type = NULL;
    switch(step->type){
    case MAP:
        type = MapsMgmt_GetArgType(step->bStep.stepName);
        break;
    case FILTER:
        type = FiltersMgmt_GetArgType(step->bStep.stepName);
        break;
    case EXTRACTKEY:
        type = ExtractorsMgmt_GetArgType(step->bStep.stepName);
        break;
    case REDUCE:
        type = ReducersMgmt_GetArgType(step->bStep.stepName);
        break;
    default:
        break;
    }
    if(type && type->serialize){
        type->serialize(step->bStep.arg, bw);
    }
}

static void FlatExecutionPlan_Serialize(FlatExecutionPlan* fep, BufferWriter* bw){
    RediStar_BWWriteBuffer(bw, fep->id, EXECUTION_PLAN_ID_LEN);
    FlatExecutionPlan_SerializeReader(fep->reader, bw);
    RediStar_BWWriteLong(bw, array_len(fep->steps));
    for(int i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* step = fep->steps + i;
        FlatExecutionPlan_SerializeStep(step, bw);
    }
    FlatExecutionPlan_SerializeWriter(fep->writer, bw);
}

static FlatExecutionWriter* FlatExecutionPlan_DeserializeWriter(BufferReader* br){
    char* writerName = RediStar_BRReadString(br);
    void* arg = NULL;
    ArgType* type = ReadersMgmt_GetArgType(writerName);
    if(type && type->deserialize){
        arg = type->deserialize(br);
    }
    FlatExecutionWriter* writer = FlatExecutionPlan_NewWriter(writerName, arg);
    return writer;
}

static FlatExecutionReader* FlatExecutionPlan_DeserializeReader(BufferReader* br){
    char* readerName = RediStar_BRReadString(br);
    void* arg = NULL;
    ArgType* type = ReadersMgmt_GetArgType(readerName);
    if(type && type->deserialize){
        arg = type->deserialize(br);
    }
    FlatExecutionReader* reader = FlatExecutionPlan_NewReader(readerName, arg);
    return reader;
}

static FlatExecutionStep FlatExecutionPlan_DeserializeStep(BufferReader* br){
    FlatExecutionStep step;
    step.type = RediStar_BRReadLong(br);
    step.bStep.stepName = RS_STRDUP(RediStar_BRReadString(br));
    step.bStep.arg = NULL;
    ArgType* type = NULL;
    switch(step.type){
    case MAP:
        type = MapsMgmt_GetArgType(step.bStep.stepName);
        break;
    case FILTER:
        type = FiltersMgmt_GetArgType(step.bStep.stepName);
        break;
    case EXTRACTKEY:
        type = ExtractorsMgmt_GetArgType(step.bStep.stepName);
        break;
    case REDUCE:
        type = ReducersMgmt_GetArgType(step.bStep.stepName);
        break;
    default:
        break;
    }
    if(type && type->deserialize){
        step.bStep.arg = type->deserialize(br);
    }
    return step;
}

static FlatExecutionPlan* FlatExecutionPlan_Deserialize(BufferReader* br){
    FlatExecutionPlan* ret = FlatExecutionPlan_New();
    size_t idLen;
    char* fepId = RediStar_BRReadBuffer(br, &idLen);
    assert(idLen == EXECUTION_PLAN_ID_LEN);
    memcpy(ret->id, fepId, EXECUTION_PLAN_ID_LEN);
    ret->reader = FlatExecutionPlan_DeserializeReader(br);
    long numberOfSteps = RediStar_BRReadLong(br);
    for(int i = 0 ; i < numberOfSteps ; ++i){
        ret->steps = array_append(ret->steps, FlatExecutionPlan_DeserializeStep(br));
    }
    ret->writer = FlatExecutionPlan_DeserializeWriter(br);
    return ret;
}

static void ExecutionPlan_Distribute(ExecutionPlan* ep, RedisModuleCtx *rctx){
    RedisModule_ThreadSafeContextLock(rctx);
    if(!(RedisModule_GetContextFlags(rctx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        RedisModule_ThreadSafeContextUnlock(rctx);
        return;
    }
    RedisModule_ThreadSafeContextUnlock(rctx);
    Buffer* buff = Buffer_Create();
    BufferWriter bw;
    BufferWriter_Init(&bw, buff);
    FlatExecutionPlan_Serialize(ep->fep, &bw);
    size_t numOfNodes;
    RedisModule_ThreadSafeContextLock(rctx);
    char **nodesList = RedisModule_GetClusterNodesList(rctx, &numOfNodes);
    for(size_t i = 0 ; i < numOfNodes ; ++i){
        char *node = nodesList[i];
        RedisModule_SendClusterMessage(rctx, node, NEW_EP_MSG_TYPE, buff->buff, buff->size);
    }
    RedisModule_FreeClusterNodesList(nodesList);
    RedisModule_ThreadSafeContextUnlock(rctx);
    Buffer_Free(buff);
}

static void ExecutionPlan_WriteResults(ExecutionPlan* ep, RedisModuleCtx* rctx){
    ep->writerStep.w->Start(rctx, ep->writerStep.w->ctx);
    for(size_t i = 0 ; i < array_len(ep->results) ; ++i){
        Record* r = ep->results[i];
        ep->writerStep.w->Write(rctx, ep->writerStep.w->ctx, r);
    }
    ep->writerStep.w->Done(rctx, ep->writerStep.w->ctx);
}

static void ExecutionPlan_SerializeResults(ExecutionPlan* ep, RedisModuleCtx* rctx){
    Buffer* buff = Buffer_Create();
    RedisModule_ThreadSafeContextLock(rctx);
    for(size_t i = 0 ; i < array_len(ep->results) ; ++i){
        BufferWriter bw;
        BufferWriter_Init(&bw, buff);
        Record* r = ep->results[i];
        RediStar_BWWriteBuffer(&bw, ep->fep->id, EXECUTION_PLAN_ID_LEN); // serialize execution plan id
        RS_SerializeRecord(&bw, r);
        RediStar_FreeRecord(r);
        RedisModule_SendClusterMessage(rctx, ep->fep->id, NEW_RECORD_MSG_TYPE, buff->buff, buff->size);
        Buffer_Clear(buff);
    }
    RedisModule_SendClusterMessage(rctx, ep->fep->id, DONE_SENDING_RECORDS_MSG_TYPE, ep->fep->id, EXECUTION_PLAN_ID_LEN);
    RedisModule_ThreadSafeContextUnlock(rctx);
    Buffer_Free(buff);
}

static bool ExecutionPlan_Execute(ExecutionPlan* ep, RedisModuleCtx* rctx){
    Record* record = NULL;
    char* err = NULL;

    if(ep->status == WAITING_FOR_CLUSTER_RESULTS){
        ExecutionPlan_WriteResults(ep, rctx);
        return true;
    }

    while((record = ExecutionPlan_NextRecord(ep->start, rctx, &err))){
        if(err){
            Record* r = RediStar_StringRecordCreate(err);
            RedisModule_ThreadSafeContextLock(rctx);
            ep->results = array_append(ep->results, r);
            RedisModule_ThreadSafeContextUnlock(rctx);
            break;
        }
        RedisModule_ThreadSafeContextLock(rctx);
        ep->results = array_append(ep->results, record);
        RedisModule_ThreadSafeContextUnlock(rctx);
    }
    if(err){
        Record* r = RediStar_StringRecordCreate(err);
        RedisModule_ThreadSafeContextLock(rctx);
        ep->results = array_append(ep->results, r);
        RedisModule_ThreadSafeContextUnlock(rctx);
    }

    if(!(RedisModule_GetContextFlags(rctx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        ExecutionPlan_WriteResults(ep, rctx);
        return true;
    }else{
        // we need to send the results to the initializer node unless we are the initializer
        // in this case we need to wait for all the results the arrived from the other nodes
        if(memcmp(ep->fep->id, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN) != 0){
            ExecutionPlan_SerializeResults(ep, rctx);
            return true;
        } else {
            // I am the initiator, lets sleep till we get all the responses.
            ep->status = WAITING_FOR_CLUSTER_RESULTS;
            return false;
        }
    }
}

static void* ExecutionPlan_ThreadMain(void *arg){
    while(true){
        pthread_mutex_lock(&epData.mutex);
        listNode *node = listFirst(epData.executionPlansToRun);
        if(!node){
            pthread_mutex_unlock(&epData.mutex);
            usleep(10000);
            continue;
        }
        ExecutionPlan* ep = node->value;
        listDelNode(epData.executionPlansToRun, node);
        pthread_mutex_unlock(&epData.mutex);
        RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(ep->bc);
        if(!ep->bc){
            ((void**)rctx)[1] = modulePointer;
        }
        if(ep->status == CREATED){
            if(RedisModule_GetMyClusterID()){
                if(memcmp(ep->fep->id, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN) == 0){
                   ExecutionPlan_Distribute(ep, rctx);
                }
            }
            ep->status = RUNNING;
        }
        bool isDone = ExecutionPlan_Execute(ep, rctx);
        if(isDone){
            if(ep->bc){
                RedisModule_UnblockClient(ep->bc, NULL);
            }
            ExecutionPlan_Free(ep, rctx);
        }
        RedisModule_FreeThreadSafeContext(rctx);
    }
}

static void ExecutionPlan_AddToRunList(ExecutionPlan* ep){
    pthread_mutex_lock(&epData.mutex);
    listAddNodeTail(epData.executionPlansToRun, ep);
    pthread_mutex_unlock(&epData.mutex);

}

static void ExecutionPlan_OnReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Buffer buff = (Buffer){
        .buff = (char*)payload,
        .size = len,
        .cap = len,
    };
    BufferReader br;
    BufferReader_Init(&br, &buff);
    FlatExecutionPlan* fep = FlatExecutionPlan_Deserialize(&br);
    FlatExecutionPlan_Run(fep, NULL);
}

static void ExecutionPlan_OnRecordReceived(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    Buffer buff;
    buff.buff = (char*)payload;
    buff.size = len;
    buff.cap = len;
    BufferReader br;
    BufferReader_Init(&br, &buff);
    size_t epIdLen;
    char* epId = RediStar_BRReadBuffer(&br, &epIdLen);
    assert(epIdLen == EXECUTION_PLAN_ID_LEN);
    Record* r = RS_DeserializeRecord(&br);
    dictEntry *entry = dictFind(epData.epDict, epId);
    assert(entry);
    ExecutionPlan* ep = dictGetVal(entry);
    ep->results = array_append(ep->results, r);
}

static void ExecutionPlan_DoneSendingRecords(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len){
    dictEntry *entry = dictFind(epData.epDict, payload);
    assert(entry);
    ExecutionPlan* ep = dictGetVal(entry);
    ++ep->totalShardsCompleted;
    assert(RedisModule_GetClusterSize() - 1 >= ep->totalShardsCompleted);
    if((RedisModule_GetClusterSize() - 1) == ep->totalShardsCompleted){ // no need to wait to myself
        ExecutionPlan_AddToRunList(ep);
    }
}

void ExecutionPlan_Initialize(RedisModuleCtx *ctx, size_t numberOfworkers){
    modulePointer = ((void**)ctx)[1];
    epData.executionPlansToRun = listCreate();
    epData.epDict = dictCreate(&dictTypeHeapStrings, NULL);
    pthread_mutex_init(&epData.mutex, NULL);
    epData.workers = array_new(pthread_t, numberOfworkers);

    RedisModule_RegisterClusterMessageReceiver(ctx, NEW_EP_MSG_TYPE, ExecutionPlan_OnReceived);
    RedisModule_RegisterClusterMessageReceiver(ctx, NEW_RECORD_MSG_TYPE, ExecutionPlan_OnRecordReceived);
    RedisModule_RegisterClusterMessageReceiver(ctx, DONE_SENDING_RECORDS_MSG_TYPE, ExecutionPlan_DoneSendingRecords);

    for(size_t i = 0 ; i < numberOfworkers ; ++i){
        pthread_t thread;
        epData.workers = array_append(epData.workers, thread);
        pthread_create(epData.workers + i, NULL, ExecutionPlan_ThreadMain, NULL);
    }
}

static Record* ExecutionPlan_FilterNextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
    Record* record = NULL;
    while((record = ExecutionPlan_NextRecord(step->prev, rctx, err))){
        bool filterRes = step->filter.filter(rctx, record, step->filter.stepArg.stepArg, err);
        if(*err){
            RediStar_FreeRecord(record);
            return NULL;
        }
        if(filterRes){
            return record;
        }else{
            RediStar_FreeRecord(record);
        }
    }
    return NULL;
}

static Record* ExecutionPlan_MapNextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
    Record* record = ExecutionPlan_NextRecord(step->prev, rctx, err);
    if(record == NULL){
        return NULL;
    }
    if(*err){
        RediStar_FreeRecord(record);
        return NULL;
    }
    if(record != NULL){
        record = step->map.map(rctx, record, step->map.stepArg.stepArg, err);
        if(*err){
            if(record){
                RediStar_FreeRecord(record);
            }
            return NULL;
        }
    }
    return record;
}

static Record* ExecutionPlan_ExtractKeyNextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
    size_t buffLen;
    Record* record = ExecutionPlan_NextRecord(step->prev, rctx, err);
    if(record == NULL){
        return NULL;
    }
    char* buff = step->extractKey.extractor(rctx, record, step->extractKey.extractorArg.stepArg, &buffLen, err);
    if(*err){
        RediStar_FreeRecord(record);
        return NULL;
    }
    char* newBuff = RS_ALLOC(buffLen + 1);
    memcpy(newBuff, buff, buffLen);
    newBuff[buffLen] = '\0';
    Record* r = RediStar_KeyRecordCreate();
    RediStar_KeyRecordSetKey(r, newBuff, buffLen);
    RediStar_KeyRecordSetVal(r, record);
    return r;
}

static Record* ExecutionPlan_GroupNextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
#define GROUP_RECORD_INIT_LEN 10
    dict* d = dictCreate(&dictTypeHeapStrings, NULL);
    Record* record = NULL;
    if(step->group.groupedRecords == NULL){
        step->group.groupedRecords = array_new(Record*, GROUP_RECORD_INIT_LEN);
        while((record = ExecutionPlan_NextRecord(step->prev, rctx, err))){
            assert(RediStar_RecordGetType(record) == KEY_RECORD);
            size_t keyLen;
            char* key = RediStar_KeyRecordGetKey(record, &keyLen);
            if(*err){
                RediStar_FreeRecord(record);
                break;
            }
            dictEntry* entry = dictFind(d, key);
            Record* r = NULL;
            if(!entry){
                r = RediStar_KeyRecordCreate();
                RediStar_KeyRecordSetKey(r, key, keyLen);
                RediStar_KeyRecordSetKey(record, NULL, 0);
                Record* val  = RediStar_ListRecordCreate(GROUP_RECORD_INIT_LEN);
                RediStar_KeyRecordSetVal(r, val);
                dictAdd(d, key, r);
                step->group.groupedRecords = array_append(step->group.groupedRecords, r);
            }else{
                r = dictGetVal(entry);
            }
            Record* listRecord = RediStar_KeyRecordGetVal(r);
            RediStar_ListRecordAdd(listRecord, RediStar_KeyRecordGetVal(record));
            RediStar_KeyRecordSetVal(record, NULL);
            RediStar_FreeRecord(record);
        }
        dictRelease(d);
    }
    if(array_len(step->group.groupedRecords) == 0){
        return NULL;
    }
    return array_pop(step->group.groupedRecords);
}

static Record* ExecutionPlan_ReduceNextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
    Record* record = ExecutionPlan_NextRecord(step->prev, rctx, err);
    if(!record){
        return NULL;
    }
    if(*err){
        if(record){
            RediStar_FreeRecord(record);
        }
        return NULL;
    }
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    size_t keyLen;
    char* key = RediStar_KeyRecordGetKey(record, &keyLen);
    Record* r = step->reduce.reducer(rctx, key, keyLen, RediStar_KeyRecordGetVal(record), step->reduce.reducerArg.stepArg, err);
    RediStar_KeyRecordSetVal(record, r);
    return record;
}

static Record* ExecutionPlan_NextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err){
    Record* record = NULL;
    Record* r;
    dictEntry* entry;
    switch(step->type){
    case READER:
        return step->reader.r->Next(rctx, step->reader.r->ctx);
        break;
    case MAP:
        return ExecutionPlan_MapNextRecord(step, rctx, err);
        break;
    case FILTER:
        return ExecutionPlan_FilterNextRecord(step, rctx, err);
        break;
    case EXTRACTKEY:
        return ExecutionPlan_ExtractKeyNextRecord(step, rctx, err);
        break;
    case GROUP:
        return ExecutionPlan_GroupNextRecord(step, rctx, err);
        break;
    case REDUCE:
        return ExecutionPlan_ReduceNextRecord(step, rctx, err);
        break;
    case REPARTITION:
        return ExecutionPlan_NextRecord(step->prev, rctx, err);
    default:
        assert(false);
        return NULL;
    }
}

void FlatExecutionPlan_Run(FlatExecutionPlan* fep, RedisModuleCtx* rctx){
    ExecutionPlan* ep = ExecutionPlan_New(fep);
    if(rctx){
        ep->bc = RedisModule_BlockClient(rctx, NULL, NULL, NULL, 1000000000);
    }
    ExecutionPlan_AddToRunList(ep);
}

static WriterStep ExecutionPlan_NewWriter(FlatExecutionWriter* writer){
    RediStar_WriterCallback callback = WritersMgmt_Get(writer->writer);
    ArgType* type = WritersMgmt_GetArgType(writer->writer);
    assert(callback); // todo: handle as error in future
    return (WriterStep){.w = callback(writer->arg), .type = type};
}

static ReaderStep ExecutionPlan_NewReader(FlatExecutionReader* reader){
    RediStar_ReaderCallback callback = ReadersMgmt_Get(reader->reader);
    ArgType* type = ReadersMgmt_GetArgType(reader->reader);
    assert(callback); // todo: handle as error in future
    return (ReaderStep){.r = callback(reader->arg), .type = type};
}

static ExecutionStep* ExecutionPlan_NewExecutionStep(FlatExecutionStep* step){
    ExecutionStep* es = RS_ALLOC(sizeof(*es));
    es->type = step->type;
    switch(step->type){
    case MAP:
        es->map.map = MapsMgmt_Get(step->bStep.stepName);
        es->map.stepArg.type = MapsMgmt_GetArgType(step->bStep.stepName);
        es->map.stepArg.stepArg = step->bStep.arg;
        break;
    case FILTER:
        es->filter.filter = FiltersMgmt_Get(step->bStep.stepName);
        es->filter.stepArg.type = FiltersMgmt_GetArgType(step->bStep.stepName);
        es->filter.stepArg.stepArg = step->bStep.arg;
        break;
    case EXTRACTKEY:
        es->extractKey.extractor = ExtractorsMgmt_Get(step->bStep.stepName);
        es->extractKey.extractorArg.type = ExtractorsMgmt_GetArgType(step->bStep.stepName);
        es->extractKey.extractorArg.stepArg = step->bStep.arg;
        break;
    case REDUCE:
        es->reduce.reducer = ReducersMgmt_Get(step->bStep.stepName);
        es->reduce.reducerArg.type = ReducersMgmt_GetArgType(step->bStep.stepName);
        es->reduce.reducerArg.stepArg = step->bStep.arg;
        break;
    case GROUP:
        es->group.groupedRecords = NULL;
        break;
    case REPARTITION:
        break;
    default:
        assert(false);
    }
    return es;
}

static ExecutionStep* ExecutionPlan_NewReaderExecutionStep(ReaderStep reader){
    ExecutionStep* es = RS_ALLOC(sizeof(*es));
    es->type = READER;
    es->reader = reader;
    es->prev = NULL;
    return es;
}

static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep){
    ExecutionPlan* ret = RS_ALLOC(sizeof(*ret));
    ret->writerStep = ExecutionPlan_NewWriter(fep->writer);
    ExecutionStep* last = NULL;
    for(int i = array_len(fep->steps) - 1 ; i >= 0 ; --i){
        FlatExecutionStep* s = fep->steps + i;
        ExecutionStep* es = ExecutionPlan_NewExecutionStep(s);
        if(last){
            last->prev = es;
        }else{
            ret->start = es;
        }
        last = es;
    }
    ReaderStep rs = ExecutionPlan_NewReader(fep->reader);
    ExecutionStep* reader = ExecutionPlan_NewReaderExecutionStep(rs);
    if(last){
        last->prev = reader;
    }else{
        ret->start = reader;
    }
    ret->bc = NULL;
    ret->fep = fep;
    ret->totalShardsCompleted = 0;
    ret->results = array_new(Record*, 100);
    ret->status = CREATED;
    dictAdd(epData.epDict, fep->id, ret);
    return ret;
}

void ExecutionStep_Free(ExecutionStep* es, RedisModuleCtx *ctx){
    if(es->prev){
        ExecutionStep_Free(es->prev, ctx);
    }
    switch(es->type){
    case MAP:
        if (es->map.stepArg.type && es->map.stepArg.type->free){
            es->map.stepArg.type->free(es->map.stepArg.stepArg);
        }
        break;
    case FILTER:
        if (es->filter.stepArg.type && es->filter.stepArg.type->free){
            es->filter.stepArg.type->free(es->filter.stepArg.stepArg);
        }
        break;
    case EXTRACTKEY:
        if (es->extractKey.extractorArg.type && es->extractKey.extractorArg.type->free){
            es->extractKey.extractorArg.type->free(es->extractKey.extractorArg.stepArg);
        }
        break;
    case REDUCE:
        if(es->reduce.reducerArg.type && es->reduce.reducerArg.type->free){
            es->reduce.reducerArg.type->free(es->reduce.reducerArg.stepArg);
        }
        break;
    case REPARTITION:
        break;
    case READER:
        if(es->reader.type && es->reader.type->free){
            es->reader.type->free(es->reader.r->ctx);
        }
        RS_FREE(es->reader.r);
        break;
    case GROUP:
        if(es->group.groupedRecords){
            for(size_t i = 0 ; i < array_len(es->group.groupedRecords) ; ++i){
                Record* r = es->group.groupedRecords[i];
                RediStar_FreeRecord(r);
            }
            array_free(es->group.groupedRecords);
        }
        break;
    default:
        assert(false);
    }
    RS_FREE(es);
}

static void ExecutionPlan_Free(ExecutionPlan* ep, RedisModuleCtx *ctx){
    dictDelete(epData.epDict, ep->fep->id);
    FlatExecutionPlan_Free(ep->fep);
    ExecutionStep_Free(ep->start, ctx);
    if(ep->writerStep.type && ep->writerStep.type->free){
        ep->writerStep.type->free(ep->writerStep.w->ctx);
    }
    RS_FREE(ep->writerStep.w);
    array_free(ep->results);
    RS_FREE(ep);
}

static FlatExecutionReader* FlatExecutionPlan_NewReader(char* reader, void* readerArg){
    FlatExecutionReader* res = RS_ALLOC(sizeof(*res));
    res->reader = RS_STRDUP(reader);
    res->arg = readerArg;
    return res;
}

static FlatExecutionWriter* FlatExecutionPlan_NewWriter(char* writer, void* writerArg){
    FlatExecutionWriter* res = RS_ALLOC(sizeof(*res));
    res->writer = RS_STRDUP(writer);
    res->arg = writerArg;
    return res;
}

FlatExecutionPlan* FlatExecutionPlan_New(){
#define STEPS_INITIAL_CAP 10
    FlatExecutionPlan* res = RS_ALLOC(sizeof(*res));
    res->reader = NULL;
    res->writer = NULL;
    res->steps = array_new(FlatExecutionStep, STEPS_INITIAL_CAP);
    memset(res->id, 0, EXECUTION_PLAN_ID_LEN);
    if(RedisModule_GetMyClusterID()){
        memcpy(res->id, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
        memcpy(res->id + REDISMODULE_NODE_ID_LEN, &lastId, sizeof(long long));
    }
    ++lastId;
    return res;
}

static void FlatExecutionPlan_Free(FlatExecutionPlan* fep){
    RS_FREE(fep->reader->reader);
    RS_FREE(fep->reader);
    RS_FREE(fep->writer->writer);
    RS_FREE(fep->writer);
    for(size_t i = 0 ; i < array_len(fep->steps) ; ++i){
        FlatExecutionStep* step = fep->steps + i;
        RS_FREE(step->bStep.stepName);
    }
    array_free(fep->steps);
    RS_FREE(fep);
}

void FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader, void* readerArg){
    fep->reader = FlatExecutionPlan_NewReader(reader, readerArg);
}

void FlatExecutionPlan_SetWriter(FlatExecutionPlan* fep, char* writer, void* writerArg){
    fep->writer = FlatExecutionPlan_NewWriter(writer, writerArg);
}

static void FlatExecutionPlan_AddBasicStep(FlatExecutionPlan* fep, const char* callbackName, void* arg, enum StepType type){
    FlatExecutionStep s;
    s.type = type;
    s.bStep.arg = arg;
    if(callbackName){
        s.bStep.stepName = RS_STRDUP(callbackName);
    }else{
        s.bStep.stepName = NULL;
    }
    fep->steps = array_append(fep->steps, s);
}

void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, callbackName, arg, MAP);
}

void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg){
    FlatExecutionPlan_AddBasicStep(fep, callbackName, arg, FILTER);
}

void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                  const char* reducerName, void* reducerArg){
    FlatExecutionStep extractKey;
    FlatExecutionPlan_AddBasicStep(fep, extraxtorName, extractorArg, EXTRACTKEY);
    FlatExecutionPlan_AddBasicStep(fep, NULL, NULL, REPARTITION);
    FlatExecutionPlan_AddBasicStep(fep, NULL, NULL, GROUP);
    FlatExecutionPlan_AddBasicStep(fep, reducerName, reducerArg, REDUCE);
}
