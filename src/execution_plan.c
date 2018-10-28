#include "execution_plan.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include <assert.h>
#include <stdbool.h>
#include "redistar.h"
#include "redistar_memory.h"
#include "utils/dict.h"
#include "utils/adlist.h"
#include <pthread.h>
#include <unistd.h>

typedef struct ExecutionPlansData{
    list* executionPlansToRun;
    list* executionPlansWaiting;
    pthread_mutex_t mutex;
    pthread_t* workers;
}ExecutionPlansData;

ExecutionPlansData epData;

static Record* ExecutionPlan_NextRecord(ExecutionStep* step, RedisModuleCtx* rctx, char** err);
static ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep);
static void FlatExecutionPlan_Free(FlatExecutionPlan* fep);
static void ExecutionPlan_Free(ExecutionPlan* ep, RedisModuleCtx *ctx);

static void ExecutionPlan_Execute(ExecutionPlan* ep, RedisModuleCtx* rctx){
    Record* record = NULL;
    ep->writerStep.w->Start(rctx, ep->writerStep.w->ctx);
    char* err = NULL;
    while((record = ExecutionPlan_NextRecord(ep->start, rctx, &err))){
        if(err){
            Record* r = RediStar_StringRecordCreate(err);
            ep->writerStep.w->Write(rctx, ep->writerStep.w->ctx, r);
            break;
        }
        ep->writerStep.w->Write(rctx, ep->writerStep.w->ctx, record);
    }
    if(err){
        Record* r = RediStar_StringRecordCreate(err);
        ep->writerStep.w->Write(rctx, ep->writerStep.w->ctx, r);
    }
    ep->writerStep.w->Done(rctx, ep->writerStep.w->ctx);
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
        ExecutionPlan_Execute(ep, rctx);
        if(ep->bc){
            RedisModule_UnblockClient(ep->bc, NULL);
        }
        ExecutionPlan_Free(ep, rctx);
        RedisModule_FreeThreadSafeContext(rctx);
    }
}

static void ExecutionPlan_AddToRunList(ExecutionPlan* ep){
    pthread_mutex_lock(&epData.mutex);
    listAddNodeTail(epData.executionPlansToRun, ep);
    pthread_mutex_unlock(&epData.mutex);

}

void ExecutionPlan_InitializeWorkers(size_t numberOfworkers){
    epData.executionPlansToRun = listCreate();
    epData.executionPlansWaiting = listCreate();
    pthread_mutex_init(&epData.mutex, NULL);
    epData.workers = array_new(pthread_t, numberOfworkers);
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
    FlatExecutionPlan_Free(ep->fep);
    ExecutionStep_Free(ep->start, ctx);
    if(ep->writerStep.type && ep->writerStep.type->free){
        ep->writerStep.type->free(ep->writerStep.w->ctx);
    }
    RS_FREE(ep->writerStep.w);
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
