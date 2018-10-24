#include "execution_plan.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include <assert.h>
#include <stdbool.h>
#include "redistar.h"
#include "redistar_memory.h"
#include "utils/dict.h"

static Record* ExecutionPlan_NextRecord(ExecutionStep* step, char** err);

static Record* ExecutionPlan_FilterNextRecord(ExecutionStep* step, char** err){
    Record* record = NULL;
    while((record = ExecutionPlan_NextRecord(step->prev, err))){
        bool filterRes = step->filter.filter(record, step->filter.stepArg, err);
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

static Record* ExecutionPlan_MapNextRecord(ExecutionStep* step, char** err){
    Record* record = ExecutionPlan_NextRecord(step->prev, err);
    if(record == NULL){
        return NULL;
    }
    if(*err){
        RediStar_FreeRecord(record);
        return NULL;
    }
    if(record != NULL){
        record = step->map.map(record, step->map.stepArg, err);
        if(*err){
            if(record){
                RediStar_FreeRecord(record);
            }
            return NULL;
        }
    }
    return record;
}

static Record* ExecutionPlan_ExtractKeyNextRecord(ExecutionStep* step, char** err){
    size_t buffLen;
    Record* record = ExecutionPlan_NextRecord(step->prev, err);
    if(record == NULL){
        return NULL;
    }
    char* buff = step->extractKey.extractor(record, step->extractKey.extractorArg, &buffLen, err);
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

static Record* ExecutionPlan_GroupNextRecord(ExecutionStep* step, char** err){
#define GROUP_RECORD_INIT_LEN 10
    dict* d = dictCreate(&dictTypeHeapStrings, NULL);
    Record* record = NULL;
    if(step->group.groupedRecords == NULL){
        step->group.groupedRecords = array_new(Record*, GROUP_RECORD_INIT_LEN);
        while((record = ExecutionPlan_NextRecord(step->prev, err))){
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

static Record* ExecutionPlan_ReduceNextRecord(ExecutionStep* step, char** err){
    Record* record = ExecutionPlan_NextRecord(step->prev, err);
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
    Record* r = step->reduce.reducer(key, keyLen, RediStar_KeyRecordGetVal(record), step->reduce.reducerArg, err);
    RediStar_KeyRecordSetVal(record, r);
    return record;
}

static Record* ExecutionPlan_NextRecord(ExecutionStep* step, char** err){
    Record* record = NULL;
    Record* r;
    dictEntry* entry;
    switch(step->type){
    case READER:
        return step->reader->Next(step->reader->ctx);
        break;
    case MAP:
        return ExecutionPlan_MapNextRecord(step, err);
        break;
    case FILTER:
        return ExecutionPlan_FilterNextRecord(step, err);
        break;
    case EXTRACTKEY:
        return ExecutionPlan_ExtractKeyNextRecord(step, err);
        break;
    case GROUP:
        return ExecutionPlan_GroupNextRecord(step, err);
        break;
    case REDUCE:
        return ExecutionPlan_ReduceNextRecord(step, err);
        break;
    case REPARTITION:
        return ExecutionPlan_NextRecord(step->prev, err);
    default:
        assert(false);
        return NULL;
    }
}

void ExecutionPlan_Run(ExecutionPlan* ep){
    Record* record = NULL;
    ep->writer->Start(ep->writer->ctx);
    char* err = NULL;
    while((record = ExecutionPlan_NextRecord(ep->start, &err))){
        if(err){
            Record* r = RediStar_StringRecordCreate(err);
            ep->writer->Write(ep->writer->ctx, r);
            break;
        }
        ep->writer->Write(ep->writer->ctx, record);
    }
    if(err){
        Record* r = RediStar_StringRecordCreate(err);
        ep->writer->Write(ep->writer->ctx, r);
    }
    ep->writer->Done(ep->writer->ctx);
}

static Writer* ExecutionPlan_NewWriter(FlatExecutionWriter* writer){
    RediStar_WriterCallback callback = WritersMgmt_Get(writer->writer);
    assert(callback); // todo: handle as error in future
    return callback(writer->arg);
}

static ExecutionStep* ExecutionPlan_NewExecutionStep(FlatExecutionStep* step){
    ExecutionStep* es = RS_ALLOC(sizeof(*es));
    es->type = step->type;
    switch(step->type){
    case MAP:
        es->map.map = MapsMgmt_Get(step->bStep.stepName);
        es->map.stepArg = step->bStep.arg;
        break;
    case FILTER:
        es->filter.filter = FiltersMgmt_Get(step->bStep.stepName);
        es->filter.stepArg = step->bStep.arg;
        break;
    case EXTRACTKEY:
        es->extractKey.extractor = ExtractorsMgmt_Get(step->bStep.stepName);
        es->extractKey.extractorArg = step->bStep.arg;
        break;
    case REDUCE:
        es->reduce.reducer = ReducersMgmt_Get(step->bStep.stepName);
        es->reduce.reducerArg = step->bStep.arg;
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

static Reader* ExecutionPlan_NewReader(FlatExecutionReader* reader){
    RediStar_ReaderCallback callback =  ReadersMgmt_Get(reader->reader);
    assert(callback); // todo: handle as error in future
    return callback(reader->arg);
}

static ExecutionStep* ExecutionPlan_NewReaderExecutionStep(Reader* r){
    ExecutionStep* es = RS_ALLOC(sizeof(*es));
    es->type = READER;
    es->reader = r;
    es->prev = NULL;
    return es;
}

ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep){
    ExecutionPlan* ret = RS_ALLOC(sizeof(*ret));
    ret->writer = ExecutionPlan_NewWriter(fep->writer);
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
    ret->reader = ExecutionPlan_NewReader(fep->reader);
    ExecutionStep* reader = ExecutionPlan_NewReaderExecutionStep(ret->reader);
    if(last){
        last->prev = reader;
    }else{
        ret->start = reader;
    }
    return ret;
}

void ExecutionStep_Free(ExecutionStep* es){
    if(es->prev){
        ExecutionStep_Free(es->prev);
    }
    switch(es->type){
    case MAP:
    case FILTER:
    case REPARTITION:
    case EXTRACTKEY:
    case REDUCE:
        break;
    case READER:
        es->reader->Free(es->reader->ctx);
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

void ExecutionPlan_Free(ExecutionPlan* ep){
    ExecutionStep_Free(ep->start);
    ep->writer->Free(ep->writer->ctx);
    RS_FREE(ep->reader);
    RS_FREE(ep->writer);
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

void FlatExecutionPlan_Free(FlatExecutionPlan* fep){
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
