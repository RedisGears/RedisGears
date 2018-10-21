#include "execution_plan.h"
#include "utils/arr_rm_alloc.h"
#include "mgmt.h"
#include <assert.h>
#include <stdbool.h>
#include "redistar.h"
#include "redistar_memory.h"
#include "utils/dict.h"

static Record* ExecutionPlan_NextRecord(ExecutionStep* step);

static void ExecutionPlan_Group(ExecutionStep* step){
#define GROUP_RECORD_INIT_LEN 10
    assert(step->type == GROUPBY);
    step->groupBy.groupedRecords = array_new(Record*, GROUP_RECORD_INIT_LEN);
    dict* d = dictCreate(&dictTypeHeapStrings, NULL);
    Record* record = NULL;
    while((record = ExecutionPlan_NextRecord(step->prev))){
        size_t buffLen;
        char* buff = step->groupBy.extractor(record, step->groupBy.extractorArg, &buffLen);
        char* newBuff = RS_ALLOC(buffLen + 1);
        memcpy(newBuff, buff, buffLen);
        newBuff[buffLen] = '\0';
        dictEntry* entry = dictFind(d, newBuff);
        Record* r = NULL;
        if(!entry){
            r = RediStar_KeyRecordCreate();
            RediStar_KeyRecordSetKey(r, newBuff, buffLen);
            Record* val  = RediStar_ListRecordCreate(GROUP_RECORD_INIT_LEN);
            RediStar_KeyRecordSetVal(r, val);
            dictAdd(d, newBuff, r);
            step->groupBy.groupedRecords = array_append(step->groupBy.groupedRecords, r);
        }else{
            r = dictGetVal(entry);
            RS_FREE(newBuff);
        }
        Record* listRecord = RediStar_KeyRecordGetVal(r);
        RediStar_ListRecordAdd(listRecord, record);
    }
    dictRelease(d);
}

static Record* ExecutionPlan_NextRecord(ExecutionStep* step){
    Record* record = NULL;
    switch(step->type){
    case READER:
        return step->reader->Next(step->reader->ctx);
        break;
    case MAP:
        record = ExecutionPlan_NextRecord(step->prev);
        if(record != NULL){
            record = step->map.map(record, step->map.stepArg);
        }
        return record;
        break;
    case FILTER:
        record = NULL;
        while((record = ExecutionPlan_NextRecord(step->prev))){
            if(step->filter.filter(record, step->filter.stepArg)){
                return record;
            }else{
                RediStar_FreeRecord(record);
            }
        }
        return NULL;
        break;
    case GROUPBY:
        if(!step->groupBy.groupedRecords){
            ExecutionPlan_Group(step);
        }
        if(array_len(step->groupBy.groupedRecords) <= step->groupBy.index){
            return NULL;
        }
        record = step->groupBy.groupedRecords[step->groupBy.index++];
        assert(RediStar_RecordGetType(record) == KEY_RECORD);
        assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == LIST_RECORD);
        size_t keyLen;
        char* key = RediStar_KeyRecordGetKey(record, &keyLen);
        Record* listRecord = RediStar_KeyRecordGetVal(record);
        Record* reducedRecord = step->groupBy.reducer(key, keyLen, listRecord, step->groupBy.reducerArg);
        RediStar_KeyRecordSetVal(record, reducedRecord);
        return record;
        break;
    default:
        assert(false);
        return NULL;
    }
}

void ExecutionPlan_Run(ExecutionPlan* ep){
    Record* record = NULL;
    ep->writer->Start(ep->writer->ctx);
    while((record = ExecutionPlan_NextRecord(ep->start))){
        ep->writer->Write(ep->writer->ctx, record);
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
    case GROUPBY:
        es->groupBy.extractor = ExtractorsMgmt_Get(step->gbStep.extractorName);
        es->groupBy.extractorArg = step->gbStep.extractorArg;
        es->groupBy.reducer = ReducersMgmt_Get(step->gbStep.reducerName);
        es->groupBy.reducerArg = step->gbStep.reducerArg;
        es->groupBy.groupedRecords = NULL;
        es->groupBy.index = 0;
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
        break;
    case READER:
        es->reader->Free(es->reader->ctx);
        break;
    case GROUPBY:
        if(es->groupBy.groupedRecords){
            array_free(es->groupBy.groupedRecords);
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
        switch(step->type){
        case MAP:
        case FILTER:
            RS_FREE(step->bStep.stepName);
            break;
        case GROUPBY:
            RS_FREE(step->gbStep.extractorName);
            RS_FREE(step->gbStep.reducerName);
            break;
        default:
            assert(false);
        }
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
    s.bStep.stepName = RS_STRDUP(callbackName);
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
    FlatExecutionStep s;
    s.type = GROUPBY;
    s.gbStep.extractorName = RS_STRDUP(extraxtorName);
    s.gbStep.extractorArg = extractorArg;
    s.gbStep.reducerName = RS_STRDUP(reducerName);
    s.gbStep.reducerArg = reducerArg;
    fep->steps = array_append(fep->steps, s);
}
