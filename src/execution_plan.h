/*
 * execution_plan.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXECUTION_PLAN_H_
#define SRC_EXECUTION_PLAN_H_

#include "redistar.h"

enum StepType{
    MAP, FILTER, READER, GROUP, EXTRACTKEY, REPARTITION, REDUCE
};

typedef struct ExecutionStepArg{
    void* stepArg;
    ArgType* type;
}ExecutionStepArg;

typedef struct MapExecutionStep{
    RediStar_MapCallback map;
    ExecutionStepArg stepArg;
}MapExecutionStep;

typedef struct FilterExecutionStep{
    RediStar_FilterCallback filter;
    ExecutionStepArg stepArg;
}FilterExecutionStep;

typedef struct ExtractKeyExecutionStep{
    RediStar_ExtractorCallback extractor;
    ExecutionStepArg extractorArg;
}ExtractKeyExecutionStep;

typedef struct GroupExecutionStep{
    Record** groupedRecords;
}GroupExecutionStep;

typedef struct ReduceExecutionStep{
    RediStar_ReducerCallback reducer;
    ExecutionStepArg reducerArg;
}ReduceExecutionStep;

typedef struct RepartitionExecutionStep{

}RepartitionExecutionStep;

typedef struct ReaderStep{
    Reader* r;
    ArgType* type;
}ReaderStep;

typedef struct WriterStep{
    Writer* w;
    ArgType* type;
}WriterStep;

typedef struct ExecutionStep{
    struct ExecutionStep* prev;
    union{
        MapExecutionStep map;
        FilterExecutionStep filter;
        ExtractKeyExecutionStep extractKey;
        RepartitionExecutionStep repartion;
        GroupExecutionStep group;
        ReduceExecutionStep reduce;
        ReaderStep reader;
    };
    enum StepType type;
}ExecutionStep;

typedef struct FlatExecutionPlan FlatExecutionPlan;

typedef struct ExecutionPlan{
    ExecutionStep* start;
    WriterStep writerStep;
    RedisModuleBlockedClient* bc;
    FlatExecutionPlan* fep;
}ExecutionPlan;

typedef struct FlatBasicStep{
    char* stepName;
    void* arg;
}FlatBasicStep;

typedef struct FlatExecutionStep{
    union{
        FlatBasicStep bStep;
    };
    enum StepType type;
}FlatExecutionStep;

typedef struct FlatExecutionReader{
    char* reader;
    void* arg;
}FlatExecutionReader;

typedef struct FlatExecutionWriter{
    char* writer;
    void* arg;
}FlatExecutionWriter;

typedef struct FlatExecutionPlan{
    FlatExecutionReader* reader;
    FlatExecutionStep* steps;
    FlatExecutionWriter* writer;
}FlatExecutionPlan;

FlatExecutionPlan* FlatExecutionPlan_New();
void FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader, void* readerArg);
void FlatExecutionPlan_SetWriter(FlatExecutionPlan* fep, char* writer, void* writerArg);
void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                  const char* reducerName, void* reducerArg);
void FlatExecutionPlan_Run(FlatExecutionPlan* fep, RedisModuleCtx *ctx);

void ExecutionPlan_InitializeWorkers(size_t numberOfworkers);


#endif /* SRC_EXECUTION_PLAN_H_ */
