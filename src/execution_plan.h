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

typedef struct MapExecutionStep{
    RediStar_MapCallback map;
    void* stepArg;
}MapExecutionStep;

typedef struct FilterExecutionStep{
    RediStar_FilterCallback filter;
    void* stepArg;
}FilterExecutionStep;

typedef struct ExtractKeyExecutionStep{
    RediStar_ExtractorCallback extractor;
    void* extractorArg;
}ExtractKeyExecutionStep;

typedef struct GroupExecutionStep{
    Record** groupedRecords;
}GroupExecutionStep;

typedef struct ReduceExecutionStep{
    RediStar_ReducerCallback reducer;
    void* reducerArg;
}ReduceExecutionStep;

typedef struct RepartitionExecutionStep{

}RepartitionExecutionStep;

typedef struct ExecutionStep{
    struct ExecutionStep* prev;
    union{
        MapExecutionStep map;
        FilterExecutionStep filter;
        ExtractKeyExecutionStep extractKey;
        RepartitionExecutionStep repartion;
        GroupExecutionStep group;
        ReduceExecutionStep reduce;
        Reader* reader;
    };
    enum StepType type;
}ExecutionStep;

typedef struct ExecutionPlan{
    Reader* reader;
    ExecutionStep* start;
    Writer* writer;
    RedisModuleBlockedClient* bc;
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
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);
void FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader, void* readerArg);
void FlatExecutionPlan_SetWriter(FlatExecutionPlan* fep, char* writer, void* writerArg);
void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                  const char* reducerName, void* reducerArg);

ExecutionPlan* ExecutionPlan_New(FlatExecutionPlan* fep);
void ExecutionPlan_Free(ExecutionPlan* ep, RedisModuleCtx *ctx);
void ExecutionPlan_Run(ExecutionPlan* ep, RedisModuleCtx *ctx);

void ExecutionPlan_InitializeWorkers(size_t numberOfworkers);


#endif /* SRC_EXECUTION_PLAN_H_ */
