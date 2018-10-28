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
    MAP=1, FILTER, READER, GROUP, EXTRACTKEY, REPARTITION, REDUCE
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

typedef enum ExecutionPlanStatus{
    CREATED, RUNNING, WAITING_FOR_CLUSTER_RESULTS
}ExecutionPlanStatus;

typedef struct ExecutionPlan{
    ExecutionStep* start;
    WriterStep writerStep;
    RedisModuleBlockedClient* bc;
    FlatExecutionPlan* fep;
    size_t totalShardsCompleted;
    Record** results;
    ExecutionPlanStatus status;
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

#define EXECUTION_PLAN_ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0

typedef struct FlatExecutionPlan{
    char id[EXECUTION_PLAN_ID_LEN];
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

void ExecutionPlan_Initialize(RedisModuleCtx *ctx, size_t numberOfworkers);


#endif /* SRC_EXECUTION_PLAN_H_ */
