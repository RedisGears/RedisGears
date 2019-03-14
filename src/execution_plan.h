/*
 * execution_plan.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXECUTION_PLAN_H_
#define SRC_EXECUTION_PLAN_H_

#include <stdbool.h>
#include "redisgears.h"
#include "utils/dict.h"

enum StepType{
    MAP=1,
    FILTER,
    READER,
    GROUP,
    EXTRACTKEY,
    REPARTITION,
    REDUCE,
    COLLECT,
    FOREACH,
    FLAT_MAP,
    LIMIT,
    ACCUMULATE,
	ACCUMULATE_BY_KEY,
};

typedef struct FlatExecutionPlan FlatExecutionPlan;
typedef struct ExecutionPlan ExecutionPlan;

typedef struct ExecutionStepArg{
    void* stepArg;
    ArgType* type;
}ExecutionStepArg;

typedef struct MapExecutionStep{
    RedisGears_MapCallback map;
    ExecutionStepArg stepArg;
}MapExecutionStep;

typedef struct FlatMapExecutionStep{
    MapExecutionStep mapStep;
    Record* pendings;
}FlatMapExecutionStep;

typedef struct FilterExecutionStep{
    RedisGears_FilterCallback filter;
    ExecutionStepArg stepArg;
}FilterExecutionStep;

typedef struct ExtractKeyExecutionStep{
    RedisGears_ExtractorCallback extractor;
    ExecutionStepArg extractorArg;
}ExtractKeyExecutionStep;

typedef struct GroupExecutionStep{
    Record** groupedRecords;
    dict* d;
    bool isGrouped;
}GroupExecutionStep;

typedef struct ReduceExecutionStep{
    RedisGears_ReducerCallback reducer;
    ExecutionStepArg reducerArg;
}ReduceExecutionStep;

typedef struct RepartitionExecutionStep{
    bool stoped;
    Record** pendings;
    size_t totalShardsCompleted;
}RepartitionExecutionStep;

typedef struct CollectExecutionStep{
    bool stoped;
    Record** pendings;
    size_t totalShardsCompleted;
}CollectExecutionStep;

typedef struct LimitExecutionStep{
    ExecutionStepArg stepArg;
    size_t currRecordIndex;
}LimitExecutionStep;

typedef struct ReaderStep{
    Reader* r;
}ReaderStep;

typedef struct ForEachExecutionStep{
    RedisGears_ForEachCallback forEach;
    ExecutionStepArg stepArg;
}ForEachExecutionStep;

typedef struct AccumulateExecutionStep{
    RedisGears_AccumulateCallback accumulate;
    ExecutionStepArg stepArg;
    Record* accumulator;
    bool isDone;
}AccumulateExecutionStep;

typedef struct AccumulateByKeyExecutionStep{
    RedisGears_AccumulateByKeyCallback accumulate;
    ExecutionStepArg stepArg;
    dict* accumulators;
    dictIterator *iter;
}AccumulateByKeyExecutionStep;

typedef struct ExecutionStep{
    struct ExecutionStep* prev;
    size_t stepId;
    union{
        MapExecutionStep map;
        FlatMapExecutionStep flatMap;
        FilterExecutionStep filter;
        ExtractKeyExecutionStep extractKey;
        RepartitionExecutionStep repartion;
        GroupExecutionStep group;
        ReduceExecutionStep reduce;
        CollectExecutionStep collect;
        ReaderStep reader;
        ForEachExecutionStep forEach;
        LimitExecutionStep limit;
        AccumulateExecutionStep accumulate;
        AccumulateByKeyExecutionStep accumulateByKey;
    };
    enum StepType type;
    long long executionDuration;
}ExecutionStep;

typedef struct FlatExecutionPlan FlatExecutionPlan;

typedef enum ExecutionPlanStatus{
    CREATED, RUNNING, WAITING_FOR_RECIEVED_NOTIFICATION, WAITING_FOR_RUN_NOTIFICATION, WAITING_FOR_CLUSTER_TO_COMPLETE
}ExecutionPlanStatus;

#define EXECUTION_PLAN_ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0
#define EXECUTION_PLAN_STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

typedef struct WorkerData{
	struct event_base* eb;
	int notifyPipe[2];
	pthread_t thread;
}WorkerData;

typedef struct ExecutionPlan{
    char id[EXECUTION_PLAN_ID_LEN];
    char idStr[EXECUTION_PLAN_STR_ID_LEN];
    ExecutionStep** steps;
    FlatExecutionPlan* fep;
    size_t totalShardsRecieved;
    size_t totalShardsCompleted;
    Record** results;
    ExecutionPlanStatus status;
    bool isDone;
    bool sentRunRequest;
    RedisGears_OnExecutionDoneCallback callback;
    void* privateData;
    FreePrivateData freeCallback;
    long long executionDuration;
    WorkerData* assignWorker;
    bool isErrorOccure;
}ExecutionPlan;

typedef struct FlatBasicStep{
    char* stepName;
    ExecutionStepArg arg;
}FlatBasicStep;

typedef struct FlatAccumulatorStep{
    FlatBasicStep ctr;
    FlatBasicStep accumulator;
}FlatAccumulatorStep;

typedef struct FlatExecutionStep{
    FlatBasicStep bStep;
    enum StepType type;
}FlatExecutionStep;

typedef struct FlatExecutionReader{
    char* reader;
}FlatExecutionReader;

typedef struct FlatExecutionPlan{
    FlatExecutionReader* reader;
    FlatExecutionStep* steps;
}FlatExecutionPlan;

FlatExecutionPlan* FlatExecutionPlan_New();
bool FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader);
void FlatExecutionPlan_AddForEachStep(FlatExecutionPlan* fep, char* forEach, void* writerArg);
void FlatExecutionPlan_AddAccumulateStep(FlatExecutionPlan* fep, char* accumulator, void* arg);
void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFlatMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                      const char* reducerName, void* reducerArg);
void FlatExecutionPlan_AddAccumulateByKeyStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                              const char* accumulateName, void* accumulateArg);
void FlatExecutionPlan_AddCollectStep(FlatExecutionPlan* fep);
void FlatExecutionPlan_AddLimitStep(FlatExecutionPlan* fep, size_t offset, size_t len);
void FlatExecutionPlan_AddRepartitionStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg);
int FlatExecutionPlan_Register(FlatExecutionPlan* fep, char* key);
ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, char* eid, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData);
long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep);
long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep);
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);

void ExecutionPlan_Initialize(size_t numberOfworkers);
void ExecutionPlan_SendFreeMsg(ExecutionPlan* ep);
void ExecutionPlan_Free(ExecutionPlan* ep);


int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
ExecutionPlan* ExecutionPlan_FindById(const char* id);
ExecutionPlan* ExecutionPlan_FindByStrId(const char* id);

#endif /* SRC_EXECUTION_PLAN_H_ */
