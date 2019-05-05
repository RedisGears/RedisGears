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
#include "commands.h"
#include "utils/dict.h"
#ifdef WITHPYTHON
#include <redisgears_python.h>
#endif
#define STEP_TYPES \
    X(NONE, "none") \
    X(MAP, "map") \
    X(FILTER, "filter") \
    X(READER, "reader") \
    X(GROUP, "group") \
    X(EXTRACTKEY, "extractkey") \
    X(REPARTITION, "repartition") \
    X(REDUCE, "reduce") \
    X(COLLECT, "collect") \
    X(FOREACH, "foreach") \
    X(FLAT_MAP, "flatmap") \
    X(LIMIT, "limit") \
    X(ACCUMULATE, "accumulate") \
    X(ACCUMULATE_BY_KEY, "accumulatebykey")

enum StepType{
#define X(a, b) a,
    STEP_TYPES
#undef X
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
    Gears_dict* d;
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
    Gears_dict* accumulators;
    Gears_dictIterator *iter;
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

typedef enum ActionResult ActionResult;

ActionResult EPStatus_CreatedAction(ExecutionPlan*);
ActionResult EPStatus_RunningAction(ExecutionPlan*);
ActionResult EPStatus_PendingReceiveAction(ExecutionPlan*);
ActionResult EPStatus_PendingRunAction(ExecutionPlan*);
ActionResult EPStatus_PendingClusterAction(ExecutionPlan*);
ActionResult EPStatus_InitiatorTerminationAction(ExecutionPlan*);
ActionResult EPStatus_DoneAction(ExecutionPlan*);

#define EXECUTION_PLAN_STATUSES \
    X(CREATED=0, "created", EPStatus_CreatedAction) \
    X(RUNNING, "running", EPStatus_RunningAction) \
    X(WAITING_FOR_RECIEVED_NOTIFICATION, "pending_receive", EPStatus_PendingReceiveAction) \
    X(WAITING_FOR_RUN_NOTIFICATION, "pending_run", EPStatus_PendingRunAction) \
    X(WAITING_FOR_CLUSTER_TO_COMPLETE, "pending_cluster", EPStatus_PendingClusterAction) \
    X(WAITING_FOR_INITIATOR_TERMINATION, "pending_termination", EPStatus_InitiatorTerminationAction) \
    X(DONE, "done", EPStatus_DoneAction)

typedef enum ExecutionPlanStatus{
#define X(a, b, c) a,
    EXECUTION_PLAN_STATUSES
#undef X
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
    Record** errors;
    ExecutionPlanStatus status;
    bool sentRunRequest;
    RedisGears_OnExecutionDoneCallback callback;
    void* privateData;
    FreePrivateData freeCallback;
    long long executionDuration;
    WorkerData* assignWorker;
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
void FlatExecutionPlan_AddLocalAccumulateByKeyStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                              const char* accumulateName, void* accumulateArg);
void FlatExecutionPlan_AddCollectStep(FlatExecutionPlan* fep);
void FlatExecutionPlan_AddLimitStep(FlatExecutionPlan* fep, size_t offset, size_t len);
void FlatExecutionPlan_AddRepartitionStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg);
int FlatExecutionPlan_Register(FlatExecutionPlan* fep, char* key);
const char* FlatExecutionPlan_GetReader(FlatExecutionPlan* fep);
ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, char* eid, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData);
long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep);
long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep);
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);

void ExecutionPlan_Initialize(size_t numberOfworkers);
void ExecutionPlan_SendFreeMsg(ExecutionPlan* ep);
void ExecutionPlan_Free(ExecutionPlan* ep, bool needLock);


int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_ExecutionGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
ExecutionPlan* ExecutionPlan_FindById(const char* id);
ExecutionPlan* ExecutionPlan_FindByStrId(const char* id);

#endif /* SRC_EXECUTION_PLAN_H_ */
