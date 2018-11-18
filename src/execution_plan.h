/*
 * execution_plan.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_EXECUTION_PLAN_H_
#define SRC_EXECUTION_PLAN_H_

#include "redistar.h"
#include <stdbool.h>

enum StepType{
    MAP=1,
    FILTER,
    READER,
    GROUP,
    EXTRACTKEY,
    REPARTITION,
    REDUCE,
    COLLECT,
    WRITER,
    FLAT_MAP,
    LIMIT,
};

typedef struct FlatExecutionPlan FlatExecutionPlan;
typedef struct ExecutionPlan ExecutionPlan;

typedef struct ExecutionStepArg{
    void* stepArg;
    ArgType* type;
}ExecutionStepArg;

typedef struct MapExecutionStep{
    RediStar_MapCallback map;
    ExecutionStepArg stepArg;
}MapExecutionStep;

typedef struct FlatMapExecutionStep{
    MapExecutionStep mapStep;
    Record* pendings;
}FlatMapExecutionStep;

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

typedef struct WriterExecutionStep{
    RediStar_WriterCallback write;
    ExecutionStepArg stepArg;
}WriterExecutionStep;

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
        WriterExecutionStep writer;
        LimitExecutionStep limit;
    };
    enum StepType type;
}ExecutionStep;

typedef struct FlatExecutionPlan FlatExecutionPlan;

typedef enum ExecutionPlanStatus{
    CREATED, RUNNING, WAITING_FOR_CLUSTER_RESULTS
}ExecutionPlanStatus;

#define EXECUTION_PLAN_ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0
#define EXECUTION_PLAN_STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

typedef struct ExecutionPlan{
    char id[EXECUTION_PLAN_ID_LEN];
    char idStr[EXECUTION_PLAN_STR_ID_LEN];
    ExecutionStep** steps;
    FlatExecutionPlan* fep;
    size_t totalShardsCompleted;
    Record** results;
    ExecutionPlanStatus status;
    bool isDone;
    RediStar_OnExecutionDoneCallback callback;
    void* privateData;
    FreePrivateData freeCallback;
}ExecutionPlan;

typedef struct FlatBasicStep{
    char* stepName;
    ExecutionStepArg arg;
}FlatBasicStep;

typedef struct FlatExecutionStep{
    union{
        FlatBasicStep bStep;
    };
    enum StepType type;
}FlatExecutionStep;

typedef struct FlatExecutionReader{
    char* reader;
}FlatExecutionReader;

typedef struct FlatExecutionPlan{
	char* name;
    FlatExecutionReader* reader;
    FlatExecutionStep* steps;
    bool distributed;
}FlatExecutionPlan;

FlatExecutionPlan* FlatExecutionPlan_New(const char* name);
void FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader);
void FlatExecutionPlan_AddWriter(FlatExecutionPlan* fep, char* writer, void* writerArg);
void FlatExecutionPlan_AddMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFlatMapStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddFilterStep(FlatExecutionPlan* fep, const char* callbackName, void* arg);
void FlatExecutionPlan_AddGroupByStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg,
                                  const char* reducerName, void* reducerArg);
void FlatExecutionPlan_AddCollectStep(FlatExecutionPlan* fep);
void FlatExecutionPlan_AddLimitStep(FlatExecutionPlan* fep, size_t offset, size_t len);
void FlatExecutionPlan_AddRepartitionStep(FlatExecutionPlan* fep, const char* extraxtorName, void* extractorArg);
bool FlatExecutionPlan_IsBroadcasted(FlatExecutionPlan* fep);
bool FlatExecutionPlan_Broadcast(FlatExecutionPlan* fep);
int FlatExecutionPlan_Register(FlatExecutionPlan* fep);
ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, char* eid, void* arg, RediStar_OnExecutionDoneCallback callback, void* privateData);
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);
void FlatExecutionPlan_Distribute(FlatExecutionPlan* fep, RedisModuleCtx *rctx);

void ExecutionPlan_Initialize(RedisModuleCtx *ctx, size_t numberOfworkers);
void ExecutionPlan_Free(ExecutionPlan* ep, RedisModuleCtx *ctx);


int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_FlatExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
FlatExecutionPlan* ExecutionPlan_FindByName(const char* name);
ExecutionPlan* ExecutionPlan_FindById(const char* id);
ExecutionPlan* ExecutionPlan_FindByStrId(const char* id);

#endif /* SRC_EXECUTION_PLAN_H_ */
