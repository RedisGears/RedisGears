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
#include "utils/adlist.h"
#include "utils/buffer.h"
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

#define RG_INNER_REGISTER_COMMAND "RG.INNERREGISTER"
#define RG_INNER_UNREGISTER_COMMAND "RG.INNERUNREGISTER"

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
    unsigned long long executionDuration;
}ExecutionStep;

typedef enum ActionResult{
    CONTINUE, STOP, COMPLETED
}ActionResult;

ActionResult EPStatus_CreatedAction(ExecutionPlan*);
ActionResult EPStatus_RunningAction(ExecutionPlan*);
ActionResult EPStatus_PendingReceiveAction(ExecutionPlan*);
ActionResult EPStatus_PendingRunAction(ExecutionPlan*);
ActionResult EPStatus_PendingClusterAction(ExecutionPlan*);
ActionResult EPStatus_InitiatorTerminationAction(ExecutionPlan*);
ActionResult EPStatus_DoneAction(ExecutionPlan*);
ActionResult EPStatus_AbortedAction(ExecutionPlan*);

#define EXECUTION_PLAN_STATUSES \
    X(CREATED=0, "created", EPStatus_CreatedAction) \
    X(RUNNING, "running", EPStatus_RunningAction) \
    X(WAITING_FOR_RECIEVED_NOTIFICATION, "pending_receive", EPStatus_PendingReceiveAction) \
    X(WAITING_FOR_RUN_NOTIFICATION, "pending_run", EPStatus_PendingRunAction) \
    X(WAITING_FOR_CLUSTER_TO_COMPLETE, "pending_cluster", EPStatus_PendingClusterAction) \
    X(WAITING_FOR_INITIATOR_TERMINATION, "pending_termination", EPStatus_InitiatorTerminationAction) \
    X(DONE, "done", EPStatus_DoneAction) \
    X(ABORTED, "aborted", EPStatus_DoneAction)

typedef enum ExecutionPlanStatus{
#define X(a, b, c) a,
    EXECUTION_PLAN_STATUSES
#undef X
}ExecutionPlanStatus;

#define EXECUTION_PLAN_ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0
#define EXECUTION_PLAN_STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

typedef struct WorkerData{
    Gears_list* notifications;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t thread;
}WorkerData;

typedef struct OnDoneData{
    RedisGears_OnExecutionDoneCallback callback;
    void* privateData;
}OnDoneData;

#define ExecutionFlags int
#define EFDone 0x01
#define EFIsOnDoneCallback 0x02
#define EFIsFreedOnDoneCallback 0x04
#define EFSentRunRequest 0x08
#define EFIsLocal 0x10
#define EFIsLocalyFreedOnDoneCallback 0x20
#define EFStarted 0x40

#define EPTurnOnFlag(ep, f) ep->flags |= f
#define EPTurnOffFlag(ep, f) ep->flags &= ~f
#define EPIsFlagOn(ep, f) (ep->flags & f)
#define EPIsFlagOff(ep, f) (!(ep->flags & f))

typedef struct ExecutionPlan{
    char id[EXECUTION_PLAN_ID_LEN];
    char idStr[EXECUTION_PLAN_STR_ID_LEN];
    ExecutionStep** steps;
    FlatExecutionPlan* fep;
    size_t totalShardsRecieved;
    size_t totalShardsCompleted;
    Record** results;
    Record** errors;
    volatile ExecutionPlanStatus status;
    ExecutionFlags flags;
    OnDoneData* onDoneData; // Array of callbacks to run on done
    RedisGears_ExecutionOnStartCallback onStartCallback;
    void* executionPD;
    long long executionDuration;
    WorkerData* assignWorker;
    ExecutionMode mode;
    Gears_listNode* nodeOnExecutionsList;
}ExecutionPlan;

typedef struct FlatBasicStep{
    char* stepName;
    ExecutionStepArg arg;
}FlatBasicStep;

typedef struct FlatExecutionStep{
    FlatBasicStep bStep;
    enum StepType type;
}FlatExecutionStep;

typedef struct FlatExecutionReader{
    char* reader;
}FlatExecutionReader;

#define EXECUTION_POOL_SIZE 1
typedef struct FlatExecutionPlan{
    char id[EXECUTION_PLAN_ID_LEN];
    char idStr[EXECUTION_PLAN_STR_ID_LEN];
    char* desc;
    size_t refCount;
    FlatExecutionReader* reader;
    FlatExecutionStep* steps;
    void* PD;
    char* PDType;
    ExecutionPlan* executionPool[EXECUTION_POOL_SIZE];
    size_t executionPoolSize;
    Gears_Buffer* serializedFep;
    FlatBasicStep onExecutionStartStep;
}FlatExecutionPlan;

typedef struct ExecutionCtx{
    RedisModuleCtx* rctx;
    ExecutionPlan* ep;
    char* err;
}ExecutionCtx;

#define ExecutionCtx_Initialize(c, e) (ExecutionCtx){ \
        .rctx = c,\
        .ep = e,\
        .err = NULL,\
    }

FlatExecutionPlan* FlatExecutionPlan_New();
void FlatExecutionPlan_AddToRegisterDict(FlatExecutionPlan* fep);
void FlatExecutionPlan_RemoveFromRegisterDict(FlatExecutionPlan* fep);
const char* FlatExecutionPlan_Serialize(FlatExecutionPlan* fep, size_t* len);
FlatExecutionPlan* FlatExecutionPlan_Deserialize(const char* data, size_t len);
bool FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader);
void FlatExecutionPlan_SetPrivateData(FlatExecutionPlan* fep, const char* type, void* PD);
void FlatExecutionPlan_SetDesc(FlatExecutionPlan* fep, const char* desc);
void FlatExecutionPlan_AddForEachStep(FlatExecutionPlan* fep, char* forEach, void* writerArg);
void FlatExecutionPlan_SetOnStartStep(FlatExecutionPlan* fep, char* onStartCallback, void* onStartArg);
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
int FlatExecutionPlan_Register(FlatExecutionPlan* fep, ExecutionMode mode, void* key);
const char* FlatExecutionPlan_GetReader(FlatExecutionPlan* fep);
ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData);
long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep);
long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep);
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);

void ExecutionPlan_Initialize(size_t numberOfworkers);
void ExecutionPlan_SendFreeMsg(ExecutionPlan* ep);
void ExecutionPlan_Free(ExecutionPlan* ep);

int ExecutionPlan_DumpRegistrations(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_InnerUnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_UnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_InnerRegister(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_ExecutionGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
ExecutionPlan* ExecutionPlan_FindById(const char* id);
ExecutionPlan* ExecutionPlan_FindByStrId(const char* id);
Reader* ExecutionPlan_GetReader(ExecutionPlan* ep);

#endif /* SRC_EXECUTION_PLAN_H_ */
