/*
 * execution_plan.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#pragma once

#include "redisgears.h"
#include "commands.h"
#include "utils/dict.h"
#include "utils/adlist.h"
#include "utils/buffer.h"

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
typedef struct SessionRegistrationCtx SessionRegistrationCtx;

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

typedef struct ExecutionPendingCtx ExecutionPendingCtx;

typedef struct StepPendingCtx{
    int refCount;
    size_t maxSize;
    Gears_list* records;
    size_t stepId;
    ExecutionPendingCtx* epctx;
}StepPendingCtx;

typedef struct ExecutionPendingCtx{
    int refCount;
    char id[ID_LEN];
    size_t len;
    StepPendingCtx** pendingCtxs;
    WorkerData* assignWorker;
}ExecutionPendingCtx;

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
    StepPendingCtx* pendingCtx;
    bool isDone;
}ExecutionStep;

typedef enum ActionResult{
    CONTINUE, STOP, STOP_WITHOUT_TIMEOUT, COMPLETED
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
    X(ABORTED, "aborted", EPStatus_AbortedAction)

typedef enum ExecutionPlanStatus{
#define X(a, b, c) a,
    EXECUTION_PLAN_STATUSES
#undef X
}ExecutionPlanStatus;

typedef enum WorkerStatus{
    WorkerStatus_Running, WorkerStatus_ShuttingDown
}WorkerStatus;

typedef struct WorkerData{
    size_t refCount;
    Gears_list* notifications;
    pthread_mutex_t lock;
    RedisModuleCtx* ctx;
    WorkerStatus status;
    ExecutionThreadPool* pool;
}WorkerData;

#define ExecutionFlags int
#define EFDone 0x01
#define EFIsOnDoneCallback 0x02
#define EFIsFreedOnDoneCallback 0x04
#define EFSentRunRequest 0x08
#define EFIsLocal 0x10
#define EFIsLocalyFreedOnDoneCallback 0x20
#define EFStarted 0x40
#define EFWaiting 0x80

#define EPTurnOnFlag(ep, f) (ep->flags |= f)
#define EPTurnOffFlag(ep, f) (ep->flags &= ~f)
#define EPIsFlagOn(ep, f) (ep->flags & f)
#define EPIsFlagOff(ep, f) (!(ep->flags & f))

#define FlatExecutionFlags int
#define FEFRegistered 0x01

#define FEPTurnOnFlag(fep, f) fep->flags |= f
#define FEPTurnOffFlag(fep, f) fep->flags &= ~f
#define FEPIsFlagOn(fep, f) (fep->flags & f)
#define FEPIsFlagOff(fep, f) (!(fep->flags & f))

typedef struct ExecutionCallbacData{
    RedisGears_ExecutionCallback callback;
    void* pd;
}ExecutionCallbacData;

typedef struct ExecutionPlan{
    char id[ID_LEN];
    char idStr[STR_ID_LEN];
    ExecutionStep** steps;
    FlatExecutionPlan* fep;
    size_t totalShardsRecieved;
    size_t totalShardsCompleted;
    Record** results;
    Record** errors;
    volatile ExecutionPlanStatus status;
    ExecutionFlags flags;
    RunFlags runFlags;
    ExecutionCallbacData* onDoneData; // Array of callbacks to run on done
    RedisGears_ExecutionOnStartCallback onStartCallback;
    RedisGears_ExecutionOnUnpausedCallback onUnpausedCallback;
    ExecutionCallbacData* runCallbacks;
    ExecutionCallbacData* holdCallbacks;
    void* executionPD;
    long long executionDuration;
    WorkerData* assignWorker;
    ExecutionMode mode;
    Gears_listNode* nodeOnExecutionsList;
    volatile bool isPaused;
    RedisModuleTimerID maxIdleTimer;
    bool maxIdleTimerSet;
    bool registered;
    StepPendingCtx** pendingCtxs;
    AbortCallback abort;
    void* abortPD;
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

typedef struct RegistrationData {
    FlatExecutionPlan *fep;
    void* args;
    ExecutionMode mode;
} RegistrationData;

#define SESSION_REGISTRATION_OP_CODE_UNREGISTER 1
#define SESSION_REGISTRATION_OP_CODE_REGISTER 2
#define SESSION_REGISTRATION_OP_CODE_SESSION_UNLINK 3
#define SESSION_REGISTRATION_OP_CODE_SESSION_DESERIALIZE 4
#define SESSION_REGISTRATION_OP_CODE_DONE 5
typedef struct SessionRegistrationCtx {
    int requireDeserialization;
    Plugin *p;
    char **sessionsToUnlink;
    char **idsToUnregister;
    void *usedSession;
    RegistrationData *registrationsData;
    Gears_Buffer* buff;
    Gears_BufferWriter bw;
    long long maxIdle;
} SessionRegistrationCtx;

#define EXECUTION_POOL_SIZE 1
typedef struct FlatExecutionPlan{
    char id[ID_LEN];
    char idStr[STR_ID_LEN];
    char* desc;
    size_t refCount;
    FlatExecutionReader* reader;
    FlatExecutionStep* steps;
    void* PD;
    char* PDType;
    ExecutionPlan* executionPool[EXECUTION_POOL_SIZE];
    size_t executionPoolSize;
    Gears_Buffer* serializedFep;

    /* Call on each shard when execution created from this FEP start to run for the first time */
    FlatBasicStep onExecutionStartStep;

    /* Call on each shard when execution is registered, also when it registered when loaded from rdb or aof */
    FlatBasicStep onRegisteredStep;

    /* Call on each shard when execution unregistered */
    FlatBasicStep onUnregisteredStep;

    /* Call on each shard when execution is unpaused, pause could happened because
     * the execution is waiting for results from other shards or because the execution
     * is waiting for records that are async processed by the user (maybe on another thread
     * or process)
     */
    FlatBasicStep onUnpausedStep;

    long long executionMaxIdleTime;
    ExecutionThreadPool* executionThreadPool;
    FlatExecutionFlags flags;
}FlatExecutionPlan;

typedef struct ExecutionCtx{
    RedisModuleCtx* rctx;
    ExecutionPlan* ep;
    ExecutionStep* step;
    char* err;
    Record* originRecord;
    Record** actualPlaceHolder;
    Record* asyncRecordCreated;
}ExecutionCtx;

#define ExecutionCtx_Initialize(c, e, s) (ExecutionCtx){ \
        .rctx = c,\
        .ep = e,\
        .step = s,\
        .err = NULL,\
        .originRecord = NULL, \
        .actualPlaceHolder = NULL, \
        .asyncRecordCreated = NULL, \
    }

extern char* statusesNames[];
extern char* stepsNames[];

#define DURATION2MS(d)  (long long)(d/(long long)1000000)

FlatExecutionPlan* FlatExecutionPlan_New();
void FlatExecutionPlan_AddToRegisterDict(FlatExecutionPlan* fep);
void FlatExecutionPlan_RemoveFromRegisterDict(FlatExecutionPlan* fep);
int FlatExecutionPlan_Serialize(Gears_BufferWriter* bw, FlatExecutionPlan* fep, char** err);
FlatExecutionPlan* FlatExecutionPlan_Deserialize(Gears_BufferReader* br, char** err, int encver);
bool FlatExecutionPlan_SetReader(FlatExecutionPlan* fep, char* reader);
void FlatExecutionPlan_SetPrivateData(FlatExecutionPlan* fep, const char* type, void* PD);
void* FlatExecutionPlan_GetPrivateData(FlatExecutionPlan* fep);
void FlatExecutionPlan_SetDesc(FlatExecutionPlan* fep, const char* desc);
void FlatExecutionPlan_AddForEachStep(FlatExecutionPlan* fep, char* forEach, void* writerArg);
void FlatExecutionPlan_SetOnStartStep(FlatExecutionPlan* fep, const char* onStartCallback, void* onStartArg);
void FlatExecutionPlan_SetOnUnPausedStep(FlatExecutionPlan* fep, const char* onSUnpausedCallback, void* onUnpausedArg);
void FlatExecutionPlan_SetOnRegisteredStep(FlatExecutionPlan* fep, const char* onRegisteredCallback, void* onRegisteredArg);
void FlatExecutionPlan_SetOnUnregisteredStep(FlatExecutionPlan* fep, const char* onRegisteredCallback, void* onRegisteredArg);
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
int FlatExecutionPlan_PrepareForRegister(SessionRegistrationCtx *srctx, FlatExecutionPlan* fep, ExecutionMode mode, void* args, char** err);
void FlatExecutionPlan_AddRegistrationToUnregister(SessionRegistrationCtx *srctx, const char *id);
void FlatExecutionPlan_AddSessionToUnlink(SessionRegistrationCtx *srctx, const char *id);
Record* FlatExecutionPlane_RegistrationCtxUpgrade(ExecutionCtx* rctx, Record *data, void* arg);
void FlatExecutionPlan_Register(SessionRegistrationCtx *srctx);
const char* FlatExecutionPlan_GetReader(FlatExecutionPlan* fep);
ExecutionPlan* FlatExecutionPlan_Run(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err, RunFlags runFlags);
long long FlatExecutionPlan_GetExecutionDuration(ExecutionPlan* ep);
long long FlatExecutionPlan_GetReadDuration(ExecutionPlan* ep);
void FlatExecutionPlan_Free(FlatExecutionPlan* fep);
void FlatExecutionPlan_SetThreadPool(FlatExecutionPlan* fep, ExecutionThreadPool* tp);
FlatExecutionPlan* FlatExecutionPlan_DeepCopy(FlatExecutionPlan* fep);

void ExecutionPlan_Initialize();
void ExecutionPlan_SendFreeMsg(ExecutionPlan* ep);
void ExecutionPlan_Free(ExecutionPlan* ep);

void ExecutionPlan_DumpSingleRegistration(RedisModuleCtx *ctx, FlatExecutionPlan* fep, int flags);
int ExecutionPlan_DumpRegistrations(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
size_t ExecutionPlan_NRegistrations();
size_t ExecutionPlan_NExecutions();
void ExecutionPlan_InfoRegistrations(RedisModuleInfoCtx *ctx, int for_crash_report);
int ExecutionPlan_InnerUnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_UnregisterExecution(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_ExecutionsDump(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ExecutionPlan_InnerRegister(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
FlatExecutionPlan* FlatExecutionPlan_FindByStrId(const char* id);
ExecutionPlan* ExecutionPlan_FindById(const char* id);
ExecutionPlan* ExecutionPlan_FindByStrId(const char* id);
Reader* ExecutionPlan_GetReader(ExecutionPlan* ep);

ExecutionThreadPool* ExectuionPlan_GetThreadPool(const char* name);
ExecutionThreadPool* ExecutionPlan_CreateThreadPool(const char* name, size_t numOfThreads);
ExecutionThreadPool* ExecutionPlan_DefineThreadPool(const char* name, void* poolCtx, ExecutionPoolAddJob addJob);

WorkerData* ExecutionPlan_CreateWorker(ExecutionThreadPool* pool);
WorkerData* ExecutionPlan_WorkerGetShallowCopy(WorkerData* wd);
void ExecutionPlan_FreeWorker(WorkerData* wd);

void ExecutionPlan_Clean();

StepPendingCtx* ExecutionPlan_PendingCtxGetShallowCopy(StepPendingCtx* pctx);
void ExecutionPlan_PendingCtxFree(StepPendingCtx* pctx);
StepPendingCtx* ExecutionPlan_PendingCtxCreate(ExecutionPlan* ep, ExecutionStep* step, size_t maxSize);

SessionRegistrationCtx* SessionRegistrationCtx_Create();
SessionRegistrationCtx* SessionRegistrationCtx_CreateFromBuff(const char *buff, size_t len);
void SessionRegistrationCtx_Free(SessionRegistrationCtx* srctx);
