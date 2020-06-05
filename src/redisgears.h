/*
 * redisgears.h
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

#ifndef SRC_REDISGEARG_H_
#define SRC_REDISGEARG_H_

#include <stdbool.h>
#include "redismodule.h"
#include "utils/arr_rm_alloc.h"

#define REDISGEARS_LLAPI_VERSION 1

#define MODULE_API_FUNC(x) (*x)

/**
 * Arr(type) need to be created with array_new and free with array_free macros included from "utils/arr.h"
 */
#define Arr(x) x*

/*
 * Opaque sturcts
 */
typedef struct ExecutionPlan ExecutionPlan;
typedef struct ExecutionCtx ExecutionCtx;
typedef struct FlatExecutionPlan FlatExecutionPlan;
typedef struct Record Record;
typedef struct WorkerData WorkerData;
typedef struct ExecutionThreadPool ExecutionThreadPool;

/**
 * Execttion modes:
 * 1. ExecutionModeSync       - execution will run on the same thread that trigger it
 *                              also implies that the execution is local and not distributed
 *
 * 2. ExecutionModeAsync      - execution will run in another thread
 *
 * 3. ExecutionModeAsyncLocal - execition will run in an async thread but will still be local
 *                              to the current shard
 */
#define ExecutionMode int
#define ExecutionModeSync 1
#define ExecutionModeAsync 2
#define ExecutionModeAsyncLocal 3

/**
 * On execution failure policies (relevent only for stream reader)
 */
#define OnFailedPolicy int
#define OnFailedPolicyUnknown 0
#define OnFailedPolicyContinue 1
#define OnFailedPolicyAbort 2
#define OnFailedPolicyRetry 3

/******************************* READERS *******************************/

typedef struct Gears_BufferWriter Gears_BufferWriter;
typedef struct Gears_BufferReader Gears_BufferReader;
typedef struct ArgType ArgType;

/**
 * Arguments callbacks definition
 */
typedef void (*ArgFree)(void* arg);
typedef void* (*ArgDuplicate)(void* arg);
typedef int (*ArgSerialize)(void* arg, Gears_BufferWriter* bw, char** err);
typedef void* (*ArgDeserialize)(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err);
typedef char* (*ArgToString)(void* arg);

/**
 * Reader instance definition. There is a single reader instance for each execution
 */
typedef struct Reader{
    void* ctx;
    Record* (*next)(ExecutionCtx* rctx, void* ctx);
    void (*free)(void* ctx);
    void (*reset)(void* ctx, void * arg);
    void (*serialize)(void* ctx, Gears_BufferWriter* bw);
    void (*deserialize)(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br);
}Reader;

/**
 * Default readers to use
 */
#define KeysReader KeysReader
#define StreamReader StreamReader
#define CommandReader CommandReader
#define ShardIDReader ShardIDReader

/**
 * Create a new argument type with the given name and callbacks.
 */
ArgType* MODULE_API_FUNC(RedisGears_CreateType)(char* name, int version, ArgFree free, ArgDuplicate dup, ArgSerialize serialize, ArgDeserialize deserialize, ArgToString tostring);

/**
 * Function that allows to read/write from buffers. Use when implementing serialize/deserialize
 */
#define LONG_READ_ERROR LLONG_MAX
#define BUFF_READ_ERROR NULL

void MODULE_API_FUNC(RedisGears_BWWriteLong)(Gears_BufferWriter* bw, long val);
void MODULE_API_FUNC(RedisGears_BWWriteString)(Gears_BufferWriter* bw, const char* str);
void MODULE_API_FUNC(RedisGears_BWWriteBuffer)(Gears_BufferWriter* bw, const char* buff, size_t len);
long MODULE_API_FUNC(RedisGears_BRReadLong)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadString)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadBuffer)(Gears_BufferReader* br, size_t* len);

/**
 * On done callback definition
 */
typedef void (*RedisGears_OnExecutionDoneCallback)(ExecutionPlan* ctx, void* privateData);
typedef void (*RedisGears_ExecutionOnStartCallback)(ExecutionCtx* ctx, void* arg);
typedef void (*RedisGears_ExecutionOnUnpausedCallback)(ExecutionCtx* ctx, void* arg);
typedef void (*RedisGears_FlatExecutionOnRegisteredCallback)(FlatExecutionPlan* fep, void* arg);

/**
 * Reader callbacks definition.
 */
typedef Reader* (*RedisGears_CreateReaderCallback)(void* arg);
typedef int (*RedisGears_ReaderRegisterCallback)(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, char** err);
typedef void (*RedisGears_ReaderUnregisterCallback)(FlatExecutionPlan* fep, bool abortPending);
typedef void (*RedisGears_ReaderSerializeRegisterArgsCallback)(void* arg, Gears_BufferWriter* bw);
typedef void* (*RedisGears_ReaderDeserializeRegisterArgsCallback)(Gears_BufferReader* br);
typedef void (*RedisGears_ReaderFreeArgsCallback)(void* args);
typedef void (*RedisGears_ReaderDumpRegistrationData)(RedisModuleCtx* ctx, FlatExecutionPlan* fep);
typedef void (*RedisGears_ReaderRdbSave)(RedisModuleIO *rdb);
typedef void (*RedisGears_ReaderRdbLoad)(RedisModuleIO *rdb, int encver);
typedef void (*RedisGears_ReaderClear)();

/**
 * callbacks for reader implementation.
 * registerTrigger and unregisterTrigger are promissed to be called when the GIL is acquired
 * while create might be called any time.
 */
typedef struct RedisGears_ReaderCallbacks{
    RedisGears_CreateReaderCallback create;
    RedisGears_ReaderRegisterCallback registerTrigger;
    RedisGears_ReaderUnregisterCallback unregisterTrigger;
    RedisGears_ReaderSerializeRegisterArgsCallback serializeTriggerArgs;
    RedisGears_ReaderDeserializeRegisterArgsCallback deserializeTriggerArgs;
    RedisGears_ReaderFreeArgsCallback freeTriggerArgs;
    RedisGears_ReaderDumpRegistrationData dumpRegistratioData;
    RedisGears_ReaderRdbSave rdbSave;
    RedisGears_ReaderRdbLoad rdbLoad;
    RedisGears_ReaderClear clear;
}RedisGears_ReaderCallbacks;


/**
 * Operations/Steps callbacks definition
 */
typedef void (*RedisGears_ForEachCallback)(ExecutionCtx* rctx, Record *data, void* arg);
typedef Record* (*RedisGears_MapCallback)(ExecutionCtx* rctx, Record *data, void* arg);
typedef bool (*RedisGears_FilterCallback)(ExecutionCtx* rctx, Record *data, void* arg);
typedef char* (*RedisGears_ExtractorCallback)(ExecutionCtx* rctx, Record *data, void* arg, size_t* len);
typedef Record* (*RedisGears_ReducerCallback)(ExecutionCtx* rctx, char* key, size_t keyLen, Record *records, void* arg);
typedef Record* (*RedisGears_AccumulateCallback)(ExecutionCtx* rctx, Record *accumulate, Record *r, void* arg);
typedef Record* (*RedisGears_AccumulateByKeyCallback)(ExecutionCtx* rctx, char* key, Record *accumulate, Record *r, void* arg);

/**
 * Reader ctx definition
 */
typedef struct KeysReaderCtx KeysReaderCtx;
typedef struct StreamReaderCtx StreamReaderCtx;
typedef struct StreamReaderTriggerArgs StreamReaderTriggerArgs;
typedef struct KeysReaderTriggerArgs KeysReaderTriggerArgs;
typedef struct CommandReaderTriggerArgs CommandReaderTriggerArgs;

StreamReaderCtx* MODULE_API_FUNC(RedisGears_StreamReaderCtxCreate)(const char* streamName, const char* streamId);
void MODULE_API_FUNC(RedisGears_StreamReaderCtxFree)(StreamReaderCtx*);

KeysReaderCtx* MODULE_API_FUNC(RedisGears_KeysReaderCtxCreate)(const char* match, bool readValue, const char* event, bool noScan);
void MODULE_API_FUNC(RedisGears_KeysReaderCtxFree)(KeysReaderCtx*);

StreamReaderTriggerArgs* MODULE_API_FUNC(RedisGears_StreamReaderTriggerArgsCreate)(const char* prefix, size_t batchSize, size_t durationMS, OnFailedPolicy onFailedPolicy, size_t retryInterval, bool trimStream);
void MODULE_API_FUNC(RedisGears_StreamReaderTriggerArgsFree)(StreamReaderTriggerArgs* args);

KeysReaderTriggerArgs* MODULE_API_FUNC(RedisGears_KeysReaderTriggerArgsCreate)(const char* prefix, Arr(char*) eventTypes, Arr(int) keyTypes, bool readValue);
void MODULE_API_FUNC(RedisGears_KeysReaderTriggerArgsFree)(KeysReaderTriggerArgs* args);

CommandReaderTriggerArgs* MODULE_API_FUNC(RedisGears_CommandReaderTriggerArgsCreate)(const char* trigger);
void MODULE_API_FUNC(RedisGears_CommandReaderTriggerArgsFree)(CommandReaderTriggerArgs* args);

/**
 * Records handling
 */

typedef struct RecordType RecordType;

typedef struct Record{
    RecordType* type;
}Record;

extern RecordType* listRecordType;
extern RecordType* stringRecordType;
extern RecordType* errorRecordType;
extern RecordType* longRecordType;
extern RecordType* doubleRecordType;
extern RecordType* keyRecordType;
extern RecordType* keysHandlerRecordType;
extern RecordType* hashSetRecordType;

typedef int (*RecordSendReply)(Record* record, RedisModuleCtx* rctx);
typedef int (*RecordSerialize)(Gears_BufferWriter* bw, Record* base, char** err);
typedef Record* (*RecordDeserialize)(Gears_BufferReader* br);
typedef void (*RecordFree)(Record* base);

RecordType* MODULE_API_FUNC(RedisGears_RecordTypeCreate)(const char* name, size_t size,
                                                         RecordSendReply,
                                                         RecordSerialize,
                                                         RecordDeserialize,
                                                         RecordFree);
Record*  MODULE_API_FUNC(RedisGears_RecordCreate)(RecordType* type);

void MODULE_API_FUNC(RedisGears_FreeRecord)(Record* record);
RecordType* MODULE_API_FUNC(RedisGears_RecordGetType)(Record* r);
Record* MODULE_API_FUNC(RedisGears_KeyRecordCreate)();
void MODULE_API_FUNC(RedisGears_KeyRecordSetKey)(Record* r, char* key, size_t len);
void MODULE_API_FUNC(RedisGears_KeyRecordSetVal)(Record* r, Record* val);
Record* MODULE_API_FUNC(RedisGears_KeyRecordGetVal)(Record* r);
char* MODULE_API_FUNC(RedisGears_KeyRecordGetKey)(Record* r, size_t* len);
Record* MODULE_API_FUNC(RedisGears_ListRecordCreate)(size_t initSize);
size_t MODULE_API_FUNC(RedisGears_ListRecordLen)(Record* listRecord);
void MODULE_API_FUNC(RedisGears_ListRecordAdd)(Record* listRecord, Record* r);
Record* MODULE_API_FUNC(RedisGears_ListRecordGet)(Record* listRecord, size_t index);
Record* MODULE_API_FUNC(RedisGears_ListRecordPop)(Record* listRecord);
Record* MODULE_API_FUNC(RedisGears_StringRecordCreate)(char* val, size_t len);
char* MODULE_API_FUNC(RedisGears_StringRecordGet)(Record* r, size_t* len);
void MODULE_API_FUNC(RedisGears_StringRecordSet)(Record* r, char* val, size_t len);
Record* MODULE_API_FUNC(RedisGears_DoubleRecordCreate)(double val);
double MODULE_API_FUNC(RedisGears_DoubleRecordGet)(Record* r);
void MODULE_API_FUNC(RedisGears_DoubleRecordSet)(Record* r, double val);
Record* MODULE_API_FUNC(RedisGears_LongRecordCreate)(long val);
long MODULE_API_FUNC(RedisGears_LongRecordGet)(Record* r);
void MODULE_API_FUNC(RedisGears_LongRecordSet)(Record* r, long val);
Record* MODULE_API_FUNC(RedisGears_KeyHandlerRecordCreate)(RedisModuleKey* handler);
RedisModuleKey* MODULE_API_FUNC(RedisGears_KeyHandlerRecordGet)(Record* r);
Record* MODULE_API_FUNC(RedisGears_HashSetRecordCreate)();
int MODULE_API_FUNC(RedisGears_HashSetRecordSet)(Record* r, char* key, Record* val);
Record* MODULE_API_FUNC(RedisGears_HashSetRecordGet)(Record* r, char* key);
Arr(char*) MODULE_API_FUNC(RedisGears_HashSetRecordGetAllKeys)(Record* r);

/**
 * Register operations functions
 */
int MODULE_API_FUNC(RedisGears_RegisterFlatExecutionPrivateDataType)(ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterReader)(char* name, RedisGears_ReaderCallbacks* callbacks);
int MODULE_API_FUNC(RedisGears_RegisterForEach)(char* name, RedisGears_ForEachCallback reader, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterMap)(char* name, RedisGears_MapCallback map, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterAccumulator)(char* name, RedisGears_AccumulateCallback accumulator, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterAccumulatorByKey)(char* name, RedisGears_AccumulateByKeyCallback accumulator, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterFilter)(char* name, RedisGears_FilterCallback filter, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterGroupByExtractor)(char* name, RedisGears_ExtractorCallback extractor, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterReducer)(char* name, RedisGears_ReducerCallback reducer, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterExecutionOnStartCallback)(char* name, RedisGears_ExecutionOnStartCallback callback, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterExecutionOnUnpausedCallback)(char* name, RedisGears_ExecutionOnUnpausedCallback callback, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterFlatExecutionOnRegisteredCallback)(char* name, RedisGears_FlatExecutionOnRegisteredCallback callback, ArgType* type);

#define RGM_RegisterReader(name) RedisGears_RegisterReader(#name, &name);
#define RGM_RegisterMap(name, type) RedisGears_RegisterMap(#name, name, type);
#define RGM_RegisterAccumulator(name, type) RedisGears_RegisterAccumulator(#name, name, type);
#define RGM_RegisterAccumulatorByKey(name, type) RedisGears_RegisterAccumulatorByKey(#name, name, type);
#define RGM_RegisterFilter(name, type) RedisGears_RegisterFilter(#name, name, type);
#define RGM_RegisterForEach(name, type) RedisGears_RegisterForEach(#name, name, type);
#define RGM_RegisterGroupByExtractor(name, type) RedisGears_RegisterGroupByExtractor(#name, name, type);
#define RGM_RegisterReducer(name, type) RedisGears_RegisterReducer(#name, name, type);
#define RGM_RegisterExecutionOnStartCallback(name, type) RedisGears_RegisterExecutionOnStartCallback(#name, name, type);
#define RGM_RegisterExecutionOnUnpausedCallback(name, type) RedisGears_RegisterExecutionOnUnpausedCallback(#name, name, type);
#define RGM_RegisterFlatExecutionOnRegisteredCallback(name, type) RedisGears_RegisterFlatExecutionOnRegisteredCallback(#name, name, type);

/**
 * Create flat execution plan with the given reader.
 * It is possible to continue adding operation such as map, filter, group by, and so on using the return context.
 */
FlatExecutionPlan* MODULE_API_FUNC(RedisGears_CreateCtx)(char* readerName);
int MODULE_API_FUNC(RedisGears_SetDesc)(FlatExecutionPlan* ctx, const char* desc);
void MODULE_API_FUNC(RedisGears_SetMaxIdleTime)(FlatExecutionPlan* fep, long long executionMaxIdleTime);
#define RGM_CreateCtx(readerName) RedisGears_CreateCtx(#readerName)

/**
 * Private data will be available on the following location:
 * 1. Serialize and Deserialize of the flat execution steps arguments
 * 2. Execution of each of the flat execution steps
 *
 * The assumption is that the private data might be big, on python for example the
 * private data holds the entire python requirements. And so, follow this assumption
 * the private data is not kept in its serialized form like the rest of the flat
 * execution. Instead each time the execution serialized we call the serialize function
 * of the private data type.
 *
 * Notica that this means that serialization of private data must be consistant and must produce
 * the same outcome each time it called.
 */
void MODULE_API_FUNC(RedisGears_SetFlatExecutionPrivateData)(FlatExecutionPlan* fep, const char* type, void* PD);
void* MODULE_API_FUNC(RedisGears_GetFlatExecutionPrivateDataFromFep)(FlatExecutionPlan* fep);

/**
 * On register, each execution that will be created, will fire this callback on start.
 */
int MODULE_API_FUNC(RedisGears_SetFlatExecutionOnStartCallback)(FlatExecutionPlan* fep, const char* callback, void* arg);
#define RGM_SetFlatExecutionOnStartCallback(ctx, name, arg) RedisGears_SetFlatExecutionOnStartCallback(ctx, #name, arg)

/**
 * If registration is distributed it might paused and unpaused durring the run.
 * This callback will be called each time it unpaused.
 */
int MODULE_API_FUNC(RedisGears_SetFlatExecutionOnUnpausedCallback)(FlatExecutionPlan* fep, const char* callback, void* arg);
#define RGM_SetFlatExecutionOnUnpausedCallback(ctx, name, arg) RedisGears_SetFlatExecutionOnUnpausedCallback(ctx, #name, arg)

/**
 * Will be fire on each shard right after registration finished
 */
int MODULE_API_FUNC(RedisGears_SetFlatExecutionOnRegisteredCallback)(FlatExecutionPlan* fep, const char* callback, void* arg);
#define RGM_SetFlatExecutionOnRegisteredCallback(ctx, name, arg) RedisGears_SetFlatExecutionOnRegisteredCallback(ctx, #name, arg)

/******************************* Flat Execution plan operations *******************************/

int MODULE_API_FUNC(RedisGears_Map)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_Map(ctx, name, arg) RedisGears_Map(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Accumulate)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_Accumulate(ctx, name, arg) RedisGears_Accumulate(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Filter)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_Filter(ctx, name, arg) RedisGears_Filter(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_GroupBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg);
#define RGM_GroupBy(ctx, extractor, extractorArg, reducer, reducerArg)\
    RedisGears_GroupBy(ctx, #extractor, extractorArg, #reducer, reducerArg)

int MODULE_API_FUNC(RedisGears_AccumulateBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* accumulateName, void* accumulateArg);
#define RGM_AccumulateBy(ctx, extractor, extractorArg, accumulate, accumulateArg)\
		RedisGears_AccumulateBy(ctx, #extractor, extractorArg, #accumulate, accumulateArg)

int MODULE_API_FUNC(RedisGears_LocalAccumulateBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* accumulateName, void* accumulateArg);
#define RGM_LocalAccumulateBy(ctx, extractor, extractorArg, accumulate, accumulateArg)\
		RedisGears_LocalAccumulateBy(ctx, #extractor, extractorArg, #accumulate, accumulateArg)

int MODULE_API_FUNC(RedisGears_Collect)(FlatExecutionPlan* ctx);
#define RGM_Collect(ctx) RedisGears_Collect(ctx)

int MODULE_API_FUNC(RedisGears_Repartition)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg);
#define RGM_Repartition(ctx, extractor, extractorArg) RedisGears_Repartition(ctx, #extractor, extractorArg)

int MODULE_API_FUNC(RedisGears_FlatMap)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_FlatMap(ctx, name, arg) RedisGears_FlatMap(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Limit)(FlatExecutionPlan* ctx, size_t offset, size_t len);
#define RGM_Limit(ctx, offset, len) RedisGears_Limit(ctx, offset, len)

ExecutionPlan* MODULE_API_FUNC(RedisGears_Run)(FlatExecutionPlan* ctx, ExecutionMode mode, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData, WorkerData* worker, char** err);
#define RGM_Run(ctx, mode, arg, callback, privateData, err) RedisGears_Run(ctx, mode, arg, callback, privateData, NULL, err)

int MODULE_API_FUNC(RedisGears_Register)(FlatExecutionPlan* fep, ExecutionMode mode, void* arg, char** err);
#define RGM_Register(ctx, mode, arg, err) RedisGears_Register(ctx, mode, arg, err)

int MODULE_API_FUNC(RedisGears_ForEach)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_ForEach(ctx, name, arg) RedisGears_ForEach(ctx, #name, arg)

const char* MODULE_API_FUNC(RedisGears_GetReader)(FlatExecutionPlan* fep);

void MODULE_API_FUNC(RedisGears_DropLocalyOnDone)(ExecutionPlan* ctx, void* privateData);

typedef void (*FreePrivateData)(void* privateData);

/******************************* Execution plan operations *******************************/

bool MODULE_API_FUNC(RedisGears_IsDone)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RedisGears_GetRecordsLen)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RedisGears_GetErrorsLen)(ExecutionPlan* ctx);
const char* MODULE_API_FUNC(RedisGears_GetId)(ExecutionPlan* ctx);
Record* MODULE_API_FUNC(RedisGears_GetRecord)(ExecutionPlan* ctx, long long i);
Record* MODULE_API_FUNC(RedisGears_GetError)(ExecutionPlan* ctx, long long i);
ExecutionPlan* MODULE_API_FUNC(RedisGears_GetExecution)(const char* id);

/**
 * Will drop the execution once the execution is done
 */
void MODULE_API_FUNC(RedisGears_DropExecution)(ExecutionPlan* gearsCtx);

/**
 * Abort a running or created (and not yet started) execution
 *
 * Currently we can only abort a local execution.
 * Aborting a global execution can only be done via RG.ABORTEXECUTION command
 *
 * return REDISMODULE_OK if the execution was aborted and REDISMODULE_ERR otherwise
 */
int MODULE_API_FUNC(RedisGears_AbortExecution)(ExecutionPlan* gearsCtx);

long long MODULE_API_FUNC(RedisGears_GetTotalDuration)(ExecutionPlan* gearsCtx);
long long MODULE_API_FUNC(RedisGears_GetReadDuration)(ExecutionPlan* gearsCtx);
void MODULE_API_FUNC(RedisGears_FreeFlatExecution)(FlatExecutionPlan* gearsCtx);

void MODULE_API_FUNC(RedisGears_SetError)(ExecutionCtx* ectx, char* err);
RedisModuleCtx* MODULE_API_FUNC(RedisGears_GetRedisModuleCtx)(ExecutionCtx* ectx);
void* MODULE_API_FUNC(RedisGears_GetFlatExecutionPrivateData)(ExecutionCtx* ectx);
void* MODULE_API_FUNC(RedisGears_GetPrivateData)(ExecutionCtx* ectx);
void MODULE_API_FUNC(RedisGears_SetPrivateData)(ExecutionCtx* ctx, void* PD);

bool MODULE_API_FUNC(RedisGears_AddOnDoneCallback)(ExecutionPlan* ep, RedisGears_OnExecutionDoneCallback callback, void* privateData);

const char* MODULE_API_FUNC(RedisGears_GetMyHashTag)();

ExecutionThreadPool* MODULE_API_FUNC(RedisGears_ExecutionThreadPoolCreate)(const char* name, size_t numOfThreads);
WorkerData* MODULE_API_FUNC(RedisGears_WorkerDataCreate)(ExecutionThreadPool* pool);
void MODULE_API_FUNC(RedisGears_WorkerDataFree)(WorkerData* worker);
WorkerData* MODULE_API_FUNC(RedisGears_WorkerDataGetShallowCopy)(WorkerData* worker);

const char* MODULE_API_FUNC(RedisGears_GetCompiledOs)();
int MODULE_API_FUNC(RedisGears_GetLLApiVersion)();

void MODULE_API_FUNC(RedisGears_ReturnResultsAndErrors)(ExecutionPlan* ep, RedisModuleCtx *ctx);

#define REDISGEARS_MODULE_INIT_FUNCTION(ctx, name) \
        RedisGears_ ## name = RedisModule_GetSharedAPI(ctx, "RedisGears_" #name);\
        if(!RedisGears_ ## name){\
            RedisModule_Log(ctx, "warning", "could not initialize RedisGears_" #name "\r\n");\
            return REDISMODULE_ERR; \
        }

static int RedisGears_Initialize(RedisModuleCtx* ctx){
    if(!RedisModule_GetSharedAPI){
        RedisModule_Log(ctx, "warning", "redis version is not compatible with module shared api, use redis 5.0.4 or above.");
        return REDISMODULE_ERR;
    }

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetLLApiVersion);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, CreateType);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BWWriteLong);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BWWriteString);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BWWriteBuffer);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BRReadLong);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BRReadString);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, BRReadBuffer);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterReader);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterForEach);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterAccumulator);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterAccumulatorByKey);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterMap);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterFilter);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterGroupByExtractor);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterReducer);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, CreateCtx);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetDesc);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetMaxIdleTime);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterFlatExecutionPrivateDataType);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetFlatExecutionPrivateData);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetFlatExecutionPrivateDataFromFep);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Map);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Accumulate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, AccumulateBy);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, LocalAccumulateBy);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Filter);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Run);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Register);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ForEach);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GroupBy);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Collect);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Repartition);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, FlatMap);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, Limit);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, FreeFlatExecution);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetReader);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StreamReaderCtxCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StreamReaderCtxFree);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeysReaderCtxCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeysReaderCtxFree);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StreamReaderTriggerArgsCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StreamReaderTriggerArgsFree);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeysReaderTriggerArgsCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeysReaderTriggerArgsFree);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, CommandReaderTriggerArgsCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, CommandReaderTriggerArgsFree);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetExecution);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, IsDone);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetRecordsLen);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetErrorsLen);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetRecord);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetError);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, DropExecution);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetId);

	REDISGEARS_MODULE_INIT_FUNCTION(ctx, RecordCreate);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, RecordTypeCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, FreeRecord);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RecordGetType);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyRecordSetKey);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyRecordSetVal);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyRecordGetVal);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyRecordGetKey);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ListRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ListRecordLen);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ListRecordAdd);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ListRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ListRecordPop);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StringRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StringRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, StringRecordSet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, DoubleRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, DoubleRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, DoubleRecordSet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, LongRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, LongRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, LongRecordSet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyHandlerRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeyHandlerRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, HashSetRecordCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, HashSetRecordSet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, HashSetRecordGet);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, HashSetRecordGetAllKeys);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, AddOnDoneCallback);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetTotalDuration);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetReadDuration);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetError);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetRedisModuleCtx);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetFlatExecutionPrivateData);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetPrivateData);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetPrivateData);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetFlatExecutionOnStartCallback);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetFlatExecutionOnRegisteredCallback);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterExecutionOnStartCallback);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterExecutionOnUnpausedCallback);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterFlatExecutionOnRegisteredCallback);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, DropLocalyOnDone);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetMyHashTag);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ExecutionThreadPoolCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, WorkerDataCreate);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, WorkerDataFree);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, WorkerDataGetShallowCopy);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, ReturnResultsAndErrors);

    if(RedisGears_GetLLApiVersion() < REDISGEARS_LLAPI_VERSION){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

#endif /* SRC_REDISGEARG_H_ */
