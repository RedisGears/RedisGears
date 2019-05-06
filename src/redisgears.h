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

#define REDISGEARS_LLAPI_VERSION 1

#define MODULE_API_FUNC(x) (*x)

typedef struct ExecutionPlan ExecutionPlan;
typedef struct FlatExecutionPlan FlatExecutionPlan;
typedef struct Record Record;
typedef struct StreamReaderCtx StreamReaderCtx;


#define KEY_HANDLER_RECORD_TYPE 1
#define LONG_RECORD_TYPE 2
#define DOUBLE_RECORD_TYPE 3
#define STRING_RECORD_TYPE 4
#define LIST_RECORD_TYPE 5
#define KEY_RECORD_TYPE 6
#define HASH_SET_RECORD_TYPE 7

/******************************* READERS *******************************/

typedef struct Gears_BufferWriter Gears_BufferWriter;
typedef struct Gears_BufferReader Gears_BufferReader;

typedef struct ArgType ArgType;

typedef void (*ArgFree)(void* arg);
typedef void* (*ArgDuplicate)(void* arg);
typedef void (*ArgSerialize)(void* arg, Gears_BufferWriter* bw);
typedef void* (*ArgDeserialize)(Gears_BufferReader* br);
typedef char* (*ArgToString)(void* arg);

typedef struct Reader{
    void* ctx;
    void (*registerTrigger)(FlatExecutionPlan* fep, void* arg);
    Record* (*next)(RedisModuleCtx* rctx, void* ctx);
    void (*free)(void* ctx);
    void (*serialize)(void* ctx, Gears_BufferWriter* bw);
    void (*deserialize)(void* ctx, Gears_BufferReader* br);
}Reader;

#define KeysReader KeysReader
#define StreamReader StreamReader

/******************************* args *********************************/

ArgType* MODULE_API_FUNC(RedisGears_CreateType)(char* name, ArgFree free, ArgDuplicate dup, ArgSerialize serialize, ArgDeserialize deserialize, ArgToString tostring);
void MODULE_API_FUNC(RedisGears_BWWriteLong)(Gears_BufferWriter* bw, long val);
void MODULE_API_FUNC(RedisGears_BWWriteString)(Gears_BufferWriter* bw, char* str);
void MODULE_API_FUNC(RedisGears_BWWriteBuffer)(Gears_BufferWriter* bw, char* buff, size_t len);
long MODULE_API_FUNC(RedisGears_BRReadLong)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadString)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadBuffer)(Gears_BufferReader* br, size_t* len);

/******************************* Filters *******************************/

/******************************* Mappers *******************************/

/******************************* GroupByExtractors *********************/

/******************************* GroupByReducers ***********************/


typedef void (*RedisGears_OnExecutionDoneCallback)(ExecutionPlan* ctx, void* privateData);
typedef Reader* (*RedisGears_ReaderCallback)(void* arg);
typedef void (*RedisGears_ForEachCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef Record* (*RedisGears_MapCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef bool (*RedisGears_FilterCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef char* (*RedisGears_ExtractorCallback)(RedisModuleCtx* rctx, Record *data, void* arg, size_t* len, char** err);
typedef Record* (*RedisGears_ReducerCallback)(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err);
typedef Record* (*RedisGears_AccumulateCallback)(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err);
typedef Record* (*RedisGears_AccumulateByKeyCallback)(RedisModuleCtx* rctx, char* key, Record *accumulate, Record *r, void* arg, char** err);

typedef struct KeysReaderCtx KeysReaderCtx;
KeysReaderCtx* MODULE_API_FUNC(RedisGears_KeysReaderCtxCreate)(char* match);
StreamReaderCtx* MODULE_API_FUNC(RedisGears_StreamReaderCtxCreate)(const char* streamName, const char* streamId);

void MODULE_API_FUNC(RedisGears_FreeRecord)(Record* record);
int MODULE_API_FUNC(RedisGears_RecordGetType)(Record* r);
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
char** MODULE_API_FUNC(RedisGears_HashSetRecordGetAllKeys)(Record* r, size_t* len);
void MODULE_API_FUNC(RedisGears_HashSetRecordFreeKeysArray)(char** keyArr);

int MODULE_API_FUNC(RedisGears_RegisterReader)(char* name, RedisGears_ReaderCallback reader);
int MODULE_API_FUNC(RedisGears_RegisterForEach)(char* name, RedisGears_ForEachCallback reader, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterMap)(char* name, RedisGears_MapCallback map, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterAccumulator)(char* name, RedisGears_AccumulateCallback accumulator, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterAccumulatorByKey)(char* name, RedisGears_AccumulateByKeyCallback accumulator, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterFilter)(char* name, RedisGears_FilterCallback filter, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterGroupByExtractor)(char* name, RedisGears_ExtractorCallback extractor, ArgType* type);
int MODULE_API_FUNC(RedisGears_RegisterReducer)(char* name, RedisGears_ReducerCallback reducer, ArgType* type);

#define RGM_RegisterReader(name) RedisGears_RegisterReader(#name, name);
#define RGM_RegisterMap(name, type) RedisGears_RegisterMap(#name, name, type);
#define RGM_RegisterAccumulator(name, type) RedisGears_RegisterAccumulator(#name, name, type);
#define RGM_RegisterAccumulatorByKey(name, type) RedisGears_RegisterAccumulatorByKey(#name, name, type);
#define RGM_RegisterFilter(name, type) RedisGears_RegisterFilter(#name, name, type);
#define RGM_RegisterForEach(name, type) RedisGears_RegisterForEach(#name, name, type);
#define RGM_RegisterGroupByExtractor(name, type) RedisGears_RegisterGroupByExtractor(#name, name, type);
#define RGM_RegisterReducer(name, type) RedisGears_RegisterReducer(#name, name, type);

/**
 * Create an execution plan with the given reader.
 * It is possible to continue adding operation such as map, filter, group by, and so on using the return context.
 */
FlatExecutionPlan* MODULE_API_FUNC(RedisGears_CreateCtx)(char* readerName);
#define RGM_CreateCtx(readerName) RedisGears_CreateCtx(#readerName)

/******************************* Execution plan operations *******************************/

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

ExecutionPlan* MODULE_API_FUNC(RedisGears_Run)(FlatExecutionPlan* ctx, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData);
#define RGM_Run(ctx, arg, callback, privateData) RedisGears_Run(ctx, arg, callback, privateData)

int MODULE_API_FUNC(RedisGears_Register)(FlatExecutionPlan* fep, char* arg);
#define RGM_Register(ctx, arg) RedisGears_Register(ctx, arg)

int MODULE_API_FUNC(RedisGears_ForEach)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RGM_ForEach(ctx, name, arg) RedisGears_ForEach(ctx, #name, arg)

const char* MODULE_API_FUNC(RedisGears_GetReader)(FlatExecutionPlan* fep);

typedef void (*FreePrivateData)(void* privateData);

bool MODULE_API_FUNC(RedisGears_RegisterExecutionDoneCallback)(ExecutionPlan* ctx, RedisGears_OnExecutionDoneCallback callback);
bool MODULE_API_FUNC(RedisGears_IsDone)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RedisGears_GetRecordsLen)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RedisGears_GetErrorsLen)(ExecutionPlan* ctx);
void* MODULE_API_FUNC(RedisGears_GetPrivateData)(ExecutionPlan* ctx);
void MODULE_API_FUNC(RedisGears_SetPrivateData)(ExecutionPlan* ctx, void* privateData, FreePrivateData freeCallback);
const char* MODULE_API_FUNC(RedisGears_GetId)(ExecutionPlan* ctx);
Record* MODULE_API_FUNC(RedisGears_GetRecord)(ExecutionPlan* ctx, long long i);
Record* MODULE_API_FUNC(RedisGears_GetError)(ExecutionPlan* ctx, long long i);
ExecutionPlan* MODULE_API_FUNC(RedisGears_GetExecution)(const char* id);
void MODULE_API_FUNC(RedisGears_DropExecution)(ExecutionPlan* gearsCtx);
long long MODULE_API_FUNC(RedisGears_GetTotalDuration)(ExecutionPlan* gearsCtx);
long long MODULE_API_FUNC(RedisGears_GetReadDuration)(ExecutionPlan* gearsCtx);
void MODULE_API_FUNC(RedisGears_FreeFlatExecution)(FlatExecutionPlan* gearsCtx);

int MODULE_API_FUNC(RedisGears_GetLLApiVersion)();

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
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, KeysReaderCtxCreate);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetExecution);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, IsDone);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetRecordsLen);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetErrorsLen);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetRecord);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetError);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, RegisterExecutionDoneCallback);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetPrivateData);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, SetPrivateData);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, DropExecution);
	REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetId);

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
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, HashSetRecordFreeKeysArray);

    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetTotalDuration);
    REDISGEARS_MODULE_INIT_FUNCTION(ctx, GetReadDuration);



    if(RedisGears_GetLLApiVersion() < REDISGEARS_LLAPI_VERSION){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

#endif /* SRC_REDISGEARG_H_ */
