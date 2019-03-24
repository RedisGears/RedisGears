/*
 * redispark.h
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

enum RecordType{
    KEY_HANDLER_RECORD = 1,
    LONG_RECORD,
    DOUBLE_RECORD,
    STRING_RECORD,
    LIST_RECORD,
    KEY_RECORD,
    HASH_SET_RECORD,
};

/******************************* READERS *******************************/

typedef struct Gears_BufferWriter Gears_BufferWriter;
typedef struct Gears_BufferReader Gears_BufferReader;

typedef struct ArgType ArgType;

typedef void (*ArgFree)(void* arg);
typedef void* (*ArgDuplicate)(void* arg);
typedef void (*ArgSerialize)(void* arg, Gears_BufferWriter* bw);
typedef void* (*ArgDeserialize)(Gears_BufferReader* br);
typedef void (*RegisterTrigger)(FlatExecutionPlan* fep);

typedef struct Reader{
    void* ctx;
    void (*registerTrigger)(FlatExecutionPlan* fep, void* arg);
    Record* (*next)(RedisModuleCtx* rctx, void* ctx);
    void (*free)(void* ctx);
    void (*serialize)(void* ctx, Gears_BufferWriter* bw);
    void (*deserialize)(void* ctx, Gears_BufferReader* br);
}Reader;

Reader* KeysReader(void* arg);
Reader* StreamReader(void* arg);
StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId);

/******************************* Writers *******************************/
void KeyRecordWriter(RedisModuleCtx* rctx, Record *data, void* arg, char** err);

/******************************* args *********************************/

ArgType* MODULE_API_FUNC(RedisGears_CreateType)(char* name, ArgFree free, ArgDuplicate dup, ArgSerialize serialize, ArgDeserialize deserialize);
void MODULE_API_FUNC(RedisGears_BWWriteLong)(Gears_BufferWriter* bw, long val);
void MODULE_API_FUNC(RedisGears_BWWriteString)(Gears_BufferWriter* bw, char* str);
void MODULE_API_FUNC(RedisGears_BWWriteBuffer)(Gears_BufferWriter* bw, char* buff, size_t len);
long MODULE_API_FUNC(RedisGears_BRReadLong)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadString)(Gears_BufferReader* br);
char* MODULE_API_FUNC(RedisGears_BRReadBuffer)(Gears_BufferReader* br, size_t* len);

/******************************* Filters *******************************/

/******************************* Mappers *******************************/
Record* GetValueMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err);

/******************************* GroupByExtractors *********************/
char* KeyRecordStrValueExtractor(RedisModuleCtx* rctx, Record *data, void* arg, size_t* len, char** err);

/******************************* GroupByReducers ***********************/
Record* CountReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err);


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

void MODULE_API_FUNC(RedisGears_FreeRecord)(Record* record);
enum RecordType MODULE_API_FUNC(RedisGears_RecordGetType)(Record* r);
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

#define RSM_RegisterReader(name) RedisGears_RegisterReader(#name, name);
#define RSM_RegisterMap(name, type) RedisGears_RegisterMap(#name, name, type);
#define RSM_RegisterAccumulator(name, type) RedisGears_RegisterAccumulator(#name, name, type);
#define RSM_RegisterAccumulatorByKey(name, type) RedisGears_RegisterAccumulatorByKey(#name, name, type);
#define RSM_RegisterFilter(name, type) RedisGears_RegisterFilter(#name, name, type);
#define RSM_RegisterForEach(name, type) RedisGears_RegisterForEach(#name, name, type);
#define RSM_RegisterGroupByExtractor(name, type) RedisGears_RegisterGroupByExtractor(#name, name, type);
#define RSM_RegisterReducer(name, type) RedisGears_RegisterReducer(#name, name, type);

/**
 * Create an execution plan with the given reader.
 * It is possible to continue adding operation such as map, filter, group by, and so on using the return context.
 */
FlatExecutionPlan* MODULE_API_FUNC(RedisGears_CreateCtx)(char* readerName);
#define RSM_CreateCtx(readerName) RedisGears_CreateCtx(#readerName)

/******************************* Execution plan operations *******************************/

int MODULE_API_FUNC(RedisGears_Map)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Map(ctx, name, arg) RedisGears_Map(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Accumulate)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Accumulate(ctx, name, arg) RedisGears_Accumulate(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Filter)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Filter(ctx, name, arg) RedisGears_Filter(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_GroupBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg);
#define RSM_GroupBy(ctx, extractor, extractorArg, reducer, reducerArg)\
    RedisGears_GroupBy(ctx, #extractor, extractorArg, #reducer, reducerArg)

int MODULE_API_FUNC(RedisGears_AccumulateBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* accumulateName, void* accumulateArg);
#define RSM_AccumulateBy(ctx, extractor, extractorArg, accumulate, accumulateArg)\
		RedisGears_AccumulateBy(ctx, #extractor, extractorArg, #accumulate, accumulateArg)

int MODULE_API_FUNC(RedisGears_LocalAccumulateBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* accumulateName, void* accumulateArg);
#define RSM_LocalAccumulateBy(ctx, extractor, extractorArg, accumulate, accumulateArg)\
		RedisGears_LocalAccumulateBy(ctx, #extractor, extractorArg, #accumulate, accumulateArg)

int MODULE_API_FUNC(RedisGears_Collect)(FlatExecutionPlan* ctx);
#define RSM_Collect(ctx) RedisGears_Collect(ctx)

int MODULE_API_FUNC(RedisGears_Repartition)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg);
#define RSM_Repartition(ctx, extractor, extractorArg) RedisGears_Repartition(ctx, #extractor, extractorArg)

int MODULE_API_FUNC(RedisGears_FlatMap)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_FlatMap(ctx, name, arg) RedisGears_FlatMap(ctx, #name, arg)

int MODULE_API_FUNC(RedisGears_Limit)(FlatExecutionPlan* ctx, size_t offset, size_t len);
#define RSM_Limit(ctx, offset, len) RedisGears_Limit(ctx, offset, len)

ExecutionPlan* MODULE_API_FUNC(RedisGears_Run)(FlatExecutionPlan* ctx, void* arg, RedisGears_OnExecutionDoneCallback callback, void* privateData);
#define RSM_Run(ctx, arg, callback, privateData) RedisGears_Run(ctx, arg, callback, privateData)

int MODULE_API_FUNC(RedisGears_Register)(FlatExecutionPlan* fep, char* arg);
#define RSM_Register(ctx, arg) RedisGears_Register(ctx, arg)

int MODULE_API_FUNC(RedisGears_ForEach)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_ForEach(ctx, name, arg) RedisGears_ForEach(ctx, #name, arg)

const char* MODULE_API_FUNC(RedisGears_GetReader)(FlatExecutionPlan* fep);

StreamReaderCtx* MODULE_API_FUNC(RedisGears_StreamReaderCtxCreate)(const char* streamName, const char* streamId);

typedef void (*FreePrivateData)(void* privateData);

bool MODULE_API_FUNC(RedisGears_RegisterExecutionDoneCallback)(ExecutionPlan* ctx, RedisGears_OnExecutionDoneCallback callback);
bool MODULE_API_FUNC(RedisGears_IsDone)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RedisGears_GetRecordsLen)(ExecutionPlan* ctx);
void* MODULE_API_FUNC(RedisGears_GetPrivateData)(ExecutionPlan* ctx);
void MODULE_API_FUNC(RedisGears_SetPrivateData)(ExecutionPlan* ctx, void* privateData, FreePrivateData freeCallback);
const char* MODULE_API_FUNC(RedisGears_GetId)(ExecutionPlan* ctx);
Record* MODULE_API_FUNC(RedisGears_GetRecord)(ExecutionPlan* ctx, long long i);
ExecutionPlan* MODULE_API_FUNC(RedisGears_GetExecution)(const char* id);
void MODULE_API_FUNC(RedisGears_DropExecution)(ExecutionPlan* starCtx);
long long MODULE_API_FUNC(RedisGears_GetTotalDuration)(ExecutionPlan* starCtx);
long long MODULE_API_FUNC(RedisGears_GetReadDuration)(ExecutionPlan* starCtx);
void MODULE_API_FUNC(RedisGears_FreeFlatExecution)(FlatExecutionPlan* starCtx);

int MODULE_API_FUNC(RedisGears_GetLLApiVersion)();

#define REDISLAMBDA_MODULE_INIT_FUNCTION(name) \
        if (RedisModule_GetApi("RedisGears_" #name, ((void **)&RedisGears_ ## name))) { \
            printf("could not initialize RedisGears_" #name "\r\n");\
            return REDISMODULE_ERR; \
        }

static int RedisGears_Initialize(){
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetLLApiVersion);

    REDISLAMBDA_MODULE_INIT_FUNCTION(CreateType);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BWWriteLong);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BWWriteString);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BWWriteBuffer);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BRReadLong);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BRReadString);
    REDISLAMBDA_MODULE_INIT_FUNCTION(BRReadBuffer);

    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterReader);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterForEach);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterAccumulator);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterAccumulatorByKey);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterMap);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterFilter);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterGroupByExtractor);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterReducer);
    REDISLAMBDA_MODULE_INIT_FUNCTION(CreateCtx);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Map);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Accumulate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(AccumulateBy);
    REDISLAMBDA_MODULE_INIT_FUNCTION(LocalAccumulateBy);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Filter);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Run);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Register);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ForEach);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GroupBy);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Collect);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Repartition);
    REDISLAMBDA_MODULE_INIT_FUNCTION(FlatMap);
    REDISLAMBDA_MODULE_INIT_FUNCTION(Limit);
    REDISLAMBDA_MODULE_INIT_FUNCTION(FreeFlatExecution);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetReader);
    REDISLAMBDA_MODULE_INIT_FUNCTION(StreamReaderCtxCreate);

    REDISLAMBDA_MODULE_INIT_FUNCTION(GetExecution);
    REDISLAMBDA_MODULE_INIT_FUNCTION(IsDone);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetRecordsLen);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetRecord);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RegisterExecutionDoneCallback);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetPrivateData);
	REDISLAMBDA_MODULE_INIT_FUNCTION(SetPrivateData);
	REDISLAMBDA_MODULE_INIT_FUNCTION(DropExecution);
	REDISLAMBDA_MODULE_INIT_FUNCTION(GetId);

    REDISLAMBDA_MODULE_INIT_FUNCTION(FreeRecord);
    REDISLAMBDA_MODULE_INIT_FUNCTION(RecordGetType);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyRecordSetKey);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyRecordSetVal);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyRecordGetVal);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyRecordGetKey);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ListRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ListRecordLen);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ListRecordAdd);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ListRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(ListRecordPop);
    REDISLAMBDA_MODULE_INIT_FUNCTION(StringRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(StringRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(StringRecordSet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(DoubleRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(DoubleRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(DoubleRecordSet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(LongRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(LongRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(LongRecordSet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyHandlerRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(KeyHandlerRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(HashSetRecordCreate);
    REDISLAMBDA_MODULE_INIT_FUNCTION(HashSetRecordSet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(HashSetRecordGet);
    REDISLAMBDA_MODULE_INIT_FUNCTION(HashSetRecordGetAllKeys);
    REDISLAMBDA_MODULE_INIT_FUNCTION(HashSetRecordFreeKeysArray);

    REDISLAMBDA_MODULE_INIT_FUNCTION(GetTotalDuration);
    REDISLAMBDA_MODULE_INIT_FUNCTION(GetReadDuration);



    if(RedisGears_GetLLApiVersion() < REDISGEARS_LLAPI_VERSION){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

#endif /* SRC_REDISGEARG_H_ */
