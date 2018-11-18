/*
 * redispark.h
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

#ifndef SRC_REDISTAR_H_
#define SRC_REDISTAR_H_

#include <stdbool.h>
#include "redismodule.h"

#define MODULE_API_FUNC(x) (*x)

typedef struct ExecutionPlan ExecutionPlan;
typedef struct FlatExecutionPlan FlatExecutionPlan;
typedef struct Record Record;

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

typedef struct BufferWriter BufferWriter;
typedef struct BufferReader BufferReader;

typedef struct ArgType ArgType;

typedef void (*ArgFree)(void* arg);
typedef void (*ArgSerialize)(void* arg, BufferWriter* bw);
typedef void* (*ArgDeserialize)(BufferReader* br);

typedef struct Reader{
    void* ctx;
    Record* (*Next)(RedisModuleCtx* rctx, void* ctx);
    void (*free)(void* ctx);
    void (*serialize)(void* ctx, BufferWriter* bw);
    void (*deserialize)(void* ctx, BufferReader* br);
}Reader;

Reader* KeysReader(void* arg);

/******************************* Writers *******************************/
void KeyRecordWriter(RedisModuleCtx* rctx, Record *data, void* arg, char** err);

/******************************* args *********************************/

ArgType* MODULE_API_FUNC(RediStar_CreateType)(char* name, ArgFree free, ArgSerialize serialize, ArgDeserialize deserialize);
void MODULE_API_FUNC(RediStar_BWWriteLong)(BufferWriter* bw, long val);
void MODULE_API_FUNC(RediStar_BWWriteString)(BufferWriter* bw, char* str);
void MODULE_API_FUNC(RediStar_BWWriteBuffer)(BufferWriter* bw, char* buff, size_t len);
long MODULE_API_FUNC(RediStar_BRReadLong)(BufferReader* br);
char* MODULE_API_FUNC(RediStar_BRReadString)(BufferReader* br);
char* MODULE_API_FUNC(RediStar_BRReadBuffer)(BufferReader* br, size_t* len);

/******************************* Filters *******************************/

/******************************* Mappers *******************************/
Record* GetValueMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err);

/******************************* GroupByExtractors *********************/
char* KeyRecordStrValueExtractor(RedisModuleCtx* rctx, Record *data, void* arg, size_t* len, char** err);

/******************************* GroupByReducers ***********************/
Record* CountReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err);


typedef void (*RediStar_OnExecutionDoneCallback)(ExecutionPlan* ctx, void* privateData);
typedef Reader* (*RediStar_ReaderCallback)(void* arg);
typedef void (*RediStar_WriterCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef Record* (*RediStar_MapCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef bool (*RediStar_FilterCallback)(RedisModuleCtx* rctx, Record *data, void* arg, char** err);
typedef char* (*RediStar_ExtractorCallback)(RedisModuleCtx* rctx, Record *data, void* arg, size_t* len, char** err);
typedef Record* (*RediStar_ReducerCallback)(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err);

typedef struct KeysReaderCtx KeysReaderCtx;
KeysReaderCtx* MODULE_API_FUNC(RediStar_KeysReaderCtxCreate)(char* match);

void MODULE_API_FUNC(RediStar_FreeRecord)(Record* record);
enum RecordType MODULE_API_FUNC(RediStar_RecordGetType)(Record* r);
Record* MODULE_API_FUNC(RediStar_KeyRecordCreate)();
void MODULE_API_FUNC(RediStar_KeyRecordSetKey)(Record* r, char* key, size_t len);
void MODULE_API_FUNC(RediStar_KeyRecordSetVal)(Record* r, Record* val);
Record* MODULE_API_FUNC(RediStar_KeyRecordGetVal)(Record* r);
char* MODULE_API_FUNC(RediStar_KeyRecordGetKey)(Record* r, size_t* len);
Record* MODULE_API_FUNC(RediStar_ListRecordCreate)(size_t initSize);
size_t MODULE_API_FUNC(RediStar_ListRecordLen)(Record* listRecord);
void MODULE_API_FUNC(RediStar_ListRecordAdd)(Record* listRecord, Record* r);
Record* MODULE_API_FUNC(RediStar_ListRecordGet)(Record* listRecord, size_t index);
Record* MODULE_API_FUNC(RediStar_ListRecordPop)(Record* listRecord);
Record* MODULE_API_FUNC(RediStar_StringRecordCreate)(char* val);
char* MODULE_API_FUNC(RediStar_StringRecordGet)(Record* r);
void MODULE_API_FUNC(RediStar_StringRecordSet)(Record* r, char* val);
Record* MODULE_API_FUNC(RediStar_DoubleRecordCreate)(double val);
double MODULE_API_FUNC(RediStar_DoubleRecordGet)(Record* r);
void MODULE_API_FUNC(RediStar_DoubleRecordSet)(Record* r, double val);
Record* MODULE_API_FUNC(RediStar_LongRecordCreate)(long val);
long MODULE_API_FUNC(RediStar_LongRecordGet)(Record* r);
void MODULE_API_FUNC(RediStar_LongRecordSet)(Record* r, long val);
Record* MODULE_API_FUNC(RediStar_KeyHandlerRecordCreate)(RedisModuleKey* handler);
RedisModuleKey* MODULE_API_FUNC(RediStar_KeyHandlerRecordGet)(Record* r);
Record* MODULE_API_FUNC(RediStar_HashSetRecordCreate)();
int MODULE_API_FUNC(RediStar_HashSetRecordSet)(Record* r, char* key, Record* val);
Record* MODULE_API_FUNC(RediStar_HashSetRecordGet)(Record* r, char* key);
char** MODULE_API_FUNC(RediStar_HashSetRecordGetAllKeys)(Record* r, size_t* len);
void MODULE_API_FUNC(RediStar_HashSetRecordFreeKeysArray)(char** keyArr);

int MODULE_API_FUNC(RediStar_RegisterReader)(char* name, RediStar_ReaderCallback reader);
int MODULE_API_FUNC(RediStar_RegisterWriter)(char* name, RediStar_WriterCallback reader, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterMap)(char* name, RediStar_MapCallback map, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterFilter)(char* name, RediStar_FilterCallback filter, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterGroupByExtractor)(char* name, RediStar_ExtractorCallback extractor, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterReducer)(char* name, RediStar_ReducerCallback reducer, ArgType* type);

#define RSM_RegisterReader(name) RediStar_RegisterReader(#name, name);
#define RSM_RegisterMap(name, type) RediStar_RegisterMap(#name, name, type);
#define RSM_RegisterFilter(name, type) RediStar_RegisterFilter(#name, name, type);
#define RSM_RegisterWriter(name, type) RediStar_RegisterWriter(#name, name, type);
#define RSM_RegisterGroupByExtractor(name, type) RediStar_RegisterGroupByExtractor(#name, name, type);
#define RSM_RegisterReducer(name, type) RediStar_RegisterReducer(#name, name, type);

/**
 * Create an execution plan with the given reader.
 * It is possible to continue adding operation such as map, filter, group by, and so on using the return context.
 */
FlatExecutionPlan* MODULE_API_FUNC(RediStar_CreateCtx)(char* name, char* readerName);
#define RSM_CreateCtx(name, readerName) RediStar_CreateCtx(name, #readerName)

/******************************* Execution plan operations *******************************/

int MODULE_API_FUNC(RediStar_Map)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Map(ctx, name, arg) RediStar_Map(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_Filter)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Filter(ctx, name, arg) RediStar_Filter(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_GroupBy)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg);
#define RSM_GroupBy(ctx, extractor, extractorArg, reducer, reducerArg)\
    RediStar_GroupBy(ctx, #extractor, extractorArg, #reducer, reducerArg)

int MODULE_API_FUNC(RediStar_Collect)(FlatExecutionPlan* ctx);
#define RSM_Collect(ctx) RediStar_Collect(ctx)

int MODULE_API_FUNC(RediStar_Repartition)(FlatExecutionPlan* ctx, char* extraxtorName, void* extractorArg);
#define RSM_Repartition(ctx, extractor, extractorArg) RediStar_Repartition(ctx, #extractor, extractorArg)

int MODULE_API_FUNC(RediStar_FlatMap)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_FlatMap(ctx, name, arg) RediStar_FlatMap(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_Limit)(FlatExecutionPlan* ctx, size_t offset, size_t len);
#define RSM_Limit(ctx, offset, len) RediStar_Limit(ctx, offset, len)
/******************************* Execution plan runners *******************************/

/*
 * There are three ways to start running an execution plan
 * 1. Write in using a writer
 * 2. just run it, store the result inside the redis memory (not in the key space) and later read it.
 */
ExecutionPlan* MODULE_API_FUNC(RediStar_Run)(FlatExecutionPlan* ctx, void* arg, RediStar_OnExecutionDoneCallback callback, void* privateData);
#define RSM_Run(ctx, arg, callback, privateData) RediStar_Run(ctx, arg, callback, privateData)

int MODULE_API_FUNC(RediStar_Write)(FlatExecutionPlan* ctx, char* name, void* arg);
#define RSM_Write(ctx, name, arg) RediStar_Write(ctx, #name, arg)

typedef void (*FreePrivateData)(void* privateData);

bool MODULE_API_FUNC(RediStar_RegisterExecutionDoneCallback)(ExecutionPlan* ctx, RediStar_OnExecutionDoneCallback callback);
bool MODULE_API_FUNC(RediStar_IsDone)(ExecutionPlan* ctx);
long long MODULE_API_FUNC(RediStar_GetRecordsLen)(ExecutionPlan* ctx);
void* MODULE_API_FUNC(RediStar_GetPrivateData)(ExecutionPlan* ctx);
void MODULE_API_FUNC(RediStar_SetPrivateData)(ExecutionPlan* ctx, void* privateData, FreePrivateData freeCallback);
const char* MODULE_API_FUNC(RediStar_GetId)(ExecutionPlan* ctx);
Record* MODULE_API_FUNC(RediStar_GetRecord)(ExecutionPlan* ctx, long long i);
ExecutionPlan* MODULE_API_FUNC(RediStar_GetExecution)(const char* id);
FlatExecutionPlan* MODULE_API_FUNC(RediStar_GetFlatExecution)(const char* name);
void MODULE_API_FUNC(RediStar_DropExecution)(ExecutionPlan* starCtx, RedisModuleCtx* ctx);
void MODULE_API_FUNC(RediStar_DropFlatExecution)(FlatExecutionPlan* starCtx, RedisModuleCtx* ctx);

#define REDISTAR_MODULE_INIT_FUNCTION(name) \
        if (RedisModule_GetApi("RediStar_" #name, ((void **)&RediStar_ ## name))) { \
            printf("could not initialize RediStar_" #name "\r\n");\
            return false; \
        }

static bool RediStar_Initialize(){
    REDISTAR_MODULE_INIT_FUNCTION(CreateType);
    REDISTAR_MODULE_INIT_FUNCTION(BWWriteLong);
    REDISTAR_MODULE_INIT_FUNCTION(BWWriteString);
    REDISTAR_MODULE_INIT_FUNCTION(BWWriteBuffer);
    REDISTAR_MODULE_INIT_FUNCTION(BRReadLong);
    REDISTAR_MODULE_INIT_FUNCTION(BRReadString);
    REDISTAR_MODULE_INIT_FUNCTION(BRReadBuffer);

    REDISTAR_MODULE_INIT_FUNCTION(RegisterReader);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterWriter);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterMap);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterFilter);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterGroupByExtractor);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterReducer);
    REDISTAR_MODULE_INIT_FUNCTION(CreateCtx);
    REDISTAR_MODULE_INIT_FUNCTION(Map);
    REDISTAR_MODULE_INIT_FUNCTION(Filter);
    REDISTAR_MODULE_INIT_FUNCTION(Run);
    REDISTAR_MODULE_INIT_FUNCTION(Write);
    REDISTAR_MODULE_INIT_FUNCTION(GroupBy);
    REDISTAR_MODULE_INIT_FUNCTION(Collect);
    REDISTAR_MODULE_INIT_FUNCTION(Repartition);
    REDISTAR_MODULE_INIT_FUNCTION(FlatMap);
    REDISTAR_MODULE_INIT_FUNCTION(Limit);

    REDISTAR_MODULE_INIT_FUNCTION(GetExecution);
    REDISTAR_MODULE_INIT_FUNCTION(GetFlatExecution);
    REDISTAR_MODULE_INIT_FUNCTION(IsDone);
    REDISTAR_MODULE_INIT_FUNCTION(GetRecordsLen);
    REDISTAR_MODULE_INIT_FUNCTION(GetRecord);
    REDISTAR_MODULE_INIT_FUNCTION(RegisterExecutionDoneCallback);
    REDISTAR_MODULE_INIT_FUNCTION(GetPrivateData);
	REDISTAR_MODULE_INIT_FUNCTION(SetPrivateData);
	REDISTAR_MODULE_INIT_FUNCTION(DropExecution);
	REDISTAR_MODULE_INIT_FUNCTION(GetId);

    REDISTAR_MODULE_INIT_FUNCTION(FreeRecord);
    REDISTAR_MODULE_INIT_FUNCTION(RecordGetType);
    REDISTAR_MODULE_INIT_FUNCTION(KeyRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(KeyRecordSetKey);
    REDISTAR_MODULE_INIT_FUNCTION(KeyRecordSetVal);
    REDISTAR_MODULE_INIT_FUNCTION(KeyRecordGetVal);
    REDISTAR_MODULE_INIT_FUNCTION(KeyRecordGetKey);
    REDISTAR_MODULE_INIT_FUNCTION(ListRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(ListRecordLen);
    REDISTAR_MODULE_INIT_FUNCTION(ListRecordAdd);
    REDISTAR_MODULE_INIT_FUNCTION(ListRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(ListRecordPop);
    REDISTAR_MODULE_INIT_FUNCTION(StringRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(StringRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(StringRecordSet);
    REDISTAR_MODULE_INIT_FUNCTION(DoubleRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(DoubleRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(DoubleRecordSet);
    REDISTAR_MODULE_INIT_FUNCTION(LongRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(LongRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(LongRecordSet);
    REDISTAR_MODULE_INIT_FUNCTION(KeyHandlerRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(KeyHandlerRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(HashSetRecordCreate);
    REDISTAR_MODULE_INIT_FUNCTION(HashSetRecordSet);
    REDISTAR_MODULE_INIT_FUNCTION(HashSetRecordGet);
    REDISTAR_MODULE_INIT_FUNCTION(HashSetRecordGetAllKeys);
    REDISTAR_MODULE_INIT_FUNCTION(HashSetRecordFreeKeysArray);

    return true;
}

#endif /* SRC_REDISTAR_H_ */
