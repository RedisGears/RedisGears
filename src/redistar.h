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

typedef struct RediStarCtx RediStarCtx;
typedef struct Record Record;

enum RecordType{
    KEY_HANDLER_RECORD = 1,
    LONG_RECORD,
    DOUBLE_RECORD,
    STRING_RECORD,
    LIST_RECORD,
    KEY_RECORD,
};

/******************************* READERS *******************************/

typedef struct Reader{
    void* ctx;
    Record* (*Next)(RedisModuleCtx* rctx, void* ctx);
}Reader;

Reader* KeysReader(void* arg);

/******************************* Writers *******************************/

typedef struct Writer{
    void* ctx;
    void (*Start)(RedisModuleCtx* rctx, void* ctx);
    void (*Write)(RedisModuleCtx* rctx, void* ctx, Record* record);
    void (*Done)(RedisModuleCtx* rctx, void* ctx);
}Writer;

Writer* ReplyWriter(void* arg);

/******************************* args *********************************/
typedef struct BufferWriter BufferWriter;
typedef struct BufferReader BufferReader;

typedef void (*ArgFree)(void* arg);
typedef void (*ArgSerialize)(void* arg, BufferWriter* bw);
typedef void* (*ArgDeserialize)(BufferReader* br);

typedef struct ArgType ArgType;

ArgType* MODULE_API_FUNC(RediStar_CreateType)(char* name, ArgFree free, ArgSerialize serialize, ArgDeserialize deserialize);
void MODULE_API_FUNC(RediStar_BWWriteLong)(BufferWriter* bw, long val);
void MODULE_API_FUNC(RediStar_BWWriteString)(BufferWriter* bw, char* str);
void MODULE_API_FUNC(RediStar_BWWriteBuffer)(BufferWriter* bw, char* buff, size_t len);
long MODULE_API_FUNC(RediStar_BRReadLong)(BufferReader* br);
char* MODULE_API_FUNC(RediStar_BRReadString)(BufferReader* br);
char* MODULE_API_FUNC(RediStar_BRReadBuffer)(BufferReader* br, size_t* len);

/******************************* Filters *******************************/

/******************************* Mappers *******************************/

/******************************* GroupByExtractors *********************/
char* KeyRecordStrValueExtractor(RedisModuleCtx* rctx, Record *data, void* arg, size_t* len, char** err);

/******************************* GroupByReducers ***********************/
Record* CountReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err);


typedef void (*RediStar_OnExecutionDoneCallback)(RediStarCtx* ctx, void* privateData);
typedef Reader* (*RediStar_ReaderCallback)(void* arg);
typedef Writer* (*RediStar_WriterCallback)(void* arg);
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

int MODULE_API_FUNC(RediStar_RegisterReader)(char* name, RediStar_ReaderCallback reader, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterWriter)(char* name, RediStar_WriterCallback reader, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterMap)(char* name, RediStar_MapCallback map, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterFilter)(char* name, RediStar_FilterCallback filter, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterGroupByExtractor)(char* name, RediStar_ExtractorCallback extractor, ArgType* type);
int MODULE_API_FUNC(RediStar_RegisterReducer)(char* name, RediStar_ReducerCallback reducer, ArgType* type);

#define RSM_RegisterReader(name, type) RediStar_RegisterReader(#name, name, type);
#define RSM_RegisterMap(name, type) RediStar_RegisterMap(#name, name, type);
#define RSM_RegisterFilter(name, type) RediStar_RegisterFilter(#name, name, type);
#define RSM_RegisterWriter(name, type) RediStar_RegisterWriter(#name, name, type);
#define RSM_RegisterGroupByExtractor(name, type) RediStar_RegisterGroupByExtractor(#name, name, type);
#define RSM_RegisterReducer(name, type) RediStar_RegisterReducer(#name, name, type);

/**
 * Create an execution plan with the given reader.
 * It is possible to continue adding operation such as map, filter, group by, and so on using the return context.
 */
RediStarCtx* MODULE_API_FUNC(RediStar_CreateCtx)(char* name, char* readerName, void* arg);
#define RSM_CreateCtx(name, readerName, arg) RediStar_CreateCtx(name, #readerName, arg)

/******************************* Execution plan operations *******************************/

int MODULE_API_FUNC(RediStar_Map)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Map(ctx, name, arg) RediStar_Map(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_Filter)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Filter(ctx, name, arg) RediStar_Filter(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_GroupBy)(RediStarCtx* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg);
#define RSM_GroupBy(ctx, extractor, extractorArg, reducer, reducerArg)\
    RediStar_GroupBy(ctx, #extractor, extractorArg, #reducer, reducerArg)

int MODULE_API_FUNC(RediStar_Collect)(RediStarCtx* ctx);
#define RSM_Collect(ctx) RediStar_Collect(ctx)

/******************************* Execution plan runners *******************************/

/*
 * There are three ways to start running an execution plan
 * 1. Write in using a writer
 * 2. just run it, store the result inside the redis memory (not in the key space) and later read it.
 */
int MODULE_API_FUNC(RediStar_Run)(RediStarCtx* ctx, RediStar_OnExecutionDoneCallback callback, void* privateData);
#define RSM_Run(ctx, callback, privateData) RediStar_Run(ctx, callback, privateData)

int MODULE_API_FUNC(RediStar_Write)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Write(ctx, name, arg) RediStar_Write(ctx, #name, arg)

#define PROXY_MODULE_INIT_FUNCTION(name) \
        if (RedisModule_GetApi("RediStar_" #name, ((void **)&RediStar_ ## name))) { \
            printf("could not initialize RediStar_" #name "\r\n");\
            return false; \
        }

static bool RediStar_Initialize(){
    PROXY_MODULE_INIT_FUNCTION(CreateType);
    PROXY_MODULE_INIT_FUNCTION(BWWriteLong);
    PROXY_MODULE_INIT_FUNCTION(BWWriteString);
    PROXY_MODULE_INIT_FUNCTION(BWWriteBuffer);
    PROXY_MODULE_INIT_FUNCTION(BRReadLong);
    PROXY_MODULE_INIT_FUNCTION(BRReadString);
    PROXY_MODULE_INIT_FUNCTION(BRReadBuffer);

    PROXY_MODULE_INIT_FUNCTION(RegisterReader);
    PROXY_MODULE_INIT_FUNCTION(RegisterWriter);
    PROXY_MODULE_INIT_FUNCTION(RegisterMap);
    PROXY_MODULE_INIT_FUNCTION(RegisterFilter);
    PROXY_MODULE_INIT_FUNCTION(RegisterGroupByExtractor);
    PROXY_MODULE_INIT_FUNCTION(RegisterReducer);
    PROXY_MODULE_INIT_FUNCTION(CreateCtx);
    PROXY_MODULE_INIT_FUNCTION(Map);
    PROXY_MODULE_INIT_FUNCTION(Filter);
    PROXY_MODULE_INIT_FUNCTION(Run);
    PROXY_MODULE_INIT_FUNCTION(Write);
    PROXY_MODULE_INIT_FUNCTION(GroupBy);
    PROXY_MODULE_INIT_FUNCTION(Collect);

    PROXY_MODULE_INIT_FUNCTION(FreeRecord);
    PROXY_MODULE_INIT_FUNCTION(RecordGetType);
    PROXY_MODULE_INIT_FUNCTION(KeyRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(KeyRecordSetKey);
    PROXY_MODULE_INIT_FUNCTION(KeyRecordSetVal);
    PROXY_MODULE_INIT_FUNCTION(KeyRecordGetVal);
    PROXY_MODULE_INIT_FUNCTION(KeyRecordGetKey);
    PROXY_MODULE_INIT_FUNCTION(ListRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(ListRecordLen);
    PROXY_MODULE_INIT_FUNCTION(ListRecordAdd);
    PROXY_MODULE_INIT_FUNCTION(ListRecordGet);
    PROXY_MODULE_INIT_FUNCTION(StringRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(StringRecordGet);
    PROXY_MODULE_INIT_FUNCTION(StringRecordSet);
    PROXY_MODULE_INIT_FUNCTION(DoubleRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(DoubleRecordGet);
    PROXY_MODULE_INIT_FUNCTION(DoubleRecordSet);
    PROXY_MODULE_INIT_FUNCTION(LongRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(LongRecordGet);
    PROXY_MODULE_INIT_FUNCTION(LongRecordSet);
    PROXY_MODULE_INIT_FUNCTION(KeyHandlerRecordCreate);
    PROXY_MODULE_INIT_FUNCTION(KeyHandlerRecordGet);

    return true;
}

#endif /* SRC_REDISTAR_H_ */
