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

typedef struct RediStarCtx RediStarCtx;
typedef struct Record Record;

enum RecordType{
    KEY_HANDLER_RECORD,
    LONG_RECORD,
    DOUBLE_RECORD,
    STRING_RECORD,
    LIST_RECORD,
    KEY_RECORD,
#ifdef WITHPYTHON
    PY_RECORD
#endif
};

/******************************* READERS *******************************/

typedef struct Reader{
    void* ctx;
    Record* (*Next)(RedisModuleCtx* rctx, void* ctx);
    void (*Free)(RedisModuleCtx* rctx, void* ctx);
}Reader;

Reader* KeysReader(void* arg);

/******************************* Writers *******************************/

typedef struct Writer{
    void* ctx;
    void (*Start)(RedisModuleCtx* rctx, void* ctx);
    void (*Write)(RedisModuleCtx* rctx, void* ctx, Record* record);
    void (*Done)(RedisModuleCtx* rctx, void* ctx);
    void (*Free)(RedisModuleCtx* rctx, void* ctx);
}Writer;

Writer* ReplyWriter(void* arg);

/******************************* Filters *******************************/
bool TypeFilter(Record *data, void* arg, char** err);

/******************************* Mappers *******************************/
Record* ValueToRecordMapper(Record *record, void* redisModuleCtx, char** err);

/******************************* GroupByExtractors *********************/
char* KeyRecordStrValueExtractor(Record *data, void* arg, size_t* len, char** err);

/******************************* GroupByReducers ***********************/
Record* CountReducer(char* key, size_t keyLen, Record *records, void* arg, char** err);

typedef Reader* (*RediStar_ReaderCallback)(void* arg);
typedef Writer* (*RediStar_WriterCallback)(void* arg);
typedef Record* (*RediStar_MapCallback)(Record *data, void* arg, char** err);
typedef bool (*RediStar_FilterCallback)(Record *data, void* arg, char** err);
typedef char* (*RediStar_ExtractorCallback)(Record *data, void* arg, size_t* len, char** err);
typedef Record* (*RediStar_ReducerCallback)(char* key, size_t keyLen, Record *records, void* arg, char** err);

#define MODULE_API_FUNC(x) (*x)

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

int MODULE_API_FUNC(RediStar_RegisterReader)(char* name, RediStar_ReaderCallback reader);
int MODULE_API_FUNC(RediStar_RegisterWriter)(char* name, RediStar_WriterCallback reader);
int MODULE_API_FUNC(RediStar_RegisterMap)(char* name, RediStar_MapCallback map);
int MODULE_API_FUNC(RediStar_RegisterFilter)(char* name, RediStar_FilterCallback filter);
int MODULE_API_FUNC(RediStar_RegisterGroupByExtractor)(char* name, RediStar_ExtractorCallback extractor);
int MODULE_API_FUNC(RediStar_RegisterReducer)(char* name, RediStar_ReducerCallback reducer);

#define RSM_RegisterReader(name) RediStar_RegisterReader(#name, name);
#define RSM_RegisterMap(name) RediStar_RegisterMap(#name, name);
#define RSM_RegisterFilter(name) RediStar_RegisterFilter(#name, name);
#define RSM_RegisterWriter(name) RediStar_RegisterWriter(#name, name);
#define RSM_RegisterGroupByExtractor(name) RediStar_RegisterGroupByExtractor(#name, name);
#define RSM_RegisterReducer(name) RediStar_RegisterReducer(#name, name);

RediStarCtx* MODULE_API_FUNC(RediStar_Load)(char* name, RedisModuleCtx *ctx, void* arg);
#define RSM_Load(name, ctx, arg) RediStar_Load(#name, ctx, arg)

int MODULE_API_FUNC(RediStar_Map)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Map(ctx, name, arg) RediStar_Map(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_Filter)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Filter(ctx, name, arg) RediStar_Filter(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_Write)(RediStarCtx* ctx, char* name, void* arg);
#define RSM_Write(ctx, name, arg) RediStar_Write(ctx, #name, arg)

int MODULE_API_FUNC(RediStar_GroupBy)(RediStarCtx* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg);
#define RSM_GroupBy(ctx, extractor, extractorArg, reducer, reducerArg)\
    RediStar_GroupBy(ctx, #extractor, extractorArg, #reducer, reducerArg)

#define PROXY_MODULE_INIT_FUNCTION(name) \
        if (RedisModule_GetApi("RediStar_" #name, ((void **)&RediStar_ ## name))) { \
            printf("could not initialize RediStar_" #name "\r\n");\
            return false; \
        }

static bool RediStar_Initialize(){
    PROXY_MODULE_INIT_FUNCTION(RegisterReader);
    PROXY_MODULE_INIT_FUNCTION(RegisterWriter);
    PROXY_MODULE_INIT_FUNCTION(RegisterMap);
    PROXY_MODULE_INIT_FUNCTION(RegisterFilter);
    PROXY_MODULE_INIT_FUNCTION(RegisterGroupByExtractor);
    PROXY_MODULE_INIT_FUNCTION(RegisterReducer);
    PROXY_MODULE_INIT_FUNCTION(Load);
    PROXY_MODULE_INIT_FUNCTION(Map);
    PROXY_MODULE_INIT_FUNCTION(Filter);
    PROXY_MODULE_INIT_FUNCTION(Write);
    PROXY_MODULE_INIT_FUNCTION(GroupBy);
    PROXY_MODULE_INIT_FUNCTION(KeysReaderCtxCreate);

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
