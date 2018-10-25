/*
 * module.c
 *
 *  Created on: Oct 15, 2018
 *      Author: meir
 */

#include "redistar.h"
#include "redismodule.h"
#include "version.h"
#include "mgmt.h"
#include "execution_plan.h"
#include "example.h"
#include "utils/arr_rm_alloc.h"
#include "redistar_memory.h"
#ifdef WITHPYTHON
#include "redistar_python.h"
#endif
#include "record.h"
#include <stdbool.h>

int moduleRegisterApi(const char *funcname, void *funcptr);

typedef struct RediStarCtx{
    FlatExecutionPlan* fep;
    RedisModuleCtx *ctx;
}RediStarCtx;

#define REGISTER_API(name) \
    if(moduleRegisterApi("RediStar_" #name, RS_ ## name)){\
        printf("could not register RediStar_" #name "\r\n");\
        return false;\
    }

static int RS_RegisterReader(char* name, RediStar_ReaderCallback reader, ArgType* type){
    return ReadersMgmt_Add(name, reader, type);
}

static int RS_RegisterWriter(char* name, RediStar_WriterCallback writer, ArgType* type){
    return WritersMgmt_Add(name, writer, type);
}

static int RS_RegisterMap(char* name, RediStar_MapCallback map, ArgType* type){
    return MapsMgmt_Add(name, map, type);
}

static int RS_RegisterFilter(char* name, RediStar_FilterCallback filter, ArgType* type){
    return FiltersMgmt_Add(name, filter, type);
}

static int RS_RegisterGroupByExtractor(char* name, RediStar_ExtractorCallback extractor, ArgType* type){
    return ExtractorsMgmt_Add(name, extractor, type);
}

static int RS_RegisterReducer(char* name, RediStar_ReducerCallback reducer, ArgType* type){
    return ReducersMgmt_Add(name, reducer, type);
}

static RediStarCtx* RS_Load(char* name, RedisModuleCtx *ctx, void* arg){
    RediStarCtx* res = RS_ALLOC(sizeof(*res));
    res->fep = FlatExecutionPlan_New();
    FlatExecutionPlan_SetReader(res->fep, name, arg);
    res->ctx = ctx;
    return res;
}

static int RS_Map(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_AddMapStep(ctx->fep, name, arg);
    return 1;
}

static int RS_Filter(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_AddFilterStep(ctx->fep, name, arg);
    return 1;
}

static int RS_GroupBy(RediStarCtx* ctx, char* extraxtorName, void* extractorArg, char* reducerName, void* reducerArg){
    FlatExecutionPlan_AddGroupByStep(ctx->fep, extraxtorName, extractorArg, reducerName, reducerArg);
    return 1;
}

static int RS_Write(RediStarCtx* ctx, char* name, void* arg){
    FlatExecutionPlan_SetWriter(ctx->fep, name, arg);
    FlatExecutionPlan_Run(ctx->fep, ctx->ctx);
    RS_FREE(ctx);
    return 1;
}

ArgType* RS_CreateType(char* name, ArgFree free, ArgSerialize serialize, ArgDeserialize deserialize){
    ArgType* ret = RS_ALLOC(sizeof(*ret));
    *ret = (ArgType){
        .type = RS_STRDUP(name),
        .free = free,
        .serialize = serialize,
        .deserialize = deserialize,
    };
    return ret;
}

static bool RediStar_RegisterApi(){
    REGISTER_API(CreateType);

    REGISTER_API(RegisterReader);
    REGISTER_API(RegisterWriter);
    REGISTER_API(RegisterMap);
    REGISTER_API(RegisterFilter);
    REGISTER_API(RegisterGroupByExtractor);
    REGISTER_API(RegisterReducer);
    REGISTER_API(Load);
    REGISTER_API(Map);
    REGISTER_API(Filter);
    REGISTER_API(GroupBy);
    REGISTER_API(Write);

    REGISTER_API(FreeRecord);
    REGISTER_API(RecordGetType);
    REGISTER_API(KeyRecordCreate);
    REGISTER_API(KeyRecordSetKey);
    REGISTER_API(KeyRecordSetVal);
    REGISTER_API(KeyRecordGetVal);
    REGISTER_API(KeyRecordGetKey);
    REGISTER_API(ListRecordCreate);
    REGISTER_API(ListRecordLen);
    REGISTER_API(ListRecordAdd);
    REGISTER_API(ListRecordGet);
    REGISTER_API(StringRecordCreate);
    REGISTER_API(StringRecordGet);
    REGISTER_API(StringRecordSet);
    REGISTER_API(DoubleRecordCreate);
    REGISTER_API(DoubleRecordGet);
    REGISTER_API(DoubleRecordSet);
    REGISTER_API(LongRecordCreate);
    REGISTER_API(LongRecordGet);
    REGISTER_API(LongRecordSet);
    REGISTER_API(KeyHandlerRecordCreate);
    REGISTER_API(KeyHandlerRecordGet);

    return true;
}

ArgType* GetKeysReaderArgType();
ArgType* GetKeysWriterArgType();

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "RediStar", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(!RediStar_RegisterApi()){
        RedisModule_Log(ctx, "warning", "could not register RediStar api\r\n");
        return REDISMODULE_ERR;
    }

    if(!RediStar_Initialize()){
        RedisModule_Log(ctx, "warning", "could not initialize RediStar api\r\n");
        return REDISMODULE_ERR;
    }

    Mgmt_Init();

    RSM_RegisterReader(KeysReader, GetKeysReaderArgType());
    RSM_RegisterWriter(ReplyWriter, GetKeysWriterArgType());
    RSM_RegisterGroupByExtractor(KeyRecordStrValueExtractor, NULL);
    RSM_RegisterReducer(CountReducer, NULL);

    ExecutionPlan_InitializeWorkers(1);

#ifdef WITHPYTHON
    RediStarPy_Init(ctx);
#endif

    if (RedisModule_CreateCommand(ctx, "example", Example_CommandCallback, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command example");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}



