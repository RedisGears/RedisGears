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
}RediStarCtx;

#define REGISTER_API(name) \
    if(moduleRegisterApi("RediStar_" #name, RS_ ## name)){\
        printf("could not register RediStar_" #name "\r\n");\
        return false;\
    }

static int RS_RegisterReader(char* name, RediStar_ReaderCallback reader){
    return ReadersMgmt_Add(name, reader);
}

static int RS_RegisterWriter(char* name, RediStar_WriterCallback writer){
    return WritersMgmt_Add(name, writer);
}

static int RS_RegisterMap(char* name, RediStar_MapCallback map){
    return MapsMgmt_Add(name, map);
}

static int RS_RegisterFilter(char* name, RediStar_FilterCallback filter){
    return FiltersMgmt_Add(name, filter);
}

static int RS_RegisterGroupByExtractor(char* name, RediStar_ExtractorCallback extractor){
    return ExtractorsMgmt_Add(name, extractor);
}

static int RS_RegisterReducer(char* name, RediStar_ReducerCallback reducer){
    return ReducersMgmt_Add(name, reducer);
}

static RediStarCtx* RS_Load(char* name, void* arg){
    RediStarCtx* res = RS_ALLOC(sizeof(*res));
    res->fep = FlatExecutionPlan_New();
    FlatExecutionPlan_SetReader(res->fep, name, arg);
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
    ExecutionPlan* ep = ExecutionPlan_New(ctx->fep);
    ExecutionPlan_Run(ep);
    ExecutionPlan_Free(ep);
    FlatExecutionPlan_Free(ctx->fep);
    RS_FREE(ctx);
    return 1;
}

KeysReaderCtx* RS_KeysReaderCtxCreate(RedisModuleCtx* ctx, char* match);

static bool RediStar_RegisterApi(){
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
    REGISTER_API(KeysReaderCtxCreate);

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

    RSM_RegisterReader(KeysReader);
    RSM_RegisterWriter(ReplyWriter);
    RSM_RegisterFilter(TypeFilter);
    RSM_RegisterMap(ValueToRecordMapper);
    RSM_RegisterGroupByExtractor(KeyRecordStrValueExtractor);
    RSM_RegisterReducer(CountReducer);

#ifdef WITHPYTHON
    RediStarPy_Init(ctx);
#endif

    if (RedisModule_CreateCommand(ctx, "example", Example_CommandCallback, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command example");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}



