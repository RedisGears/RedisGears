/*
 * config.c
 *
 *  Created on: 7 Jan 2019
 *      Author: root
 */

#include "config.h"
#include "redisgears_memory.h"
#include "utils/arr.h"
#include <stdbool.h>
#include <assert.h>

#define PYTHON_HOME_DIR "PYTHON_HOME_DIR"

typedef struct ArgsIterator{
    int currIndex;
    RedisModuleString** argv;
    int argc;
}ArgsIterator;

RedisModuleString* ArgsIterator_Next(ArgsIterator* iter){
    if(iter->currIndex >= iter->argc){
        return NULL;
    }
    return iter->argv[iter->currIndex++];
}

typedef enum ConfigValType{
    STR, LONG, DOUBLE
}ConfigValType;

typedef struct ConfigVal{
    union{
        char* str;
        long long longVal;
        double doubleVal;
    }val;
    ConfigValType type;
}ConfigVal;

typedef struct RedisGears_Config{
    ConfigVal pythonHomeDir;
    ConfigVal maxExecutions;
}RedisGears_Config;

typedef const ConfigVal* (*GetValueCallback)();
typedef bool (*SetValueCallback)(ArgsIterator* iter);

typedef struct Gears_ConfigVal{
    char* name;
    GetValueCallback getter;
    SetValueCallback setter;
    bool configurableAtRunTime;
}Gears_ConfigVal;

static RedisGears_Config DefaultGearsConfig;

static const ConfigVal* ConfigVal_PythonHomeDirGet(){
    return &DefaultGearsConfig.pythonHomeDir;
}

static bool ConfigVal_PythonHomeDirSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val){
        return false;
    }
    if(getenv(PYTHON_HOME_DIR)){
        printf("warning setting PythonHomeDir will take no effect cause its defined in env var\r\n");
        return true;
    }
    RG_FREE(DefaultGearsConfig.pythonHomeDir.val.str);
    const char* valStr = RedisModule_StringPtrLen(val, NULL);
    DefaultGearsConfig.pythonHomeDir.val.str = RG_STRDUP(valStr);
    return true;
}

static const ConfigVal* ConfigVal_MaxExecutionsGet(){
    return &DefaultGearsConfig.maxExecutions;
}

static bool ConfigVal_MaxExecutionsSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.maxExecutions.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static Gears_ConfigVal Gears_ConfigVals[] = {
    {
        .name = "PythonHomeDir",
        .getter = ConfigVal_PythonHomeDirGet,
        .setter = ConfigVal_PythonHomeDirSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "MaxExecutions",
        .getter = ConfigVal_MaxExecutionsGet,
        .setter = ConfigVal_MaxExecutionsSet,
        .configurableAtRunTime = true,
    },
    {
        NULL,
    },
};

static int GearsConfig_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    ArgsIterator iter = {
            .currIndex = 0,
            .argv = argv,
            .argc = argc,
    };

    ArgsIterator_Next(&iter); // skip command name
    RedisModuleString* arg = NULL;
    while((arg = ArgsIterator_Next(&iter))) {
        const char* configVal = RedisModule_StringPtrLen(arg, NULL);
        RedisModule_Log(ctx, "warning", "checking config value %s", configVal);
        bool found = false;
        for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
            if(strcasecmp(configVal, val->name) == 0) {
                if (!val->setter(&iter)) {
                    RedisModule_Log(ctx, "warning", "failed reading config value %s", configVal);
                    return REDISMODULE_ERR;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            RedisModule_Log(ctx, "warning", "unknown config value %s", configVal);
            return REDISMODULE_ERR;
        }
    }
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static void GearsConfig_ReplyWithConfVal(RedisModuleCtx *ctx, const ConfigVal* confVal){
    switch(confVal->type){
    case STR:
        RedisModule_ReplyWithStringBuffer(ctx, confVal->val.str, strlen(confVal->val.str));
        break;
    case LONG:
        RedisModule_ReplyWithLongLong(ctx, confVal->val.longVal);
        break;
    case DOUBLE:
        RedisModule_ReplyWithDouble(ctx, confVal->val.doubleVal);
        break;
    default:
        assert(false);
    }
}

static int GearsConfig_Get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define UNKNOWN_CONFIG_PARAM "unknown config parameter"
    ArgsIterator iter = {
            .currIndex = 0,
            .argv = argv,
            .argc = argc,
    };

    ArgsIterator_Next(&iter); // skip command name
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleString* arg = NULL;
    int n_found = 0;
    while((arg = ArgsIterator_Next(&iter))){
        const char* configVal = RedisModule_StringPtrLen(arg, NULL);
        bool found = false;
        for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
            if(strcasecmp(configVal, val->name) == 0){
                const ConfigVal* confVal = val->getter(configVal);
                GearsConfig_ReplyWithConfVal(ctx, confVal);
                found = true;
                ++n_found;
                break;
            }
        }
        if(!found){
            RedisModule_Log(ctx, "warning", "unknown config value %s", configVal);
            RedisModule_ReplyWithStringBuffer(ctx, UNKNOWN_CONFIG_PARAM, strlen(UNKNOWN_CONFIG_PARAM));
            return REDISMODULE_ERR;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, n_found);
    return REDISMODULE_OK;
}

const char* GearsConfig_GetPythonHomeDir(){
    return DefaultGearsConfig.pythonHomeDir.val.str;
}

long long GearsConfig_GetMaxExecutions(){
    return DefaultGearsConfig.maxExecutions.val.longVal;
}

static void GearsConfig_Print(RedisModuleCtx* ctx){
    for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
        const ConfigVal* v = val->getter();
        switch(v->type){
        case STR:
            RedisModule_Log(ctx, "notice", "%s:%s", val->name, v->val.str);
            break;
        case LONG:
            RedisModule_Log(ctx, "notice", "%s:%lld", val->name, v->val.longVal);
            break;
        case DOUBLE:
            RedisModule_Log(ctx, "notice", "%s:%lf", val->name, v->val.doubleVal);
            break;
        default:
            assert(0);
        }
    }
}

#ifndef CPYTHON_PATH
#define CPYTHON_PATH "/usr/bin/"
#endif

int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    DefaultGearsConfig = (RedisGears_Config){
        .pythonHomeDir = {
            .val.str = getenv(PYTHON_HOME_DIR) ? RG_STRDUP(getenv(PYTHON_HOME_DIR)) : RG_STRDUP(CPYTHON_PATH),
            .type = STR,
        },
        .maxExecutions = {
            .val.longVal = 1000,
            .type = LONG,
        },
    };

    if (RedisModule_CreateCommand(ctx, "rg.configget", GearsConfig_Get, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.configget");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.configset", GearsConfig_Set, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.configset");
        return REDISMODULE_ERR;
    }

    // insert a dummy leading arg to emulate a 'rg.configset' command
    RedisModuleString** argv1 = array_newlen(RedisModuleString*, argc + 1);
    memcpy(&argv1[1], &argv[0], argc * sizeof(RedisModuleString*));
    argv1[0] = NULL;
    GearsConfig_Set(ctx, argv1, argc + 1);
    array_free(argv1);

    GearsConfig_Print(ctx);
    return REDISMODULE_OK;
}

