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

static int error_fmt(RedisModuleCtx *ctx, const char *fmt, const char* configVal) {
    RedisModule_Log(ctx, "warning", fmt, configVal);
    char err[256] = "ERR ";
    snprintf(err + 4, sizeof(err) - 4, fmt, configVal);
    err[sizeof(err)-1] = '\0';
    RedisModule_ReplyWithSimpleString(ctx, err);
    return REDISMODULE_ERR;
}

static int _GearsConfig_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool hasCommand){
    ArgsIterator iter = {
            .currIndex = 0,
            .argv = argv,
            .argc = argc,
    };

    if (hasCommand) ArgsIterator_Next(&iter); // skip command name
    RedisModuleString* arg = NULL;
    while((arg = ArgsIterator_Next(&iter))) {
        const char* configVal = RedisModule_StringPtrLen(arg, NULL);
        bool found = false;
        for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
            if(strcasecmp(configVal, val->name) == 0) {
                if (!val->configurableAtRunTime) {
                    return error_fmt(ctx, "config value %s not modifiable at runtime", configVal);
                }
                if (!val->setter(&iter)) return error_fmt(ctx, "failed setting config value %s", configVal);
                found = true;
                break;
            }
        }
        if (!found) return error_fmt(ctx, "Unsupported config parameter %s", configVal);
    }
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int GearsConfig_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    return _GearsConfig_Set(ctx, argv, argc, true);
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
            RedisModule_ReplySetArrayLength(ctx, ++n_found);
            return error_fmt(ctx, "Unsupported config parameter: %s", configVal);
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

#define DEF_COMMAND(cmd, handler) \
    do { \
        if (RedisModule_CreateCommand(ctx, "rg." #cmd, handler, "readonly", 0, 0, 0) != REDISMODULE_OK) { \
            RedisModule_Log(ctx, "warning", "could not register command %s", #cmd); \
            return REDISMODULE_ERR; \
        } \
    } while (false)

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

    DEF_COMMAND(configget, GearsConfig_Get);
    DEF_COMMAND(configset, GearsConfig_Set);

    _GearsConfig_Set(ctx, argv, argc, false);
    GearsConfig_Print(ctx);

    return REDISMODULE_OK;
}

