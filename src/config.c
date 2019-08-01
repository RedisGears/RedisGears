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
	ConfigVal profileExecutions;
	ConfigVal pythonAttemptTraceback;
	ConfigVal maxPythonSubInterpreterMemory;
	ConfigVal pythonExecutionTimeout;
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

static const ConfigVal* ConfigVal_ProfileExecutionsGet(){
	return &DefaultGearsConfig.profileExecutions;
}

static bool ConfigVal_ProfileExecutionsSet(ArgsIterator* iter){
	RedisModuleString* val = ArgsIterator_Next(iter);
	if(!val) return false;
    long long n;

	if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.profileExecutions.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_PythonAttemptTracebackGet(){
	return &DefaultGearsConfig.pythonAttemptTraceback;
}

static const ConfigVal* ConfigVal_MaxPythonMemoryGet(){
    return &DefaultGearsConfig.maxPythonSubInterpreterMemory;
}

static const ConfigVal* ConfigVal_PythonExecutionTimeoutGet(){
    return &DefaultGearsConfig.pythonExecutionTimeout;
}

static bool ConfigVal_PythonExecutionTimeoutSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n < 0){
            return false;
        }
        DefaultGearsConfig.pythonExecutionTimeout.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static bool ConfigVal_PythonAttemptTracebackSet(ArgsIterator* iter){
	RedisModuleString* val = ArgsIterator_Next(iter);
	if(!val) return false;
    long long n;

	if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.pythonAttemptTraceback.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static bool ConfigVal_MaxPythonMemorySet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n < 0){
            return false;
        }
        DefaultGearsConfig.maxPythonSubInterpreterMemory.val.longVal = n;
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
        .name = "ProfileExecutions",
        .getter = ConfigVal_ProfileExecutionsGet,
        .setter = ConfigVal_ProfileExecutionsSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "PythonAttemptTraceback",
        .getter = ConfigVal_PythonAttemptTracebackGet,
        .setter = ConfigVal_PythonAttemptTracebackSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "MaxPythonMemory",
        .getter = ConfigVal_MaxPythonMemoryGet,
        .setter = ConfigVal_MaxPythonMemorySet,
        .configurableAtRunTime = false, // for security reasons
    },
    {
        .name = "PythonExecutionTimeout",
        .getter = ConfigVal_PythonExecutionTimeoutGet,
        .setter = ConfigVal_PythonExecutionTimeoutSet,
        .configurableAtRunTime = false, // for security reasons
    },
    {
        NULL,
    },
};

static void config_error(RedisModuleCtx *ctx, const char *fmt, const char* configItem) {
    RedisModule_Log(ctx, "warning", fmt, configItem);

    char fmt1[256] = "(error) ";
    strncat(fmt1, fmt, sizeof(fmt1));
    RedisModuleString* rms = RedisModule_CreateStringPrintf(ctx, fmt1, configItem);
    const char* err = RedisModule_StringPtrLen(rms, NULL);
    RedisModule_ReplyWithSimpleString(ctx, err);
    RedisModule_FreeString(ctx, rms);
}

static int GearsConfig_Set_with_iterator(RedisModuleCtx *ctx, ArgsIterator *iter, bool isFirstInitialization) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    bool error = false;
    int n_values = 0;
    RedisModuleString* arg;
    while ((arg = ArgsIterator_Next(iter))) {
        const char* configName = RedisModule_StringPtrLen(arg, NULL);
        bool found = false;
        for (Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++) {
            if (strcasecmp(configName, val->name) == 0) {
                found = true;
                ++n_values;
                if (!val->configurableAtRunTime && !isFirstInitialization) {
                    error = true;
                    config_error(ctx, "Config value %s not modifiable at runtime", configName);
                    ArgsIterator_Next(iter); // skip value
                    break;
                }
                if (val->setter(iter)) {
                    RedisModule_ReplyWithSimpleString(ctx, "OK");
                } else {
                    error = true;
                    config_error(ctx, "Failed setting config value %s", configName);
                }
                break;
            }
        }
        if (!found) {
            error = true;
            ++n_values;
            config_error(ctx, "Unsupported config parameter %s", configName);
            ArgsIterator_Next(iter); // skip value
        }
    }

    RedisModule_ReplySetArrayLength(ctx, n_values);
    return error ? REDISMODULE_ERR : REDISMODULE_OK;
}

static int GearsConfig_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    ArgsIterator iter = {
            .currIndex = 1, // skip command name
            .argv = argv,
            .argc = argc,
    };
    GearsConfig_Set_with_iterator(ctx, &iter, false);
    return REDISMODULE_OK; // redis expects REDISMODULE_ERR only on catastrophes
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

static int GearsConfig_Get_with_iterator(RedisModuleCtx *ctx, ArgsIterator *iter) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    bool error = false;
    int n_values = 0;
    RedisModuleString* arg;
    while ((arg = ArgsIterator_Next(iter))) {
        const char* configName = RedisModule_StringPtrLen(arg, NULL);
        bool found = false;
        for (Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++) {
            if (strcasecmp(configName, val->name) == 0) {
                const ConfigVal* confVal = val->getter(configName);
                GearsConfig_ReplyWithConfVal(ctx, confVal);
                found = true;
                ++n_values;
                break;
            }
        }
        if (!found) {
            error = true;
            ++n_values;
            config_error(ctx, "Unsupported config parameter: %s", configName);
        }
    }

    RedisModule_ReplySetArrayLength(ctx, n_values);
    return error ? REDISMODULE_ERR : REDISMODULE_OK;
}

static int GearsConfig_Get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    ArgsIterator iter = {
            .currIndex = 1, // skip command name
            .argv = argv,
            .argc = argc,
    };
    GearsConfig_Get_with_iterator(ctx, &iter);
    return REDISMODULE_OK; // redis expects REDISMODULE_ERR only on catastrophes
}

const char* GearsConfig_GetPythonHomeDir(){
    return DefaultGearsConfig.pythonHomeDir.val.str;
}

long long GearsConfig_GetMaxExecutions(){
    return DefaultGearsConfig.maxExecutions.val.longVal;
}

long long GearsConfig_GetProfileExecutions(){
	return DefaultGearsConfig.profileExecutions.val.longVal;
}

long long GearsConfig_GetPythonAttemptTraceback(){
	return DefaultGearsConfig.pythonAttemptTraceback.val.longVal;
}

long long GearsConfig_GetMaxMemoryForPythonSubInterpreter(){
    return DefaultGearsConfig.maxPythonSubInterpreterMemory.val.longVal;
}

long long GearsConfig_GetPythonExecutionTimeout(){
    return DefaultGearsConfig.pythonExecutionTimeout.val.longVal;
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
        .profileExecutions = {
            .val.longVal = 0,
            .type = LONG,
        },
        .pythonAttemptTraceback = {
            .val.longVal = 1,
            .type = LONG,
        },
        .maxPythonSubInterpreterMemory = {
            .val.longVal = 536870912, // 512M
            .type = LONG,
        },
        .pythonExecutionTimeout = {
            .val.longVal = 5,
            .type = LONG,
        },
    };

    DEF_COMMAND(configget, GearsConfig_Get);
    DEF_COMMAND(configset, GearsConfig_Set);

    ArgsIterator iter = {
        .currIndex = 0,
        .argv = argv,
        .argc = argc,
    };  
    GearsConfig_Set_with_iterator(ctx, &iter, true);
    GearsConfig_Print(ctx);

    return REDISMODULE_OK;
}

