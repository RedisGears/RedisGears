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

extern char DependenciesUrl[];
extern char DependenciesSha256[];

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
    ConfigVal maxExecutions;
    ConfigVal maxExecutionsPerRegistration;
    ConfigVal profileExecutions;
    ConfigVal pythonAttemptTraceback;
    ConfigVal createVenv;
    ConfigVal executionThreads;
    ConfigVal executionMaxIdleTime;
    ConfigVal pythonInstallReqMaxIdleTime;
    ConfigVal dependenciesUrl;
    ConfigVal dependenciesSha256;
    ConfigVal pythonInstallationDir;
    ConfigVal downloadDeps;
    ConfigVal foreceDownloadDepsOnEnterprise;
    ConfigVal sendMsgRetries;
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

static const ConfigVal* ConfigVal_MaxExecutionsPerRegistrationGet(){
    return &DefaultGearsConfig.maxExecutionsPerRegistration;
}

static bool ConfigVal_MaxExecutionsPerRegistrationSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.maxExecutionsPerRegistration.val.longVal = n;
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

static const ConfigVal* ConfigVal_DependenciesUrlGet(){
    return &DefaultGearsConfig.dependenciesUrl;
}

static bool ConfigVal_DependenciesUrlSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val){
        return false;
    }
    RG_FREE(DefaultGearsConfig.dependenciesUrl.val.str);
    const char* valStr = RedisModule_StringPtrLen(val, NULL);
    DefaultGearsConfig.dependenciesUrl.val.str = RG_STRDUP(valStr);
    return true;
}

static const ConfigVal* ConfigVal_DependenciesSha256Get(){
    return &DefaultGearsConfig.dependenciesSha256;
}

static bool ConfigVal_DependenciesSha256Set(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val){
        return false;
    }
    RG_FREE(DefaultGearsConfig.dependenciesSha256.val.str);
    const char* valStr = RedisModule_StringPtrLen(val, NULL);
    DefaultGearsConfig.dependenciesSha256.val.str = RG_STRDUP(valStr);
    return true;
}

static const ConfigVal* ConfigVal_PythonInstallationDirGet(){
    return &DefaultGearsConfig.pythonInstallationDir;
}

static bool ConfigVal_PythonInstallationDirSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val){
        return false;
    }
    RG_FREE(DefaultGearsConfig.pythonInstallationDir.val.str);
    const char* valStr = RedisModule_StringPtrLen(val, NULL);
    DefaultGearsConfig.pythonInstallationDir.val.str = RG_STRDUP(valStr);
    return true;
}

static const ConfigVal* ConfigVal_PythonAttemptTracebackGet(){
	return &DefaultGearsConfig.pythonAttemptTraceback;
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

static const ConfigVal* ConfigVal_CreateVenvGet(){
    return &DefaultGearsConfig.createVenv;
}

static bool ConfigVal_CreateVenvSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.createVenv.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_DownloadDepsGet(){
    return &DefaultGearsConfig.downloadDeps;
}

static bool ConfigVal_DownloadDepsSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.downloadDeps.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_ForceDownloadDepsOnEnterpriseGet(){
    return &DefaultGearsConfig.foreceDownloadDepsOnEnterprise;
}

static bool ConfigVal_ForceDownloadDepsOnEnterpriseSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        DefaultGearsConfig.foreceDownloadDepsOnEnterprise.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_SendMsgRetriesGet(){
    return &DefaultGearsConfig.sendMsgRetries;
}

static bool ConfigVal_SendMsgRetriesSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n < 0){
            return false;
        }
        DefaultGearsConfig.sendMsgRetries.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_ExecutionThreadsGet(){
    return &DefaultGearsConfig.executionThreads;
}

static bool ConfigVal_ExecutionThreadsSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n <= 0){
            return false;
        }
        DefaultGearsConfig.executionThreads.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_ExecutionMaxIdleTimeGet(){
    return &DefaultGearsConfig.executionMaxIdleTime;
}

static bool ConfigVal_ExecutionMaxIdleTimeSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n <= 0){
            return false;
        }
        DefaultGearsConfig.executionMaxIdleTime.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static const ConfigVal* ConfigVal_PythonInstallReqMaxIdleTimeGet(){
    return &DefaultGearsConfig.pythonInstallReqMaxIdleTime;
}

static bool ConfigVal_PythonInstallReqMaxIdleTimeSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val) return false;
    long long n;

    if (RedisModule_StringToLongLong(val, &n) == REDISMODULE_OK) {
        if(n <= 0){
            return false;
        }
        DefaultGearsConfig.pythonInstallReqMaxIdleTime.val.longVal = n;
        return true;
    } else {
        return false;
    }
}

static Gears_dict* Gears_ExtraConfig = NULL;

static Gears_ConfigVal Gears_ConfigVals[] = {
    {
        .name = "MaxExecutions",
        .getter = ConfigVal_MaxExecutionsGet,
        .setter = ConfigVal_MaxExecutionsSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "MaxExecutionsPerRegistration",
        .getter = ConfigVal_MaxExecutionsPerRegistrationGet,
        .setter = ConfigVal_MaxExecutionsPerRegistrationSet,
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
        .name = "DependenciesUrl",
        .getter = ConfigVal_DependenciesUrlGet,
        .setter = ConfigVal_DependenciesUrlSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "DependenciesSha256",
        .getter = ConfigVal_DependenciesSha256Get,
        .setter = ConfigVal_DependenciesSha256Set,
        .configurableAtRunTime = false,
    },
    {
        .name = "CreateVenv",
        .getter = ConfigVal_CreateVenvGet,
        .setter = ConfigVal_CreateVenvSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "ExecutionThreads",
        .getter = ConfigVal_ExecutionThreadsGet,
        .setter = ConfigVal_ExecutionThreadsSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "ExecutionMaxIdleTime",
        .getter = ConfigVal_ExecutionMaxIdleTimeGet,
        .setter = ConfigVal_ExecutionMaxIdleTimeSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "PythonInstallReqMaxIdleTime",
        .getter = ConfigVal_PythonInstallReqMaxIdleTimeGet,
        .setter = ConfigVal_PythonInstallReqMaxIdleTimeSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "PythonInstallationDir",
        .getter = ConfigVal_PythonInstallationDirGet,
        .setter = ConfigVal_PythonInstallationDirSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "DownloadDeps",
        .getter = ConfigVal_DownloadDepsGet,
        .setter = ConfigVal_DownloadDepsSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "ForceDownloadDepsOnEnterprise",
        .getter = ConfigVal_ForceDownloadDepsOnEnterpriseGet,
        .setter = ConfigVal_ForceDownloadDepsOnEnterpriseSet,
        .configurableAtRunTime = false,
    },
    {
        .name = "SendMsgRetries",
        .getter = ConfigVal_SendMsgRetriesGet,
        .setter = ConfigVal_SendMsgRetriesSet,
        .configurableAtRunTime = true,
    },
    {
        NULL,
    },
};

static void config_error(RedisModuleCtx *ctx, const char *fmt, const char* configItem, bool sendReply) {
    RedisModule_Log(ctx, "warning", fmt, configItem);

    if(sendReply){
        char fmt1[256] = "(error) ";
        strncat(fmt1, fmt, sizeof(fmt1)-1);
        RedisModuleString* rms = RedisModule_CreateStringPrintf(ctx, fmt1, configItem);
        const char* err = RedisModule_StringPtrLen(rms, NULL);
        RedisModule_ReplyWithError(ctx, err);
        RedisModule_FreeString(ctx, rms);
    }
}


static int GearsConfig_Set_with_iterator(RedisModuleCtx *ctx, ArgsIterator *iter, bool isFirstInitialization) {
#define SEND_REPLY_IF_NEEDED(replyCode) if(!isFirstInitialization) { replyCode; }
    SEND_REPLY_IF_NEEDED(RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN));
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
                    config_error(ctx, "Config value %s not modifiable at runtime", configName, !isFirstInitialization);
                    ArgsIterator_Next(iter); // skip value
                    break;
                }
                if (val->setter(iter)) {
                    SEND_REPLY_IF_NEEDED(RedisModule_ReplyWithSimpleString(ctx, "OK"));
                } else {
                    error = true;
                    config_error(ctx, "Failed setting config value %s", configName, !isFirstInitialization);
                }
                break;
            }
        }
        if (!found) {
            ++n_values;
            RedisModuleString* valStr = ArgsIterator_Next(iter);
            if(!valStr){
                error = true;
                config_error(ctx, "Value was not given to %s", configName, !isFirstInitialization);
                continue;
            }
            const char* valCStr = RedisModule_StringPtrLen(valStr, NULL);
            Gears_dictEntry *existing = NULL;
            Gears_dictEntry *entry = Gears_dictAddRaw(Gears_ExtraConfig, (char*)configName, &existing);
            if(!entry){
                char* val = Gears_dictGetVal(existing);
                RG_FREE(val);
                entry = existing;
            }
            Gears_dictSetVal(Gears_ExtraConfig, entry, RG_STRDUP(valCStr));
            SEND_REPLY_IF_NEEDED(RedisModule_ReplyWithSimpleString(ctx, "OK - value was saved in extra config dictionary"));
        }
    }

    SEND_REPLY_IF_NEEDED(RedisModule_ReplySetArrayLength(ctx, n_values));
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
        RedisModule_Assert(false);
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
            char* valCStr = Gears_dictFetchValue(Gears_ExtraConfig, configName);
            if(valCStr){
                RedisModule_ReplyWithStringBuffer(ctx, valCStr, strlen(valCStr));
            }else{
                config_error(ctx, "Unsupported config parameter: %s", configName, true);
            }
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

long long GearsConfig_GetMaxExecutionsPerRegistration(){
    return DefaultGearsConfig.maxExecutionsPerRegistration.val.longVal;
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

const char* GearsConfig_GetExtraConfigVals(const char* key){
    return Gears_dictFetchValue(Gears_ExtraConfig, key);
}

const char* GearsConfig_GetPythonInstallationDir(){
    return DefaultGearsConfig.pythonInstallationDir.val.str;
}

const char* GearsConfig_GetDependenciesUrl(){
    return DefaultGearsConfig.dependenciesUrl.val.str;
}
const char* GearsConfig_GetDependenciesSha256(){
    return DefaultGearsConfig.dependenciesSha256.val.str;
}

long long GearsConfig_CreateVenv(){
    return DefaultGearsConfig.createVenv.val.longVal;
}

long long GearsConfig_DownloadDeps(){
    return DefaultGearsConfig.downloadDeps.val.longVal;
}

long long GearsConfig_ForceDownloadDepsOnEnterprise(){
    return DefaultGearsConfig.foreceDownloadDepsOnEnterprise.val.longVal;
}

long long GearsConfig_ExecutionThreads(){
    return DefaultGearsConfig.executionThreads.val.longVal;
}

long long GearsConfig_ExecutionMaxIdleTime(){
    return DefaultGearsConfig.executionMaxIdleTime.val.longVal;
}

long long GearsConfig_SendMsgRetries(){
    return DefaultGearsConfig.sendMsgRetries.val.longVal;
}

long long GearsConfig_PythonInstallReqMaxIdleTime(){
    return DefaultGearsConfig.executionMaxIdleTime.val.longVal;
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
            RedisModule_Assert(0);
        }
    }

    Gears_dictIterator *iter = Gears_dictGetIterator(Gears_ExtraConfig);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        const char* key = Gears_dictGetKey(entry);
        const char* val = Gears_dictGetVal(entry);
        RedisModule_Log(ctx, "notice", "%s:%s", key, val);
    }
    Gears_dictReleaseIterator(iter);
}

#define DEF_COMMAND(cmd, handler) \
    do { \
        if (RedisModule_CreateCommand(ctx, "rg." #cmd, handler, "readonly", 0, 0, 0) != REDISMODULE_OK) { \
            RedisModule_Log(ctx, "warning", "could not register command %s", #cmd); \
            return REDISMODULE_ERR; \
        } \
    } while (false)

int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    DefaultGearsConfig = (RedisGears_Config){
        .maxExecutions = {
            .val.longVal = 1000,
            .type = LONG,
        },
        .maxExecutionsPerRegistration = {
            .val.longVal = 100,
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
        .dependenciesUrl = {
            .val.str = RG_STRDUP(DependenciesUrl),
            .type = STR,
        },
        .dependenciesSha256 = {
            .val.str = RG_STRDUP(DependenciesSha256),
            .type = STR,
        },
        .createVenv = {
            .val.longVal = 0,
            .type = LONG,
        },
        .executionThreads = {
            .val.longVal = 3,
            .type = LONG,
        },
        .executionMaxIdleTime = {
            .val.longVal = 5000,
            .type = LONG,
        },
        .pythonInstallReqMaxIdleTime = {
            .val.longVal = 30000,
            .type = LONG,
        },
        .pythonInstallationDir = {
            .val.str = RG_STRDUP("/var/opt/redislabs/modules/rg"),
            .type = STR,
        },
        .downloadDeps = {
            .val.longVal = 1,
            .type = LONG,
        },
        .foreceDownloadDepsOnEnterprise = {
            .val.longVal = 0,
            .type = LONG,
        },
        .sendMsgRetries = {
            .val.longVal = 3,
            .type = LONG,
        },
    };

    Gears_ExtraConfig = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    DEF_COMMAND(configget, GearsConfig_Get);
    DEF_COMMAND(configset, GearsConfig_Set);

    ArgsIterator iter = {
        .currIndex = 0,
        .argv = argv,
        .argc = argc,
    };  
    if(GearsConfig_Set_with_iterator(ctx, &iter, true) != REDISMODULE_OK){
        return REDISMODULE_ERR;
    }
    GearsConfig_Print(ctx);

    return REDISMODULE_OK;
}

