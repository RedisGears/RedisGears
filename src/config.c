/*
 * config.c
 *
 *  Created on: 7 Jan 2019
 *      Author: root
 */

#include "config.h"
#include "redisgears_memory.h"
#include "utils/arr.h"

#include <assert.h>

typedef struct ConfigHook{
    BeforeConfigSet before;
    AfterConfigSet after;
    GetConfig getConfig;
}ConfigHook;

ConfigHook* configHooks;

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
    STR, LONG, DOUBLE, ARRAY
}ConfigValType;

typedef struct ConfigVal{
    union{
        char* str;
        long long longVal;
        double doubleVal;
        char** vals;
    }val;
    ConfigValType type;
}ConfigVal;

typedef struct RedisGears_Config{
    ConfigVal maxExecutions;
    ConfigVal maxExecutionsPerRegistration;
    ConfigVal profileExecutions;
    ConfigVal executionThreads;
    ConfigVal executionMaxIdleTime;
    ConfigVal sendMsgRetries;
    ConfigVal plugins;
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

static const ConfigVal* ConfigVal_PluginsGet(){
    return &DefaultGearsConfig.plugins;
}

static bool ConfigVal_PluginsSet(ArgsIterator* iter){
    RedisModuleString* val = ArgsIterator_Next(iter);
    if(!val){
        return false;
    }
    const char* valStr = RedisModule_StringPtrLen(val, NULL);
    array_append(DefaultGearsConfig.plugins.val.vals, RG_STRDUP(valStr));
    return true;
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
        .name = "SendMsgRetries",
        .getter = ConfigVal_SendMsgRetriesGet,
        .setter = ConfigVal_SendMsgRetriesSet,
        .configurableAtRunTime = true,
    },
    {
        .name = "Plugin",
        .getter = ConfigVal_PluginsGet,
        .setter = ConfigVal_PluginsSet,
        .configurableAtRunTime = false,
    },
    {
        NULL,
    },
};

static void config_error(RedisModuleCtx *ctx, const char *fmt, const char* configItem, bool sendReply) {
    RedisModule_Log(staticCtx, "warning", fmt, configItem);

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
            bool hookError = false;
            for(size_t i = 0 ; i < array_len(configHooks) ; ++i){
                if(configHooks[i].before){
                    char* err = NULL;
                    if(configHooks[i].before(configName, valCStr, &err) != REDISMODULE_OK){
                        if(!err){
                            rg_asprintf(&err, "Failed setting config value for key '%s' with value '%s'", configName, valCStr);
                        }
                        hookError = true;
                        RedisModule_Log(staticCtx, "warning", "%s", err);
                        if(!isFirstInitialization){
                            RedisModule_ReplyWithError(ctx, err);
                        }
                        RG_FREE(err);
                        break;
                    }
                }
            }

            if(hookError){
                error = true;
                continue;
            }

            Gears_dictEntry *existing = NULL;
            Gears_dictEntry *entry = Gears_dictAddRaw(Gears_ExtraConfig, (char*)configName, &existing);
            if(!entry){
                char* val = Gears_dictGetVal(existing);
                RG_FREE(val);
                entry = existing;
            }
            Gears_dictSetVal(Gears_ExtraConfig, entry, RG_STRDUP(valCStr));

            for(size_t i = 0 ; i < array_len(configHooks) ; ++i){
                if(configHooks[i].after){
                    configHooks[i].after(configName, valCStr);
                }
            }

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
    case ARRAY:
        RedisModule_ReplyWithArray(ctx, array_len(confVal->val.vals));
        for(size_t i = 0 ; i < array_len(confVal->val.vals) ; ++i){
            RedisModule_ReplyWithStringBuffer(ctx, confVal->val.vals[i], strlen(confVal->val.vals[i]));
        }
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
            if (strcmp(configName, val->name) == 0) {
                const ConfigVal* confVal = val->getter(configName);
                GearsConfig_ReplyWithConfVal(ctx, confVal);
                found = true;
                ++n_values;
                break;
            }
        }
        if (!found) {
            bool foundInPlugin = false;
            ++n_values;
            for(size_t i = 0 ; i < array_len(configHooks) ; ++i){
                if(configHooks[i].getConfig){
                    char* err = NULL;
                    if(configHooks[i].getConfig(configName, ctx) == REDISMODULE_OK){
                        // pluging took responsibility of the config
                        foundInPlugin = true;
                        break;
                    }
                }
            }

            if(!foundInPlugin){
                const char* val = Gears_dictFetchValue(Gears_ExtraConfig, (char*)configName);
                if (val) {
                    RedisModule_ReplyWithCString(ctx, val);
                }else{
                    config_error(ctx, "Unsupported config parameter: %s", configName, true);
                    error = true;
                }
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

const char* GearsConfig_GetExtraConfigVals(const char* key){
    return Gears_dictFetchValue(Gears_ExtraConfig, key);
}

char** GearsConfig_GetPlugins(){
    return DefaultGearsConfig.plugins.val.vals;
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

static char* GearsConfig_ArrayConfigValToStr(void* val){
    return RG_STRDUP(val);
}

static void GearsConfig_Print(RedisModuleCtx* ctx){
    char* arrayStr = NULL;
    for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
        const ConfigVal* v = val->getter();
        switch(v->type){
        case STR:
            RedisModule_Log(staticCtx, "notice", "%s:%s", val->name, v->val.str);
            break;
        case LONG:
            RedisModule_Log(staticCtx, "notice", "%s:%lld", val->name, v->val.longVal);
            break;
        case DOUBLE:
            RedisModule_Log(staticCtx, "notice", "%s:%lf", val->name, v->val.doubleVal);
            break;
        case ARRAY:
            arrayStr = ArrToStr((void**)v->val.vals, array_len(v->val.vals), GearsConfig_ArrayConfigValToStr, ',');
            RedisModule_Log(staticCtx, "notice", "%s:%s", val->name, arrayStr);
            RG_FREE(arrayStr);
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
        RedisModule_Log(staticCtx, "notice", "%s:%s", key, val);
    }
    Gears_dictReleaseIterator(iter);
}

#define DEF_COMMAND(cmd, handler) \
    do { \
        if (RedisModule_CreateCommand(staticCtx, "rg." #cmd, handler, "readonly", 0, 0, 0) != REDISMODULE_OK) { \
            RedisModule_Log(ctx, "warning", "could not register command %s", #cmd); \
            return REDISMODULE_ERR; \
        } \
    } while (false)

void GearsConfig_AddHooks(BeforeConfigSet before, AfterConfigSet after, GetConfig getConfig){
    ConfigHook hook = {.before = before, .after = after, .getConfig = getConfig};
    configHooks = array_append(configHooks, hook);
}

int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    configHooks = array_new(ConfigHook, 10);

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
        .executionThreads = {
            .val.longVal = 3,
            .type = LONG,
        },
        .executionMaxIdleTime = {
            .val.longVal = 5000,
            .type = LONG,
        },
        .sendMsgRetries = {
            .val.longVal = 3,
            .type = LONG,
        },
        .plugins = {
            .val.vals = array_new(char*, 10),
            .type = ARRAY,
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

