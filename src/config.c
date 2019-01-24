/*
 * config.c
 *
 *  Created on: 7 Jan 2019
 *      Author: root
 */

#include "config.h"
#include "redisgears_memory.h"
#include <stdbool.h>
#include <assert.h>

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
	RG_FREE(DefaultGearsConfig.pythonHomeDir.val.str);
	const char* valStr = RedisModule_StringPtrLen(val, NULL);
	DefaultGearsConfig.pythonHomeDir.val.str = RG_STRDUP(valStr);
	return true;
}

static Gears_ConfigVal Gears_ConfigVals[] = {
		{
				.name = "PythonHomeDir",
				.getter = ConfigVal_PythonHomeDirGet,
				.setter = ConfigVal_PythonHomeDirSet,
				.configurableAtRunTime = false,
		},
		{
				NULL,
		},
};

static int  GearsCOnfig_Set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_ReplyWithError(ctx, "not implemented yet");
	return REDISMODULE_OK;
}

static void GearsCOnfig_ReplyWithConfVal(RedisModuleCtx *ctx, const ConfigVal* confVal){
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

static int  GearsCOnfig_Get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define UNKNOWN_CONFIG_PARAM "unknown config parameter"
	ArgsIterator iter = {
			.currIndex = 0,
			.argv = argv,
			.argc = argc,
	};

	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	RedisModuleString* curr = NULL;
	while((curr = ArgsIterator_Next(&iter))){
		const char* configVal = RedisModule_StringPtrLen(curr, NULL);
		for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
			if(strcasecmp(configVal, val->name) == 0){
				const ConfigVal* confVal = val->getter(configVal);
				GearsCOnfig_ReplyWithConfVal(ctx, confVal);
				continue;
			}
		}
		RedisModule_ReplyWithStringBuffer(ctx, UNKNOWN_CONFIG_PARAM, strlen(UNKNOWN_CONFIG_PARAM));
	}
	RedisModule_ReplySetArrayLength(ctx, iter.currIndex);

	return REDISMODULE_OK;
}

const char* GearsCOnfig_GetPythonHomeDir(){
	return DefaultGearsConfig.pythonHomeDir.val.str;
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

int GearsConfig_Init(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
	DefaultGearsConfig = (RedisGears_Config){
		.pythonHomeDir = {
				.val.str = RG_STRDUP("/usr/bin/"),
				.type = STR,
		},
	};


	if (RedisModule_CreateCommand(ctx, "rg.configget", GearsCOnfig_Get, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.configget");
		return REDISMODULE_ERR;
	}

	if (RedisModule_CreateCommand(ctx, "rg.configset", GearsCOnfig_Set, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.configset");
		return REDISMODULE_ERR;
	}

	ArgsIterator iter = {
			.currIndex = 0,
			.argv = argv,
			.argc = argc,
	};

	RedisModuleString* curr = NULL;
	while((curr = ArgsIterator_Next(&iter))){
		const char* configVal = RedisModule_StringPtrLen(curr, NULL);
		bool found = false;
		for(Gears_ConfigVal* val = &Gears_ConfigVals[0]; val->name != NULL ; val++){
			if(strcasecmp(configVal, val->name) == 0){
				if(!val->setter(&iter)){
					RedisModule_Log(ctx, "warning", "failed reading config value %s", configVal);
					return REDISMODULE_ERR;
				}
				found = true;
				break;
			}
		}
		if(!found){
			RedisModule_Log(ctx, "warning", "unknown config value %s", configVal);
			return REDISMODULE_ERR;
		}
	}

	GearsConfig_Print(ctx);
	return REDISMODULE_OK;
}

