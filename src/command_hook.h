#ifndef SRC_COMMAND_HOOK_H_
#define SRC_COMMAND_HOOK_H_

#include "redismodule.h"

typedef struct CommandHookCtx CommandHookCtx;

typedef int (*HookCallback)(RedisModuleCtx* ctx, RedisModuleString** argv, size_t argc, void* pd);

/*
 * Unregister the given hook, return the private data so the caller will be able to free it.
 */
void* CommandHook_Unhook(CommandHookCtx* hook);

/*
 * Register a hook on dmc with key prefix, returns NULL on error and set a string describing the error in err.
 */
CommandHookCtx* CommandHook_Hook(const char* cmd, const char* keyPrefix, HookCallback callback, void* pd, char** err);

/*
 * Verify that it is possible to hook the command
 * On success return REDISMODULE_OK and REDISMODULE_ERR on error.
 */
int CommandHook_VerifyHook(const char* cmd, const char* keyPrefix, char** err);

/*
 * Used when `getkeys-api` flag is set on a command, calling RedisModule_GetCommandKeys to get
 * command keys, and then declare it to Redis.
 */
void CommandHook_DeclareKeys(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/*
 * Used when `getkeys-api` flag is set on a command and RedisModule_GetCommandKeys is not exists (on
 * old Redis version). In this case we need to get:
 * * first key
 * * last key
 * * jump
 * * number of arguments
 *
 * Base on this parameters, use RedisModule_KeyAtPos to declare the keys.
 */
void CommandHook_DeclareKeysLegacy(RedisModuleCtx *ctx, size_t nArgs, int first, int last, int jump);

int CommandHook_Init();

#endif /* SRC_COMMAND_HOOK_H_ */
