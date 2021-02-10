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

int CommandHook_Init();

#endif /* SRC_COMMAND_HOOK_H_ */
