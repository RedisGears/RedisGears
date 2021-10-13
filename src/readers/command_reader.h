
#ifndef SRC_READERS_COMMAND_READER_H_
#define SRC_READERS_COMMAND_READER_H_

#include "redisgears.h"
extern RedisGears_ReaderCallbacks CommandReader;

int CommandReader_Initialize(RedisModuleCtx* ctx);
CommandReaderTriggerArgs* CommandReaderTriggerArgs_CreateTrigger(const char* trigger, int inOrder);
CommandReaderTriggerArgs* CommandReaderTriggerArgs_CreateHook(const char* hook, const char* keyPrefix, int inOrder);
void CommandReaderTriggerArgs_Free(CommandReaderTriggerArgs* crtArgs);

CommandReaderTriggerCtx* CommandReaderTriggerCtx_Get(ExecutionCtx* eCtx);
CommandReaderTriggerCtx* CommandReaderTriggerCtx_GetShallowCopy(CommandReaderTriggerCtx* crtCtx);
void CommandReaderTriggerCtx_Free(CommandReaderTriggerCtx* crtCtx);
RedisModuleCallReply* CommandReaderTriggerCtx_CallNext(CommandReaderTriggerCtx* crtCtx, RedisModuleString** argv, size_t argc);

#endif /* SRC_READERS_COMMAND_READER_H_ */
