
#ifndef SRC_READERS_COMMAND_READER_H_
#define SRC_READERS_COMMAND_READER_H_

#include "redisgears.h"
extern RedisGears_ReaderCallbacks CommandReader;

int CommandReader_Initialize(RedisModuleCtx* ctx);
CommandReaderTriggerArgs* CommandReaderTriggerArgs_Create(const char* trigger);
void CommandReaderTriggerArgs_Free(CommandReaderTriggerArgs* crtArgs);

#endif /* SRC_READERS_COMMAND_READER_H_ */
