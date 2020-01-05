
#ifndef SRC_KEYS_READER_H_
#define SRC_KEYS_READER_H_

#include "redisgears.h"

extern RedisGears_ReaderCallbacks KeysReader;
extern RedisGears_ReaderCallbacks KeysOnlyReader;

int KeysReader_Initialize(RedisModuleCtx* ctx);
KeysReaderTriggerArgs* KeysReaderTriggerArgs_Create(const char* regex, char** eventTypes, int* keyTypes);


#endif /* SRC_KEYS_READER_H_ */
