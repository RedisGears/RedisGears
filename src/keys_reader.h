
#ifndef SRC_KEYS_READER_H_
#define SRC_KEYS_READER_H_

#include "redisgears.h"

extern RedisGears_ReaderCallbacks KeysReader;
extern RedisGears_ReaderCallbacks KeysOnlyReader;

int KeysReader_Initialize(RedisModuleCtx* ctx);

/*
 * Create a KeysReaderTriggerArgs from the given values.
 * regex - regex to register on, Copied and not owned by the function.
 * eventTypes - array of events types to filter, function takes ownership on this value, the caller should not use it anymore
 * keyTypes - array of keys types to filter, function takes ownership on this value, the caller should not use it anymore
 */
KeysReaderTriggerArgs* KeysReaderTriggerArgs_Create(const char* regex, char** eventTypes, int* keyTypes);

void KeysReaderTriggerArgs_Free(KeysReaderTriggerArgs* args);


#endif /* SRC_KEYS_READER_H_ */
