
#ifndef SRC_KEYS_READER_H_
#define SRC_KEYS_READER_H_

#include "redisgears.h"

extern RedisGears_ReaderCallbacks KeysReader;

int KeysReader_Initialize(RedisModuleCtx* ctx);

/*
 * Create a KeysReaderTriggerArgs from the given values.
 * regex - regex to register on, Copied and not owned by the function.
 * eventTypes - array of events types to filter, function takes ownership on this value, the caller should not use it anymore
 * keyTypes - array of keys types to filter, function takes ownership on this value, the caller should not use it anymore
 */
KeysReaderTriggerArgs* KeysReaderTriggerArgs_Create(const char* prefix, char** eventTypes, int* keyTypes, bool readValue);
int KeysReaderTriggerArgs_SetReadRecordCallback(KeysReaderTriggerArgs* krta, const char* readRecordCallback);
void KeysReaderTriggerArgs_Free(KeysReaderTriggerArgs* args);


KeysReaderCtx* KeysReaderCtx_Create(const char* match, bool readValue, const char* event, bool exactMatch);
int KeysReaderCtx_SetReadRecordCallback(KeysReaderCtx* krCtx, const char* readRecordCallback);
void KeysReaderCtx_Free(void* ctx);

#endif /* SRC_KEYS_READER_H_ */
