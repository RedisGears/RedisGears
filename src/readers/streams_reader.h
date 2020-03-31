/*
 * streams_reader.h
 *
 *  Created on: May 6, 2019
 *      Author: root
 */

#ifndef SRC_STREAMS_READER_H_
#define SRC_STREAMS_READER_H_

#include "redismodule.h"

extern RedisGears_ReaderCallbacks StreamReader;
StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId);
void StreamReaderCtx_Free(StreamReaderCtx* readerCtx);
StreamReaderTriggerArgs* StreamReaderTriggerArgs_Create(const char* streamPrefix, size_t batchSize, size_t durationMS, OnFailedPolicy onFailedPolicy, size_t retryInterval, bool trimStream);
void StreamReaderTriggerArgs_Free(StreamReaderTriggerArgs* args);

#endif /* SRC_STREAMS_READER_H_ */
