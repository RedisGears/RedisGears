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
StreamReaderTriggerArgs* StreamReaderTriggerArgs_Create(const char* streamName, size_t batchSize, size_t durationInSec);

#endif /* SRC_STREAMS_READER_H_ */
