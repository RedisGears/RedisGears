/*
 * streams_reader.h
 *
 *  Created on: May 6, 2019
 *      Author: root
 */

#ifndef SRC_STREAMS_READER_H_
#define SRC_STREAMS_READER_H_

#include "redismodule.h"

Reader* StreamReader(void* arg);
StreamReaderCtx* StreamReaderCtx_Create(const char* streamName, const char* streamId);

#endif /* SRC_STREAMS_READER_H_ */
