/*
 * keys_reader.h
 *
 *  Created on: 9 Jan 2019
 *      Author: root
 */

#ifndef SRC_KEYS_READER_H_
#define SRC_KEYS_READER_H_

#include "redisgears.h"

int KeysReader_Initialize(RedisModuleCtx* ctx);
Reader* KeysReader(void* arg);
KeysReaderCtx* KeysReaderCtx_Create(char* match);


#endif /* SRC_KEYS_READER_H_ */
