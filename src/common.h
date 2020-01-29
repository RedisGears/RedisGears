/*
 * common.h
 *
 *  Created on: 10 Nov 2018
 *      Author: root
 */

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_

#include "utils/dict.h"

#define ID_LEN REDISMODULE_NODE_ID_LEN + sizeof(long long) + 1 // the +1 is for the \0
#define STR_ID_LEN  REDISMODULE_NODE_ID_LEN + 13

extern Gears_dictType* dictTypeHeapIdsPtr;

void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);

#endif /* SRC_COMMANDS_H_ */

