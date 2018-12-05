/*
 * memory.h
 *
 *  Created on: Oct 18, 2018
 *      Author: meir
 */

#ifndef SRC_REDISGEARS_MEMORY_H_
#define SRC_REDISGEARS_MEMORY_H_

#include "redismodule.h"
#include "stdlib.h"
#include <string.h>

#ifdef VALGRIND
#define RS_ALLOC malloc
#define RS_REALLOC realloc
#define RS_FREE free
#define RS_STRDUP strdup
#else
#define RS_ALLOC RedisModule_Alloc
#define RS_REALLOC RedisModule_Realloc
#define RS_FREE RedisModule_Free
#define RS_STRDUP RedisModule_Strdup
#endif



#endif /* SRC_REDISGEARS_MEMORY_H_ */
