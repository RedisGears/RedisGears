/*
 * memory.h
 *
 *  Created on: Oct 18, 2018
 *      Author: meir
 */

#ifndef SRC_REDISGEARG_MEMORY_H_
#define SRC_REDISGEARG_MEMORY_H_

#include "redismodule.h"
#include <stdlib.h>
#include <string.h>

#ifdef VALGRIND
#define RG_ALLOC malloc
#define RG_CALLOC calloc
#define RG_REALLOC realloc
#define RG_FREE free
#define RG_STRDUP strdup
#else
#define RG_ALLOC RedisModule_Alloc
#define RG_CALLOC RedisModule_Calloc
#define RG_REALLOC RedisModule_Realloc
#define RG_FREE RedisModule_Free
#define RG_STRDUP RedisModule_Strdup
#endif



#endif /* SRC_REDISGEARG_MEMORY_H_ */
