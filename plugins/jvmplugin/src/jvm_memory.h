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
#define JVM_ALLOC malloc
#define JVM_CALLOC calloc
#define JVM_REALLOC realloc
#define JVM_FREE free
#define JVM_STRDUP strdup
#else
#define JVM_ALLOC RedisModule_Alloc
#define JVM_CALLOC RedisModule_Calloc
#define JVM_REALLOC RedisModule_Realloc
#define JVM_FREE RedisModule_Free
#define JVM_STRDUP RedisModule_Strdup
#endif



#endif /* SRC_REDISGEARG_MEMORY_H_ */
