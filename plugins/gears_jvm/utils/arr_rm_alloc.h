#ifndef ARR_RM_ALLOC_H_
#define ARR_RM_ALLOC_H_

/* A wrapper for arr.h that sets the allocation functions to those of the RedisModule_Alloc &
 * friends. This file should not be included alongside arr.h, and should not be included from .h
 * files in general */

#include "../jvm_memory.h"

/* Define the allcation functions before including arr.h */
#define array_alloc_fn JVM_ALLOC
#define array_realloc_fn JVM_REALLOC
#define array_free_fn JVM_FREE

#include "arr.h"

#endif
