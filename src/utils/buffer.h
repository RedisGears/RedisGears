/*
 * buffer.h
 *
 *  Created on: Sep 23, 2018
 *      Author: meir
 */

#ifndef SRC_UTILS_BUFFER_H_
#define SRC_UTILS_BUFFER_H_

#include <stddef.h>
#include "redismodule.h"

#define DEFAULT_INITIAL_CAP 50

typedef struct Buffer{
    size_t cap;
    size_t size;
    char* buff;
}Buffer;

#define Buffer_Create() Buffer_New(DEFAULT_INITIAL_CAP)

Buffer* Buffer_New(size_t initialCap);
void Buffer_Free(Buffer* buff);
void Buffer_Add(Buffer* buff, char* data, size_t len);
void Buffer_Clear(Buffer* buff);



#endif /* SRC_UTILS_BUFFER_H_ */
