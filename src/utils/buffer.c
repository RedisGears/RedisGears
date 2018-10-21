/*
 * buffer.c
 *
 *  Created on: Sep 23, 2018
 *      Author: meir
 */

#include <redistar_memory.h>
#include "buffer.h"
#include <string.h>


Buffer* Buffer_New(size_t initialCap){
    Buffer* ret = RS_ALLOC(sizeof(*ret));
    *ret = (Buffer){
            .cap = initialCap,
            .size = 0,
            .buff = RS_ALLOC(initialCap * sizeof(char)),
    };
    return ret;
}

void Buffer_Free(Buffer* buff){
    RS_FREE(buff->buff);
    RS_FREE(buff);
}

void Buffer_Add(Buffer* buff, char* data, size_t len){
    if (buff->size + len > buff->cap){
        buff->cap = buff->size + len;
        buff->buff = RedisModule_Realloc(buff->buff, buff->cap);
    }
    memcpy(buff->buff + buff->size, data, len);
    buff->size += len;
}

void Buffer_Clear(Buffer* buff){
    buff->size = 0;
}

