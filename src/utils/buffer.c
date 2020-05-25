/*
 * buffer.c
 *
 *  Created on: Sep 23, 2018
 *      Author: meir
 */

#include "buffer.h"
#include <string.h>
#include "../redisgears_memory.h"
#include "redisgears.h"


Gears_Buffer* Gears_BufferNew(size_t initialCap){
    Gears_Buffer* ret = RG_ALLOC(sizeof(*ret));
    ret->cap = initialCap;
    ret->size = 0;
    ret->buff = RG_ALLOC(initialCap * sizeof(char));
    return ret;
}

void Gears_BufferFree(Gears_Buffer* buff){
    RG_FREE(buff->buff);
    RG_FREE(buff);
}

void Gears_BufferAdd(Gears_Buffer* buff, const char* data, size_t len){
    if (buff->size + len >= buff->cap){
        buff->cap = buff->size + len;
        buff->buff = RG_REALLOC(buff->buff, buff->cap);
    }
    memcpy(buff->buff + buff->size, data, len);
    buff->size += len;
}

void Gears_BufferClear(Gears_Buffer* buff){
    buff->size = 0;
}

void Gears_BufferWriterInit(Gears_BufferWriter* bw, Gears_Buffer* buff){
    bw->buff = buff;
}

void Gears_BufferWriterWriteLong(Gears_BufferWriter* bw, long val){
    Gears_BufferAdd(bw->buff, (char*)&val, sizeof(long));
}

void Gears_BufferWriterWriteString(Gears_BufferWriter* bw, const char* str){
    Gears_BufferWriterWriteBuff(bw, str, strlen(str) + 1);
}

void Gears_BufferWriterWriteBuff(Gears_BufferWriter* bw, const char* buff, size_t len){
    Gears_BufferWriterWriteLong(bw, len);
    Gears_BufferAdd(bw->buff, buff, len);
}

void Gears_BufferReaderInit(Gears_BufferReader* br, Gears_Buffer* buff){
    br->buff = buff;
    br->location = 0;
}

long Gears_BufferReaderReadLong(Gears_BufferReader* br){
    if(br->location + sizeof(long) > br->buff->size){
        return LONG_READ_ERROR;
    }
    long ret = *(long*)(&br->buff->buff[br->location]);
    br->location += sizeof(long);
    return ret;
}

char* Gears_BufferReaderReadBuff(Gears_BufferReader* br, size_t* len){
    *len = (size_t)Gears_BufferReaderReadLong(br);
    if((*len == LONG_READ_ERROR) || (br->location + *len > br->buff->size)){
        return BUFF_READ_ERROR;
    }
    char* ret = br->buff->buff + br->location;
    br->location += *len;
    return ret;
}

char* Gears_BufferReaderReadString(Gears_BufferReader* br){
    size_t len;
    return Gears_BufferReaderReadBuff(br, &len);
}

