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
    ret->cap = initialCap;
    ret->size = 0;
    ret->buff = RS_ALLOC(initialCap * sizeof(char));
    return ret;
}

void Buffer_Free(Buffer* buff){
    RS_FREE(buff->buff);
    RS_FREE(buff);
}

void Buffer_Add(Buffer* buff, char* data, size_t len){
    if (buff->size + len >= buff->cap){
        buff->cap = buff->size + len;
        buff->buff = RS_REALLOC(buff->buff, buff->cap);
    }
    memcpy(buff->buff + buff->size, data, len);
    buff->size += len;
}

void Buffer_Clear(Buffer* buff){
    buff->size = 0;
}

void BufferWriter_Init(BufferWriter* bw, Buffer* buff){
    bw->buff = buff;
}

void BufferWriter_WriteLong(BufferWriter* bw, long val){
    Buffer_Add(bw->buff, (char*)&val, sizeof(long));
}

void BufferWriter_WriteString(BufferWriter* bw, char* str){
    BufferWriter_WriteBuff(bw, str, strlen(str) + 1);
}

void BufferWriter_WriteBuff(BufferWriter* bw, char* buff, size_t len){
    BufferWriter_WriteLong(bw, len);
    Buffer_Add(bw->buff, buff, len);
}

void BufferReader_Init(BufferReader* br, Buffer* buff){
    br->buff = buff;
    br->location = 0;
}

long BufferReader_ReadLong(BufferReader* br){
    long ret = *(long*)(&br->buff->buff[br->location]);
    br->location += sizeof(long);
    return ret;
}

char* BufferReader_ReadBuff(BufferReader* br, size_t* len){
    *len = (size_t)BufferReader_ReadLong(br);
    char* ret = br->buff->buff + br->location;
    br->location += *len;
    return ret;
}

char* BufferReader_ReadString(BufferReader* br){
    size_t len;
    return BufferReader_ReadBuff(br, &len);
}

