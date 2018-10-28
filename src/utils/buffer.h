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

typedef struct BufferWriter{
    Buffer* buff;
}BufferWriter;

void BufferWriter_Init(BufferWriter* bw, Buffer* buff);
void BufferWriter_WriteLong(BufferWriter* bw, long val);
void BufferWriter_WriteString(BufferWriter* bw, char* str);
void BufferWriter_WriteBuff(BufferWriter* bw, char* buff, size_t len);

typedef struct BufferReader{
    Buffer* buff;
    size_t location;
}BufferReader;

void BufferReader_Init(BufferReader* br, Buffer* buff);
long BufferReader_ReadLong(BufferReader* br);
char* BufferReader_ReadBuff(BufferReader* br, size_t* len);
char* BufferReader_ReadString(BufferReader* br);




#endif /* SRC_UTILS_BUFFER_H_ */
