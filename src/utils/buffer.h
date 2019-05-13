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

typedef struct Gears_Buffer{
    size_t cap;
    size_t size;
    char* buff;
}Gears_Buffer;

#define Gears_BufferCreate() Gears_BufferNew(DEFAULT_INITIAL_CAP)

Gears_Buffer* Gears_BufferNew(size_t initialCap);
void Gears_BufferFree(Gears_Buffer* buff);
void Gears_BufferAdd(Gears_Buffer* buff, const char* data, size_t len);
void Gears_BufferClear(Gears_Buffer* buff);

typedef struct Gears_BufferWriter{
    Gears_Buffer* buff;
}Gears_BufferWriter;

void Gears_BufferWriterInit(Gears_BufferWriter* bw, Gears_Buffer* buff);
void Gears_BufferWriterWriteLong(Gears_BufferWriter* bw, long val);
void Gears_BufferWriterWriteString(Gears_BufferWriter* bw, const char* str);
void Gears_BufferWriterWriteBuff(Gears_BufferWriter* bw, const char* buff, size_t len);

typedef struct Gears_BufferReader{
    Gears_Buffer* buff;
    size_t location;
}Gears_BufferReader;

void Gears_BufferReaderInit(Gears_BufferReader* br, Gears_Buffer* buff);
long Gears_BufferReaderReadLong(Gears_BufferReader* br);
char* Gears_BufferReaderReadBuff(Gears_BufferReader* br, size_t* len);
char* Gears_BufferReaderReadString(Gears_BufferReader* br);




#endif /* SRC_UTILS_BUFFER_H_ */
