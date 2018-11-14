/*
 * mgmt.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_MGMT_H_
#define SRC_MGMT_H_

#include <stdbool.h>
#include "redistar.h"

typedef struct ArgType{
    char* type;
    ArgFree free;
    ArgSerialize serialize;
    ArgDeserialize deserialize;
}ArgType;

bool FiltersMgmt_Add(const char* name, RediStar_FilterCallback callback, ArgType* type);
RediStar_FilterCallback FiltersMgmt_Get(const char* name);
ArgType* FiltersMgmt_GetArgType(const char* name);

bool MapsMgmt_Add(const char* name, RediStar_MapCallback callback, ArgType* type);
RediStar_MapCallback MapsMgmt_Get(const char* name);
ArgType* MapsMgmt_GetArgType(const char* name);

bool ReadersMgmt_Add(const char* name, RediStar_ReaderCallback callback, ArgType* type);
RediStar_ReaderCallback ReadersMgmt_Get(const char* name);
ArgType* ReadersMgmt_GetArgType(const char* name);

bool WritersMgmt_Add(const char* name, RediStar_WriterCallback callback, ArgType* type);
RediStar_WriterCallback WritersMgmt_Get(const char* name);
ArgType* WritersMgmt_GetArgType(const char* name);

bool ExtractorsMgmt_Add(const char* name, RediStar_ExtractorCallback callback, ArgType* type);
RediStar_ExtractorCallback ExtractorsMgmt_Get(const char* name);
ArgType* ExtractorsMgmt_GetArgType(const char* name);

bool ReducersMgmt_Add(const char* name, RediStar_ReducerCallback callback, ArgType* type);
RediStar_ReducerCallback ReducersMgmt_Get(const char* name);
ArgType* ReducersMgmt_GetArgType(const char* name);

void Mgmt_Init();


#endif /* SRC_MGMT_H_ */
