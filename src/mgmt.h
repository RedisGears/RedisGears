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

bool FiltersMgmt_Add(char* name, RediStar_FilterCallback callback, ArgType* type);
RediStar_FilterCallback FiltersMgmt_Get(char* name);
ArgType* FiltersMgmt_GetArgType(char* name);

bool MapsMgmt_Add(char* name, RediStar_MapCallback callback, ArgType* type);
RediStar_MapCallback MapsMgmt_Get(char* name);
ArgType* MapsMgmt_GetArgType(char* name);

bool ReadersMgmt_Add(char* name, RediStar_ReaderCallback callback, ArgType* type);
RediStar_ReaderCallback ReadersMgmt_Get(char* name);
ArgType* ReadersMgmt_GetArgType(char* name);

bool WritersMgmt_Add(char* name, RediStar_WriterCallback callback, ArgType* type);
RediStar_WriterCallback WritersMgmt_Get(char* name);
ArgType* WritersMgmt_GetArgType(char* name);

bool ExtractorsMgmt_Add(char* name, RediStar_ExtractorCallback callback, ArgType* type);
RediStar_ExtractorCallback ExtractorsMgmt_Get(char* name);
ArgType* ExtractorsMgmt_GetArgType(char* name);

bool ReducersMgmt_Add(char* name, RediStar_ReducerCallback callback, ArgType* type);
RediStar_ReducerCallback ReducersMgmt_Get(char* name);
ArgType* ReducersMgmt_GetArgType(char* name);

void Mgmt_Init();


#endif /* SRC_MGMT_H_ */
