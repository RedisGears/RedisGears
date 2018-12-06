/*
 * mgmt.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_MGMT_H_
#define SRC_MGMT_H_

#include <stdbool.h>

#include "redisgears.h"

typedef struct ArgType{
    char* type;
    ArgFree free;
    ArgSerialize serialize;
    ArgDeserialize deserialize;
}ArgType;

bool FiltersMgmt_Add(const char* name, RedisGears_FilterCallback callback, ArgType* type);
RedisGears_FilterCallback FiltersMgmt_Get(const char* name);
ArgType* FiltersMgmt_GetArgType(const char* name);

bool MapsMgmt_Add(const char* name, RedisGears_MapCallback callback, ArgType* type);
RedisGears_MapCallback MapsMgmt_Get(const char* name);
ArgType* MapsMgmt_GetArgType(const char* name);

bool ReadersMgmt_Add(const char* name, RedisGears_ReaderCallback callback, ArgType* type);
RedisGears_ReaderCallback ReadersMgmt_Get(const char* name);
ArgType* ReadersMgmt_GetArgType(const char* name);

bool ForEachsMgmt_Add(const char* name, RedisGears_ForEachCallback callback, ArgType* type);
RedisGears_ForEachCallback ForEachsMgmt_Get(const char* name);
ArgType* ForEachsMgmt_GetArgType(const char* name);

bool ExtractorsMgmt_Add(const char* name, RedisGears_ExtractorCallback callback, ArgType* type);
RedisGears_ExtractorCallback ExtractorsMgmt_Get(const char* name);
ArgType* ExtractorsMgmt_GetArgType(const char* name);

bool ReducersMgmt_Add(const char* name, RedisGears_ReducerCallback callback, ArgType* type);
RedisGears_ReducerCallback ReducersMgmt_Get(const char* name);
ArgType* ReducersMgmt_GetArgType(const char* name);

bool AccumulatesMgmt_Add(const char* name, RedisGears_AccumulateCallback callback, ArgType* type);
RedisGears_AccumulateCallback AccumulatesMgmt_Get(const char* name);
ArgType* AccumulatesMgmt_GetArgType(const char* name);

void Mgmt_Init();


#endif /* SRC_MGMT_H_ */
