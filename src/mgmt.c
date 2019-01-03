#include "utils/dict.h"
#include "mgmt.h"
#include "redisgears_memory.h"

#define GENERATE(apiName)\
    typedef struct apiName ## Holder{\
        ArgType* type;\
        void* callback;\
    }apiName ## Holder;\
    dict* apiName ## dict = NULL;\
    void apiName ## sMgmt_Init(){\
        apiName ## dict = dictCreate(&dictTypeHeapStrings, NULL);\
    }\
    bool apiName ## sMgmt_Add(const char* name, RedisGears_ ## apiName ## Callback callback, ArgType* type){\
        apiName ## Holder* holder = RG_ALLOC(sizeof(*holder));\
        holder->type = type;\
        holder->callback = callback;\
        return dictAdd(apiName ## dict, (void*)name, holder);\
    }\
    RedisGears_ ## apiName ## Callback apiName ## sMgmt_Get(const char* name){\
        dictEntry *entry = dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        apiName ## Holder* holder = dictGetVal(entry);\
        return holder->callback;\
    }\
    ArgType* apiName ## sMgmt_GetArgType(const char* name){\
        dictEntry *entry = dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        apiName ## Holder* holder = dictGetVal(entry);\
        return holder->type;\
    }


GENERATE(Filter)
GENERATE(Map)
GENERATE(Reader)
GENERATE(ForEach)
GENERATE(Extractor)
GENERATE(Reducer)
GENERATE(Accumulate)
GENERATE(AccumulateByKey)

void Mgmt_Init(){
    FiltersMgmt_Init();
    MapsMgmt_Init();
    ReadersMgmt_Init();
    ForEachsMgmt_Init();
    ExtractorsMgmt_Init();
    ReducersMgmt_Init();
    AccumulatesMgmt_Init();
    AccumulateByKeysMgmt_Init();
}
