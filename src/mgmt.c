#include "utils/dict.h"
#include "redistar_memory.h"
#include "mgmt.h"

#define GENERATE(apiName)\
    typedef struct apiName ## Holder{\
        ArgType* type;\
        void* callback;\
    }apiName ## Holder;\
    dict* apiName ## dict = NULL;\
    void apiName ## sMgmt_Init(){\
        apiName ## dict = dictCreate(&dictTypeHeapStrings, NULL);\
    }\
    bool apiName ## sMgmt_Add(char* name, RediStar_ ## apiName ## Callback callback, ArgType* type){\
        apiName ## Holder* holder = RS_ALLOC(sizeof(*holder));\
        holder->type = type;\
        holder->callback = callback;\
        return dictAdd(apiName ## dict, name, holder);\
    }\
    RediStar_ ## apiName ## Callback apiName ## sMgmt_Get(char* name){\
        dictEntry *entry = dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        apiName ## Holder* holder = dictGetVal(entry);\
        return holder->callback;\
    }\
    ArgType* apiName ## sMgmt_GetArgType(char* name){\
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
GENERATE(Writer)
GENERATE(Extractor)
GENERATE(Reducer)

void Mgmt_Init(){
    FiltersMgmt_Init();
    MapsMgmt_Init();
    ReadersMgmt_Init();
    WritersMgmt_Init();
    ExtractorsMgmt_Init();
    ReducersMgmt_Init();
}
