#include "utils/dict.h"
#include "mgmt.h"

#define GENERATE(apiName)\
    dict* apiName ## dict = NULL;\
    void apiName ## sMgmt_Init(){\
        apiName ## dict = dictCreate(&dictTypeHeapStrings, NULL);\
    }\
    bool apiName ## sMgmt_Add(char* name, RediStar_ ## apiName ## Callback callback){\
        return dictAdd(apiName ## dict, name, callback);\
    }\
    RediStar_ ## apiName ## Callback apiName ## sMgmt_Get(char* name){\
        dictEntry *entry = dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        return dictGetVal(entry);\
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
