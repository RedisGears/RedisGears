#include "utils/dict.h"
#include "mgmt.h"
#include "redisgears_memory.h"

#define GENERATE(apiName)\
    Gears_dict* apiName ## dict = NULL;\
    void apiName ## sMgmt_Init(){\
        apiName ## dict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);\
    }\
    bool apiName ## sMgmt_Add(const char* name, RedisGears_ ## apiName ## Callback callback, ArgType* type){\
        MgmtDataHolder* holder = RG_ALLOC(sizeof(*holder));\
        holder->type = type;\
        holder->callback = callback;\
        return Gears_dictAdd(apiName ## dict, (void*)name, holder);\
    }\
    RedisGears_ ## apiName ## Callback apiName ## sMgmt_Get(const char* name){\
        Gears_dictEntry *entry = Gears_dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        MgmtDataHolder* holder = Gears_dictGetVal(entry);\
        return holder->callback;\
    }\
    ArgType* apiName ## sMgmt_GetArgType(const char* name){\
        Gears_dictEntry *entry = Gears_dictFind(apiName ## dict, name);\
        if(!entry){\
            return NULL;\
        }\
        MgmtDataHolder* holder = Gears_dictGetVal(entry);\
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
GENERATE(FepPrivateData)
GENERATE(ExecutionOnStart)
GENERATE(ExecutionOnUnpaused)
GENERATE(FlatExecutionOnRegistered)

void Mgmt_Init(){
    FiltersMgmt_Init();
    MapsMgmt_Init();
    ReadersMgmt_Init();
    ForEachsMgmt_Init();
    ExtractorsMgmt_Init();
    ReducersMgmt_Init();
    AccumulatesMgmt_Init();
    AccumulateByKeysMgmt_Init();
    FepPrivateDatasMgmt_Init();
    ExecutionOnStartsMgmt_Init();
    ExecutionOnUnpausedsMgmt_Init();
    FlatExecutionOnRegisteredsMgmt_Init();
}
