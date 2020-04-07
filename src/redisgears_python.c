#include <Python.h>
#include "record.h"
#include "redisai.h"
#include "globals.h"
#include "commands.h"
#include "config.h"
#include <marshal.h>
#include <assert.h>
#include <dirent.h>
#include <redisgears.h>
#include <redisgears_memory.h>
#include <redisgears_python.h>
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "lock_handler.h"
#include "common.h"
#include "GearsBuilder.auto.h"
#include "cloudpickle.auto.h"
#include "version.h"

#define SUB_INTERPRETER_TYPE "subInterpreterType"

/* TODO: this needs to be exported from RAI via an API */
typedef struct RAI_Error {
 int code;
 char* detail;
 char* detail_oneline;
} RAI_Error;

static PyObject* pyGlobals;
PyObject* GearsError;
PyObject* ForceStoppedError;

RecordType* pythonRecordType;

typedef struct PythonRecord{
    Record base;
    PyObject* obj;
}PythonRecord;

static Record* PyObjRecordCreate(){
    PythonRecord* ret = (PythonRecord*)RedisGears_RecordCreate(pythonRecordType);
    ret->obj = NULL;
    return &ret->base;
}

static PyObject* PyObjRecordGet(Record* base){
    assert(base->type == pythonRecordType);
    PythonRecord* r = (PythonRecord*)base;
    return r->obj;
}

static void PyObjRecordSet(Record* base, PyObject* obj){
    assert(base->type == pythonRecordType);
    PythonRecord* r = (PythonRecord*)base;
    r->obj = obj;
}


/*
 * Contains thread pacific data like:
 * - the sub-interpreter
 * - Does execution was triggered
 * - Does time event was trigger
 * - Does the execution is blocking or not
 * - Lock counter, to allow thread to acquire the lock multiple times
 * - doneFunction, in case execution was trigger we want to know what to do once its done
 */
pthread_key_t pythonThreadCtxKey;

/*
 * Thread spacific data
 */
typedef struct PythonThreadCtx{
    int lockCounter;
    RedisModuleCtx* currentCtx;
    ExecutionPlan* createdExecution;
    DoneCallbackFunction doneFunction;
    PythonSessionCtx* currSession;
}PythonThreadCtx;

/* default onDone function */
static void onDone(ExecutionPlan* ep, void* privateData);
char* getPyError();

/* callback that get gears remote builder and run it, used for python client */
static PyObject *runGearsRemoteBuilderCallback;

#define PYTHON_ERROR "error running python code"

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, char** err);
static void TimeEvent_Free(void *value);
static int RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw, char** err);
static PythonThreadCtx* GetPythonThreadCtx();

static long long CurrSessionId = 0;

Gears_dict* SessionsDict = NULL;

static char* venvDir = NULL;

typedef struct PythonRequirementCtx{
    size_t refCount;
    char* basePath;
    char* name;
    char** wheels;
}PythonRequirementCtx;

Gears_dict* RequirementsDict = NULL;

typedef struct PythonSessionCtx{
    size_t refCount;
    char sessionId[ID_LEN];
    char sessionIdStr[STR_ID_LEN];
    PyObject* globalsDict;
    PythonRequirementCtx** requirements;
}PythonSessionCtx;

static PyObject* GearsPyDict_GetItemString(PyObject* dict, const char* key){
    return (dict ? PyDict_GetItemString(dict, key) : NULL);
}

static bool PythonRequirementCtx_DownloadRequirement(PythonRequirementCtx* req){
#define RETRY 3
#define RETRY_SLEEP_IN_SEC 1
    int exitCode;
    for(size_t i = 0 ; i < RETRY; ++i){
        if(GearsConfig_CreateVenv()){
            exitCode = ExecCommand(NULL, "/bin/bash -c \"source %s/bin/activate;cd %s;python -m pip wheel %s\"", venvDir, req->basePath, req->name);
        }else{
            exitCode = ExecCommand(NULL, "/bin/bash -c \"cd %s;%s/bin/python3 -m pip wheel %s\"", req->basePath, venvDir, req->name);
        }
        if(exitCode != 0){
            sleep(RETRY_SLEEP_IN_SEC);
            continue;
        }
        break;
    }
    if(exitCode != 0){
        return false;
    }

    // fills the wheels array
    DIR *dr = opendir(req->basePath);
    assert(dr);
    struct dirent *de;
    while ((de = readdir(dr))){
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0){
            continue;
        }
        char* c = de->d_name;
        req->wheels = array_append(req->wheels, RG_STRDUP(de->d_name));
    }
    closedir(dr);

    return true;
}

static bool PythonRequirementCtx_InstallRequirement(PythonRequirementCtx* req){
    char* filesInDir = array_new(char, 10);
    for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
        char* c = req->wheels[i];
        while(*c){
            filesInDir = array_append(filesInDir, *c);
            ++c;
        }
        filesInDir = array_append(filesInDir, ' ');
    }
    filesInDir[array_len(filesInDir) - 1] = '\0';

    int exitCode;
    if(GearsConfig_CreateVenv()){
        exitCode = ExecCommand(NULL, "/bin/bash -c \"source %s/bin/activate;cd %s;python -m pip install %s\"", venvDir, req->basePath, filesInDir);
    }else{
        exitCode = ExecCommand(NULL, "/bin/bash -c \"cd %s;%s/bin/python3 -m pip install %s\"", req->basePath, venvDir, filesInDir);
    }
    array_free(filesInDir);
    return exitCode == 0;
}

static void PythonRequirementCtx_VerifyBasePath(PythonRequirementCtx* req){
    char* c = req->basePath;
    while(*c){
        if(*c != '/'){
            return;
        }
        c++;
    }
    RedisModule_Log(NULL, "warning", "Fatal!!!, failed verifying basePath of requirment. name:'%s', basePath:'%s'", req->name, req->basePath);
    assert(false);
}

static void PythonRequirementCtx_Free(PythonRequirementCtx* reqCtx){
    if(--reqCtx->refCount){
        return;
    }

    PythonRequirementCtx_VerifyBasePath(reqCtx);
    ExecCommand(NULL, "rm -rf %s", reqCtx->basePath);
    Gears_dictDelete(RequirementsDict, reqCtx->name);
    RG_FREE(reqCtx->name);
    RG_FREE(reqCtx->basePath);
    if(reqCtx->wheels){
        for(size_t i = 0 ; i < array_len(reqCtx->wheels) ; i++){
            RG_FREE(reqCtx->wheels[i]);
        }
        array_free(reqCtx->wheels);
    }
    RG_FREE(reqCtx);
}

static PythonRequirementCtx* PythonRequirementCtx_Get(const char* requirement){
    PythonRequirementCtx* ret = Gears_dictFetchValue(RequirementsDict, requirement);
    if(ret){
        ++ret->refCount;
    }
    return ret;
}

static PythonRequirementCtx* PythonRequirementCtx_Create(const char* requirement){

    // first lets create basePath
    int exitCode = ExecCommand(NULL, "mkdir -p %s/%s", venvDir, requirement);
    if(exitCode != 0){
        return NULL;
    }

    PythonRequirementCtx* ret = RG_ALLOC(sizeof(*ret));
    ret->name = RG_STRDUP(requirement);
    rg_asprintf(&ret->basePath, "%s/%s", venvDir, ret->name);
    ret->wheels = array_new(char*, 10);
    // refCount is starting from 2, one hold by RequirementsDict and once by the called.
    // currently we basically never delete requirments so we will know not to reinstall them
    // to save time
    ret->refCount = 2;

    Gears_dictAdd(RequirementsDict, ret->name, ret);

    PythonRequirementCtx_VerifyBasePath(ret);

    return ret;
}

static char* PythonRequirementCtx_WheelToStr(void* wheel){
    return RG_STRDUP(wheel);
}

static char* PythonRequirementCtx_ToStr(void* val){
    PythonRequirementCtx* req = val;
    char* res;
    char* wheelsStr = ArrToStr((void**)req->wheels, array_len(req->wheels), PythonRequirementCtx_WheelToStr);
    rg_asprintf(&res, "{'name':'%s', 'basePath':'%s', 'wheels':%s}", req->name, req->basePath, wheelsStr);
    RG_FREE(wheelsStr);
    return res;
}

static PythonRequirementCtx* PythonRequirementCtx_Deserialize(Gears_BufferReader* br, char** err){
    const char* name = RedisGears_BRReadString(br);
    bool reqExists = true;
    PythonRequirementCtx* req = PythonRequirementCtx_Get(name);
    if(!req){
        reqExists = false;
        req = PythonRequirementCtx_Create(name);
    }

    long nWheels = RedisGears_BRReadLong(br);
    for(size_t i = 0 ; i < nWheels ; ++i){
        const char* fileName = RedisGears_BRReadString(br);
        size_t dataLen;
        const char* data = RedisGears_BRReadBuffer(br, &dataLen);

        if(!reqExists){
            char* filePath;
            rg_asprintf(&filePath, "%s/%s", req->basePath, fileName);

            FILE *f = fopen(filePath, "wb");
            if(!f){
                PythonRequirementCtx_Free(req);
                RG_FREE(filePath);
                *err = RG_STRDUP("Failed open file for write");
                return NULL;
            }

            size_t dataWriten = fwrite(data, 1, dataLen, f);
            if(dataWriten != dataLen){
                PythonRequirementCtx_Free(req);
                RG_FREE(filePath);
                *err = RG_STRDUP("Failed writing data to file");
                return NULL;
            }

            fclose(f);
            RG_FREE(filePath);

            req->wheels = array_append(req->wheels, RG_STRDUP(fileName));
        }
    }

    if(!reqExists){
        if(!PythonRequirementCtx_InstallRequirement(req)){
            PythonRequirementCtx_Free(req);
            *err = RG_STRDUP("Failed installing requirement");
            return NULL;
        }
    }

    return req;
}

static int PythonRequirementCtx_Serialize(PythonRequirementCtx* req, Gears_BufferWriter* bw, char** err){
    RedisGears_BWWriteString(bw, req->name);
    RedisGears_BWWriteLong(bw, array_len(req->wheels));
    for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
        char* wheel = req->wheels[i];
        char* filePath;
        rg_asprintf(&filePath, "%s/%s", req->basePath, wheel);

        FILE *f = fopen(filePath, "rb");
        if(!f){
            RG_FREE(filePath);
            rg_asprintf(err, "Could not open file %s", filePath);
            RedisModule_Log(NULL, "warning", *err);
            return REDISMODULE_ERR;
        }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

        char *data = RG_ALLOC(fsize);
        size_t readData = fread(data, 1, fsize, f);
        if(readData != fsize){
            RG_FREE(data);
            RG_FREE(filePath);
            rg_asprintf(err, "Could read data from file %s", filePath);
            RedisModule_Log(NULL, "warning", *err);
            return REDISMODULE_ERR;
        }
        fclose(f);

        RedisGears_BWWriteString(bw, wheel);
        RedisGears_BWWriteBuffer(bw, data, fsize);

        RG_FREE(data);
        RG_FREE(filePath);
    }
    return REDISMODULE_OK;
}

static void* PythonSessionCtx_ShellowCopy(void* arg){
    PythonSessionCtx* session = arg;
    ++session->refCount;
    return session;
}

static void PythonSessionCtx_FreeRequirementsList(PythonRequirementCtx** requirementsList){
    for(size_t i = 0 ; i < array_len(requirementsList) ; ++i){
        PythonRequirementCtx_Free(requirementsList[i]);
    }
    array_free(requirementsList);
}

static void PythonSessionCtx_Free(void* arg){
    PythonSessionCtx* session = arg;
    if(--session->refCount == 0){
        // delete the session working dir
        Gears_dictDelete(SessionsDict, session->sessionId);
        PythonSessionCtx_FreeRequirementsList(session->requirements);
        void* old = RedisGearsPy_Lock(NULL);
        Py_DECREF(session->globalsDict);
        RedisGearsPy_Unlock(old);
        RG_FREE(session);
    }
}

static PythonSessionCtx* PythonSessionCtx_Get(char* id){
    return Gears_dictFetchValue(SessionsDict, id);
}

static PythonSessionCtx* PythonSessionCtx_CreateWithId(char* id, const char** requirementsList, size_t requirementsListLen){
    // creating a new global dict for this session
    void* old = RedisGearsPy_Lock(NULL);

    PyObject* globalDict = PyDict_Copy(pyGlobals);

    PythonSessionCtx* session = RG_ALLOC(sizeof(*session));
    *session = (PythonSessionCtx){
            .refCount = 1,
            .globalsDict = globalDict,
    };
    SetId(NULL, session->sessionId, session->sessionIdStr, &CurrSessionId);

    session->requirements = array_new(PythonRequirementCtx*, 10);

    Gears_dictAdd(SessionsDict, session->sessionId, session);

    for(size_t i = 0 ; i < requirementsListLen ; ++i){
        PythonRequirementCtx* req = PythonRequirementCtx_Get(requirementsList[i]);

        if(!req){
            req = PythonRequirementCtx_Create(requirementsList[i]);

            if(!req){
                PythonSessionCtx_Free(session);
                session = NULL;
                break;
            }

            if(!PythonRequirementCtx_DownloadRequirement(req)){
                PythonRequirementCtx_Free(req);
                PythonSessionCtx_Free(session);
                session = NULL;
                break;
            }

            if(!PythonRequirementCtx_InstallRequirement(req)){
                PythonRequirementCtx_Free(req);
                PythonSessionCtx_Free(session);
                session = NULL;
                break;
            }
        }

        session->requirements = array_append(session->requirements , req);
    }

    RedisGearsPy_Unlock(old);

    return session;
}

static PythonSessionCtx* PythonSessionCtx_Create(const char** requirementsList, size_t requirementsListLen){

    PythonSessionCtx* session = PythonSessionCtx_CreateWithId(NULL, requirementsList, requirementsListLen);

    return session;
}

static int PythonSessionCtx_Serialize(void* arg, Gears_BufferWriter* bw, char** err){
    PythonSessionCtx* session = arg;
    RedisGears_BWWriteBuffer(bw, session->sessionId, ID_LEN);
    RedisGears_BWWriteLong(bw, array_len(session->requirements));
    for(size_t i = 0 ; i < array_len(session->requirements) ; ++i){
        if(PythonRequirementCtx_Serialize(session->requirements[i], bw, err) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

static void* PythonSessionCtx_Deserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, char** err){
    size_t len;
    char* id = RedisGears_BRReadBuffer(br, &len);

    PythonSessionCtx* s = PythonSessionCtx_Get(id);
    if(!s){
        s = PythonSessionCtx_CreateWithId(id, NULL, 0);
        if(!s){
            *err = RG_STRDUP("Could not create session");
            return NULL;
        }
    }else{
        s = PythonSessionCtx_ShellowCopy(s);
    }
    size_t requirementsLen = RedisGears_BRReadLong(br);
    for(size_t i = 0 ; i < requirementsLen ; ++i){
        PythonRequirementCtx* req = PythonRequirementCtx_Deserialize(br, err);
        if(!req){
            PythonSessionCtx_Free(s);
            return NULL;
        }
        s->requirements = array_append(s->requirements , req);
    }
    return s;
}

static char* PythonSessionCtx_ToString(void* arg){
    PythonSessionCtx* s = arg;
    char* depsListStr = ArrToStr((void**)s->requirements, array_len(s->requirements), PythonRequirementCtx_ToStr);
    char* ret;
    rg_asprintf(&ret, "{'sessionId':'%s', 'depsList':%s}", s->sessionIdStr, depsListStr);
    RG_FREE(depsListStr);
    return ret;
}

static void RedisGearsPy_OnRegistered(FlatExecutionPlan* fep, void* arg){
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    assert(sctx);

    PyObject* callback = arg;

    void* old = RedisGearsPy_Lock(sctx);

    assert(PyFunction_Check(callback));

    PyObject* pArgs = PyTuple_New(0);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        char* error = getPyError();
        RedisModule_Log(NULL, "warning", "Error occured on RedisGearsPy_OnRegistered, error=%s", error);
        RG_FREE(error);
        RedisGearsPy_Unlock(old);
        return;
    }
    if(ret != Py_None){
        Py_DECREF(ret);
    }

    RedisGearsPy_Unlock(old);

}

static void RedisGearsPy_OnExecutionUnpausedCallback(ExecutionCtx* ctx, void* arg){
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(ctx);
    void* old = RedisGearsPy_Lock(sctx);
    PyThreadState *state = PyGILState_GetThisThreadState();
    RedisGears_SetPrivateData(ctx, (void*)state->thread_id);
    RedisGearsPy_Unlock(old);
}

static PythonThreadCtx* GetPythonThreadCtx(){
    PythonThreadCtx* ptctx = pthread_getspecific(pythonThreadCtxKey);
    if(!ptctx){
        ptctx = RG_ALLOC(sizeof(*ptctx));
        *ptctx = (PythonThreadCtx){
                .lockCounter = 0,
                .currentCtx = NULL,
                .createdExecution = NULL,
                .doneFunction = onDone,
        };
        pthread_setspecific(pythonThreadCtxKey, ptctx);
    }
    return ptctx;
}

bool RedisGearsPy_IsLockAcquired(){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    return ptctx->lockCounter > 0;
}

PythonSessionCtx* RedisGearsPy_Lock(PythonSessionCtx* currSession){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PythonSessionCtx* oldSession = ptctx->currSession;
    ptctx->currSession = currSession;
    if(ptctx->lockCounter == 0){
        PyGILState_STATE oldState = PyGILState_Ensure();
        assert(oldState == PyGILState_UNLOCKED);
    }
    ++ptctx->lockCounter;
    return oldSession;
}

void RedisGearsPy_Unlock(PythonSessionCtx* prevSession){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    assert(ptctx);
    assert(ptctx->lockCounter > 0);
    ptctx->currSession = prevSession;
    if(--ptctx->lockCounter == 0){
        assert(!prevSession);
        PyGILState_Release(PyGILState_UNLOCKED);
    }
}

/*
 * Wrapper for a flat execution that allows it leave inside the python interpreter.
 */
typedef struct PyFlatExecution{
   PyObject_HEAD
   FlatExecutionPlan* fep;
} PyFlatExecution;

static PyObject* map(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to map function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "map argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_Map(pfep->fep, RedisGearsPy_PyCallbackMapper, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* filter(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to filter function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "filter argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_Filter(pfep->fep, RedisGearsPy_PyCallbackFilter, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* localAccumulateby(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "wrong number of args to groupby function");
        return NULL;
    }
    PyObject* extractor = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(extractor, &PyFunction_Type)){
        PyErr_SetString(GearsError, "groupby extractor argument must be a function");
        return NULL;
    }
    PyObject* accumulator = PyTuple_GetItem(args, 1);
    if(!PyObject_TypeCheck(accumulator, &PyFunction_Type)){
        PyErr_SetString(GearsError, "groupby reducer argument must be a function");
        return NULL;
    }
    Py_INCREF(extractor);
    Py_INCREF(accumulator);
    RGM_LocalAccumulateBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackAccumulateByKey, accumulator);
    RGM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    Py_INCREF(self);
    return self;
}

static PyObject* accumulateby(PyObject *self, PyObject *args){
	PyFlatExecution* pfep = (PyFlatExecution*)self;
	if(PyTuple_Size(args) != 2){
	    PyErr_SetString(GearsError, "wrong number of args to groupby function");
        return NULL;
	}
	PyObject* extractor = PyTuple_GetItem(args, 0);
	if(!PyObject_TypeCheck(extractor, &PyFunction_Type)){
        PyErr_SetString(GearsError, "groupby extractor argument must be a function");
        return NULL;
    }
	PyObject* accumulator = PyTuple_GetItem(args, 1);
	if(!PyObject_TypeCheck(accumulator, &PyFunction_Type)){
        PyErr_SetString(GearsError, "groupby reducer argument must be a function");
        return NULL;
    }
	Py_INCREF(extractor);
	Py_INCREF(accumulator);
	RGM_AccumulateBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackAccumulateByKey, accumulator);
	RGM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
	Py_INCREF(self);
	return self;
}

static PyObject* groupby(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "wrong number of args to batchgroupby function");
        return NULL;
    }
    PyObject* extractor = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(extractor, &PyFunction_Type)){
        PyErr_SetString(GearsError, "batchgroupby extractor argument must be a function");
        return NULL;
    }
    PyObject* reducer = PyTuple_GetItem(args, 1);
    if(!PyObject_TypeCheck(extractor, &PyFunction_Type)){
        PyErr_SetString(GearsError, "batchgroupby reducer argument must be a function");
        return NULL;
    }
    Py_INCREF(extractor);
    Py_INCREF(reducer);
    RGM_GroupBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackReducer, reducer);
    RGM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    Py_INCREF(self);
    return self;
}

static PyObject* collect(PyObject *self, PyObject *args){
    if(PyTuple_Size(args) != 0){
        PyErr_SetString(GearsError, "wrong number of args to collect function");
        return NULL;
    }
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    RGM_Collect(pfep->fep);
    Py_INCREF(self);
    return self;
}

static PyObject* foreach(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to foreach function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "foreach argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_ForEach(pfep->fep, RedisGearsPy_PyCallbackForEach, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* repartition(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to repartition function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "repartition argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_Repartition(pfep->fep, RedisGearsPy_PyCallbackExtractor, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* flatmap(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to flatmap function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "flatmap argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_FlatMap(pfep->fep, RedisGearsPy_PyCallbackFlatMapper, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* limit(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) < 1 || PyTuple_Size(args) > 2){
        PyErr_SetString(GearsError, "wrong number of args to limit function");
        return NULL;
    }
    PyObject* len = PyTuple_GetItem(args, 0);
    if(!PyLong_Check(len)){
        PyErr_SetString(GearsError, "limit argument must be a number");
        return NULL;
    }
    long lenLong = PyLong_AsLong(len);
    long offsetLong = 0;
    if(PyTuple_Size(args) > 1){
        PyObject* offset= PyTuple_GetItem(args, 1);
        if(!PyLong_Check(offset)){
            PyErr_SetString(GearsError, "limit argument must be a number");
            return NULL;
        }
        offsetLong = PyLong_AsLong(offset);
    }
    RGM_Limit(pfep->fep, (size_t)offsetLong, (size_t)lenLong);
    Py_INCREF(self);
    return self;
}

static PyObject* accumulate(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "wrong number of args to accumulate function");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    if(!PyObject_TypeCheck(callback, &PyFunction_Type)){
        PyErr_SetString(GearsError, "accumulate argument must be a function");
        return NULL;
    }
    Py_INCREF(callback);
    RGM_Accumulate(pfep->fep, RedisGearsPy_PyCallbackAccumulate, callback);
    Py_INCREF(self);
    return self;
}

static void onDone(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResultsAndErrors(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void onDoneSerializeResults(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    RedisModule_ReplyWithArray(rctx, 2);

    // sending results
    long long len = RedisGears_GetRecordsLen(ep);
    RedisModule_ReplyWithArray(rctx, len);
    Gears_Buffer* buff = Gears_BufferCreate();
    for(long long i = 0 ; i < len ; ++i){
        Record* r = RedisGears_GetRecord(ep, i);
        assert(RedisGears_RecordGetType(r) == pythonRecordType);
        PyObject* obj = PyObjRecordGet(r);
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        RedisGearsPy_PyCallbackSerialize(obj, &bw, NULL);
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, buff);
        size_t len;
        char* serializedObj = RedisGears_BRReadBuffer(&br, &len);
        RedisModule_ReplyWithStringBuffer(rctx, serializedObj, len);
        Gears_BufferClear(buff);
    }
    Gears_BufferFree(buff);

    // sending errors
    Command_ReturnErrors(ep, rctx);

    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void dropExecutionOnDone(ExecutionPlan* ep, void* privateData){
    RedisGears_DropExecution(ep);
}

static void* runCreateStreamReaderArgs(const char* pattern, PyObject *kargs){
    const char* defaultFromIdStr = "0-0";
    const char* fromIdStr = defaultFromIdStr;
    PyObject* pyFromId = GearsPyDict_GetItemString(kargs, "fromId");
    if(pyFromId){
        if(!PyUnicode_Check(pyFromId)){
            PyErr_SetString(GearsError, "fromId argument must be a string");
            return NULL;
        }
        fromIdStr = PyUnicode_AsUTF8AndSize(pyFromId, NULL);
    }
    return RedisGears_StreamReaderCtxCreate(pattern, fromIdStr);
}

static void* runCreateKeysReaderArgs(const char* pattern, PyObject *kargs){
    bool noScan = false;
    PyObject* pyNoScan = GearsPyDict_GetItemString(kargs, "noScan");
    if(pyNoScan){
        if(!PyBool_Check(pyNoScan)){
            PyErr_SetString(GearsError, "exactMatch value is not boolean");
            return NULL;
        }
        if(pyNoScan == Py_True){
            noScan = true;
        }
    }
    bool readValue = true;
    PyObject* pyReadValue = GearsPyDict_GetItemString(kargs, "readValue");
    if(pyReadValue){
        if(!PyBool_Check(pyReadValue)){
            PyErr_SetString(GearsError, "readValue value is not boolean");
            return NULL;
        }
        if(pyReadValue == Py_False){
            readValue = false;
        }
    }
    return RedisGears_KeysReaderCtxCreate(pattern, readValue, NULL, noScan);
}

static PyObject* run(PyObject *self, PyObject *args,  PyObject *kargs){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyFlatExecution* pfep = (PyFlatExecution*)self;

    if(ptctx->createdExecution){
        // it is not possible to run 2 executions in single script currently
        PyErr_SetString(GearsError, "Can not run more then 1 executions in a single script");
        return NULL;
    }

    if(RGM_SetFlatExecutionOnUnpausedCallback(pfep->fep, RedisGearsPy_OnExecutionUnpausedCallback, NULL) != REDISMODULE_OK){
        PyErr_SetString(GearsError, "Failed setting on start callback");
        return NULL;
    }

    const char* defaultRegexStr = "*";
    const char* patternStr = defaultRegexStr;
    void* arg;
    if (strcmp(RedisGears_GetReader(pfep->fep), "PythonReader") == 0){
        if(PyTuple_Size(args) != 1){
            PyErr_SetString(GearsError, "python reader function is not given");
            return NULL;
        }
        arg = PyTuple_GetItem(args, 0);
        if(!PyObject_TypeCheck(arg, &PyFunction_Type)){
            PyErr_SetString(GearsError, "pyreader argument must be a function");
            return NULL;
        }
        Py_INCREF((PyObject*)arg);
    }else{
        if(PyTuple_Size(args) > 0){
            PyObject* pattern = PyTuple_GetItem(args, 0);
            if(!PyUnicode_Check(pattern)){
                PyErr_SetString(GearsError, "regex argument must be a string");
                return NULL;
            }
            patternStr = PyUnicode_AsUTF8AndSize(pattern, NULL);
        }
        if(strcmp(RedisGears_GetReader(pfep->fep), "StreamReader") == 0){
            arg = runCreateStreamReaderArgs(patternStr, kargs);
        }else if(strcmp(RedisGears_GetReader(pfep->fep), "KeysReader") == 0){
            arg = runCreateKeysReaderArgs(patternStr, kargs);
        }else{
            PyErr_SetString(GearsError, "Given reader do not support run or not exists");
            return NULL;
        }
    }

    if(arg == NULL){
        return NULL;
    }

    char* err = NULL;
    ExecutionPlan* ep = RGM_Run(pfep->fep, ExecutionModeAsync, arg, NULL, NULL, &err);
    if(!ep){
        if(err){
            PyErr_SetString(GearsError, err);
            RG_FREE(err);
        }else{
            PyErr_SetString(GearsError, "failed creating execution");
        }
        if(strcmp(RedisGears_GetReader(pfep->fep), "StreamReader") == 0){
            RedisGears_StreamReaderCtxFree(arg);
        }else if(strcmp(RedisGears_GetReader(pfep->fep), "KeysReader") == 0){
            RedisGears_KeysReaderCtxFree(arg);
        }else if(strcmp(RedisGears_GetReader(pfep->fep), "PythonReader") == 0){
            Py_DECREF((PyObject*)arg);
        }else{
            RedisModule_Log(NULL, "warning", "unknown reader when try to free reader args");
            assert(false);
        }
        return NULL;
    }
    if(!ptctx->currentCtx){
        RedisGears_AddOnDoneCallback(ep, dropExecutionOnDone, NULL);
    }else{
        ptctx->createdExecution = ep;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static int registerStrKeyTypeToInt(const char* keyType){
    if(strcmp(keyType, "string") == 0){
        return REDISMODULE_KEYTYPE_STRING;
    }
    if(strcmp(keyType, "list") == 0){
        return REDISMODULE_KEYTYPE_LIST;
    }
    if(strcmp(keyType, "hash") == 0){
        return REDISMODULE_KEYTYPE_HASH;
    }
    if(strcmp(keyType, "set") == 0){
        return REDISMODULE_KEYTYPE_SET;
    }
    if(strcmp(keyType, "zset") == 0){
        return REDISMODULE_KEYTYPE_ZSET;
    }
    if(strcmp(keyType, "module") == 0){
        return REDISMODULE_KEYTYPE_MODULE;
    }
    return -1;
}

static void* registerCreateKeysArgs(PyObject *kargs, const char* prefix, ExecutionMode mode){
    Arr(char*) eventTypes = NULL;
    Arr(int) keyTypes = NULL;

    // getting even types white list (no list == all event types)
    PyObject* pyEventTypes = GearsPyDict_GetItemString(kargs, "eventTypes");
    if(pyEventTypes && pyEventTypes != Py_None){
        PyObject* eventTypesIterator = PyObject_GetIter(pyEventTypes);
        if(!eventTypesIterator){
            PyErr_SetString(GearsError, "given eventTypes is not iterable");
            return NULL;
        }
        eventTypes = array_new(char*, 10);
        PyObject* event = NULL;
        while((event = PyIter_Next(eventTypesIterator))){
            if(!PyUnicode_Check(event)){
                Py_DECREF(eventTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                PyErr_SetString(GearsError, "given event type is not string");
                return NULL;
            }
            const char* eventTypeStr = PyUnicode_AsUTF8AndSize(event, NULL);
            char* eventTypeStr1 = RG_STRDUP(eventTypeStr);
            eventTypes = array_append(eventTypes, eventTypeStr1);
        }
        Py_DECREF(eventTypesIterator);
    }

    // getting key types white list (no list == all key types)
    PyObject* pyKeyTypes = GearsPyDict_GetItemString(kargs, "keyTypes");
    if(pyKeyTypes && pyKeyTypes != Py_None){
        PyObject* keyTypesIterator = PyObject_GetIter(pyKeyTypes);
        if(!keyTypesIterator){
            PyErr_SetString(GearsError, "given keyTypes is not iterable");
            return NULL;
        }
        keyTypes = array_new(char*, 10);
        PyObject* keyType = NULL;
        while((keyType = PyIter_Next(keyTypesIterator))){
            if(!PyUnicode_Check(keyType)){
                Py_DECREF(keyTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                array_free(keyTypes);
                PyErr_SetString(GearsError, "given key type is not string");
                return NULL;
            }
            const char* keyTypeStr = PyUnicode_AsUTF8AndSize(keyType, NULL);
            int keyTypeInt = registerStrKeyTypeToInt(keyTypeStr);
            if(keyTypeInt == -1){
                Py_DECREF(keyTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                array_free(keyTypes);
                PyErr_SetString(GearsError, "unknown key type");
                return NULL;
            }
            keyTypes = array_append(keyTypes, keyTypeInt);
        }
        Py_DECREF(keyTypesIterator);
    }

    bool readValue = true;
    PyObject* pyReadValue = GearsPyDict_GetItemString(kargs, "readValue");
    if(pyReadValue){
        if(!PyBool_Check(pyReadValue)){
            PyErr_SetString(GearsError, "readValue is not boolean");
            return NULL;
        }
        if(pyReadValue == Py_False){
            readValue = false;
        }
    }

    return RedisGears_KeysReaderTriggerArgsCreate(prefix, eventTypes, keyTypes, readValue);
}

static OnFailedPolicy getOnFailedPolicy(const char* onFailurePolicyStr){
    if(strcasecmp(onFailurePolicyStr, "Continue") == 0){
        return OnFailedPolicyContinue;
    }
    if(strcasecmp(onFailurePolicyStr, "Abort") == 0){
        return OnFailedPolicyAbort;
    }
    if(strcasecmp(onFailurePolicyStr, "Retry") == 0){
        return OnFailedPolicyRetry;
    }
    return OnFailedPolicyUnknown;
}

static void* registerCreateStreamArgs(PyObject *kargs, const char* prefix, ExecutionMode mode){
    size_t batch = 1;
    PyObject* pyBatch = GearsPyDict_GetItemString(kargs, "batch");
    if(pyBatch){
        if(PyNumber_Check(pyBatch)){
            batch = PyNumber_AsSsize_t(pyBatch, NULL);
        }else{
            PyErr_SetString(GearsError, "batch argument must be a number");
            return NULL;
        }
    }

    size_t durationMS = 0;
    PyObject* pydurationInSec = GearsPyDict_GetItemString(kargs, "duration");
    if(pydurationInSec){
        if(PyNumber_Check(pydurationInSec)){
            durationMS = PyNumber_AsSsize_t(pydurationInSec, NULL);
        }else{
            PyErr_SetString(GearsError, "duration argument must be a number");
            return NULL;
        }
    }

    OnFailedPolicy onFailedPolicy = OnFailedPolicyContinue;
    PyObject* pyOnFailedPolicy = GearsPyDict_GetItemString(kargs, "onFailedPolicy");
    if(pyOnFailedPolicy){
        if(PyUnicode_Check(pyOnFailedPolicy)){
            const char* onFailedPolicyStr = PyUnicode_AsUTF8AndSize(pyOnFailedPolicy, NULL);
            onFailedPolicy = getOnFailedPolicy(onFailedPolicyStr);
            if(onFailedPolicy == OnFailedPolicyUnknown){
                PyErr_SetString(GearsError, "onFailedPolicy must be Continue/Abort/Retry");
                return NULL;
            }
            if(mode == ExecutionModeSync && onFailedPolicy != OnFailedPolicyContinue){
                PyErr_SetString(GearsError, "ExecutionMode sync can only be combined with OnFailedPolicy Continue");
                return NULL;
            }
        }else{
            PyErr_SetString(GearsError, "onFailedPolicy must be a string (Continue/Abort/Retry)");
            return NULL;
        }
    }

    size_t retryInterval = 1;
    PyObject* pyRetryInterval = GearsPyDict_GetItemString(kargs, "onFailedRetryInterval");
    if(pyRetryInterval){
        if(PyNumber_Check(pyRetryInterval)){
            retryInterval = PyNumber_AsSsize_t(pyRetryInterval, NULL);
        }else{
            PyErr_SetString(GearsError, "retryInterval argument must be a number");
            return NULL;
        }
    }

    bool trimStream = true;
    PyObject* pyTrimStream = GearsPyDict_GetItemString(kargs, "trimStream");
    if(pyTrimStream){
        if(PyBool_Check(pyTrimStream)){
            if(Py_True == pyTrimStream){
                trimStream = true;
            }else{
                trimStream = false;
            }
        }else{
            PyErr_SetString(GearsError, "pyTrimmStream argument must be a boolean");
            return NULL;
        }
    }

    return RedisGears_StreamReaderTriggerArgsCreate(prefix, batch, durationMS, onFailedPolicy, retryInterval, trimStream);
}

static void* registerCreateCommandArgs(PyObject *kargs){
    const char* trigger = NULL;
    PyObject* pyTrigger = GearsPyDict_GetItemString(kargs, "trigger");
    if(!pyTrigger){
        PyErr_SetString(GearsError, "trigger argument was not given");
        return NULL;
    }
    if(!PyUnicode_Check(pyTrigger)){
        PyErr_SetString(GearsError, "trigger argument is not string");
        return NULL;
    }
    trigger = PyUnicode_AsUTF8AndSize(pyTrigger, NULL);
    return RedisGears_CommandReaderTriggerArgsCreate(trigger);
}

static void* registerCreateArgs(FlatExecutionPlan* fep, PyObject *kargs, ExecutionMode mode){
    const char* reader = RedisGears_GetReader(fep);
    if (strcmp(reader, "CommandReader") == 0){
        return registerCreateCommandArgs(kargs);
    }

    char* defaultPrefixStr = "*";
    const char* prefixStr = defaultPrefixStr;
    PyObject* prefix = GearsPyDict_GetItemString(kargs, "prefix");
    if(prefix){
        if(PyUnicode_Check(prefix)){
            prefixStr = PyUnicode_AsUTF8AndSize(prefix, NULL);
        }else{
            PyErr_SetString(GearsError, "regex argument must be a string");
            return NULL;
        }
    }

    if (strcmp(reader, "KeysReader") == 0){
        return registerCreateKeysArgs(kargs, prefixStr, mode);
    }else if (strcmp(reader, "StreamReader") == 0){
        return registerCreateStreamArgs(kargs, prefixStr, mode);
    }
    PyErr_SetString(GearsError, "given reader does not exists or does not support register");
    return NULL;
}

static PyObject* registerExecution(PyObject *self, PyObject *args, PyObject *kargs){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyFlatExecution* pfep = (PyFlatExecution*)self;

    if(RGM_SetFlatExecutionOnUnpausedCallback(pfep->fep, RedisGearsPy_OnExecutionUnpausedCallback, NULL) != REDISMODULE_OK){
        PyErr_SetString(GearsError, "Failed setting on start callback");
        return NULL;
    }

    PyObject* pymode = GearsPyDict_GetItemString(kargs, "mode");
    ExecutionMode mode = ExecutionModeAsync;
    if(pymode){
        if(PyUnicode_Check(pymode)){
            const char* modeStr = PyUnicode_AsUTF8AndSize(pymode, NULL);
            if(strcmp(modeStr, "async") == 0){
                mode = ExecutionModeAsync;
            }else if(strcmp(modeStr, "sync") == 0){
                mode = ExecutionModeSync;
            }else if(strcmp(modeStr, "async_local") == 0){
                mode = ExecutionModeAsyncLocal;
            }else{
                PyErr_SetString(GearsError, "unknown execution mode");
                return NULL;
            }
        }else{
            PyErr_SetString(GearsError, "execution mode must be a string");
            return NULL;
        }
    }

    PyObject* onRegistered = GearsPyDict_GetItemString(kargs, "onRegistered");
    if(onRegistered && onRegistered != Py_None){
        if(!PyFunction_Check(onRegistered)){
            PyErr_SetString(GearsError, "OnRegistered argument must be a function");
            return NULL;
        }
        Py_INCREF(onRegistered);
        if(RGM_SetFlatExecutionOnRegisteredCallback(pfep->fep, RedisGearsPy_OnRegistered, onRegistered) != REDISMODULE_OK){
            PyErr_SetString(GearsError, "Failed setting on OnRegistered callback");
            Py_DECREF(onRegistered);
            return NULL;
        }
    }

    void* executionArgs = registerCreateArgs(pfep->fep, kargs, mode);
    if(executionArgs == NULL){
        return NULL;
    }

    char* err = NULL;
    int status = RGM_Register(pfep->fep, mode, executionArgs, &err);
    if(!status){
        if(err){
            PyErr_SetString(GearsError, err);
            RG_FREE(err);
        }else{
            PyErr_SetString(GearsError, "Failed register execution");
        }
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

typedef struct PyAtomic{
   PyObject_HEAD
   RedisModuleCtx* ctx;
} PyAtomic;

static PyObject* atomicEnter(PyObject *self, PyObject *args){
    PyAtomic* pyAtomic = (PyAtomic*)self;
    LockHandler_Acquire(pyAtomic->ctx);
    RedisModule_Replicate(pyAtomic->ctx, "multi", "");
    Py_INCREF(self);
    return self;
}

static PyObject* atomicExit(PyObject *self, PyObject *args){
    PyAtomic* pyAtomic = (PyAtomic*)self;
    LockHandler_Release(pyAtomic->ctx);
    RedisModule_Replicate(pyAtomic->ctx, "exec", "");
    Py_INCREF(self);
    return self;
}

static void PyAtomic_Destruct(PyObject *pyObj){
    PyAtomic* pyAtomic = (PyAtomic*)pyObj;
    RedisModule_FreeThreadSafeContext(pyAtomic->ctx);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

PyMethodDef PyAtomicMethods[] = {
    {"__enter__", atomicEnter, METH_VARARGS, "acquire the GIL"},
    {"__exit__", atomicExit, METH_VARARGS, "release the GIL"},
    {NULL, NULL, 0, NULL}
};

/* Flat Execution operations */
PyMethodDef PyFlatExecutionMethods[] = {
    {"map", map, METH_VARARGS, "map operation on each record"},
    {"filter", filter, METH_VARARGS, "filter operation on each record"},
    {"batchgroupby", groupby, METH_VARARGS, "batch groupby operation on each record"},
	{"groupby", accumulateby, METH_VARARGS, "groupby operation on each record"},
	{"localgroupby", localAccumulateby, METH_VARARGS, "local groupby operation on each record"},
    {"collect", collect, METH_VARARGS, "collect all the records to the initiator"},
    {"foreach", foreach, METH_VARARGS, "perform the given callback on each record"},
    {"repartition", repartition, METH_VARARGS, "repartition the records according to the extracted data"},
    {"flatmap", flatmap, METH_VARARGS, "flat map a record to many records"},
    {"limit", limit, METH_VARARGS, "limit the results to a give size and offset"},
    {"accumulate", accumulate, METH_VARARGS, "accumulate the records to a single record"},
    {"run", (PyCFunction)run, METH_VARARGS|METH_KEYWORDS, "start the execution"},
    {"register", (PyCFunction)registerExecution, METH_VARARGS|METH_KEYWORDS, "register the execution on an event"},
    {NULL, NULL, 0, NULL}
};

static PyObject *PyFlatExecution_ToStr(PyObject * pyObj){
    return PyUnicode_FromString("PyFlatExecution");
}

static void PyFlatExecution_Destruct(PyObject *pyObj){
    PyFlatExecution* pfep = (PyFlatExecution*)pyObj;
    if(pfep->fep){
        RedisGears_FreeFlatExecution(pfep->fep);
    }
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static PyTypeObject PyAtomicType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyAtomic",     /* tp_name */
    sizeof(PyAtomic),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyAtomic_Destruct,         /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyAtomic",                /* tp_doc */
};

static PyTypeObject PyFlatExecutionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyFlatExecution",       /* tp_name */
    sizeof(PyFlatExecution),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyFlatExecution_Destruct,  /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    PyFlatExecution_ToStr,     /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyFlatExecution",         /* tp_doc */
};

static PyObject* atomicCtx(PyObject *cls, PyObject *args){
    PyAtomic* pyAtomic = PyObject_New(PyAtomic, &PyAtomicType);
    pyAtomic->ctx = RedisModule_GetThreadSafeContext(NULL);
    return (PyObject*)pyAtomic;
}

static PyObject* gearsCtx(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    const char* readerStr = "KeysReader";
    const char* descStr = NULL;
    if(PyTuple_Size(args) > 0){
        PyObject* reader = PyTuple_GetItem(args, 0);
        if(!PyUnicode_Check(reader)){
            PyErr_SetString(GearsError, "reader argument must be a string");
            return NULL;
        }
        readerStr = PyUnicode_AsUTF8AndSize(reader, NULL);
    }
    if(PyTuple_Size(args) > 1){
        PyObject* desc = PyTuple_GetItem(args, 1);
        if(desc != Py_None && !PyUnicode_Check(desc)){
            PyErr_SetString(GearsError, "desc argument must be a string");
            return NULL;
        }
        if(desc != Py_None){
            descStr = PyUnicode_AsUTF8AndSize(desc, NULL);
        }
    }
    PyFlatExecution* pyfep = PyObject_New(PyFlatExecution, &PyFlatExecutionType);
    pyfep->fep = RedisGears_CreateCtx((char*)readerStr);
    if(!pyfep->fep){
        Py_DecRef((PyObject*)pyfep);
        PyErr_SetString(GearsError, "the given reader are not exists");
        return NULL;
    }
    if(descStr){
        RedisGears_SetDesc(pyfep->fep, descStr);
    }
    RGM_Map(pyfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    RedisGears_SetFlatExecutionPrivateData(pyfep->fep, "PySessionType", PythonSessionCtx_ShellowCopy(ptctx->currSession));
    return (PyObject*)pyfep;
}

static PyObject* saveGlobals(PyObject *cls, PyObject *args){
    pyGlobals = PyEval_GetGlobals();
    Py_INCREF(pyGlobals);
    // main var are not serialize, we want all the user define functions to
    // be serialize so we specify the module on which the user run as not_main!!
    PyDict_SetItemString(pyGlobals, "__name__", PyUnicode_FromString("not_main"));
    return PyLong_FromLong(1);
}

static PyObject* replyToPyList(RedisModuleCallReply *reply){
    if(!reply){
        Py_INCREF(Py_None);
        return Py_None;
    }
    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
        PyObject* ret = PyList_New(0);
        for(size_t i = 0 ; i < RedisModule_CallReplyLength(reply) ; ++i){
            RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply, i);
            PyObject* val = replyToPyList(subReply);
            PyList_Append(ret, val);
            Py_DECREF(val);
        }
        return ret;
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING ||
            RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
        PyObject* ret = PyUnicode_FromStringAndSize(replyStr, len);
        if(!ret){
            PyErr_Clear();
            ret = PyByteArray_FromStringAndSize(replyStr, len);
        }
        return ret;
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER){
        long long val = RedisModule_CallReplyInteger(reply);
        return PyLong_FromLongLong(val);
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* getMyHashTag(PyObject *cls, PyObject *args){
    const char* myHashTag = RedisGears_GetMyHashTag();
    if(!myHashTag){
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyObject* ret = PyUnicode_FromStringAndSize(myHashTag, strlen(myHashTag));
    return ret;
}

static PyObject* RedisConfigGet(PyObject *cls, PyObject *args){
    PyObject* key = NULL;
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "get_config function must get a single key input");
        return NULL;
    }

    key = PyTuple_GetItem(args, 0);

    if(!PyUnicode_Check(key)){
        PyErr_SetString(GearsError, "get_config key must be a string");
        return NULL;
    }

    const char* keyCStr = PyUnicode_AsUTF8AndSize(key, NULL);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);
    const char* valCStr = GearsConfig_GetExtraConfigVals(keyCStr);
    if(!valCStr){
        LockHandler_Release(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyObject* valPyStr = PyUnicode_FromString(valCStr);
    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    return valPyStr;
}

static PyObject* RedisLog(PyObject *cls, PyObject *args, PyObject *kargs){
    PyObject* logLevel = GearsPyDict_GetItemString(kargs, "level");
    PyObject* logMsg = NULL;
    if(PyTuple_Size(args) < 1 || PyTuple_Size(args) > 2){
        PyErr_SetString(GearsError, "log function must get a log message as input");
        return NULL;
    }
    if(PyTuple_Size(args) == 2){
        RedisModule_Log(NULL, "warning", "Specify log level as the first argument to log function is depricated, use key argument 'level' instead");
        logLevel = PyTuple_GetItem(args, 0);
        logMsg = PyTuple_GetItem(args, 1);
    }else{
        logMsg = PyTuple_GetItem(args, 0);
    }

    if(!PyUnicode_Check(logMsg)){
        PyErr_SetString(GearsError, "Log message must be a string");
        return NULL;
    }

    const char* logMsgCStr = PyUnicode_AsUTF8AndSize(logMsg, NULL);
    const char* logLevelCStr = "notice";

    if(logLevel){
        if(!PyUnicode_Check(logLevel)){
            PyErr_SetString(GearsError, "Log level must be a sting (debug/verbose/notice/warning)");
            return NULL;
        }
        logLevelCStr = PyUnicode_AsUTF8AndSize(logLevel, NULL);
        if(strcmp(logLevelCStr, "debug") != 0 &&
                strcmp(logLevelCStr, "verbose") != 0 &&
                strcmp(logLevelCStr, "notice") != 0 &&
                strcmp(logLevelCStr, "warning") != 0){
            PyErr_SetString(GearsError, "Log level should be one of the following : debug,verbose,notice,warning");
            return NULL;
        }
    }

    RedisModule_Log(NULL, logLevelCStr, "GEARS: %s", logMsgCStr);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* executeCommand(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) < 1){
        return PyList_New(0);
    }
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);

    PyObject* command = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(command)){
        PyErr_SetString(GearsError, "the given command must be a string");
        LockHandler_Release(rctx);
        RedisModule_FreeThreadSafeContext(rctx);
        return NULL;
    }
    const char* commandStr = PyUnicode_AsUTF8AndSize(command, NULL);

    RedisModuleString** argements = array_new(RedisModuleString*, 10);
    for(int i = 1 ; i < PyTuple_Size(args) ; ++i){
        PyObject* argument = PyTuple_GetItem(args, i);
        PyObject* argumentStr = PyObject_Str(argument);
        size_t argLen;
        const char* argumentCStr = PyUnicode_AsUTF8AndSize(argumentStr, &argLen);
        RedisModuleString* argumentRedisStr = RedisModule_CreateString(rctx, argumentCStr, argLen);
        Py_DECREF(argumentStr);
        argements = array_append(argements, argumentRedisStr);
    }

    PyObject* res = NULL;
    RedisModuleCallReply *reply = RedisModule_Call(rctx, commandStr, "!v", argements, array_len(argements));
    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
        PyErr_SetString(GearsError, replyStr);
    }else{
        res = replyToPyList(reply);
    }

    if(reply){
        RedisModule_FreeCallReply(reply);
    }

    array_free_ex(argements, RedisModule_FreeString(rctx, *(RedisModuleString**)ptr));

    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return res;
}

typedef struct PyTensor{
   PyObject_HEAD
   RAI_Tensor* t;
} PyTensor;

static PyObject *PyTensor_ToFlatList(PyTensor * pyt){
    int ndims = RedisAI_TensorNumDims(pyt->t);
    PyObject* dims = PyList_New(0);
    long long len = 0;
    long long totalElements = 1;
    for(int i = 0 ; i < ndims ; ++i){
        totalElements *= RedisAI_TensorDim(pyt->t, i);
    }
    PyObject* elements = PyList_New(0);
    for(long long j = 0 ; j < totalElements ; ++j){
        double val;
        RedisAI_TensorGetValueAsDouble(pyt->t, j, &val);
        PyObject *pyVal = PyFloat_FromDouble(val);
        PyList_Append(dims, pyVal);
    }
    return dims;
}

static bool verifyOrLoadRedisAI(){
    if(!globals.redisAILoaded){
        RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
        if(RedisAI_Initialize(ctx) != REDISMODULE_OK){
            PyErr_SetString(GearsError, "RedisAI is not loaded, it is not possible to use AI interface.");
            RedisModule_FreeThreadSafeContext(ctx);
            return false;
        }
        RedisModule_FreeThreadSafeContext(ctx);
        globals.redisAILoaded = true;
    }
    return true;
}

#define verifyRedisAILoaded() \
    if(!verifyOrLoadRedisAI()){ \
        PyErr_SetString(GearsError, "RedisAI is not loaded, it is not possible to use AI interface."); \
        return NULL;\
    }

static PyObject* tensorGetDims(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    int numDims = RedisAI_TensorNumDims(pyt->t);
    PyObject *tuple = PyTuple_New(numDims);
    for(int i = 0 ; i < numDims ; ++i){
        long long dim = RedisAI_TensorDim(pyt->t, i);
        PyObject* pyDim = PyLong_FromLongLong(dim);
        PyTuple_SetItem(tuple, i, pyDim);
    }
    return tuple;
}

static PyObject* tensorGetDataAsBlob(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    size_t size = RedisAI_TensorByteSize(pyt->t);
    char* data = RedisAI_TensorData(pyt->t);
    return PyByteArray_FromStringAndSize(data, size);
}

static PyObject* tensorToFlatList(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    return PyTensor_ToFlatList(pyt);
}

static PyObject *PyTensor_ToStr(PyObject * pyObj){
    PyTensor* pyt = (PyTensor*)pyObj;
    return PyObject_Repr(PyTensor_ToFlatList(pyt));
}

static void PyTensor_Destruct(PyObject *pyObj){
    PyTensor* pyt = (PyTensor*)pyObj;
    RedisAI_TensorFree(pyt->t);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static PyTypeObject PyTensorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyTensor",       /* tp_name */
    sizeof(PyTensor),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyTensor_Destruct,         /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    PyTensor_ToStr,            /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyTensor",                /* tp_doc */
};

static size_t getDimsRecursive(PyObject *list, long long** dims){
    if(!PyList_Check(list)){
        return 0;
    }
    long long curr_dim = PyList_Size(list);
    *dims = array_append(*dims, curr_dim);
    return 1 + getDimsRecursive(PyList_GetItem(list, 0), dims);
}

static void getAllValues(PyObject *list, double** values){
    if(!PyList_Check(list)){
        double val = PyFloat_AsDouble(list);
        *values = array_append(*values, val);
        return;
    }
    for(size_t i = 0 ; i < PyList_Size(list); ++i){
        getAllValues(PyList_GetItem(list, i), values);
    }
}

static PyObject* createTensorFromBlob(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyObject* typeName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(typeName)){
        PyErr_SetString(GearsError, "type argument must be a string");
        return NULL;
    }
    PyObject* pyBlob = PyTuple_GetItem(args, 2);
    if(!PyByteArray_Check(pyBlob)){
        PyErr_SetString(GearsError, "blob argument must be a byte array");
        return NULL;
    }
    const char* typeNameStr = PyUnicode_AsUTF8AndSize(typeName, NULL);
    PyObject* pyDims = PyTuple_GetItem(args, 1);
    long long* dims = array_new(long long, 10);
    PyObject* dimsIter = PyObject_GetIter(pyDims);
    PyObject* currDim = NULL;
    if(dimsIter == NULL){
        PyErr_Clear();
        PyErr_SetString(GearsError, "dims argument must be iterable");
        return NULL;
    }

    while((currDim = PyIter_Next(dimsIter)) != NULL){
        if(!PyLong_Check(currDim)){
            PyErr_SetString(GearsError, "dims arguments must be long");
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            array_free(dims);
            return NULL;
        }
        if(PyErr_Occurred()){
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            array_free(dims);
            return NULL;
        }
        dims = array_append(dims, PyLong_AsLong(currDim));
        Py_DECREF(currDim);
    }
    Py_DECREF(dimsIter);

    RAI_Tensor* t = RedisAI_TensorCreate(typeNameStr, dims, array_len(dims));
    size_t size = PyByteArray_Size(pyBlob);
    const char* blob = PyByteArray_AsString(pyBlob);
    RedisAI_TensorSetData(t, blob, size);
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    array_free(dims);
    return (PyObject*)pyt;
}

static PyObject* createTensorFromValues(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    RAI_Tensor* t = NULL;
    PyObject* typeName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(typeName)){
        PyErr_SetString(GearsError, "type argument must be a string");
        return NULL;
    }
    const char* typeNameStr = PyUnicode_AsUTF8AndSize(typeName, NULL);
    // todo: combine to a single function!!
    PyObject* pyDims = PyTuple_GetItem(args, 1);
    if(!PyIter_Check(pyDims)){
        PyErr_SetString(GearsError, "dims argument must be iterable");
        return NULL;
    }
    long long* dims = array_new(long long, 10);
    PyObject* dimsIter = PyObject_GetIter(pyDims);
    PyObject* currDim = NULL;
    while((currDim = PyIter_Next(dimsIter)) != NULL){
        if(!PyLong_Check(currDim)){
            PyErr_SetString(GearsError, "dims arguments must be long");
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            goto error;
        }
        if(PyErr_Occurred()){
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            goto error;
        }
        dims = array_append(dims, PyLong_AsLong(currDim));
        Py_DECREF(currDim);
    }
    Py_DECREF(dimsIter);

    t = RedisAI_TensorCreate(typeNameStr, dims, array_len(dims));

    PyObject* values = PyTuple_GetItem(args, 2);
    PyObject* valuesIter = PyObject_GetIter(pyDims);
    PyObject* currValue = NULL;
    size_t index = 0;
    while((currValue = PyIter_Next(valuesIter)) != NULL){
        if(!PyFloat_Check(currValue)){
            PyErr_SetString(GearsError, "values arguments must be double");
            Py_DECREF(currValue);
            Py_DECREF(valuesIter);
            goto error;
        }
        RedisAI_TensorSetValueFromDouble(t, index++, PyFloat_AsDouble(currValue));
        Py_DECREF(currValue);
    }
    Py_DECREF(valuesIter);

    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    array_free(dims);
    return (PyObject*)pyt;

error:
    array_free(dims);
    if(t) RedisAI_TensorFree(t);
    return NULL;
}

typedef struct PyGraphRunner{
   PyObject_HEAD
   RAI_ModelRunCtx* g;
} PyGraphRunner;

static PyObject* PyGraph_ToStr(PyObject *obj){
    return PyUnicode_FromString("PyGraphRunner to str");
}

static void PyGraphRunner_Destruct(PyObject *pyObj){
    PyGraphRunner* pyg = (PyGraphRunner*)pyObj;
    RedisAI_ModelRunCtxFree(pyg->g);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static PyTypeObject PyGraphRunnerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyGraphRunner",             /* tp_name */
    sizeof(PyGraphRunner), /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyGraphRunner_Destruct,    /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    PyGraph_ToStr,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyGraphRunner",           /* tp_doc */
};

static PyObject* createModelRunner(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyObject* keyName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(keyName)){
        PyErr_SetString(GearsError, "key argument must be a string");
        return NULL;
    }
    const char* keyNameStr = PyUnicode_AsUTF8AndSize(keyName, NULL);
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    // todo: check for type, add api for this
    RAI_Model *g = RedisModule_ModuleTypeGetValue(key);
    RAI_ModelRunCtx* runCtx = RedisAI_ModelRunCtxCreate(g);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    PyGraphRunner* pyg = PyObject_New(PyGraphRunner, &PyGraphRunnerType);
    pyg->g = runCtx;

    return (PyObject*)pyg;
}

static PyObject* modelRunnerAddInput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    PyObject* inputName = PyTuple_GetItem(args, 1);
    if(!PyUnicode_Check(inputName)){
        PyErr_SetString(GearsError, "input name argument must be a string");
        return NULL;
    }
    const char* inputNameStr = PyUnicode_AsUTF8AndSize(inputName, NULL);
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 2);
    RedisAI_ModelRunCtxAddInput(pyg->g, inputNameStr, pyt->t);
    return PyLong_FromLong(1);
}

static PyObject* modelRunnerAddOutput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    PyObject* outputName = PyTuple_GetItem(args, 1);
    if(!PyUnicode_Check(outputName)){
        PyErr_SetString(GearsError, "output name argument must be a string");
        return NULL;
    }
    const char* outputNameStr = PyUnicode_AsUTF8AndSize(outputName, NULL);
    RedisAI_ModelRunCtxAddOutput(pyg->g, outputNameStr);
    return PyLong_FromLong(1);
}

static PyObject* modelRunnerRun(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    // TODO: deal with errors better
    RAI_Error err = {0};
    RedisAI_ModelRun(pyg->g, &err);
    if (err.code) {
        printf("ERROR: %s\n", err.detail);
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyObject* tensorList = PyList_New(0);
    for(size_t i = 0 ; i < RedisAI_ModelRunCtxNumOutputs(pyg->g) ; ++i){
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ModelRunCtxOutputTensor(pyg->g, i));
        PyList_Append(tensorList, (PyObject*)pyt);
        Py_DECREF(pyt);
    }
    return tensorList;
}

typedef struct PyTorchScriptRunner{
   PyObject_HEAD
   RAI_ScriptRunCtx* s;
} PyTorchScriptRunner;

static PyObject* PyTorchScript_ToStr(PyObject *obj){
    return PyUnicode_FromString("PyTorchScriptRunner to str");
}

static void PyTorchScriptRunner_Destruct(PyObject *pyObj){
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)pyObj;
    RedisAI_ScriptRunCtxFree(pys->s);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static PyTypeObject PyTorchScriptRunnerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyTorchScriptRunner",             /* tp_name */
    sizeof(PyTorchScriptRunner), /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyTorchScriptRunner_Destruct,    /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_compare */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash */
    0,                         /* tp_call */
    PyTorchScript_ToStr,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyTorchScriptRunner",           /* tp_doc */
};

static PyObject* createScriptRunner(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyObject* keyName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(keyName)){
        PyErr_SetString(GearsError, "key name argument must be a string");
        return NULL;
    }
    PyObject* fnName = PyTuple_GetItem(args, 1);
    if(!PyUnicode_Check(fnName)){
        PyErr_SetString(GearsError, "function name argument must be a string");
        return NULL;
    }

    const char* keyNameStr = PyUnicode_AsUTF8AndSize(keyName, NULL);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    // todo: check for type, add api for this
    RAI_Script *s = RedisModule_ModuleTypeGetValue(key);

    const char* fnNameStr = PyUnicode_AsUTF8AndSize(fnName, NULL);

    RAI_ScriptRunCtx* runCtx = RedisAI_ScriptRunCtxCreate(s, fnNameStr);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    PyTorchScriptRunner* pys = PyObject_New(PyTorchScriptRunner, &PyTorchScriptRunnerType);
    pys->s = runCtx;

    return (PyObject*)pys;
}

static PyObject* scriptRunnerAddInput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 1);
    RedisAI_ScriptRunCtxAddInput(pys->s, pyt->t);
    return PyLong_FromLong(1);
}

static PyObject* scriptRunnerAddOutput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    RedisAI_ScriptRunCtxAddOutput(pys->s);
    return PyLong_FromLong(1);
}

static PyObject* scriptRunnerRun(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    // TODO: deal with errors better
    RAI_Error err = {0};
    RedisAI_ScriptRun(pys->s, &err);
    if (err.code) {
        printf("ERROR: %s\n", err.detail);
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ScriptRunCtxOutputTensor(pys->s, 0));
    return (PyObject*)pyt;
}

#define TIME_EVENT_ENCVER 1

typedef enum{
    TE_STATUS_RUNNING, TE_STATUS_ERR
}TimeEventStatus;

typedef struct TimerData{
    uint64_t period;
    RedisModuleTimerID id;
    PyObject* callback;
    PythonSessionCtx* session;
    TimeEventStatus status;
}TimerData;

#define RG_TIME_EVENT_TYPE "rg_timeev"

RedisModuleType *TimeEventType;

static void TimeEvent_Callback(RedisModuleCtx *ctx, void *data){
    TimerData* td = data;
    void* old = RedisGearsPy_Lock(td->session);
    PyObject* pArgs = PyTuple_New(0);
    PyObject_CallObject(td->callback, pArgs);
    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(NULL, "warning", "Error occured on TimeEvent_Callback, error=%s", error);
        RG_FREE(error);
        td->status =TE_STATUS_ERR;
    }else{
        td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    }
    Py_DECREF(pArgs);
    RedisGearsPy_Unlock(old);
}

static void *TimeEvent_RDBLoad(RedisModuleIO *rdb, int encver){
    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = RedisModule_LoadUnsigned(rdb);

    size_t serializedSessionLen;
    char *serializedSession = RedisModule_LoadStringBuffer(rdb, &serializedSessionLen);
    Gears_Buffer sessionBuff = {
            .buff = serializedSession,
            .size = serializedSessionLen,
            .cap = serializedSessionLen,
    };
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &sessionBuff);
    char* err = NULL;
    td->session = PythonSessionCtx_Deserialize(NULL, &br, &err);
    if(!td->session){
        RedisModule_Log(NULL, "warning", "Could not deserialize TimeEven Session, error='%s'", err);
    }
    RedisModule_Free(serializedSession);

    size_t len;
    char* buff = RedisModule_LoadStringBuffer(rdb, &len);
    Gears_Buffer b = {
            .cap = len,
            .size = len,
            .buff = buff,
    };
    Gears_BufferReader reader;
    Gears_BufferReaderInit(&reader, &b);
    td->callback = RedisGearsPy_PyCallbackDeserialize(NULL, &reader, NULL);
    assert(td->callback);

    // change callback global
    void* old = RedisGearsPy_Lock(td->session);
    PyFunctionObject* callback_func = (PyFunctionObject*)td->callback;
    PyDict_Merge(td->session->globalsDict, callback_func->func_globals, 0);
    Py_DECREF(callback_func->func_globals);
    callback_func->func_globals = td->session->globalsDict;
    Py_INCREF(callback_func->func_globals);
    RedisGearsPy_Unlock(old);

    RedisModule_Free(buff);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    RedisModule_FreeThreadSafeContext(ctx);
    return td;
}

static void TimeEvent_RDBSave(RedisModuleIO *rdb, void *value){
    TimerData* td = value;
    RedisModule_SaveUnsigned(rdb, td->period);
    Gears_Buffer* b = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, b);
    PythonSessionCtx_Serialize(td->session, &bw, NULL);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferClear(b);
    int res = RedisGearsPy_PyCallbackSerialize(td->callback, &bw, NULL);
    assert(res == REDISMODULE_OK);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferFree(b);
}

static void TimeEvent_Free(void *value){
    TimerData* td = value;
    void* old = RedisGearsPy_Lock(td->session);
    Py_DECREF(td->callback);
    RedisGearsPy_Unlock(old);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_StopTimer(ctx, td->id, NULL);
    RedisModule_FreeThreadSafeContext(ctx);
    RG_FREE(td);
}

static int TimeEvent_RegisterType(RedisModuleCtx* ctx){
    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = TimeEvent_RDBLoad,
                                 .rdb_save = TimeEvent_RDBSave,
                                 .aof_rewrite = NULL,
                                 .mem_usage = NULL,
                                 .free = TimeEvent_Free};
    TimeEventType = RedisModule_CreateDataType(ctx, RG_TIME_EVENT_TYPE, TIME_EVENT_ENCVER, &tm);
    if(!TimeEventType){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static PyObject* gearsTimeEvent(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) < 2 || PyTuple_Size(args) > 3){
        PyErr_SetString(GearsError, "not enough arguments for time event");
        return NULL;
    }
    PyObject* callback = PyTuple_GetItem(args, 1);
    if(!PyFunction_Check(callback)){
        PyErr_SetString(GearsError, "callback must be a function");
        return NULL;
    }
    PyObject* timeInSec = PyTuple_GetItem(args, 0);
    if(!PyLong_Check(timeInSec)) {
        PyErr_SetString(GearsError, "time argument must be a long");
        return NULL;
    }

    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);
    RedisModuleString* keyNameStr = NULL;
    if(PyTuple_Size(args) == 3){
        PyObject* keyName = PyTuple_GetItem(args, 2);
        if(PyUnicode_Check(keyName)){
            size_t len;
            const char* keyNameCStr = PyUnicode_AsUTF8AndSize(keyName, &len);
            keyNameStr = RedisModule_CreateString(ctx, keyNameCStr, len);
        }
    }
    long period = PyLong_AsLong(timeInSec);

    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = period;
    td->callback = callback;
    td->session = PythonSessionCtx_ShellowCopy(ptctx->currSession);
    Py_INCREF(callback);

    LockHandler_Acquire(ctx);

    if(keyNameStr){
        RedisModuleKey* key = RedisModule_OpenKey(ctx, keyNameStr, REDISMODULE_WRITE);
        if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
            TimeEvent_Free(td);
            LockHandler_Release(ctx);
            RedisModule_FreeThreadSafeContext(ctx);
            return Py_False;
        }
        RedisModule_ModuleTypeSetValue(key, TimeEventType, td);
    }

    td->id = RedisModule_CreateTimer(ctx, period * 1000, TimeEvent_Callback, td);

    LockHandler_Release(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    return Py_True;
}

PyMethodDef EmbRedisGearsMethods[] = {
    {"gearsCtx", gearsCtx, METH_VARARGS, "creating an empty gears context"},
    {"atomicCtx", atomicCtx, METH_VARARGS, "creating a atomic ctx for atomic block"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "execute a redis command and return the result"},
    {"log", (PyCFunction)RedisLog, METH_VARARGS|METH_KEYWORDS, "write a message into the redis log file"},
    {"config_get", RedisConfigGet, METH_VARARGS, "write a message into the redis log file"},
    {"getMyHashTag", getMyHashTag, METH_VARARGS, "return hash tag of the current node or None if not running on cluster"},
    {"registerTimeEvent", gearsTimeEvent, METH_VARARGS, "register a function to be called on each time period"},
    {NULL, NULL, 0, NULL}
};

PyMethodDef EmbRedisAIMethods[] = {
    {"createTensorFromValues", createTensorFromValues, METH_VARARGS, "creating a tensor object from values"},
    {"createTensorFromBlob", createTensorFromBlob, METH_VARARGS, "creating a tensor object from blob"},
    {"createModelRunner", createModelRunner, METH_VARARGS, "open TF graph by key name"},
    {"modelRunnerAddInput", modelRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"modelRunnerAddOutput", modelRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"modelRunnerRun", modelRunnerRun, METH_VARARGS, "run graph runner"},
    {"createScriptRunner", createScriptRunner, METH_VARARGS, "open a torch script by key name"},
    {"scriptRunnerAddInput", scriptRunnerAddInput, METH_VARARGS, "add input to torch script runner"},
    {"scriptRunnerAddOutput", scriptRunnerAddOutput, METH_VARARGS, "add output to torch script runner"},
    {"scriptRunnerRun", scriptRunnerRun, METH_VARARGS, "run torch script runner"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
    {"tensorGetDataAsBlob", tensorGetDataAsBlob, METH_VARARGS, "getting the tensor data as a string blob"},
    {"tensorGetDims", tensorGetDims, METH_VARARGS, "return tuple of the tensor dims"},
    {NULL, NULL, 0, NULL}
};

static int RedisGearsPy_FreeInterpreter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisGearsPy_Lock(NULL);
	Py_Finalize();
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

static int RedisGearsPy_ExecuteRemote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }

    PythonSessionCtx* session = PythonSessionCtx_Create(NULL, 0);
    if(!session){
        RedisModule_ReplyWithError(ctx, "Could not satisfy requirments");
        return REDISMODULE_OK;
    }

    // deserialize gears remote builder
    size_t len;
    const char* serializedGRB = RedisModule_StringPtrLen(argv[1], &len);

    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    RedisGears_BWWriteBuffer(&bw, serializedGRB, len);

    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, buff);

    char* err = NULL;
    PyObject * grb = RedisGearsPy_PyCallbackDeserialize(NULL, &br, &err);

    Gears_BufferFree(buff);

    if(!grb){
        if(err){
            RedisModule_Log(ctx, "warning", "could not deserialize GearsRemoteBuilder, error='%s'", *err);
            RG_FREE(err);
        }else{
            RedisModule_ReplyWithError(ctx, "could not deserialize GearsRemoteBuilder");
        }
        return REDISMODULE_OK;
    }

    void* old = RedisGearsPy_Lock(session);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    DoneCallbackFunction oldDoneFunction = ptctx->doneFunction;
    ptctx->doneFunction = onDoneSerializeResults;

    ptctx->currentCtx = ctx;

    ptctx->createdExecution = NULL;

    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, grb);
    PyObject* globalsDict = PyDict_Copy(pyGlobals);
    PyTuple_SetItem(pArgs, 1, globalsDict);
    PyObject* v = PyObject_CallObject(runGearsRemoteBuilderCallback, pArgs);
    Py_DECREF(pArgs);

    ptctx->doneFunction = oldDoneFunction;

    if(!v){
        char* err = getPyError();
        if(!err){
            RedisModule_ReplyWithError(ctx, "failed running the given script");
        }else{
            RedisModule_ReplyWithError(ctx, err);
            RG_FREE(err);
        }
        if(ptctx->createdExecution){
            // error occured, we need to abort the created execution.
            int res = RedisGears_AbortExecution(ptctx->createdExecution);
            assert(res == REDISMODULE_OK);
            RedisGears_DropExecution(ptctx->createdExecution);
        }

        RedisGearsPy_Unlock(old);

        ptctx->createdExecution = NULL;
        ptctx->currentCtx = NULL;
        return REDISMODULE_OK;
    }

    if(ptctx->createdExecution){
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 1000000);
        RedisGears_AddOnDoneCallback(ptctx->createdExecution, ptctx->doneFunction, bc);
    }else{
        RedisModule_ReplyWithSimpleString(ptctx->currentCtx, "OK");
    }
    RedisGearsPy_Unlock(old);

    ptctx->createdExecution = NULL;
    ptctx->currentCtx = NULL;

    return REDISMODULE_OK;
}

static void RedisGearsPy_GetRequirementsList(const char** requirementsList, RedisModuleString **argv, int argc){
    for(size_t i = 0 ; i < argc ; ++i){
        requirementsList[i] = RedisModule_StringPtrLen(argv[i], NULL);
    }
}

int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    ptctx->currentCtx = ctx;
    bool isBlocking = true;
    size_t requirementsArg = 3;
    if(argc >= 3){
        const char* block = RedisModule_StringPtrLen(argv[2], NULL);
        if(strcasecmp(block, "UNBLOCKING") == 0){
            isBlocking = false;
            requirementsArg = 4;
        }
    }

    size_t reqLen = argc >= requirementsArg ? argc - requirementsArg : 0;
    const char* requirementsList[reqLen];
    if(argc >= requirementsArg){
        const char* requirements = RedisModule_StringPtrLen(argv[requirementsArg - 1], NULL);
        if(strcasecmp(requirements, "REQUIREMENTS") == 0){
            RedisGearsPy_GetRequirementsList(requirementsList, argv + requirementsArg, reqLen);
        }
    }

    ptctx->createdExecution = NULL;

    PyObject *v;

    PythonSessionCtx* session = PythonSessionCtx_Create(requirementsList, reqLen);
    if(!session){
        RedisModule_ReplyWithError(ctx, "Could not satisfy requirments");
        ptctx->createdExecution = NULL;
        ptctx->currentCtx = NULL;
        return REDISMODULE_OK;
    }
    void* old = RedisGearsPy_Lock(session);
    v = PyRun_StringFlags(script, Py_file_input, ptctx->currSession->globalsDict, ptctx->currSession->globalsDict, NULL);
    PythonSessionCtx_Free(ptctx->currSession);

    if(!v){
        char* err = getPyError();
        if(!err){
            RedisModule_ReplyWithError(ctx, "failed running the given script");
        }else{
            RedisModule_ReplyWithError(ctx, err);
            RG_FREE(err);
        }

        if(ptctx->createdExecution){
            // error occured, we need to abort the created execution.
            int res = RedisGears_AbortExecution(ptctx->createdExecution);
            assert(res == REDISMODULE_OK);
            RedisGears_DropExecution(ptctx->createdExecution);
        }

        RedisGearsPy_Unlock(old);

        ptctx->createdExecution = NULL;
        ptctx->currentCtx = NULL;
        return REDISMODULE_OK;
    }

    if(ptctx->createdExecution){
        if(isBlocking){
            RedisModuleBlockedClient *bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 1000000);
            RedisGears_AddOnDoneCallback(ptctx->createdExecution, ptctx->doneFunction, bc);
        }else{
            const char* id = RedisGears_GetId(ptctx->createdExecution);
            RedisModule_ReplyWithStringBuffer(ctx, id, strlen(id));
        }
    }else{
        RedisModule_ReplyWithSimpleString(ptctx->currentCtx, "OK");
    }
    RedisGearsPy_Unlock(old);

    ptctx->createdExecution = NULL;
    ptctx->currentCtx = NULL;

    return REDISMODULE_OK;
}

int RedisGearsPy_ExecuteWithCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, DoneCallbackFunction callback){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    DoneCallbackFunction orgDoneFunction = ptctx->doneFunction;
    ptctx->doneFunction = callback;
    int res = RedisGearsPy_Execute(ctx, argv, argc);
    ptctx->doneFunction = orgDoneFunction;
    return res;
}

char* getPyError() {
    if(!PyErr_Occurred()){
        return NULL;
    }
    PyObject *pType, *pValue, *pTraceback;
    PyErr_Fetch(&pType, &pValue, &pTraceback);
    PyErr_NormalizeException(&pType, &pValue, &pTraceback);
    PyObject *pModuleName = PyUnicode_FromString("traceback");
    PyObject *pModule = PyImport_Import(pModuleName);
    PyObject *pStrTraceback = NULL;
    if(GearsConfig_GetPythonAttemptTraceback() && pTraceback != NULL && pModule != NULL){
        PyObject *pFunc = PyObject_GetAttrString(pModule, "format_exception");
        if(pFunc != NULL){
            if(PyCallable_Check(pFunc)){
                PyObject *pCall = PyObject_CallFunctionObjArgs(pFunc, pType, pValue, pTraceback, NULL);
                if(pCall){
                    pStrTraceback = PyObject_Str(pCall);
                    Py_DECREF(pCall);
                }
            }
            Py_DECREF(pFunc);
        }
        Py_DECREF(pModule);
    }
    if(pStrTraceback == NULL){
        PyObject *pStrFormat = PyUnicode_FromString("Error type: %s, Value: %s");
        PyObject* pStrType = PyObject_Str(pType);
        PyObject* pStrValue = PyObject_Str(pValue);
        PyObject *pArgs = PyTuple_New(2);
        PyTuple_SetItem(pArgs, 0, pStrType);
        PyTuple_SetItem(pArgs, 1, pStrValue);
        pStrTraceback = PyUnicode_Format(pStrFormat, pArgs);
        Py_DECREF(pArgs);
        Py_DECREF(pStrFormat);
    }
    char *strTraceback = (char*)PyUnicode_AsUTF8AndSize(pStrTraceback, NULL);
    char* err =  RG_STRDUP(strTraceback);
    Py_DECREF(pStrTraceback);
    Py_DECREF(pModuleName);
    if(pType){
        Py_DECREF(pType);
    }
    if(pValue){
        Py_DECREF(pValue);
    }
    if(pTraceback){
        Py_DECREF(pTraceback);
    }
    return err;
}

void fetchPyError(ExecutionCtx* rctx) {
    RedisGears_SetError(rctx, getPyError());
}

void RedisGearsPy_PyCallbackForEach(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        return;
    }
    if(ret != Py_None){
        Py_INCREF(Py_None);
    	Py_DECREF(ret);
    }

    RedisGearsPy_Unlock(old);
}

static Record* RedisGearsPy_PyCallbackAccumulateByKey(ExecutionCtx* rctx, char* key, Record *accumulate, Record *r, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

	PyObject* pArgs = PyTuple_New(3);
	PyObject* callback = arg;
	PyObject* currObj = PyObjRecordGet(r);
	PyObject* keyPyStr = PyUnicode_FromString(key);
	PyObjRecordSet(r, NULL);
	PyObject* oldAccumulateObj = Py_None;
	Py_INCREF(oldAccumulateObj);
	if(!accumulate){
		accumulate = PyObjRecordCreate();
	}else{
		oldAccumulateObj = PyObjRecordGet(accumulate);
	}
	PyTuple_SetItem(pArgs, 0, keyPyStr);
	PyTuple_SetItem(pArgs, 1, oldAccumulateObj);
	PyTuple_SetItem(pArgs, 2, currObj);
	PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
	Py_DECREF(pArgs);
	if(!newAccumulateObj){
	    fetchPyError(rctx);

	    RedisGearsPy_Unlock(old);
		RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
		return NULL;
	}
	PyObjRecordSet(accumulate, newAccumulateObj);

	RedisGearsPy_Unlock(old);
    RedisGears_FreeRecord(r);
	return accumulate;
}

static Record* RedisGearsPy_PyCallbackAccumulate(ExecutionCtx* rctx, Record *accumulate, Record *r, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pArgs = PyTuple_New(2);
    PyObject* callback = arg;
    PyObject* currObj = PyObjRecordGet(r);
    PyObjRecordSet(r, NULL);
    PyObject* oldAccumulateObj = Py_None;
    Py_INCREF(oldAccumulateObj);
    if(!accumulate){
        accumulate = PyObjRecordCreate();
    }else{
        oldAccumulateObj = PyObjRecordGet(accumulate);
    }
    PyTuple_SetItem(pArgs, 0, oldAccumulateObj);
    PyTuple_SetItem(pArgs, 1, currObj);
    PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newAccumulateObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
        return NULL;
    }
    PyObjRecordSet(accumulate, newAccumulateObj);

    RedisGearsPy_Unlock(old);

    RedisGears_FreeRecord(r);
    return accumulate;
}

static Record* RedisGearsPy_PyCallbackMapper(ExecutionCtx* rctx, Record *record, void* arg){
    assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        RedisGears_FreeRecord(record);
        return NULL;
    }
    PyObjRecordSet(record, newObj);

    RedisGearsPy_Unlock(old);
    return record;
}

static Record* RedisGearsPy_PyCallbackFlatMapper(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = PyObjRecordGet(record);
    PyObjRecordSet(record, NULL);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        RedisGears_FreeRecord(record);
        return NULL;
    }
    if(PyList_Check(newObj)){
        RedisGears_FreeRecord(record);
        size_t len = PyList_Size(newObj);
        record = RedisGears_ListRecordCreate(len);
        for(size_t i = 0 ; i < len ; ++i){
            PyObject* temp = PyList_GetItem(newObj, i);
            Record* pyRecord = PyObjRecordCreate();
            Py_INCREF(temp);
            PyObjRecordSet(pyRecord, temp);
            RedisGears_ListRecordAdd(record, pyRecord);
        }
        Py_DECREF(newObj);
    }else{
        PyObjRecordSet(record, newObj);
    }

    RedisGearsPy_Unlock(old);
    return record;
}

static bool RedisGearsPy_PyCallbackFilter(ExecutionCtx* rctx, Record *record, void* arg){
    assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        return false;
    }
    bool ret1 = PyObject_IsTrue(ret);

    RedisGearsPy_Unlock(old);
    return ret1;
}

static char* RedisGearsPy_PyCallbackExtractor(ExecutionCtx* rctx, Record *record, void* arg, size_t* len){
    assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(extractor, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        return "";
    }
    PyObject* retStr;
    if(!PyUnicode_Check(ret)){
        retStr = PyObject_Repr(ret);
        Py_DECREF(ret);
    }else{
        retStr = ret;
    }
    const char* retCStr = PyUnicode_AsUTF8AndSize(retStr, len);
    char* retValue = RG_ALLOC(*len + 1);
    memcpy(retValue, retCStr, *len);
    retValue[*len] = '\0';
    Py_DECREF(retStr);
    //Py_DECREF(retStr); todo: we should uncomment it after we will pass bool
    //                         that will tell the extractor to free the memory!!

    RedisGearsPy_Unlock(old);
    return retValue;
}

static Record* RedisGearsPy_PyCallbackReducer(ExecutionCtx* rctx, char* key, size_t keyLen, Record *records, void* arg){
    assert(RedisGears_RecordGetType(records) == listRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* obj = PyList_New(0);
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(records) ; ++i){
        Record* r = RedisGears_ListRecordGet(records, i);
        assert(RedisGears_RecordGetType(r) == pythonRecordType);
        PyObject* element = PyObjRecordGet(r);
        PyList_Append(obj, element);
    }
    PyObject* reducer = arg;
    PyObject* pArgs = PyTuple_New(2);
    PyObject* keyPyObj = PyUnicode_FromString(key);
    PyTuple_SetItem(pArgs, 0, keyPyObj);
    PyTuple_SetItem(pArgs, 1, obj);
    PyObject* ret = PyObject_CallObject(reducer, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(old);
        RedisGears_FreeRecord(records);
        return NULL;
    }
    Record* retRecord = PyObjRecordCreate();
    PyObjRecordSet(retRecord, ret);

    RedisGearsPy_Unlock(old);
    RedisGears_FreeRecord(records);
    return retRecord;
}

static Record* RedisGearsPy_ToPyRecordMapperInternal(Record *record, void* arg){
    Record* res = PyObjRecordCreate();
    Record* tempRecord;
    PyObject* obj;
    PyObject* temp;
    char* str;
    long longNum;
    double doubleNum;
    char* key;
    Arr(char*) keys;
    size_t len;
    if(!record){
        Py_INCREF(Py_None);
        PyObjRecordSet(res, Py_None);
        return res;
    }
    if(RedisGears_RecordGetType(record) == stringRecordType){
        str = RedisGears_StringRecordGet(record, &len);
        // try to first decode it as string, if fails create a byte array.
        obj = PyUnicode_FromStringAndSize(str, len);
        if(!obj){
            PyErr_Clear();
            obj = PyByteArray_FromStringAndSize(str, len);
        }
    }else if(RedisGears_RecordGetType(record) == longRecordType){
        longNum = RedisGears_LongRecordGet(record);
        obj = PyLong_FromLong(longNum);
    }else if(RedisGears_RecordGetType(record) == doubleRecordType){
        doubleNum = RedisGears_DoubleRecordGet(record);
        obj = PyLong_FromDouble(doubleNum);
    }else if(RedisGears_RecordGetType(record) == keyRecordType){
        key = RedisGears_KeyRecordGetKey(record, NULL);
        obj = PyDict_New();
        temp = PyUnicode_FromString(key);
        PyDict_SetItemString(obj, "key", temp);
        Py_DECREF(temp);
        tempRecord = RedisGears_KeyRecordGetVal(record);
        if(tempRecord){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(tempRecord, arg);
            assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyDict_SetItemString(obj, "value", PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }else{
            Py_INCREF(Py_None);
            PyDict_SetItemString(obj, "value", Py_None);
        }
    }else if(RedisGears_RecordGetType(record) == listRecordType){
        len = RedisGears_ListRecordLen(record);
        obj = PyList_New(0);
        for(size_t i = 0 ; i < len ; ++i){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(RedisGears_ListRecordGet(record, i), arg);
            assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyList_Append(obj, PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }
    }else if(RedisGears_RecordGetType(record) == hashSetRecordType){
        keys = RedisGears_HashSetRecordGetAllKeys(record);
        obj = PyDict_New();
        for(size_t i = 0 ; i < array_len(keys) ; ++i){
            key = keys[i];
            temp = PyUnicode_FromString(key);
            tempRecord = RedisGears_HashSetRecordGet(record, key);
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(tempRecord, arg);
            assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyDict_SetItem(obj, temp, PyObjRecordGet(tempRecord));
            Py_DECREF(temp);
            RedisGears_FreeRecord(tempRecord);
        }
        array_free(keys);
    }else if(RedisGears_RecordGetType(record) == pythonRecordType){
        obj = PyObjRecordGet(record);
        Py_INCREF(obj);
    }else{
        assert(false);
    }
    PyObjRecordSet(res, obj);
    return res;
}

static Record* RedisGearsPy_ToPyRecordMapper(ExecutionCtx* rctx, Record *record, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    Record* res = RedisGearsPy_ToPyRecordMapperInternal(record, arg);
    RedisGearsPy_Unlock(old);

    RedisGears_FreeRecord(record);

    return res;
}

static void* RedisGearsPy_PyObjectDup(void* arg){
    void* old = RedisGearsPy_Lock(NULL);
    PyObject* obj = arg;
    Py_INCREF(obj);
    RedisGearsPy_Unlock(old);
    return arg;
}

static void RedisGearsPy_PyObjectFree(void* arg){
    void* old = RedisGearsPy_Lock(NULL);
    PyObject* obj = arg;
    Py_DECREF(obj);
    RedisGearsPy_Unlock(old);
}

static char* RedisGearsPy_PyObjectToString(void* arg){
    char* objCstr = NULL;
    void* old = RedisGearsPy_Lock(NULL);
    PyObject* obj = arg;
    PyObject *objStr = PyObject_Str(obj);
    const char* objTempCstr = PyUnicode_AsUTF8AndSize(objStr, NULL);
    objCstr = RG_STRDUP(objTempCstr);
    Py_DECREF(objStr);
    RedisGearsPy_Unlock(old);
    return objCstr;
}

int RedisGearsPy_PyObjectSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    void* old = RedisGearsPy_Lock(NULL);
    PyObject* obj = arg;
    PyObject* objStr = PyMarshal_WriteObjectToString(obj, Py_MARSHAL_VERSION);
    if(!objStr){
        *err = getPyError();
        RedisModule_Log(NULL, "warning", "Error occured on RedisGearsPy_PyObjectSerialize, error=%s", *err);
        RedisGearsPy_Unlock(old);
        return REDISMODULE_ERR;
    }
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(objStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(objStr);
    RedisGearsPy_Unlock(old);
    return REDISMODULE_OK;
}

void* RedisGearsPy_PyObjectDeserialize(Gears_BufferReader* br){
    void* old = RedisGearsPy_Lock(NULL);
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject* obj = PyMarshal_ReadObjectFromString(data, len);
    RedisGearsPy_Unlock(old);
    return obj;
}

static int RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    void* old = RedisGearsPy_Lock(NULL);
    PyObject* callback = arg;
    PyObject *pickleFunction = PyDict_GetItemString(pyGlobals, "dumps");
    PyObject *args = PyTuple_New(1);
    Py_INCREF(callback);
    PyTuple_SetItem(args, 0, callback);
    PyObject * serializedStr = PyObject_CallObject(pickleFunction, args);
    if(!serializedStr || PyErr_Occurred()){
        char* internalErr = getPyError();
        PyFunctionObject* callback_func = (PyFunctionObject*)callback;
        PyObject *name = callback_func->func_name;
        const char* nameCStr = PyUnicode_AsUTF8AndSize(name, NULL);
        rg_asprintf(err, "Error occured when serialized a python callback, callback=%s error=%s", nameCStr, internalErr);
        RG_FREE(internalErr);
        RedisGearsPy_Unlock(old);
        return REDISMODULE_ERR;
    }
    Py_DECREF(args);
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(serializedStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(serializedStr);
    RedisGearsPy_Unlock(old);
    return REDISMODULE_OK;
}

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, char** err){
    void* old = RedisGearsPy_Lock(NULL);
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject *dataStr = PyBytes_FromStringAndSize(data, len);
    PyObject *loadFunction = PyDict_GetItemString(pyGlobals, "loads");
    PyObject *args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, dataStr);
    PyObject * callback = PyObject_CallObject(loadFunction, args);
    if(!callback || PyErr_Occurred()){
        char* error = getPyError();
        if(err){
            rg_asprintf(err, "Error occured when deserialized a python callback, error=%s",  error);
            RedisModule_Log(NULL, "warning", *err);
        }else{
            RedisModule_Log(NULL, "warning", "Error occured when deserialized a python callback, error=%s",  error);
        }
        RG_FREE(error);
        Py_DECREF(args);
        RedisGearsPy_Unlock(old);
        return NULL;
    }
    Py_DECREF(args);

    if(fep){
        // replace the global dictionary with the session global dictionary
        PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
        PyFunctionObject* callback_func = (PyFunctionObject*)callback;
        PyDict_Merge(sctx->globalsDict, callback_func->func_globals, 0);
        Py_DECREF(callback_func->func_globals);
        callback_func->func_globals = sctx->globalsDict;
        Py_INCREF(callback_func->func_globals);
    }

    RedisGearsPy_Unlock(old);
    return callback;
}

long long totalAllocated = 0;
long long currAllocated = 0;
long long peakAllocated = 0;

typedef struct pymem{
	size_t size;
	char data[];
}pymem;

static void* RedisGearsPy_AllocInternal(void* ctx, size_t size, bool useCalloc){
    pymem* m = NULL;
    if(useCalloc){
        m = RG_CALLOC(1, sizeof(pymem) + size);
    }else{
        m = RG_ALLOC(sizeof(pymem) + size);
    }
    m->size = size;
    totalAllocated += size;
    currAllocated += size;
    if(currAllocated > peakAllocated){
        peakAllocated = currAllocated;
    }
    return m->data;
}

static void* RedisGearsPy_Alloc(void* ctx, size_t size){
    return RedisGearsPy_AllocInternal(ctx, size, false);
}

static void* RedisGearsPy_Calloc(void* ctx, size_t n_elements, size_t size){
    return RedisGearsPy_AllocInternal(ctx, n_elements * size, true);
}

static void* RedisGearsPy_Relloc(void* ctx, void * p, size_t size){
	if(!p){
		return RedisGearsPy_Alloc(ctx, size);
	}
	pymem* m = p - sizeof(size_t);
	currAllocated -= m->size;
	totalAllocated -= m->size;
	m = RG_REALLOC(m, sizeof(pymem) + size);
	m->size = size;
	currAllocated += size;
	totalAllocated += size;
	if(currAllocated > peakAllocated){
		peakAllocated = currAllocated;
	}
	return m->data;
}

static void RedisGearsPy_Free(void* ctx, void * p){
	if(!p){
		return;
	}
	pymem* m = p - sizeof(size_t);
	currAllocated -= m->size;
	RG_FREE(m);
}

static int RedisGearsPy_Stats(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_ReplyWithArray(ctx, 6);
	RedisModule_ReplyWithStringBuffer(ctx, "TotalAllocated", strlen("TotalAllocated"));
	RedisModule_ReplyWithLongLong(ctx, totalAllocated);
	RedisModule_ReplyWithStringBuffer(ctx, "PeakAllocated", strlen("PeakAllocated"));
	RedisModule_ReplyWithLongLong(ctx, peakAllocated);
	RedisModule_ReplyWithStringBuffer(ctx, "CurrAllocated", strlen("CurrAllocated"));
	RedisModule_ReplyWithLongLong(ctx, currAllocated);
	return REDISMODULE_OK;
}

typedef struct PythonReaderCtx{
    PyObject* callback;
    PyObject* generator;
    bool isDone;
}PythonReaderCtx;

static Record* PythonReader_Next(ExecutionCtx* rctx, void* ctx){
    PythonReaderCtx* pyCtx = ctx;
    if(pyCtx->isDone){
        return NULL;
    }

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

    PyObject* pyRecord = NULL;
    if(!pyCtx->generator){
        PyObject* pArgs = PyTuple_New(0);
        PyObject* callback = pyCtx->callback;
        pyRecord = PyObject_CallObject(callback, pArgs);
        Py_DECREF(pArgs);
        if(!pyRecord){
            fetchPyError(rctx);
            RedisGearsPy_Unlock(old);
            pyCtx->isDone = true;
            return NULL;
        }
        if(PyGen_Check(pyRecord)) {
            pyCtx->generator = PyObject_GetIter(pyRecord);
            Py_DECREF(pyRecord);
            pyRecord = PyIter_Next(pyCtx->generator);
        }
    }else{
        pyRecord = PyIter_Next(pyCtx->generator);
    }
    if(!pyRecord){
        fetchPyError(rctx);
        RedisGearsPy_Unlock(old);
        pyCtx->isDone = true;
        return NULL;
    }
    RedisGearsPy_Unlock(old);
    Record* record = PyObjRecordCreate();
    PyObjRecordSet(record, pyRecord);
    return record;
}

static void PythonReader_Free(void* ctx){
    PythonReaderCtx* pyCtx = ctx;
    PyObject* callback = pyCtx->callback;
    void* old = RedisGearsPy_Lock(NULL);
    Py_DECREF(callback);
    if(pyCtx->generator){
        Py_DECREF(pyCtx->generator);
    }
    RedisGearsPy_Unlock(old);
    RG_FREE(pyCtx);
}

static void PythonReader_Serialize(void* ctx, Gears_BufferWriter* bw){
    PythonReaderCtx* pyCtx = ctx;
    int res = RedisGearsPy_PyCallbackSerialize(pyCtx->callback, bw, NULL);
    assert(res == REDISMODULE_OK);
}

static void PythonReader_Deserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){
    PythonReaderCtx* pyCtx = ctx;
    pyCtx->callback = RedisGearsPy_PyCallbackDeserialize(fep, br, NULL);
    assert(pyCtx->callback);
}

static Reader* PythonReader_Create(void* arg){
    PyObject* callback = arg;
    if(callback){
        void* old = RedisGearsPy_Lock(NULL);
        Py_INCREF(callback);
        RedisGearsPy_Unlock(old);
    }
    PythonReaderCtx* pyCtx = RG_ALLOC(sizeof(*pyCtx));
    pyCtx->callback = callback;
    pyCtx->generator = NULL;
    pyCtx->isDone = false;
    Reader* ret = RG_ALLOC(sizeof(*ret));
    *ret = (Reader){
            .ctx = pyCtx,
            .next = PythonReader_Next,
            .free = PythonReader_Free,
            .serialize = PythonReader_Serialize,
            .deserialize = PythonReader_Deserialize,
    };
    return ret;
}

RedisGears_ReaderCallbacks PythonReader = {
        .create = PythonReader_Create,
};

static char* PYENV_DIR;
static char* PYENV_HOME_DIR;
static char* PYENV_BIN_DIR;
static char* PYENV_ACTIVATE;
static char* PYENV_ACTIVATE_SCRIPT;

static void InitializeGlobalPaths(){
    rg_asprintf(&PYENV_DIR, "%s/python3_%s/", GearsConfig_GetPythonInstallationDir(), REDISGEARS_VERSION_STR);
    rg_asprintf(&PYENV_HOME_DIR, "%s/.venv/", PYENV_DIR);
    rg_asprintf(&PYENV_BIN_DIR, "%s/bin", PYENV_HOME_DIR);
    rg_asprintf(&PYENV_ACTIVATE, "%s/activate_this.py", PYENV_BIN_DIR);
    rg_asprintf(&PYENV_ACTIVATE_SCRIPT, "%s/activate", PYENV_BIN_DIR);
}


bool PyEnvExist() {
    DIR* dir = opendir(PYENV_DIR);
    if (dir) {
        closedir(dir);
        return true;
    }
    return false;
}

static int RedisGears_InstallDeps(RedisModuleCtx *ctx) {
#define SHA_256_SIZE 64
#define DEPS_FILE_PATH "/tmp/deps.%s.%s.tgz"
#define DEPS_FILE_DIR "/tmp/deps.%s.%s/"
#define LOCAL_VENV PYENV_DIR"/%s"
    const char *no_deps = getenv("GEARS_NO_DEPS");
    bool skip_deps_install = no_deps && !strcmp(no_deps, "1") || !GearsConfig_DownloadDeps();
    const char* shardUid = GetShardUniqueId();
    if (!PyEnvExist()){
        if (skip_deps_install) {
            RedisModule_Log(ctx, "warning", "No Python installation found and GEARS_NO_DEPS=1: aborting");
            return REDISMODULE_ERR;
        }
        const char* expectedSha256 = GearsConfig_GetDependenciesSha256();

        ExecCommand(ctx, "rm -rf "DEPS_FILE_PATH, shardUid, expectedSha256);

        ExecCommand(ctx, "curl -o "DEPS_FILE_PATH" %s", shardUid, expectedSha256, GearsConfig_GetDependenciesUrl());

        char* sha256Command;
        rg_asprintf(&sha256Command, "sha256sum "DEPS_FILE_PATH, shardUid, expectedSha256);
        FILE* f = popen(sha256Command, "r");
        RG_FREE(sha256Command);
        char sha256[SHA_256_SIZE];
        if(fscanf(f, "%64s", sha256) != 1){
            RedisModule_Log(ctx, "warning", "Failed to calculate sha25 on file "DEPS_FILE_PATH, shardUid, expectedSha256);
            pclose(f);
            return REDISMODULE_ERR;
        }
        pclose(f);

        if(strcmp(expectedSha256, sha256) != 0){
            RedisModule_Log(ctx, "warning", "Failed on sha 256 comparison");
            return REDISMODULE_ERR;
        }

        ExecCommand(ctx, "rm -rf "DEPS_FILE_DIR, shardUid, expectedSha256);
        ExecCommand(ctx, "mkdir -p "DEPS_FILE_DIR, shardUid, expectedSha256);

        ExecCommand(ctx, "tar -xvf "DEPS_FILE_PATH" -C "DEPS_FILE_DIR, shardUid, expectedSha256, shardUid, expectedSha256);

        ExecCommand(ctx, "mkdir -p %s", GearsConfig_GetPythonInstallationDir());
        ExecCommand(ctx, "mv "DEPS_FILE_DIR"/python3_%s/ %s", shardUid, expectedSha256, REDISGEARS_VERSION_STR, PYENV_DIR);
    }else{
        RedisModule_Log(ctx, "notice", "Found python installation under: %s", PYENV_DIR);
    }
    if(GearsConfig_CreateVenv()){
        rg_asprintf(&venvDir, "%s/.venv-%s", GearsConfig_GetPythonInstallationDir(), shardUid);
        DIR* dir = opendir(venvDir);
        if(!dir){
            ExecCommand(ctx, "mkdir -p %s", venvDir);
            setenv("VIRTUALENV_OVERRIDE_APP_DATA", venvDir, 1);
            int rc = ExecCommand(ctx, "/bin/bash -c \"%s/bin/python3 -m virtualenv %s\"", PYENV_DIR, venvDir);
            if (rc) {
                RedisModule_Log(ctx, "warning", "Failed to construct virtualenv");
                ExecCommand(ctx, "rm -rf %s", venvDir);
                return REDISMODULE_ERR;
            }
        }else{
            RedisModule_Log(ctx, "notice", "Found venv installation under: %s", venvDir);
            closedir(dir);
        }
    }else{
        // we are not operating inside virtual env
        venvDir = RG_STRDUP(PYENV_DIR);
    }
    return REDISMODULE_OK;
}

int RedisGears_SetupPyEnv(RedisModuleCtx *ctx) {
    DIR* dir = opendir(venvDir);
    assert(dir);
    closedir(dir);
    char* activateScript = NULL;
    char* activateCommand = NULL;
    rg_asprintf(&activateScript, "%s/bin/activate_this.py", venvDir);
    rg_asprintf(&activateCommand, "exec(open('%s').read(), {'__file__': '%s'})", activateScript, activateScript);
    RedisModule_Log(ctx, "notice", "Initializing Python environment with: %s", activateCommand);
    PyRun_SimpleString(activateCommand);
    RG_FREE(activateScript);
    RG_FREE(activateCommand);
    return 0;
}

static PyModuleDef EmbRedisGears = {
    PyModuleDef_HEAD_INIT,
    "redisgears",
    "export methods provide by redisgears.",
    -1,
    NULL, NULL, NULL, NULL, NULL
};

static PyModuleDef EmbRedisAI = {
    PyModuleDef_HEAD_INIT,
    "redisAI",
    "export methods provide by redisAI.",
    -1,
    NULL, NULL, NULL, NULL, NULL
};

PyMemAllocatorEx allocator = {
        .ctx = NULL,
        .malloc = RedisGearsPy_Alloc,
        .calloc = RedisGearsPy_Calloc,
        .realloc = RedisGearsPy_Relloc,
        .free = RedisGearsPy_Free,
};

static PyObject* PyInit_RedisGears(void) {
    return PyModule_Create(&EmbRedisGears);
}

static PyObject* PyInit_RedisAI(void) {
    return PyModule_Create(&EmbRedisAI);
}

void RedisGearsPy_ForceStop(unsigned long threadID){
    void* old = RedisGearsPy_Lock(NULL);
    PyThreadState_SetAsyncExc(threadID, ForceStoppedError);
    RedisGearsPy_Unlock(old);
}

static int PythonRecord_SendReply(Record* r, RedisModuleCtx* rctx){
    PyObject* obj = PyObjRecordGet(r);
    if(PyList_Check(obj)){
        size_t listLen = PyList_Size(obj);
        Record* rgl = RedisGears_ListRecordCreate(listLen);
        for(int i = 0 ; i < listLen ; ++i){
            Record* temp = PyObjRecordCreate();
            PyObject* pItem = PyList_GetItem(obj, i);
            PyObjRecordSet(temp, pItem);
            Py_INCREF(pItem);
            RedisGears_ListRecordAdd(rgl, temp);
        }
        RG_RecordSendReply(rgl, rctx);
        RedisGears_FreeRecord(rgl);
    }else if(PyLong_Check(obj)) {
        RedisModule_ReplyWithLongLong(rctx, PyLong_AsLongLong(obj));
    }else if(PyFloat_Check(obj)){
        double d = PyFloat_AsDouble(obj);
        RedisModuleString* str = RedisModule_CreateStringPrintf(NULL, "%lf", d);
        RedisModule_ReplyWithString(rctx, str);
        RedisModule_FreeString(NULL, str);
    }else if(PyUnicode_Check(obj)) {
        size_t len;
        char* str = (char*)PyUnicode_AsUTF8AndSize(obj, &len);
        RedisModule_ReplyWithStringBuffer(rctx, (char*)str, len);
    }else{
        RedisModule_ReplyWithStringBuffer(rctx, "PY RECORD", strlen("PY RECORD"));
    }
    return REDISMODULE_OK;
}

static int PythonRecord_Serialize(Gears_BufferWriter* bw, Record* base, char** err){
    PythonRecord* r = (PythonRecord*)base;
    if(RedisGearsPy_PyObjectSerialize(r->obj, bw, err) != REDISMODULE_OK){
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static void PythonRecord_Free(Record* base){
    PythonRecord* record = (PythonRecord*)base;
    if(record->obj && record->obj != Py_None){
        void* old = RedisGearsPy_Lock(NULL);
        Py_DECREF(record->obj);
        RedisGearsPy_Unlock(old);
    }
}

static Record* PythonRecord_Deserialize(Gears_BufferReader* br){
    Record* r = PyObjRecordCreate();
    PyObject* obj = RedisGearsPy_PyObjectDeserialize(br);
    PyObjRecordSet(r, obj);
    return r;
}

int RedisGearsPy_Init(RedisModuleCtx *ctx){
    InitializeGlobalPaths();

    if(RedisGears_InstallDeps(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Failed installing python dependencies");
        return REDISMODULE_ERR;
    }

    int err = pthread_key_create(&pythonThreadCtxKey, NULL);
    if(err){
        return REDISMODULE_ERR;
    }

    SessionsDict = Gears_dictCreate(dictTypeHeapIdsPtr, NULL);
    RequirementsDict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &allocator);
    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &allocator);
    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &allocator);
    char* arg = "Embeded";
    size_t len = strlen(arg);
    Py_SetPythonHome(Py_DecodeLocale(PYENV_DIR, NULL));

    EmbRedisGears.m_methods = EmbRedisGearsMethods;
    EmbRedisGears.m_size = sizeof(EmbRedisGearsMethods) / sizeof(*EmbRedisGearsMethods);
    PyImport_AppendInittab("redisgears", &PyInit_RedisGears);

    EmbRedisAI.m_methods = EmbRedisAIMethods;
    EmbRedisAI.m_size = sizeof(EmbRedisAIMethods) / sizeof(*EmbRedisAIMethods);
    PyImport_AppendInittab("redisAI", &PyInit_RedisAI);

    Py_Initialize();
    if(GearsConfig_CreateVenv()){
        // lets activate the virtual env we are operate in
        RedisGears_SetupPyEnv(ctx);
    }
    PyEval_InitThreads();
    wchar_t* arg2 = Py_DecodeLocale(arg, &len);
    PySys_SetArgv(1, &arg2);
    PyMem_RawFree(arg2);
    PyTensorType.tp_new = PyType_GenericNew;
    PyGraphRunnerType.tp_new = PyType_GenericNew;
    PyFlatExecutionType.tp_new = PyType_GenericNew;
    PyAtomicType.tp_new = PyType_GenericNew;

    PyFlatExecutionType.tp_methods = PyFlatExecutionMethods;
    PyAtomicType.tp_methods = PyAtomicMethods;

    if (PyType_Ready(&PyTensorType) < 0){
        RedisModule_Log(ctx, "warning", "PyTensorType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyGraphRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyTorchScriptRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyFlatExecutionType) < 0){
        RedisModule_Log(ctx, "warning", "PyFlatExecutionType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyAtomicType) < 0){
        RedisModule_Log(ctx, "warning", "PyAtomicType not ready");
        return REDISMODULE_ERR;
    }

    PyObject* pName = PyUnicode_FromString("gearsclient");
    PyObject* redisGearsClientModule = PyImport_Import(pName);
    Py_DECREF(pName);
    if(!redisGearsClientModule){
        RedisModule_Log(ctx, "warning", "gearsclient is not installed on the virtual env, will not be able to run with the python client.");
    }

    pName = PyUnicode_FromString("redisgears");
    PyObject* redisGearsModule = PyImport_Import(pName);
    Py_DECREF(pName);

    pName = PyUnicode_FromString("redisAI");
    PyObject* redisAIModule = PyImport_Import(pName);
    Py_DECREF(pName);

    Py_INCREF(&PyTensorType);
    Py_INCREF(&PyGraphRunnerType);
    Py_INCREF(&PyTorchScriptRunnerType);
    Py_INCREF(&PyFlatExecutionType);
    Py_INCREF(&PyAtomicType);

    PyModule_AddObject(redisAIModule, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(redisAIModule, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);
    PyModule_AddObject(redisAIModule, "PyTorchScriptRunner", (PyObject *)&PyTorchScriptRunnerType);
    PyModule_AddObject(redisGearsModule, "PyFlatExecution", (PyObject *)&PyFlatExecutionType);
    PyModule_AddObject(redisGearsModule, "PyAtomic", (PyObject *)&PyAtomicType);
    GearsError = PyErr_NewException("spam.error", NULL, NULL);
    Py_INCREF(GearsError);
    PyModule_AddObject(redisGearsModule, "GearsError", GearsError);

    ForceStoppedError = PyErr_NewException("gears.forcestopped", NULL, NULL);
    Py_INCREF(ForceStoppedError);
    PyModule_AddObject(redisGearsModule, "GearsForceStopped", ForceStoppedError);

    char* script = RG_ALLOC(src_cloudpickle_py_len + 1);
    memcpy(script, src_cloudpickle_py, src_cloudpickle_py_len);
    script[src_cloudpickle_py_len] = '\0';
    PyRun_SimpleString(script);
    RG_FREE(script);

    script = RG_ALLOC(src_GearsBuilder_py_len + 1);
    memcpy(script, src_GearsBuilder_py, src_GearsBuilder_py_len);
    script[src_GearsBuilder_py_len] = '\0';
    PyRun_SimpleString(script);
    RG_FREE(script);

    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(NULL, "warning", "Error occured on RedisGearsPy_Init, error=%s", error);
        RG_FREE(error);
        return REDISMODULE_ERR;
    }

    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(NULL, "warning", "Error occured on RedisGearsPy_Init, error=%s", error);
        RG_FREE(error);
        return REDISMODULE_ERR;
    }

    pythonRecordType = RedisGears_RecordTypeCreate("PythonRecord", sizeof(PythonRecord),
                                                   PythonRecord_SendReply,
                                                   PythonRecord_Serialize,
                                                   PythonRecord_Deserialize,
                                                   PythonRecord_Free);

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType",
                                                    RedisGearsPy_PyObjectFree,
                                                    RedisGearsPy_PyObjectDup,
                                                    RedisGearsPy_PyCallbackSerialize,
                                                    RedisGearsPy_PyCallbackDeserialize,
                                                    RedisGearsPy_PyObjectToString);

    ArgType* pySessionType = RedisGears_CreateType("PySessionType",
                                                    PythonSessionCtx_Free,
                                                    PythonSessionCtx_ShellowCopy,
                                                    PythonSessionCtx_Serialize,
                                                    PythonSessionCtx_Deserialize,
                                                    PythonSessionCtx_ToString);

    RedisGears_RegisterFlatExecutionPrivateDataType(pySessionType);

    RGM_RegisterReader(PythonReader);
    RGM_RegisterForEach(RedisGearsPy_PyCallbackForEach, pyCallbackType);
    RGM_RegisterFilter(RedisGearsPy_PyCallbackFilter, pyCallbackType);
    RGM_RegisterMap(RedisGearsPy_ToPyRecordMapper, NULL);
    RGM_RegisterMap(RedisGearsPy_PyCallbackFlatMapper, pyCallbackType);
    RGM_RegisterMap(RedisGearsPy_PyCallbackMapper, pyCallbackType);
    RGM_RegisterAccumulator(RedisGearsPy_PyCallbackAccumulate, pyCallbackType);
    RGM_RegisterAccumulatorByKey(RedisGearsPy_PyCallbackAccumulateByKey, pyCallbackType);
    RGM_RegisterGroupByExtractor(RedisGearsPy_PyCallbackExtractor, pyCallbackType);
    RGM_RegisterReducer(RedisGearsPy_PyCallbackReducer, pyCallbackType);
    RGM_RegisterExecutionOnUnpausedCallback(RedisGearsPy_OnExecutionUnpausedCallback, pyCallbackType);
    RGM_RegisterFlatExecutionOnRegisteredCallback(RedisGearsPy_OnRegistered, pyCallbackType);

    if(TimeEvent_RegisterType(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not register command timer datatype");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexecute", RedisGearsPy_Execute, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyexecute");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexecuteremote", RedisGearsPy_ExecuteRemote, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyexecuteremote");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyfreeinterpreter", RedisGearsPy_FreeInterpreter, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.pyexecute");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.pystats", RedisGearsPy_Stats, "readonly", 0, 0, 0) != REDISMODULE_OK) {
    	RedisModule_Log(ctx, "warning", "could not register command rg.pystats");
		return REDISMODULE_ERR;
    }

    runGearsRemoteBuilderCallback = PyDict_GetItemString(pyGlobals, "RunGearsRemoteBuilder");
    assert(runGearsRemoteBuilderCallback);
    Py_INCREF(runGearsRemoteBuilderCallback);

    PyEval_SaveThread();

    return REDISMODULE_OK;
}

void RedisGearsPy_Clean() {
    if(!RequirementsDict){
        return;
    }

    PythonRequirementCtx** reqs = array_new(PythonRequirementCtx*, 10);

    Gears_dictIterator *iter = Gears_dictGetIterator(RequirementsDict);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        PythonRequirementCtx* req = Gears_dictGetVal(entry);
        // he can not directly release cause it will change the dict while iterating
        array_append(reqs, req);
    }
    Gears_dictReleaseIterator(iter);

    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        PythonRequirementCtx_Free(reqs[i]);
    }

    array_free(reqs);
}
