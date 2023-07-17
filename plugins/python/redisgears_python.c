
#define REDISMODULE_MAIN

#include <Python.h>
#include "redisgears_python.h"
#include "redisai.h"
#include "globals.h"
#include "redisgears.h"
#include "redisgears_memory.h"
#include "GearsBuilder.auto.h"
#include "cloudpickle.auto.h"
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "utils/adlist.h"
#include "utils/thpool.h"
#include "utils/buffer.h"

#include <marshal.h>
#include <assert.h>
#include <dirent.h>

#include <pthread.h>

typedef struct PythonConfig{
    int createVenv;
    int downloadDeps;
    int foreceDownloadDepsOnEnterprise;
    int installReqMaxIdleTime;
    int attemptTraceback;
    int overrideAllocators;
    char* gearsPythonUrl;
    char* gearsPythonSha256;
    char* pythonInstallationDir;
}PythonConfig;

Plugin* pluginCtx;

PythonConfig pythonConfig;

static RedisModuleCtx *staticCtx = NULL;

static RedisVersion *redisVersion = NULL;

#define PY_OBJECT_TYPE_VERSION 1

#define PY_SESSION_TYPE_VERSION 3
#define PY_SESSION_TYPE_WITH_OPTIONAL_REQS 3
#define PY_SESSION_TYPE_WITH_NAMES 2
#define PY_SESSION_TYPE_WITH_DESC 2
#define PY_SESSION_TYPE_WITH_REQ_NAMES_ONLY 1

#define PY_REQ_VERSION 2
#define PY_REQ_VERSION_WITH_REQ_DICT_INDICATION 2
#define PY_REQ_VERSION_WITH_OS_VERSION 1
#define PY_SESSION_REQ_VERSION 1

#define SUB_INTERPRETER_TYPE "subInterpreterType"

#define STACK_BUFFER_SIZE 100

static PyObject* pyGlobals;
static PyObject* runCoroutineFunction;
static PyObject* createFutureFunction;
static PyObject* setFutureResultsFunction;
static PyObject* setFutureExceptionFunction;
static PyObject* profileCreateFunction;
static PyObject* profileStartFunction;
static PyObject* profileStopFunction;
static PyObject* profileGetInfoFunction;
static PyObject* initialModulesDict;
PyObject* GearsError;
PyObject* GearsFlatError;
PyObject* ForceStoppedError;

RecordType* pythonRecordType;

RedisModuleType* GearsPyRequirementDT;

pthread_mutex_t PySessionsLock;

typedef struct PythonRecord{
    Record base;
    PyObject* obj;
}PythonRecord;

static inline void GearsPyDecRef(PyObject* obj){
    RedisModule_Assert(obj->ob_refcnt > 0);
    Py_DECREF(obj);
}

static int PyInitializeRedisAI(){
    if (globals.redisAILoaded){
        return REDISMODULE_OK;
    }
    // If this API is not supported, we cannot initialize RedisAI low-level API using staticCtx.
    // Todo: use the MODULE_LOADED event to register redisAI API for older redis versions.
    if(!RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)) {
        RedisModule_Log(staticCtx, "warning",
          "Redis version is to old, please upgrade to a newer version.");
        return REDISMODULE_ERR;
    }
    RedisGears_LockHanlderAcquire(staticCtx);
    int ret = RedisAI_Initialize(staticCtx);
    if(ret == REDISMODULE_OK){
        globals.redisAILoaded = true;
    }
    RedisGears_LockHanlderRelease(staticCtx);
    return ret;
}

#define verifyRedisAILoaded() \
    if(!globals.redisAILoaded && (PyInitializeRedisAI() != REDISMODULE_OK)){ \
        PyErr_SetString(GearsError, "RedisAI is not loaded, it is not possible to use AI interface."); \
        return NULL;\
    }

static Record* PyObjRecordCreate(){
    PythonRecord* ret = (PythonRecord*)RedisGears_RecordCreate(pythonRecordType);
    ret->obj = NULL;
    return &ret->base;
}

static PyObject* PyObjRecordGet(Record* base){
    RedisModule_Assert(base->type == pythonRecordType);
    PythonRecord* r = (PythonRecord*)base;
    return r->obj;
}

static void PyObjRecordSet(Record* base, PyObject* obj){
    RedisModule_Assert(base->type == pythonRecordType);
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

#define PythonThreadCtxFlag_InsideAtomic (1 << 0)

typedef struct PythonSessionCtx PythonSessionCtx;

typedef struct PythonExecutionCtx{
    PythonSessionCtx* sCtx;
    ExecutionCtx* eCtx;
    PyObject* pyfutureCreated;
}PythonExecutionCtx;

void RedisGearsPy_Lock(PythonExecutionCtx* pectx);
void RedisGearsPy_Unlock(PythonExecutionCtx* oldectx);

#define PythonExecutionCtx_New(s, e) (PythonExecutionCtx){.sCtx = s, .eCtx = e, .pyfutureCreated = NULL}

#define RedisGearsPy_LOCK \
    PythonExecutionCtx oldpectx = {0}; \
    RedisGearsPy_Lock(&oldpectx);

#define RedisGearsPy_UNLOCK RedisGearsPy_Unlock(&oldpectx);

typedef void (*DoneCallbackFunction)(ExecutionPlan* ep, void* privateData);

/*
 * Thread spacific data
 */
typedef struct PythonThreadCtx{
    int lockCounter;
    RedisModuleCtx* currentCtx;
    ExecutionPlan* createdExecution;
    DoneCallbackFunction doneFunction;
    PythonSessionCtx* currSession;
    ExecutionCtx* currEctx;
    int flags;
    CommandReaderTriggerCtx* crtCtx;
    CommandCtx* commandCtx;
    PyObject* pyfutureCreated;
}PythonThreadCtx;

/* default onDone function */
static void onDone(ExecutionPlan* ep, void* privateData);
char* getPyError();

#define PYTHON_ERROR "error running python code"

typedef struct PythonRequirementCtx PythonRequirementCtx;

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err);
static void TimeEvent_Free(void *value);
static int RedisGearsPy_PyCallbackSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err);
static PythonThreadCtx* GetPythonThreadCtx();
static void continueFutureOnDone(ExecutionPlan* ep, void* privateData);
static void dropExecutionOnDone(ExecutionPlan* ep, void* privateData);
static PythonRequirementCtx* PythonRequirementCtx_Get(const char* requirement);
static void RedisGearsPy_AddSessionRequirementsToDict(PythonSessionCtx* session);


RedisModuleDict* SessionsDict = NULL;
Gears_list *DeadSessionsList = NULL;

static char* venvDir = NULL;

struct PythonRequirementCtx{
    size_t refCount;
    char* basePath;
    char* installName;
    char** wheels;
    volatile bool isInstalled;
    volatile bool isDownloaded;
    pthread_mutex_t installationLock;
    bool isInRequirementsDict;
};

Gears_dict* RequirementsDict = NULL;

typedef enum {
    LinkedTo_PrimaryDict, LinkedTo_TempDict, LinkedTo_None,
} LinkedTo;

typedef struct PythonSessionCtx{
    size_t refCount;
    char *sessionId;
    char *sessionDesc;
    PyObject* globalsDict;
    PyObject* profiler;
    PythonRequirementCtx** requirements;
    bool isInstallationNeeded;
    char **registrations;
    pthread_mutex_t registrationsLock;
    SessionRegistrationCtx *srctx;
    LinkedTo linkedTo;
    Gears_listNode *deadNode;
    bool fullSerialization;
}PythonSessionCtx;

static PythonSessionCtx* currentSession = NULL;

static PythonRequirementCtx* PythonRequirementCtx_Deserialize(Gears_BufferReader* br, int version, char** err);
static PythonRequirementCtx* PythonRequirement_Deserialize(Gears_BufferReader* br, char** err);
static int PythonRequirementCtx_Serialize(PythonRequirementCtx* req, Gears_BufferWriter* bw, char** err);
static int PythonRequirement_Serialize(PythonRequirementCtx* req, Gears_BufferWriter* bw, char** err);

static PyObject* GearsPyDict_GetItemString(PyObject* dict, const char* key){
    return (dict ? PyDict_GetItemString(dict, key) : NULL);
}



static bool PythonRequirementCtx_DownloadRequirement(PythonRequirementCtx* req){
#define RETRY 3
#define RETRY_SLEEP_IN_SEC 1
    int ret = true;
    int exitCode;
    pthread_mutex_lock(&req->installationLock);
    if(req->isDownloaded){
        goto done;
    }
    for(size_t i = 0 ; i < RETRY; ++i){
        if(pythonConfig.createVenv){
            exitCode = RedisGears_ExecuteCommand(NULL, "notice", "/bin/bash -c \"source %s/bin/activate;cd '%s';python -m pip wheel '%s'\"", venvDir, req->basePath, req->installName);
        }else{
            exitCode = RedisGears_ExecuteCommand(NULL, "notice", "/bin/bash -c \"cd '%s';%s/bin/python3 -m pip wheel '%s'\"", req->basePath, venvDir, req->installName);
        }
        if(exitCode != 0){
            sleep(RETRY_SLEEP_IN_SEC);
            continue;
        }
        break;
    }
    if(exitCode != 0){
        ret = false;
        goto done;
    }

    // fills the wheels array
    DIR *dr = opendir(req->basePath);
    RedisModule_Assert(dr);
    struct dirent *de;
    while ((de = readdir(dr))){
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0){
            continue;
        }
        char* c = de->d_name;
        req->wheels = array_append(req->wheels, RG_STRDUP(de->d_name));
    }
    closedir(dr);

    req->isDownloaded = true;

done:
    pthread_mutex_unlock(&req->installationLock);
    return ret;
}

static bool PythonRequirementCtx_InstallRequirement(PythonRequirementCtx* req){
    int ret = true;
    pthread_mutex_lock(&req->installationLock);
    RedisModule_Assert(array_len(req->wheels) > 0);
    char* filesInDir = array_new(char, 10);
    filesInDir = array_append(filesInDir, '\'');
    for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
        char* c = req->wheels[i];
        while(*c){
            filesInDir = array_append(filesInDir, *c);
            ++c;
        }
        filesInDir = array_append(filesInDir, '\'');
        filesInDir = array_append(filesInDir, ' ');
        filesInDir = array_append(filesInDir, '\'');
    }
    filesInDir[array_len(filesInDir) - 2] = '\0';

    int exitCode;
    if(pythonConfig.createVenv){
        exitCode = RedisGears_ExecuteCommand(NULL, "notice", "/bin/bash -c \"source %s/bin/activate; cd '%s'; python -m pip install --no-cache-dir --force-reinstall --no-index --disable-pip-version-check %s\"", venvDir, req->basePath, filesInDir);
    }else{
        exitCode = RedisGears_ExecuteCommand(NULL, "notice", "/bin/bash -c \"cd '%s'; %s/bin/python3 -m pip install --no-cache-dir --force-reinstall --no-index --disable-pip-version-check %s\"", req->basePath, venvDir, filesInDir);
    }
    array_free(filesInDir);
    if(exitCode == 0){
        req->isInstalled = true;
    } else {
        req->isInstalled = false;
    }

    ret = exitCode == 0;
done:
    pthread_mutex_unlock(&req->installationLock);
    return ret;
}

static void PythonRequirementCtx_VerifyBasePath(PythonRequirementCtx* req){
    char* c = req->basePath;
    while(*c){
        if(*c != '/'){
            return;
        }
        c++;
    }
    RedisModule_Log(staticCtx, "warning", "Fatal!!!, failed verifying basePath of requirement. name:'%s', basePath:'%s'", req->installName, req->basePath);
    RedisModule_Assert(false);
}
static void PythonRequirementCtx_Free(PythonRequirementCtx* reqCtx){
    if(--reqCtx->refCount){
        return;
    }

    PythonRequirementCtx_VerifyBasePath(reqCtx);
    RedisGears_ExecuteCommand(NULL, "notice", "rm -rf '%s'", reqCtx->basePath);
    if (reqCtx->isInRequirementsDict) {
        Gears_dictDelete(RequirementsDict, reqCtx->installName);
    } else if (reqCtx->isInstalled) {
        // reinstall old requirement if exists
        PythonRequirementCtx *oldReq = PythonRequirementCtx_Get(reqCtx->installName);
        if (oldReq) {
            PythonRequirementCtx_InstallRequirement(oldReq);
            PythonRequirementCtx_Free(oldReq);
        }
    }
    RG_FREE(reqCtx->installName);
    RG_FREE(reqCtx->basePath);
    if(reqCtx->wheels){
        for(size_t i = 0 ; i < array_len(reqCtx->wheels) ; i++){
            RG_FREE(reqCtx->wheels[i]);
        }
        array_free(reqCtx->wheels);
    }
    RG_FREE(reqCtx);
}

static char* PythonStringArray_ToStr(void* wheel){
    char* str;
    RedisGears_ASprintf(&str, "'%s'", (char*)wheel);
    return str;
}

static char* PythonRequirementCtx_ToStr(void* val){
    PythonRequirementCtx* req = val;
    char* res;
    char* wheelsStr = RedisGears_ArrToStr((void**)req->wheels, array_len(req->wheels), PythonStringArray_ToStr, ',');
    RedisGears_ASprintf(&res, "{'name':'%s', 'basePath':'%s', 'wheels':%s}", req->installName, req->basePath, wheelsStr);
    RG_FREE(wheelsStr);
    return res;
}

static char* PythonSessionRequirements_ToString(FlatExecutionPlan* fep, void* arg){
    PythonRequirementCtx** req = arg;
    return RedisGears_ArrToStr((void**)req, array_len(req), PythonRequirementCtx_ToStr, ',');
}

static void* PythonSessionRequirements_Deserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > PY_SESSION_REQ_VERSION){
        *err = RG_STRDUP("unsupported session requirements version");
        return NULL;
    }
    PythonRequirementCtx** reqs = array_new(PythonRequirementCtx*, 10);
    size_t reqsLen = RedisGears_BRReadLong(br);
    for(size_t i = 0 ; i < reqsLen ; ++i){
        PythonRequirementCtx* req = PythonRequirement_Deserialize(br, err);
        if(!req){
            goto error;
        }
        reqs = array_append(reqs, req);
    }
    return reqs;

error:
// revert
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        PythonRequirementCtx_Free(reqs[i]);
    }
    array_free(reqs);
    return NULL;
}

static int PythonSessionRequirements_Serialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    PythonRequirementCtx** reqs = arg;
    RedisGears_BWWriteLong(bw, array_len(reqs));
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        if(PythonRequirement_Serialize(reqs[i], bw, err) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

static void PythonSessionRequirements_Free(FlatExecutionPlan* fep, void* arg){
    PythonRequirementCtx** reqs = arg;
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        PythonRequirementCtx_Free(reqs[i]);
    }
    array_free(reqs);
}

static PythonRequirementCtx* PythonRequirementCtx_ShallowCopy(PythonRequirementCtx* req){
    ++req->refCount;
    return req;
}

static void* PythonSessionRequirements_Dup(void* arg){
    PythonRequirementCtx** reqs = arg;
    PythonRequirementCtx** res = array_new(PythonRequirementCtx*, 10);
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        res = array_append(res, PythonRequirementCtx_ShallowCopy(reqs[i]));
    }
    return res;
}

static void* PythonSessionRequirements_DupWithFep(FlatExecutionPlan* fep, void* arg){
    return PythonSessionRequirements_Dup(arg);
}

static PythonRequirementCtx* PythonRequirementCtx_Get(const char* requirement){
    PythonRequirementCtx* ret = Gears_dictFetchValue(RequirementsDict, requirement);
    if(ret){
        ++ret->refCount;
    }
    return ret;
}

static PythonRequirementCtx* PythonRequirementCtx_Create(const char* requirement){
#define RAND_SIZE 10
    char rand[RAND_SIZE + 1];
    RedisModule_GetRandomHexChars(rand, RAND_SIZE);
    rand[RAND_SIZE] = '\0';

    // first lets create basePath
    int exitCode = RedisGears_ExecuteCommand(NULL, "notice", "mkdir -p '%s/%s_%s'", venvDir, requirement, rand);
    if(exitCode != 0){
        return NULL;
    }

    PythonRequirementCtx* ret = RG_ALLOC(sizeof(*ret));
    ret->installName = RG_STRDUP(requirement);
    RedisGears_ASprintf(&ret->basePath, "%s/%s_%s", venvDir, ret->installName, rand);
    ret->wheels = array_new(char*, 10);
    // refCount is starting from 2, one hold by RequirementsDict and once by the caller.
    // currently we basically never delete requirements so we will know not to reinstall them
    // to save time
    ret->refCount = 1;
    pthread_mutex_init(&ret->installationLock, NULL);

    ret->isInRequirementsDict = false;

    PythonRequirementCtx_VerifyBasePath(ret);

    ret->isInstalled = false;
    ret->isDownloaded = false;

    return ret;
}

typedef struct{
    const char* fileName;
    const char* data;
    size_t len;
} wheelData;

static PythonRequirementCtx* PythonRequirementCtx_Deserialize(Gears_BufferReader* br, int version, char** err){
    if(version > PY_REQ_VERSION){
        *err = RG_STRDUP("Requirement version is not compatible, upgrade to newer RedisGears.");
        return NULL;
    }
    wheelData* wheelsData = array_new(wheelData, 10);
    PythonRequirementCtx* req = NULL;
    const char* name = RedisGears_BRReadString(br);
    if(name == BUFF_READ_ERROR){
        *err = RG_STRDUP("Bad serialization format on reading requirement name");
        goto done;
    }

    if(version >= PY_REQ_VERSION_WITH_OS_VERSION){
        const char* os = RedisGears_BRReadString(br);
        if(os == BUFF_READ_ERROR){
            *err = RG_STRDUP("Bad serialization format on reading requirement os");
            goto done;
        }
    }

    long isInReqDict = 1;
    if (version >= PY_REQ_VERSION_WITH_REQ_DICT_INDICATION) {
        isInReqDict = RedisGears_BRReadLong(br);
    }


    long nWheels = RedisGears_BRReadLong(br);
    if(nWheels == LONG_READ_ERROR){
        *err = RG_STRDUP("Bad serialization format on reading requirement wheels amount");
        goto done;
    }

    for(size_t i = 0 ; i < nWheels ; ++i){
        const char* fileName = RedisGears_BRReadString(br);
        if(fileName == BUFF_READ_ERROR){
            *err = RG_STRDUP("Bad serialization format on reading wheel file name");
            goto done;
        }
        size_t dataLen;
        const char* data = RedisGears_BRReadBuffer(br, &dataLen);
        if(data == BUFF_READ_ERROR){
            *err = RG_STRDUP("Bad serialization format on reading wheel file data");
            goto done;
        }

        wheelData wd = {
                .fileName = fileName,
                .data = data,
                .len = dataLen,
        };
        array_append(wheelsData, wd);
    }

    if (isInReqDict) {
        req = PythonRequirementCtx_Get(name);
    }

    if (!req) {
        req = PythonRequirementCtx_Create(name);
        if (isInReqDict) {
            req->refCount++;
            req->isInRequirementsDict = true;
            Gears_dictAdd(RequirementsDict, (char*)name, req);
        }
    }

    pthread_mutex_lock(&req->installationLock);
    if(!req->isDownloaded){
        for(size_t i = 0 ; i < array_len(wheelsData) ; ++i){
            char* filePath;
            RedisGears_ASprintf(&filePath, "%s/%s", req->basePath, wheelsData[i].fileName);

            FILE *f = fopen(filePath, "wb");
            if(!f){
                pthread_mutex_unlock(&req->installationLock);
                RG_FREE(filePath);
                *err = RG_STRDUP("Failed open file for write");
                goto done;
            }

            size_t dataWriten = fwrite(wheelsData[i].data, 1, wheelsData[i].len, f);
            if(dataWriten != wheelsData[i].len){
                pthread_mutex_unlock(&req->installationLock);
                RG_FREE(filePath);
                *err = RG_STRDUP("Failed writing data to file");
                goto done;
            }

            fclose(f);
            RG_FREE(filePath);

            req->wheels = array_append(req->wheels, RG_STRDUP(wheelsData[i].fileName));
        }
        req->isDownloaded = true;
    }
    pthread_mutex_unlock(&req->installationLock);

done:
    if(*err){
        if(req){
            PythonRequirementCtx_Free(req);
            req = NULL;
        }
    }
    array_free(wheelsData);
    return req;
}

static PythonRequirementCtx* PythonRequirement_Deserialize(Gears_BufferReader* br, char** err){
    int version = RedisGears_BRReadLong(br);
    if(version == LONG_READ_ERROR){
        *err = RG_STRDUP("Bad serialization format on reading requirement version");
        return NULL;
    }
    return PythonRequirementCtx_Deserialize(br, version, err);
}

static int PythonRequirementCtx_Serialize(PythonRequirementCtx* req, Gears_BufferWriter* bw, char** err){
    if(!req->isDownloaded){
        // requirement is not downloaded yet and so we can not serialized it
        // this can only happened if rdb save was trigger before download
        // completed. In this case rdb will not contains this requirement.
        // It will succeded next time.
        *err = RG_STRDUP("Requirement was not installed yet");
        return REDISMODULE_ERR;
    }
    RedisGears_BWWriteString(bw, req->installName);
    RedisGears_BWWriteString(bw, RedisGears_GetCompiledOs());
    RedisGears_BWWriteLong(bw, req->isInRequirementsDict);
    RedisGears_BWWriteLong(bw, array_len(req->wheels));
    for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
        char* wheel = req->wheels[i];
        char* filePath;
        RedisGears_ASprintf(&filePath, "%s/%s", req->basePath, wheel);

        FILE *f = fopen(filePath, "rb");
        if(!f){
            RedisGears_ASprintf(err, "Could not open file %s", filePath);
            RG_FREE(filePath);
            RedisModule_Log(staticCtx, "warning", "%s", *err);
            return REDISMODULE_ERR;
        }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

        char *data = RG_ALLOC(fsize);
        size_t readData = fread(data, 1, fsize, f);
        if(readData != fsize){
            RedisGears_ASprintf(err, "Could read data from file %s", filePath);
            RG_FREE(data);
            RG_FREE(filePath);
            RedisModule_Log(staticCtx, "warning", "%s", *err);
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

static int PythonRequirement_Serialize(PythonRequirementCtx* req, Gears_BufferWriter* bw, char** err){
    RedisGears_BWWriteLong(bw, PY_REQ_VERSION);
    return PythonRequirementCtx_Serialize(req, bw, err);
}

static void* PythonSessionCtx_ShallowCopy(void* arg){
    PythonSessionCtx* session = arg;
    ++session->refCount;
    return session;
}

static void* PythonSessionCtx_ShallowCopyWithFep(FlatExecutionPlan* fep, void* arg){
    return PythonSessionCtx_ShallowCopy(arg);
}

static void PythonSessionCtx_FreeRequirementsList(PythonRequirementCtx** requirementsList){
    for(size_t i = 0 ; i < array_len(requirementsList) ; ++i){
        PythonRequirementCtx_Free(requirementsList[i]);
    }
    array_free(requirementsList);
}

static void PythonSessionCtx_Del(PythonSessionCtx* session){
    if (session->linkedTo == LinkedTo_PrimaryDict) {
        RedisModule_DictDelC(SessionsDict, session->sessionId, strlen(session->sessionId), NULL);
        session->linkedTo = LinkedTo_None;
    } else if (session->deadNode) {
        Gears_listDelNode(DeadSessionsList, session->deadNode);
    }
}

static void PythonSessionCtx_Unlink(PythonSessionCtx* session){
    pthread_mutex_lock(&PySessionsLock);
    PythonSessionCtx_Del(session);
    Gears_listAddNodeTail(DeadSessionsList, session);
    session->deadNode = Gears_listLast(DeadSessionsList);
    pthread_mutex_unlock(&PySessionsLock);
}

static void PythonSessionCtx_Free(PythonSessionCtx* session){
    pthread_mutex_lock(&PySessionsLock);
    if(--session->refCount == 0){
        // delete the session working dir
        PythonSessionCtx_Del(session);
        pthread_mutex_unlock(&PySessionsLock);
        if (session->srctx) {
            RedisGears_SessionRegisterCtxFree(session->srctx);
        }
        RG_FREE(session->sessionId);
        if (session->sessionDesc) {
            RG_FREE(session->sessionDesc);
        }
        PythonSessionCtx_FreeRequirementsList(session->requirements);
        for (size_t i = 0 ; i < array_len(session->registrations) ; ++i) {
            RG_FREE(session->registrations[i]);
        }
        array_free(session->registrations);
        pthread_mutex_destroy(&session->registrationsLock);
        RedisGearsPy_LOCK
        PyDict_Clear(session->globalsDict);
        GearsPyDecRef(session->globalsDict);
        GearsPyDecRef(session->profiler);
        RedisGearsPy_UNLOCK
        RG_FREE(session);
    } else {
        pthread_mutex_unlock(&PySessionsLock);
    }
}

#define PythonSessionCtx_RunWithRegistrationsLock(s, code) \
    pthread_mutex_lock(&s->registrationsLock); \
    do{ \
    	code \
    } while (0); \
    pthread_mutex_unlock(&s->registrationsLock);

static void PythonSessionCtx_AddFep(PythonSessionCtx* session, char *id){
    pthread_mutex_lock(&session->registrationsLock);
    session->registrations = array_append(session->registrations, id);
    pthread_mutex_unlock(&session->registrationsLock);
}

static void PythonSessionCtx_DelFep(PythonSessionCtx* session, const char *id){
    pthread_mutex_lock(&session->registrationsLock);
    for (size_t i = 0 ; i < array_len(session->registrations) ; ++i) {
        if (strcmp(id, session->registrations[i]) == 0) {
            RG_FREE(session->registrations[i]);
            array_del_fast(session->registrations, i);
            break;
        }
    }
    pthread_mutex_unlock(&session->registrationsLock);
}

static void PythonSessionCtx_FreeWithFep(FlatExecutionPlan* fep, void* arg){
    PythonSessionCtx* session = arg;
    if (fep) {
        const char *id = RedisGears_FepGetId(fep);
        PythonSessionCtx_DelFep(session, id);
    }
    PythonSessionCtx_Free(session);
}

static PythonSessionCtx* PythonSessionCtx_Get(const char* id){
    pthread_mutex_lock(&PySessionsLock);
    PythonSessionCtx* ret = RedisModule_DictGetC(SessionsDict, (char*)id, strlen(id), NULL);
    if (ret) {
        ret = PythonSessionCtx_ShallowCopy(ret);
    }
    pthread_mutex_unlock(&PySessionsLock);
    return ret;
}

static void PythonSessionCtx_Set(PythonSessionCtx* s){
    pthread_mutex_lock(&PySessionsLock);
    PythonSessionCtx_Del(s);
    RedisModule_DictSetC(SessionsDict, s->sessionId, strlen(s->sessionId), s);
    s->linkedTo = LinkedTo_PrimaryDict;
    pthread_mutex_unlock(&PySessionsLock);
}

static int PythonSessionCtx_DownloadWheels(PythonSessionCtx* session){
    for(size_t i = 0 ; i < array_len(session->requirements) ; ++i){
        PythonRequirementCtx* req = session->requirements[i];
        if(!PythonRequirementCtx_DownloadRequirement(req)){
            return false;
        }
    }
    return true;
}

static PythonSessionCtx* PythonSessionCtx_CreateWithId(const char* id, const char* desc, const char** requirementsList, size_t requirementsListLen, bool forceReqReinstallation){
    // creating a new global dict for this session
    RedisGearsPy_LOCK

    PyObject* globalDict = PyDict_Copy(pyGlobals);

    PythonSessionCtx* session = RG_ALLOC(sizeof(*session));
    *session = (PythonSessionCtx){
            .sessionId = RG_STRDUP(id),
            .sessionDesc = desc? RG_STRDUP(desc) : NULL,
            .refCount = 1,
            .globalsDict = globalDict,
            .isInstallationNeeded = false,
            .profiler = PyObject_CallFunction(profileCreateFunction, NULL),
            .registrations = array_new(char*, 10),
            .srctx = NULL,
            .linkedTo = LinkedTo_None,
            .deadNode = NULL,
            .fullSerialization = false,

    };
    pthread_mutex_init(&session->registrationsLock, NULL);

    session->requirements = array_new(PythonRequirementCtx*, 10);

    if (!forceReqReinstallation) {
        // lets see if we already have the requirements installed
        for(size_t i = 0 ; i < requirementsListLen ; ++i){
            PythonRequirementCtx *req = PythonRequirementCtx_Get(requirementsList[i]);
            if (!req) {
                session->isInstallationNeeded = true;
                break;
            } else {
                session->requirements = array_append(session->requirements , req);
            }
        }
    } else {
        session->isInstallationNeeded = true;
    }

    if (session->isInstallationNeeded) {
        while(array_len(session->requirements) > 0) {
            PythonRequirementCtx *req = array_pop(session->requirements);
            PythonRequirementCtx_Free(req);
        }
        for(size_t i = 0 ; i < requirementsListLen ; ++i){
            PythonRequirementCtx* req = PythonRequirementCtx_Create(requirementsList[i]);

            if(!req){
                PythonSessionCtx_Free(session);
                session = NULL;
                break;
            }
            session->requirements = array_append(session->requirements , req);
        }
    }

    RedisGearsPy_UNLOCK

    return session;
}

static PythonSessionCtx* PythonSessionCtx_Create(const char *id, const char *desc, const char** requirementsList, size_t requirementsListLen, bool forceReqReinstallation){

    PythonSessionCtx* session = PythonSessionCtx_CreateWithId(id, desc, requirementsList, requirementsListLen, forceReqReinstallation);
    session->srctx = RedisGears_SessionRegisterCtxCreate(pluginCtx);
    RedisGears_SessionRegisterSetMaxIdle(session->srctx, pythonConfig.installReqMaxIdleTime);
    return session;
}

static void PythosSessionCtx_ResetModulesDict() {
    RedisGearsPy_LOCK
    /* reset modules catch */
    PyInterpreterState *interp = PyThreadState_GET()->interp;
    PyDict_Clear(interp->modules);
    PyDict_Merge(interp->modules, initialModulesDict, 1);
    RedisGearsPy_UNLOCK
}

static int PythonSessionCtx_SerializeInternals(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, bool withFullReqs, char** err){
    PythonSessionCtx* session = arg;
    RedisGears_BWWriteString(bw, session->sessionId);
    if (session->sessionDesc) {
        RedisGears_BWWriteLong(bw, 1); // description exists
        RedisGears_BWWriteString(bw, session->sessionDesc);
    } else {
        RedisGears_BWWriteLong(bw, 0); // description do not exists
    }
    if (withFullReqs) {
        RedisGears_BWWriteLong(bw, 1);
    } else {
        RedisGears_BWWriteLong(bw, 0);
    }
    RedisGears_BWWriteLong(bw, array_len(session->requirements));

    for(size_t i = 0 ; i < array_len(session->requirements) ; ++i){
        if (withFullReqs) {
            RedisGears_BWWriteLong(bw, PY_REQ_VERSION);
            if (PythonRequirementCtx_Serialize(session->requirements[i], bw, err) != REDISMODULE_OK) {
                return REDISMODULE_ERR;
            }
        } else {
            RedisGears_BWWriteString(bw, session->requirements[i]->installName);
        }
    }

    return REDISMODULE_OK;
}

static int PythonSessionCtx_Serialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    PythonSessionCtx* session = arg;
    return PythonSessionCtx_SerializeInternals(fep, arg, bw, session->fullSerialization, err);
}

static void* PythonSessionCtx_Deserialize(Gears_BufferReader* br, int version, char** err, bool useSessionsDict){
    if(version > PY_SESSION_TYPE_VERSION){
        *err = RG_STRDUP("unsupported session version");
        return NULL;
    }

    char *id;
    char *desc = NULL;
    if (version < PY_SESSION_TYPE_WITH_NAMES) {
        /* legacy id convert to str */
        size_t len;
        id = RedisGears_BRReadBuffer(br, &len);
        RedisGears_ASprintf(&id, "%.*s-%lld", REDISMODULE_NODE_ID_LEN, id, *(long long*)&id[REDISMODULE_NODE_ID_LEN]);
    } else {
        id = RG_STRDUP(RedisGears_BRReadString(br));
    }

    if (version >= PY_SESSION_TYPE_WITH_DESC) {
        if (RedisGears_BRReadLong(br)) {
            // description exists
            desc = RG_STRDUP(RedisGears_BRReadString(br));
        }
    }

    PythonSessionCtx* s = currentSession;
    if (s) {
        s = PythonSessionCtx_ShallowCopy(s);
    }
    if (!s && useSessionsDict) {
        s = PythonSessionCtx_Get(id);
    }
    bool sessionExists;
    if(!s){
        s = PythonSessionCtx_CreateWithId(id, desc, NULL, 0, false);
        if (useSessionsDict) {
            PythonSessionCtx_Set(s);
        }
        if(!s){
            *err = RG_STRDUP("Could not create session");
            return NULL;
        }
        sessionExists = false;
    }else{
        sessionExists = true;
    }
    RG_FREE(id);
    RG_FREE(desc);
    size_t withFullReqs = (version < PY_SESSION_TYPE_WITH_REQ_NAMES_ONLY);
    if (version >= PY_SESSION_TYPE_WITH_OPTIONAL_REQS) {
        withFullReqs = RedisGears_BRReadLong(br);
    }
    size_t requirementsLen = RedisGears_BRReadLong(br);
    bool requirementsInstalled = false;
    for(size_t i = 0 ; i < requirementsLen ; ++i){
        PythonRequirementCtx* req;
        if(!withFullReqs){
            const char* reqName = RedisGears_BRReadString(br);
            if(!sessionExists){
                req = PythonRequirementCtx_Get(reqName);
                if(!req){
                    RedisGears_ASprintf(err, "session missing requirement (%s)", reqName);
                    PythonSessionCtx_Free(s);
                    return NULL;
                }
            }
        }else{
            int reqVersion = 0;
            if (version >= PY_SESSION_TYPE_WITH_REQ_NAMES_ONLY) {
                version = RedisGears_BRReadLong(br);
            }
            req = PythonRequirementCtx_Deserialize(br, version, err);
            if(!req){
                if(!*err){
                    *err = RG_STRDUP("Failed deserializing requirement");
                }
                PythonSessionCtx_Free(s);
                return NULL;
            }

            if (!req->isInstalled) {
                requirementsInstalled = true;
                if(!PythonRequirementCtx_InstallRequirement(req)){
                    PythonSessionCtx_Free(s);
                    *err = RG_STRDUP("Failed installing requirements on shard, please check shard logs for more details.");
                    return NULL;
                }
            }
        }

        if(!sessionExists){
            s->requirements = array_append(s->requirements , req);
        }
    }
    if (requirementsInstalled) {
        s->isInstallationNeeded = true;
        PythosSessionCtx_ResetModulesDict();
    }
    return s;
}

static void* PythonSessionCtx_DeserializeWithFep(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    return PythonSessionCtx_Deserialize(br, version, err, true);
}

static void PythonSessionCtx_OnFepDeserialized(FlatExecutionPlan *fep){
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    const char *id = RedisGears_FepGetId(fep);
    PythonSessionCtx_AddFep(sctx, RG_STRDUP(id));
}

static char* PythonSessionCtx_ToString(void* arg){
    PythonSessionCtx* s = arg;
    char* depsListStr = RedisGears_ArrToStr((void**)s->requirements, array_len(s->requirements), PythonRequirementCtx_ToStr, ',');
    char* registrationsListStr = NULL;
    PythonSessionCtx_RunWithRegistrationsLock(s, {
            registrationsListStr = RedisGears_ArrToStr((void**)s->registrations, array_len(s->registrations), PythonStringArray_ToStr, ',');
    });
    char* ret;
    RedisGears_ASprintf(&ret, "{'sessionName':'%s',"
                              " 'sessionDescription':'%s',"
                              " 'refCount': %d,"
                              " 'linkedTo': %s,"
                              " 'dead': %s,"
                              " 'isInstallationNeeded':%d,"
                              " 'registrationsList':%s,"
                              " 'depsList':%s}",
                              s->sessionId,
                              s->sessionDesc ? s->sessionDesc : "null",
                              s->refCount,
                              s->linkedTo == LinkedTo_None? "None" : s->linkedTo == LinkedTo_PrimaryDict ? "primary" : "temprary",
                              s->deadNode ? "true" : "false",
                              s->isInstallationNeeded,
                              registrationsListStr,
                              depsListStr);
    RG_FREE(depsListStr);
    RG_FREE(registrationsListStr);
    return ret;
}

static char* PythonSessionCtx_ToStringWithFep(FlatExecutionPlan* fep, void* arg){
    return PythonSessionCtx_ToString(arg);
}

static void RedisGearsPy_OnRegistered(FlatExecutionPlan* fep, void* arg){
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    RedisModule_Assert(sctx);

    PyObject* callback = arg;

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, NULL);
    RedisGearsPy_Lock(&pectx);

    RedisModule_Assert(PyFunction_Check(callback));

    PyObject* pArgs = PyTuple_New(0);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    if(!ret){
        char* error = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error occured on RedisGearsPy_OnRegistered, error='%s'", error);
        RG_FREE(error);
        RedisGearsPy_Unlock(&pectx);
        return;
    }

    if(PyCoro_CheckExact(ret)){
        GearsPyDecRef(ret);
        RedisModule_Log(staticCtx, "warning", "Error occured on RedisGearsPy_OnRegistered, error='Coroutines are not allow'");
        RedisGearsPy_Unlock(&pectx);
        return;
    }

    if(ret != Py_None){
        GearsPyDecRef(ret);
    }

    RedisGearsPy_Unlock(&pectx);

}

static void RedisGearsPy_ForceStop(void* threadID){
    RedisGearsPy_LOCK
    PyThreadState_SetAsyncExc((unsigned long)threadID, ForceStoppedError);
    RedisGearsPy_UNLOCK
}

static void RedisGearsPy_OnExecutionUnpausedCallback(ExecutionCtx* ctx, void* arg){
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(ctx);
    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, NULL);
    RedisGearsPy_Lock(&pectx);
    PyThreadState *state = PyGILState_GetThisThreadState();
    RedisGears_SetAbortCallback(ctx, RedisGearsPy_ForceStop, (void*)state->thread_id);
    RedisGearsPy_Unlock(&pectx);
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
                .flags = 0,
                .crtCtx = NULL,
                .commandCtx = NULL,
                .pyfutureCreated = NULL,
        };
        pthread_setspecific(pythonThreadCtxKey, ptctx);
    }
    return ptctx;
}

bool RedisGearsPy_IsLockAcquired(){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    return ptctx->lockCounter > 0;
}

void RedisGearsPy_Lock(PythonExecutionCtx* pectx){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    PythonSessionCtx* oldSession = ptctx->currSession;
    ExecutionCtx* oldEctx = ptctx->currEctx;
    pectx->pyfutureCreated = ptctx->pyfutureCreated;

    ptctx->currSession = pectx->sCtx;
    ptctx->currEctx = pectx->eCtx;
    ptctx->pyfutureCreated = NULL;

    pectx->sCtx = oldSession;
    pectx->eCtx = oldEctx;

    if(ptctx->lockCounter == 0){
        PyGILState_STATE oldState = PyGILState_Ensure();
        RedisModule_Assert(oldState == PyGILState_UNLOCKED);
    }
    ++ptctx->lockCounter;
}

void RedisGearsPy_Unlock(PythonExecutionCtx* oldectx){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    RedisModule_Assert(ptctx);
    RedisModule_Assert(ptctx->lockCounter > 0);
    ptctx->currSession = oldectx->sCtx;
    ptctx->currEctx = oldectx->eCtx;
    if(ptctx->pyfutureCreated){
        GearsPyDecRef(ptctx->pyfutureCreated);
    }
    ptctx->pyfutureCreated = oldectx->pyfutureCreated;
    if(--ptctx->lockCounter == 0){
        RedisModule_Assert(!ptctx->currSession);
        PyGILState_Release(PyGILState_UNLOCKED);
    }
}

typedef struct PyExecutionSession{
    PyObject_HEAD
    PythonSessionCtx* s;
    CommandReaderTriggerCtx* crtCtx;
    CommandCtx* cmdCtx;
}PyExecutionSession;


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
    RedisGears_ReturnResultsAndErrors(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void dropExecutionOnDone(ExecutionPlan* ep, void* privateData){
    RedisGears_DropExecution(ep);
}

static void continueFutureOnDone(ExecutionPlan* ep, void* privateData){
    // create the results lists
    FlatExecutionPlan* fep = RedisGears_GetFep(ep);
    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, NULL);
    RedisGearsPy_Lock(&pectx);

    PyObject* future = privateData;
    PyObject* l = PyList_New(0);
    PyObject* results = PyList_New(0);
    PyObject* errs = PyList_New(0);

    PyList_Append(l, results);
    PyList_Append(l, errs);

    for(size_t i = 0 ; i < RedisGears_GetRecordsLen(ep) ; ++i){
        Record* res = RedisGears_GetRecord(ep, i);
        RedisModule_Assert(RedisGears_RecordGetType(res) == pythonRecordType);
        PythonRecord* pyRec = (PythonRecord*)res;
        PyList_Append(results, pyRec->obj);
    }

    for(size_t i = 0 ; i < RedisGears_GetErrorsLen(ep) ; ++i){
        Record* err = RedisGears_GetError(ep, i);
        RedisModule_Assert(RedisGears_RecordGetType(err) == RedisGears_GetErrorRecordType());
        size_t len;
        const char* errStr = RedisGears_StringRecordGet(err, &len);
        PyObject* pyErr = PyUnicode_FromStringAndSize(errStr, len);
        PyList_Append(errs, pyErr);
        GearsPyDecRef(pyErr);
    }

    GearsPyDecRef(results);
    GearsPyDecRef(errs);

    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, future);
    PyTuple_SetItem(pArgs, 1, l);
    PyObject* r = PyObject_CallObject(setFutureResultsFunction, pArgs);
    GearsPyDecRef(pArgs);
    if(!r){
        char* err = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error happened when releasing execution future, error='%s'", err);
        RG_FREE(err);
    }else{
        GearsPyDecRef(r);
    }

    RedisGearsPy_Unlock(&pectx);
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

    if(ptctx->currentCtx && ptctx->createdExecution){
        // we have currentCtx, which means we run inside rg.pyexecute command.
        // it is not possible to run 2 executions in single rg.pyexecute command.
        PyErr_SetString(GearsError, "Can not run more then 1 executions in a single script");
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

    PyObject* pymode = GearsPyDict_GetItemString(kargs, "mode");
    ExecutionMode mode = ExecutionModeAsync;
    if(pymode){
        if(PyUnicode_Check(pymode)){
            const char* modeStr = PyUnicode_AsUTF8AndSize(pymode, NULL);
            if(strcmp(modeStr, "async") == 0){
                mode = ExecutionModeAsync;
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

    if(arg == NULL){
        return NULL;
    }

    char* err = NULL;
    PyObject* res = NULL;

    RedisGears_OnExecutionDoneCallback onDoneCallback = (!ptctx->currentCtx) ? dropExecutionOnDone : NULL;

    ExecutionPlan* ep = RGM_Run(pfep->fep, mode, arg, onDoneCallback, NULL, &err);
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
            GearsPyDecRef((PyObject*)arg);
        }else{
            RedisModule_Log(staticCtx, "warning", "unknown reader when try to free reader args");
            RedisModule_Assert(false);
        }
        goto end;
    }

    if(ptctx->currentCtx){
        ptctx->createdExecution = ep;
    }

    PyObject* pArgs = PyTuple_New(0);
    PyObject* future = PyObject_CallObject(createFutureFunction, pArgs);
    GearsPyDecRef(pArgs);

    RedisGears_AddOnDoneCallback(ep, continueFutureOnDone, future);
    Py_INCREF(future);
    res = future;

end:
    return res;
}

static int registerStrKeyTypeToInt(const char* keyType){
    if(strcmp(keyType, "empty") == 0){
        return REDISMODULE_KEYTYPE_EMPTY;
    }
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
    if(strcmp(keyType, "stream") == 0){
        return REDISMODULE_KEYTYPE_STREAM;
    }
    return -1;
}

static void* registerCreateKeysArgs(PyObject *kargs, const char* prefix, ExecutionMode mode){
    Arr(char*) eventTypes = NULL;
    Arr(int) keyTypes = NULL;
    Arr(char*) hookCommands = NULL;

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
                GearsPyDecRef(eventTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                PyErr_SetString(GearsError, "given event type is not string");
                return NULL;
            }
            const char* eventTypeStr = PyUnicode_AsUTF8AndSize(event, NULL);
            char* eventTypeStr1 = RG_STRDUP(eventTypeStr);
            eventTypes = array_append(eventTypes, eventTypeStr1);
        }
        GearsPyDecRef(eventTypesIterator);
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
                GearsPyDecRef(keyTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                array_free(keyTypes);
                PyErr_SetString(GearsError, "given key type is not string");
                return NULL;
            }
            const char* keyTypeStr = PyUnicode_AsUTF8AndSize(keyType, NULL);
            int keyTypeInt = registerStrKeyTypeToInt(keyTypeStr);
            if(keyTypeInt == -1){
                GearsPyDecRef(keyTypesIterator);
                array_free_ex(eventTypes, RG_FREE(*(Arr(char*))ptr));
                array_free(keyTypes);
                PyErr_SetString(GearsError, "unknown key type");
                return NULL;
            }
            keyTypes = array_append(keyTypes, keyTypeInt);
        }
        GearsPyDecRef(keyTypesIterator);
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

    // getting hook commands
    PyObject* pyHookCommands = GearsPyDict_GetItemString(kargs, "commands");
    if(pyHookCommands && pyHookCommands != Py_None){
        PyObject* hookCommandsIterator = PyObject_GetIter(pyHookCommands);
        if(!hookCommandsIterator){
            PyErr_SetString(GearsError, "given commands is not iterable");
            return NULL;
        }
        hookCommands = array_new(char*, 10);
        PyObject* hook = NULL;
        while((hook = PyIter_Next(hookCommandsIterator))){
            if(!PyUnicode_Check(hook)){
                GearsPyDecRef(hookCommandsIterator);
                array_free_ex(hookCommands, RG_FREE(*(Arr(char*))ptr));
                PyErr_SetString(GearsError, "given command type is not string");
                return NULL;
            }
            const char* hookCommandStr = PyUnicode_AsUTF8AndSize(hook, NULL);
            char* hookCommandStr1 = RG_STRDUP(hookCommandStr);
            hookCommands = array_append(hookCommands, hookCommandStr1);
        }
        GearsPyDecRef(hookCommandsIterator);

        if(array_len(hookCommands) == 0){
            array_free(hookCommands);
            hookCommands = NULL;
        }
    }

    KeysReaderTriggerArgs* args = RedisGears_KeysReaderTriggerArgsCreate(prefix, eventTypes, keyTypes, readValue);

    if(hookCommands){
        RedisGears_KeysReaderTriggerArgsSetHookCommands(args, hookCommands);
    }

    return args;
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
    const char* keyPrefix = NULL;
    const char* hook = NULL;
    int inOrder = 0;
    PyObject* pyTrigger = GearsPyDict_GetItemString(kargs, "trigger");
    if(pyTrigger){
        if(!PyUnicode_Check(pyTrigger)){
            PyErr_SetString(GearsError, "trigger argument is not string");
            return NULL;
        }
        trigger = PyUnicode_AsUTF8AndSize(pyTrigger, NULL);
    }

    PyObject* pyInOrder = GearsPyDict_GetItemString(kargs, "inorder");
    if(pyInOrder){
        if(!PyBool_Check(pyInOrder)){
            PyErr_SetString(GearsError, "inorder argument is be boolean");
            return NULL;
        }
        inOrder = Py_True == pyInOrder ? 1 : 0;
    }

    PyObject* PykeyPrefix = GearsPyDict_GetItemString(kargs, "keyprefix");
    if(PykeyPrefix){
        if(!PyUnicode_Check(PykeyPrefix)){
            PyErr_SetString(GearsError, "keyprefix argument is not string");
            return NULL;
        }
        keyPrefix = PyUnicode_AsUTF8AndSize(PykeyPrefix, NULL);
    }

    PyObject* PyHook = GearsPyDict_GetItemString(kargs, "hook");
    if(PyHook){
        if(!PyUnicode_Check(PyHook)){
            PyErr_SetString(GearsError, "hook argument is not string");
            return NULL;
        }
        hook = PyUnicode_AsUTF8AndSize(PyHook, NULL);
    }

    if(trigger){
        if(keyPrefix || hook){
            PyErr_SetString(GearsError, "trigger argument can not be given with keyprefix or hook");
            return NULL;
        }
        return RedisGears_CommandReaderTriggerArgsCreate(trigger, inOrder);
    }else{
        if(!hook){
            PyErr_SetString(GearsError, "no trigger or hook argument was given");
            return NULL;
        }
        return RedisGears_CommandReaderTriggerArgsCreateHook(hook, keyPrefix, inOrder);
    }

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
            GearsPyDecRef(onRegistered);
            return NULL;
        }
    }

    void* executionArgs = registerCreateArgs(pfep->fep, kargs, mode);
    if(executionArgs == NULL){
        return NULL;
    }

    char *err = NULL;
    char *registrationId = NULL;
    int status;
    if (ptctx->currSession->srctx) {
        status = RedisGears_PrepareForRegister(ptctx->currSession->srctx, pfep->fep, mode, executionArgs, &err, &registrationId);
    } else {
        // we do not have session registration ctx, we will register and stand alone registration.
        status = RedisGears_RegisterFep(pluginCtx, pfep->fep, mode, executionArgs, &err, &registrationId);
    }
    if(!status){
        if(err){
            PyErr_SetString(GearsError, err);
            RG_FREE(err);
        }else{
            PyErr_SetString(GearsError, "Failed register execution");
        }
        return NULL;
    }

    PythonSessionCtx_AddFep(ptctx->currSession, registrationId);

    PyObject *ret = PyUnicode_FromString(registrationId);

    return ret;
}

#define ContinueType_Default 1
#define ContinueType_Filter 2
#define ContinueType_Flat 3
#define ContinueType_Foreach 4

typedef struct PyFuture{
   PyObject_HEAD
   Record* asyncRecord;
   int continueType;
} PyFuture;

static PyObject* futureFailed(PyObject *self, PyObject *args){
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Continue function expect an object");
        return NULL;
    }

    PyFuture* pyFuture = (PyFuture*)self;
    if(!pyFuture->asyncRecord){
        PyErr_SetString(GearsError, "Can not continue with the same future twice");
        return NULL;
    }

    PyObject* obj = PyTuple_GetItem(args, 0);
    PyObject* argumentStr = PyObject_Str(obj);
    size_t len;
    const char* argumentCStr = PyUnicode_AsUTF8AndSize(argumentStr, &len);
    Record* error = RedisGears_ErrorRecordCreate(RG_STRDUP(argumentCStr), len);

    RedisGears_AsyncRecordContinue(pyFuture->asyncRecord, error);
    pyFuture->asyncRecord = NULL;

    GearsPyDecRef(argumentStr);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* futureContinue(PyObject *self, PyObject *args){
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Continue function expect an object");
        return NULL;
    }

    PyFuture* pyFuture = (PyFuture*)self;
    if(!pyFuture->asyncRecord){
        PyErr_SetString(GearsError, "Can not continue with the same future twice");
        return NULL;
    }

    Record* record = NULL;

    PyObject* obj = PyTuple_GetItem(args, 0);
    switch(pyFuture->continueType){
    case ContinueType_Default:
        Py_INCREF(obj);
        record = PyObjRecordCreate();
        PyObjRecordSet(record, obj);
        break;
    case ContinueType_Filter:
        if(PyObject_IsTrue(obj)){
            record = RedisGears_GetDummyRecord(); // everithing other then NULL will be true;
        }
        break;
    case ContinueType_Flat:
        if(PyList_Check(obj)){
            size_t len = PyList_Size(obj);
            record = RedisGears_ListRecordCreate(len);
            for(size_t i = 0 ; i < len ; ++i){
                PyObject* temp = PyList_GetItem(obj, i);
                Record* pyRecord = PyObjRecordCreate();
                Py_INCREF(temp);
                PyObjRecordSet(pyRecord, temp);
                RedisGears_ListRecordAdd(record, pyRecord);
            }
        }else{
            // just a normal pyobject
            Py_INCREF(obj);
            record = PyObjRecordCreate();
            PyObjRecordSet(record, obj);
        }
        break;
    case ContinueType_Foreach:
        record = RedisGears_GetDummyRecord(); // continue with the old record
        break;
    default:
        PyErr_SetString(GearsError, "Can not handle future untill it returned from the callback");
        return NULL;
        break;
    }

    RedisGears_AsyncRecordContinue(pyFuture->asyncRecord, record);
    pyFuture->asyncRecord = NULL;

    Py_INCREF(Py_None);
    return Py_None;
}

typedef struct PyAtomic{
   PyObject_HEAD
   RedisModuleCtx* ctx;
} PyAtomic;

static PyObject* atomicEnter(PyObject *self, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    if(ptctx->flags & PythonThreadCtxFlag_InsideAtomic){
        PyErr_SetString(GearsError, "Already inside an atomic block");
        return NULL;
    }
    ptctx->flags |= PythonThreadCtxFlag_InsideAtomic;
    PyAtomic* pyAtomic = (PyAtomic*)self;
    RedisGears_LockHanlderAcquire(pyAtomic->ctx);
    if (redisVersion->redisMajorVersion < 7) {
        /* Before Redis 7 we need to manually wrap the atomic
         * execution with multi exec to make sure the replica
         * will also perform the commands atomically.
         * On Redis 7 and above, thanks to this PR:
         * https://github.com/redis/redis/pull/9890
         * It is not needed anymore. */
        RedisModule_Replicate(pyAtomic->ctx, "multi", "");
    }
    Py_INCREF(self);
    return self;
}

static PyObject* atomicExit(PyObject *self, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    if(!(ptctx->flags & PythonThreadCtxFlag_InsideAtomic)){
        PyErr_SetString(GearsError, "Not inside an atomic block");
        return NULL;
    }
    ptctx->flags &= ~PythonThreadCtxFlag_InsideAtomic;
    PyAtomic* pyAtomic = (PyAtomic*)self;
    if (redisVersion->redisMajorVersion < 7) {
        /* see comment on atomicEnter */
        RedisModule_Replicate(pyAtomic->ctx, "exec", "");
    }
    RedisGears_LockHanlderRelease(pyAtomic->ctx);
    Py_INCREF(Py_None);
    return Py_None;
}

// LCOV_EXCL_START

typedef struct PyTensor{
    PyObject_HEAD
    RAI_Tensor* t;
} PyTensor;

static PyObject *PyTensor_ToFlatList(PyTensor * pyt){
    int ndims = RedisAI_TensorNumDims(pyt->t);
    PyObject* flatList = PyList_New(0);
    long long len = 0;
    long long totalElements = 1;
    for(int i = 0 ; i < ndims ; ++i){
        totalElements *= RedisAI_TensorDim(pyt->t, i);
    }
    for(long long j = 0 ; j < totalElements ; ++j){
        PyObject *pyVal = NULL;
        double doubleVal;
        long long longVal;
        if(RedisAI_TensorGetValueAsDouble(pyt->t, j, &doubleVal)){
            pyVal = PyFloat_FromDouble(doubleVal);
        }else if(RedisAI_TensorGetValueAsLongLong(pyt->t, j, &longVal)){
            pyVal = PyLong_FromLongLong(longVal);
        }else{
            PyErr_SetString(GearsError, "Failed converting tensor to flat list");
            GearsPyDecRef(flatList);
            return NULL;
        }
        PyList_Append(flatList, pyVal);
        GearsPyDecRef(pyVal);
    }
    return flatList;
}

static PyObject *PyTensor_ToStr(PyObject * pyObj){
    PyTensor* pyt = (PyTensor*)pyObj;
    PyObject* flatList = PyTensor_ToFlatList(pyt);
    if(!flatList){
        return NULL;
    }
    return PyObject_Repr(flatList);
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

typedef struct PyDAGRunner{
    PyObject_HEAD
    RAI_DAGRunCtx* dag;
} PyDAGRunner;

static PyObject* PyDAG_ToStr(PyObject *obj){
    return PyUnicode_FromString("PyDAGRunner to str");
}

static void PyDAGRunner_Destruct(PyObject *pyObj){
    PyDAGRunner* pyDag = (PyDAGRunner*)pyObj;
    if (pyDag->dag) RedisAI_DAGFree(pyDag->dag);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static PyTypeObject PyDAGRunnerType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "redisgears.PyDAGRunner",             /* tp_name */
  sizeof(PyDAGRunner), /* tp_basicsize */
  0,                         /* tp_itemsize */
  PyDAGRunner_Destruct,    /* tp_dealloc */
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
  PyDAG_ToStr,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "PyDAGRunner",           /* tp_doc */
};

static bool _IsDagAPISupported(void *DagAPIFunc) {
    if (!RMAPI_FUNC_SUPPORTED(RedisAI_DAGRunCtxCreate)) {
        PyErr_SetString(GearsError, "DAG run is not supported in RedisAI version");
        return false;
    }
    return true;
}

static PyObject* createDAGRunner(PyObject *cls, PyObject *args) {
    verifyRedisAILoaded();
    if (!_IsDagAPISupported(RedisAI_DAGRunCtxCreate)) {
        return NULL;
    }
    RAI_DAGRunCtx* dagCtx = RedisAI_DAGRunCtxCreate();
    PyDAGRunner* pyDag = PyObject_New(PyDAGRunner, &PyDAGRunnerType);
    pyDag->dag = dagCtx;

    return (PyObject*) pyDag;
}
/*
 * Methods for RedisAI DAG runner
 */

bool _IsValidDag(PyDAGRunner *pyDag) {

    if(!PyObject_IsInstance((PyObject*)pyDag, (PyObject*)&PyDAGRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type DAGRunner");
        return false;
    }
    if(pyDag->dag == NULL){
        PyErr_SetString(GearsError, "PyDAGRunner is invalid");
        return false;
    }
    return true;
}

typedef int (*RAI_LoadTensorFunc)(RAI_DAGRunCtx *run_info, const char *t_name,
  RAI_Tensor *tensor);

static PyObject* _loadTensorToDAG(PyObject *self, PyObject *args, RAI_LoadTensorFunc loadTensor) {
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(loadTensor) || !_IsValidDag(pyDag)) {
        return NULL;
    }
    if(PyTuple_Size(args) != 2) {
        PyErr_SetString(GearsError, "Wrong number of args to DAG load input");
        return NULL;
    }
    PyObject* tensorName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(tensorName)) {
        PyErr_SetString(GearsError, "Tensor name argument must be a string");
        return NULL;
    }
    const char* inputNameStr = PyUnicode_AsUTF8AndSize(tensorName, NULL);
    PyTensor* pyt = (PyTensor*) PyTuple_GetItem(args, 1);
    if(!PyObject_IsInstance((PyObject*) pyt, (PyObject * ) & PyTensorType)) {
        PyErr_SetString(GearsError,
          "Given argument is not of type PyTensorType");
        return NULL;
    }
    loadTensor(pyDag->dag, inputNameStr, pyt->t);
    Py_INCREF(self);
    return self;
}


static PyObject* DAGAddInput(PyObject *self, PyObject *args) {
    return _loadTensorToDAG(self, args, RedisAI_DAGLoadTensor);
}

static PyObject* DAGAddTensorSet(PyObject *self, PyObject *args) {
    return _loadTensorToDAG(self, args, RedisAI_DAGAddTensorSet);
}

static PyObject* DAGAddTensorGet(PyObject *self, PyObject *args) {
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(RedisAI_DAGAddTensorGet) || !_IsValidDag(pyDag)) {
        return NULL;
    }
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of args to TensorSet op");
        return NULL;
    }
    PyObject* tensorName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(tensorName)){
        PyErr_SetString(GearsError, "Tensor name argument must be a string");
        return NULL;
    }

    const char* inputNameStr = PyUnicode_AsUTF8AndSize(tensorName, NULL);
    RedisAI_DAGAddTensorGet(pyDag->dag, inputNameStr);
    Py_INCREF(self);
    return self;
}

static RAI_DAGRunOp *_createModelRunOp(const char *modelNameStr, RAI_Error *err) {

    RedisGears_LockHanlderAcquire(staticCtx);
    RedisModuleString* keyRedisStr = RedisModule_CreateString(staticCtx, modelNameStr, strlen(modelNameStr));
    RAI_Model *model;
    if(RedisAI_GetModelFromKeyspace(staticCtx, keyRedisStr, &model, REDISMODULE_READ, err) != REDISMODULE_OK) {
        RedisModule_FreeString(staticCtx, keyRedisStr);
        RedisGears_LockHanlderRelease(staticCtx);
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        return NULL;
    }

    RedisModule_FreeString(staticCtx, keyRedisStr);
    RAI_DAGRunOp *modelRunOp = RedisAI_DAGCreateModelRunOp(model);
    RedisGears_LockHanlderRelease(staticCtx);
    return modelRunOp;
}

static PyObject* DAGAddModelRun(PyObject *self, PyObject *args, PyObject *kargs) {
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(RedisAI_DAGCreateModelRunOp) || !_IsValidDag(pyDag)) {
        return NULL;
    }
    PyObject* modelName = GearsPyDict_GetItemString(kargs, "name");
    if (!modelName) {
        PyErr_SetString(GearsError, "Model key name was not given");
        return NULL;
    }
    if(!PyUnicode_Check(modelName)){
        PyErr_SetString(GearsError, "Model name argument must be a string");
        return NULL;
    }

    RAI_Error *err;
    RedisAI_InitError(&err);
    const char* modelNameStr = PyUnicode_AsUTF8AndSize(modelName, NULL);

    // Create MODELRUN op after bringing model from keyspace (raise an exception if it does not exist)
    RAI_DAGRunOp *modelRunOp = _createModelRunOp(modelNameStr, err);
    if (modelRunOp == NULL) {
        RedisAI_FreeError(err);
        return NULL;
    }
    PyObject *ret = NULL;

    // Add to the modelRun op with its inputs and output keys and insert it to the DAG
    PyObject *inputsIter = NULL;
    PyObject *outputsIter = NULL;
    PyObject *modelOutputs = NULL;
    PyObject *modelInputs = GearsPyDict_GetItemString(kargs, "inputs");
    if (!modelInputs) {
        PyErr_SetString(GearsError, "Must specify model inputs");
        goto cleanup;
    }
    inputsIter = PyObject_GetIter(modelInputs);
    if (!inputsIter) {
        PyErr_SetString(GearsError, "Model inputs must be iterable");
        goto cleanup;
    }
    PyObject* input;
    while((input = (PyObject*)PyIter_Next(inputsIter)) != NULL) {
        if(!PyUnicode_Check(input)){
            PyErr_SetString(GearsError, "Input name must be a string");
            goto cleanup;
        }
        const char* inputStr = PyUnicode_AsUTF8AndSize(input, NULL);
        RedisAI_DAGRunOpAddInput(modelRunOp, inputStr);
    }

    modelOutputs = GearsPyDict_GetItemString(kargs, "outputs");
    if (!modelOutputs) {
        PyErr_SetString(GearsError, "Must specify model outputs");
        goto cleanup;
    }
    outputsIter = PyObject_GetIter(modelOutputs);
    if (!outputsIter) {
        PyErr_SetString(GearsError, "Model outputs must be iterable");
        goto cleanup;
    }
    PyObject* output;
    while((output = (PyObject*)PyIter_Next(outputsIter)) != NULL) {
        if(!PyUnicode_Check(output)){
            PyErr_SetString(GearsError, "Output name must be a string");
            goto cleanup;
        }
        const char* outputStr = PyUnicode_AsUTF8AndSize(output, NULL);
        RedisAI_DAGRunOpAddOutput(modelRunOp, outputStr);
    }
    if (RedisAI_DAGAddRunOp(pyDag->dag, modelRunOp, err) != REDISMODULE_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        goto cleanup;
    }
    Py_INCREF(self);
    ret = self;

    cleanup:
    RedisAI_FreeError(err);
    if (modelInputs) GearsPyDecRef(modelInputs);
    if (modelOutputs) GearsPyDecRef(modelOutputs);
    if (inputsIter) GearsPyDecRef(inputsIter);
    if (outputsIter) GearsPyDecRef(outputsIter);
    if (!ret) RedisAI_DAGRunOpFree(modelRunOp);
    return ret;
}

static RAI_DAGRunOp *_createScriptRunOp(const char *scriptNameStr, const char *functionNameStr,
  RAI_Error *err) {

    RedisGears_LockHanlderAcquire(staticCtx);
    RedisModuleString* keyRedisStr = RedisModule_CreateString(staticCtx, scriptNameStr, strlen(scriptNameStr));
    RAI_Script *script;
    if(RedisAI_GetScriptFromKeyspace(staticCtx, keyRedisStr, &script, REDISMODULE_READ, err) != REDISMODULE_OK) {
        RedisModule_FreeString(staticCtx, keyRedisStr);
        RedisGears_LockHanlderRelease(staticCtx);
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        return NULL;
    }

    RedisModule_FreeString(staticCtx, keyRedisStr);
    RAI_DAGRunOp *scriptRunOp = RedisAI_DAGCreateScriptRunOp(script, functionNameStr);
    RedisGears_LockHanlderRelease(staticCtx);
    return scriptRunOp;
}

static PyObject* DAGAddScriptRun(PyObject *self, PyObject *args, PyObject *kargs) {
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(RedisAI_DAGCreateScriptRunOp) || !_IsValidDag(pyDag)) {
        return NULL;
    }

    PyObject* scriptName = GearsPyDict_GetItemString(kargs, "name");
    if (!scriptName) {
        PyErr_SetString(GearsError, "Script key name was not given");
        return NULL;
    }
    if(!PyUnicode_Check(scriptName)){
        PyErr_SetString(GearsError, "Script name argument must be a string");
        return NULL;
    }
    PyObject* funcName = GearsPyDict_GetItemString(kargs, "func");
    if (!funcName) {
        PyErr_SetString(GearsError, "Function name was not given");
        return NULL;
    }
    if(!PyUnicode_Check(funcName)){
        PyErr_SetString(GearsError, "Function name argument must be a string");
        return NULL;
    }
    const char* scriptNameStr = PyUnicode_AsUTF8AndSize(scriptName, NULL);
    const char* functionNameStr = PyUnicode_AsUTF8AndSize(funcName, NULL);

    // Create SCRIPTRUN op after bringing script from keyspace (raise an exception if it does not exist)
    RAI_Error *err;
    RedisAI_InitError(&err);
    RAI_DAGRunOp *scriptRunOp = _createScriptRunOp(scriptNameStr, functionNameStr, err);
    if (scriptRunOp == NULL) {
        RedisAI_FreeError(err);
        return NULL;
    }
    PyObject *ret = NULL;

    // Add to the scriptRun op with its inputs and output keys and insert it to the DAG
    PyObject* scriptInputs = GearsPyDict_GetItemString(kargs, "inputs");
    PyObject* inputsIter = NULL;
    PyObject* scriptOutputs = NULL;
    PyObject* outputsIter = NULL;
    if (scriptInputs) {
        inputsIter = PyObject_GetIter(scriptInputs);
        if(!inputsIter) {
            PyErr_SetString(GearsError, "Script inputs must be iterable");
            goto cleanup;
        }
        PyObject* input;
        while((input = (PyObject*)PyIter_Next(inputsIter)) != NULL) {
            if(!PyUnicode_Check(input)){
                PyErr_SetString(GearsError, "Input name must be a string");
                goto cleanup;
            }
            const char* inputStr = PyUnicode_AsUTF8AndSize(input, NULL);
            RedisAI_DAGRunOpAddInput(scriptRunOp, inputStr);
        }
    }
    scriptOutputs = GearsPyDict_GetItemString(kargs, "outputs");
    if (scriptOutputs) {
        outputsIter = PyObject_GetIter(scriptOutputs);
        if(!outputsIter) {
            PyErr_SetString(GearsError, "Script outputs must be iterable");
            goto cleanup;
        }
        PyObject* output;
        while((output = (PyObject*)PyIter_Next(outputsIter)) != NULL) {
            if(!PyUnicode_Check(output)){
                PyErr_SetString(GearsError, "Output name must be a string");
                goto cleanup;
            }
            const char* outputStr = PyUnicode_AsUTF8AndSize(output, NULL);
            RedisAI_DAGRunOpAddOutput(scriptRunOp, outputStr);
        }
    }
    RedisAI_DAGAddRunOp(pyDag->dag, scriptRunOp, err);
    ret = self;
    Py_INCREF(self);

    cleanup:
    RedisAI_FreeError(err);
    if (scriptInputs) GearsPyDecRef(scriptInputs);
    if (scriptOutputs) GearsPyDecRef(scriptOutputs);
    if (inputsIter) GearsPyDecRef(inputsIter);
    if (outputsIter) GearsPyDecRef(outputsIter);
    if (!ret) RedisAI_DAGRunOpFree(scriptRunOp);
    return ret;
}

static PyObject* DAGAddOpsFromString(PyObject *self, PyObject *args) {
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(RedisAI_DAGAddOpsFromString) || !_IsValidDag(pyDag)) {
        return NULL;
    }

    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of args");
        return NULL;
    }
    PyObject* ops = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(ops)){
        PyErr_SetString(GearsError, "ops argument must be a string");
        return NULL;
    }
    const char* opsStr = PyUnicode_AsUTF8AndSize(ops, NULL);
    RAI_Error *err;
    RedisAI_InitError(&err);
    RedisGears_LockHanlderAcquire(staticCtx);
    if (RedisAI_DAGAddOpsFromString(pyDag->dag, opsStr, err) != REDISMODULE_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
        RedisGears_LockHanlderRelease(staticCtx);
        return NULL;
    }
    RedisAI_FreeError(err);
    RedisGears_LockHanlderRelease(staticCtx);
    Py_INCREF(self);
    return self;
}

static void FinishAsyncDAGRun(RAI_OnFinishCtx *onFinishCtx, void *private_data) {

    RedisGearsPy_LOCK
    PyObject* future = private_data;
    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, future);

    if (RedisAI_DAGRunError(onFinishCtx)) {
        PyObject* pyErr;
        const RAI_Error* err = RedisAI_DAGGetError(onFinishCtx);
        const char* errStr = (RedisAI_GetError((RAI_Error*)err));
        pyErr = PyUnicode_FromStringAndSize(errStr, strlen(errStr));

        PyTuple_SetItem(pArgs, 1, pyErr);
        PyObject_CallObject(setFutureExceptionFunction, pArgs);
        goto finish;
    }

    PyObject* tensorList = PyList_New(0);
    size_t n_outputs = RedisAI_DAGNumOutputs(onFinishCtx);
    for(size_t i = 0 ; i < n_outputs ; ++i){
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy((RAI_Tensor*)RedisAI_DAGOutputTensor(onFinishCtx, i));
        PyList_Append(tensorList, (PyObject*)pyt);
        GearsPyDecRef((PyObject*)pyt);
    }

    PyTuple_SetItem(pArgs, 1, tensorList);
    PyObject* r = PyObject_CallObject(setFutureResultsFunction, pArgs);
    if(!r){
        char* err = getPyError();
        RedisModule_Log(NULL, "warning", "Error happened when releasing execution future, error='%s'", err);
        RG_FREE(err);
    }else{
        GearsPyDecRef(r);
    }

    finish:
    RedisAI_DAGFree((RAI_DAGRunCtx *)onFinishCtx);
    GearsPyDecRef(pArgs);
    RedisGearsPy_UNLOCK
}

static PyObject* DAGRun(PyObject *self){
    verifyRedisAILoaded();
    PyDAGRunner *pyDag = (PyDAGRunner *)self;
    if(!_IsDagAPISupported(RedisAI_DAGRun) || !_IsValidDag(pyDag)) {
        return NULL;
    }
    PyObject* pArgs = PyTuple_New(0);
    PyObject* future = PyObject_CallObject(createFutureFunction, pArgs);
    GearsPyDecRef(pArgs);

    RAI_Error *err;
    RedisAI_InitError(&err);
    int status = RedisAI_DAGRun(pyDag->dag, FinishAsyncDAGRun, future, err);
    if (status == REDISMODULE_ERR) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        pyDag->dag = NULL;
        RedisAI_FreeError(err);
        return NULL;
    }
    pyDag->dag = NULL;
    RedisAI_FreeError(err);
    Py_INCREF(future);
    return future;
}

// LCOV_EXCL_STOP

static void PyAtomic_Destruct(PyObject *pyObj){
    PyAtomic* pyAtomic = (PyAtomic*)pyObj;
    RedisModule_FreeThreadSafeContext(pyAtomic->ctx);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

static void PyFuture_Destruct(PyObject *pyObj){
    PyFuture* pyFuture = (PyFuture*)pyObj;
    if(pyFuture->asyncRecord){
       RedisGears_FreeRecord(pyFuture->asyncRecord);
    }
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

PyMethodDef PyAtomicMethods[] = {
    {"__enter__", atomicEnter, METH_VARARGS, "acquire the GIL"},
    {"__exit__", atomicExit, METH_VARARGS, "release the GIL"},
    {NULL, NULL, 0, NULL}
};

PyMethodDef PyFutureMethods[] = {
    {"continueRun", futureContinue, METH_VARARGS, "Continue the execution after blocked"},
    {"continueFailed", futureFailed, METH_VARARGS, "Continue the execution after blocked"},
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

/* DAG runner operations */
PyMethodDef PyDAGRunnerMethods[] = {
  {"Input", DAGAddInput, METH_VARARGS, "load an input tensor to the DAG under a specific name"},
  {"TensorGet", DAGAddTensorGet, METH_VARARGS, "output a tensor within the DAG context having a specific name"},
  {"TensorSet", DAGAddTensorSet, METH_VARARGS, "load a tensor into the DAG context having a specific name"},
  {"ModelRun", (PyCFunction)DAGAddModelRun, METH_VARARGS|METH_KEYWORDS, "add a model run operation to the DAG"},
  {"ScriptRun", (PyCFunction)DAGAddScriptRun, METH_VARARGS|METH_KEYWORDS, "add a script run operation to the DAG"},
  {"OpsFromString", DAGAddOpsFromString, METH_VARARGS, "add operations to the DAG by using the redis DAGRUN command syntax"},
  {"Run", (PyCFunction)DAGRun, METH_VARARGS, "start the asynchronous execution of the DAG"},
  {NULL, NULL, 0, NULL}
};

static PyObject *PyExecutionSession_ToStr(PyObject * pyObj){
    return PyUnicode_FromString("PyExecutionSession");
}

static PyObject *PyFlatExecution_ToStr(PyObject * pyObj){
    return PyUnicode_FromString("PyFlatExecution");
}

static void PyExecutionSession_Destruct(PyObject *pyObj){
    PyExecutionSession* pyExSes = (PyExecutionSession*)pyObj;
    if(pyExSes->s){
        PythonSessionCtx_Free(pyExSes->s);
    }
    if(pyExSes->crtCtx){
        RedisGears_CommandReaderTriggerCtxFree(pyExSes->crtCtx);
    }
    if(pyExSes->cmdCtx){
        RedisGears_CommandCtxFree(pyExSes->cmdCtx);
    }
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
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

static PyTypeObject PyFutureType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.Future",       /* tp_name */
    sizeof(PyFuture),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyFuture_Destruct,         /* tp_dealloc */
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
    "PyFuture",                /* tp_doc */
};

static PyTypeObject PyExecutionSessionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyExecutionSession",       /* tp_name */
    sizeof(PyExecutionSession),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyExecutionSession_Destruct,  /* tp_dealloc */
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
    PyExecutionSession_ToStr,     /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyExecutionSession",         /* tp_doc */
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

static PyObject* gearsFutureCtx(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) != 0){
        PyErr_SetString(GearsError, "Future gets no arguments");
        return NULL;
    }

    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    if(!ptctx->currEctx){
        PyErr_SetString(GearsError, "Future object can only be created inside certain execution steps");
        return NULL;
    }

    if(ptctx->pyfutureCreated){
        PyErr_SetString(GearsError, "Can not create async record twice on the same step");
        return NULL;
    }

    char* err = NULL;
    Record *async = RedisGears_AsyncRecordCreate(ptctx->currEctx, &err);
    if(!async){
        if(!err){
            err = RG_STRDUP("Failed creating async record");
        }
        PyErr_SetString(GearsError, err);
        RG_FREE(err);
        return NULL;
    }

    PyFuture* pyfuture = PyObject_New(PyFuture, &PyFutureType);
    pyfuture->asyncRecord = async;
    pyfuture->continueType = 0;
    ptctx->pyfutureCreated = (PyObject*)pyfuture;
    Py_INCREF(ptctx->pyfutureCreated);
    return (PyObject*)pyfuture;
}

static PyObject* registerGearsThread(PyObject *cls, PyObject *args){
    RedisGears_LockHanlderRegister();
    Py_INCREF(Py_None);
    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    // called from a python thread which means that the lock counter must be 1.
    ptctx->lockCounter = 1;

    return Py_None;
}

static CommandCtx* getCommandCtx(PythonThreadCtx* ptctx){
    CommandCtx* commandCtx = NULL;
    if(ptctx->currEctx){
        commandCtx = RedisGears_CommandCtxGet(ptctx->currEctx);
    }else{
        commandCtx = ptctx->commandCtx;
    }

    return commandCtx;
}

static CommandReaderTriggerCtx* getCommandReaderTriggerCtx(PythonThreadCtx* ptctx){
    CommandReaderTriggerCtx* crtCtx = NULL;
    if(ptctx->currEctx){
        crtCtx = RedisGears_GetCommandReaderTriggerCtx(ptctx->currEctx);
    }else{
        crtCtx = ptctx->crtCtx;
    }

    return crtCtx;
}

static PyObject* getGearsSession(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyObject* res;
    if(!ptctx->currSession){
        Py_INCREF(Py_None);
        res = Py_None;
    }else{
        PyExecutionSession* pyExSes = PyObject_New(PyExecutionSession, &PyExecutionSessionType);
        pyExSes->s = PythonSessionCtx_ShallowCopy(ptctx->currSession);
        pyExSes->crtCtx = getCommandReaderTriggerCtx(ptctx);
        pyExSes->cmdCtx = getCommandCtx(ptctx);

        if(pyExSes->crtCtx){
            pyExSes->crtCtx = RedisGears_CommandReaderTriggerCtxGetShallowCopy(pyExSes->crtCtx);
        }

        if(pyExSes->cmdCtx){
           pyExSes->cmdCtx = RedisGears_CommandCtxGetShallowCopy(pyExSes->cmdCtx);
       }

        res = (PyObject*)pyExSes;
    }
    return res;
}

static PyObject* isInAtomicBlock(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyObject* res = (ptctx->flags & PythonThreadCtxFlag_InsideAtomic) ? Py_True : Py_False;
    Py_INCREF(res);
    return res;
}

static PyObject* setGearsSession(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of args given to setGearsSession");
        return NULL;
    }

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    PyObject* s = PyTuple_GetItem(args, 0);
    if(s == Py_None){
        ptctx->currSession = NULL;
    }else{
        if(!PyObject_IsInstance((PyObject*)s, (PyObject*)&PyExecutionSessionType)){
            PyErr_SetString(GearsError, "Given object is not of type GearsSession");
            return NULL;
        }

        PyExecutionSession* pyExSes = (PyExecutionSession*)s;
        ptctx->currSession = pyExSes->s;
        ptctx->crtCtx = pyExSes->crtCtx;
        ptctx->commandCtx = pyExSes->cmdCtx;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* gearsCtx(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    if(!ptctx->currSession){
        PyErr_SetString(GearsError, "Can not create a gearsCtx on a python created thread");
        return NULL;
    }
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
    char* err = NULL;
    pyfep->fep = RedisGears_CreateCtx((char*)readerStr, &err);
    if(!pyfep->fep){
        Py_DecRef((PyObject*)pyfep);
        if(!err){
            err = RG_STRDUP("the given reader are not exists");
        }
        PyErr_SetString(GearsError, err);
        RG_FREE(err);
        return NULL;
    }
    if(descStr){
        RedisGears_SetDesc(pyfep->fep, descStr);
    }
    RGM_Map(pyfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    RedisGears_SetFlatExecutionPrivateData(pyfep->fep, "PySessionType", PythonSessionCtx_ShallowCopy(ptctx->currSession));

    if(RGM_SetFlatExecutionOnUnpausedCallback(pyfep->fep, RedisGearsPy_OnExecutionUnpausedCallback, NULL) != REDISMODULE_OK){
        GearsPyDecRef((PyObject*)pyfep);
        PyErr_SetString(GearsError, "Failed setting on unpause callback");
        return NULL;
    }
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
            GearsPyDecRef(val);
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
    RedisGears_LockHanlderAcquire(ctx);
    const char* valCStr = RedisGears_GetConfig(keyCStr);
    if(!valCStr){
        RedisGears_LockHanlderRelease(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyObject* valPyStr = PyUnicode_FromString(valCStr);
    RedisGears_LockHanlderRelease(ctx);
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
        RedisModule_Log(staticCtx, "warning", "Specify log level as the first argument to log function is depricated, use key argument 'level' instead");
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

    RedisModule_Log(staticCtx, logLevelCStr, "GEARS: %s", logMsgCStr);
    Py_INCREF(Py_None);
    return Py_None;
}

static RedisModuleString** createArgs(PyObject *args){
    // Declare and initialize variables for arguments processing.
    size_t argLen;
    const char* argumentCStr = NULL;
    RedisModuleString* argumentRedisStr = NULL;
    RedisModuleString** arguments = array_new(RedisModuleString*, 10);
    for(int i = 0 ; i < PyTuple_Size(args) ; ++i){
        PyObject* argument = PyTuple_GetItem(args, i);
        if(PyByteArray_Check(argument)) {
            // Argument is bytearray.
            argLen = PyByteArray_Size(argument);
            argumentCStr = PyByteArray_AsString(argument);
            argumentRedisStr = RedisModule_CreateString(NULL, argumentCStr, argLen);
        } else if(PyBytes_Check(argument)) {
            // Argument is bytes.
            argLen = PyBytes_Size(argument);
            argumentCStr = PyBytes_AsString(argument);
            argumentRedisStr = RedisModule_CreateString(NULL, argumentCStr, argLen);
        } else {
            // Argument is string.
            PyObject* argumentStr = PyObject_Str(argument);
            argumentCStr = PyUnicode_AsUTF8AndSize(argumentStr, &argLen);
            argumentRedisStr = RedisModule_CreateString(NULL, argumentCStr, argLen);
            // Decrease ref-count after done processing the argument.
            GearsPyDecRef(argumentStr);
        }
        arguments = array_append(arguments, argumentRedisStr);
    }

    GearsPyDecRef(args);
    return arguments;
}

static PyObject* createReply(RedisModuleCallReply *reply){
    PyObject* res = NULL;
    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_UNKNOWN){
        if(errno){
            PyErr_SetString(GearsError, strerror(errno));
        }else{
            PyErr_SetString(GearsError, "Failed executing command");
        }
        res = NULL;
    }
    else if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
        PyErr_SetString(GearsError, replyStr);
    }else{
        res = replyToPyList(reply);
    }

    if(reply){
        RedisModule_FreeCallReply(reply);
    }

    return res;
}

static PyObject* executeCommand(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) < 1){
        return PyList_New(0);
    }
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(rctx);

    PyObject* command = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(command)){
        PyErr_SetString(GearsError, "the given command must be a string");
        RedisGears_LockHanlderRelease(rctx);
        RedisModule_FreeThreadSafeContext(rctx);
        return NULL;
    }
    const char* commandStr = PyUnicode_AsUTF8AndSize(command, NULL);

    RedisModuleString** arguments = createArgs(PyTuple_GetSlice(args, 1, PyTuple_Size(args)));


    RedisModuleCallReply *reply = RedisModule_Call(rctx, commandStr, "!v", arguments, array_len(arguments));

    PyObject* res = createReply(reply);

    array_free_ex(arguments, RedisModule_FreeString(rctx, *(RedisModuleString**)ptr));

    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return res;
}

// LCOV_EXCL_START

static PyObject* tensorGetDims(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to tensorGetDims");
        return NULL;
    }
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
        return NULL;
    }
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
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to tensorGetDataAsBlob");
        return NULL;
    }
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
        return NULL;
    }
    size_t size = RedisAI_TensorByteSize(pyt->t);
    char* data = RedisAI_TensorData(pyt->t);
    return PyByteArray_FromStringAndSize(data, size);
}

static PyObject* tensorToFlatList(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to tensorToFlatList");
        return NULL;
    }
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
        return NULL;
    }
    return PyTensor_ToFlatList(pyt);
}

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
    if(PyTuple_Size(args) != 3){
        PyErr_SetString(GearsError, "Wrong number of arguments given to createTensorFromBlob");
        return NULL;
    }
    PyObject* typeName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(typeName)){
        PyErr_SetString(GearsError, "type argument must be a string");
        return NULL;
    }
    PyObject* pyBlob = PyTuple_GetItem(args, 2);
    if(!PyByteArray_Check(pyBlob) && !PyBytes_Check(pyBlob) && !PyObject_CheckBuffer(pyBlob)){
        PyErr_SetString(GearsError, "blob argument must be bytes, byte array or a buffer");
        return NULL;
    }
    const char* typeNameStr = PyUnicode_AsUTF8AndSize(typeName, NULL);
    PyObject* pyDims = PyTuple_GetItem(args, 1);
    PyObject* dimsIter = PyObject_GetIter(pyDims);
    PyObject* currDim = NULL;
    if(dimsIter == NULL){
        PyErr_Clear();
        PyErr_SetString(GearsError, "dims argument must be iterable");
        return NULL;
    }
    
    // Returned object.
    PyObject* obj = NULL;
    // Dimenstions array.
    long long* dims = array_new(long long, 10);
    // Buffer input variables.
    bool buffered = false;
    Py_buffer view;
    size_t size;
    char* blob;
    bool free_blob = false;

    // Collect dims.
    while((currDim = PyIter_Next(dimsIter)) != NULL){
        if(!PyLong_Check(currDim)){
            PyErr_SetString(GearsError, "dims arguments must be long");
            GearsPyDecRef(currDim);
            GearsPyDecRef(dimsIter);
            goto clean_up;
        }
        if(PyErr_Occurred()){
            GearsPyDecRef(currDim);
            GearsPyDecRef(dimsIter);
            goto clean_up;
        }
        dims = array_append(dims, PyLong_AsLong(currDim));
        GearsPyDecRef(currDim);
    }
    GearsPyDecRef(dimsIter);

    RAI_Tensor* t = RedisAI_TensorCreate(typeNameStr, dims, array_len(dims));
    if(!t){
        PyErr_SetString(GearsError, "Failed creating tensor, make sure you put the right data type.");
        goto clean_up;
    }
    // Check expected tensor size.
    size_t expected_tensor_size = RedisAI_TensorByteSize(t);
    if(PyByteArray_Check(pyBlob)) {
        // Blob is byte array.
        size = PyByteArray_Size(pyBlob);
        blob = PyByteArray_AsString(pyBlob);
    } else if(PyBytes_Check(pyBlob)){
        // Blob is bytes.
        size = PyBytes_Size(pyBlob);
        blob = PyBytes_AsString(pyBlob);
    } else {
        // Blob is buffer.
        if(PyObject_GetBuffer(pyBlob, &view, PyBUF_STRIDED_RO) != 0){
            RedisAI_TensorFree(t);
            PyErr_SetString(GearsError, "Error getting buffer info.");
            goto clean_up;
        }

        size = view.len;
        if(!PyBuffer_IsContiguous(&view, 'A')){
            // Buffer is not contiguous - we need to copy it as a contiguous array.
            blob = RG_ALLOC(view.len);
            free_blob = true;
            if(PyBuffer_ToContiguous(blob, &view, view.len, 'A') != 0){
                RedisAI_TensorFree(t);
                PyErr_SetString(GearsError, "Error getting buffer info.");
                goto clean_up;
            }
        }
        else {
            blob = view.buf;
        }
        buffered = true;
    }
    // Validate input.
    if(size != expected_tensor_size) {
        RedisAI_TensorFree(t);
        PyErr_SetString(GearsError, "ERR data length does not match tensor shape and type");
        goto clean_up;
    }

    RedisAI_TensorSetData(t, blob, size);
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    obj = (PyObject*)pyt;

clean_up:
    if(buffered) PyBuffer_Release(&view);
    if(free_blob) RG_FREE(blob);
    array_free(dims);
    return obj;
}

static PyObject* createTensorFromValues(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 3){
        PyErr_SetString(GearsError, "Wrong number of arguments given to createTensorFromValues");
        return NULL;
    }
    RAI_Tensor* t = NULL;
    PyObject* typeName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(typeName)){
        PyErr_SetString(GearsError, "type argument must be a string");
        return NULL;
    }
    const char* typeNameStr = PyUnicode_AsUTF8AndSize(typeName, NULL);
    // todo: combine to a single function!!
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
            GearsPyDecRef(currDim);
            GearsPyDecRef(dimsIter);
            goto error;
        }
        if(PyErr_Occurred()){
            GearsPyDecRef(currDim);
            GearsPyDecRef(dimsIter);
            goto error;
        }
        dims = array_append(dims, PyLong_AsLong(currDim));
        GearsPyDecRef(currDim);
    }
    GearsPyDecRef(dimsIter);

    t = RedisAI_TensorCreate(typeNameStr, dims, array_len(dims));
    if(!t){
        PyErr_SetString(GearsError, "Failed creating tensor, make sure you put the right data type.");
        goto error;
    }

    PyObject* values = PyTuple_GetItem(args, 2);
    PyObject* valuesIter = PyObject_GetIter(values);
    if(valuesIter == NULL){
        PyErr_SetString(GearsError, "values argument must be iterable");
        goto error;
    }
    PyObject* currValue = NULL;
    size_t index = 0;
    while((currValue = PyIter_Next(valuesIter)) != NULL){
        if(!PyFloat_Check(currValue)){
            PyErr_SetString(GearsError, "values arguments must be double");
            GearsPyDecRef(currValue);
            GearsPyDecRef(valuesIter);
            goto error;
        }
        RedisAI_TensorSetValueFromDouble(t, index++, PyFloat_AsDouble(currValue));
        GearsPyDecRef(currValue);
    }
    GearsPyDecRef(valuesIter);

    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    array_free(dims);
    return (PyObject*)pyt;

error:
    array_free(dims);
    if(t) RedisAI_TensorFree(t);
    return NULL;
}

static PyObject* setTensorInKey(PyObject *cls, PyObject *args) {
    verifyRedisAILoaded();
    // Input validation: 2 arguments. args[0]: string. args[1]: tensor
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "Wrong number of arguments given to setTensorInKey");
        return NULL;
    }
    PyObject* keyName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(keyName)){
        PyErr_SetString(GearsError, "key name argument must be a string");
        return NULL;
    }

    
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 1);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
        return NULL;
    }

    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    size_t len;
    const char* keyNameCStr = PyUnicode_AsUTF8AndSize(keyName, &len);

    if(RedisGears_IsClusterMode()){
        const char* keyNodeId = RedisGears_GetNodeIdByKey(keyNameCStr);
        if(!RedisGears_ClusterIsMyId(keyNodeId)){
            PyErr_SetString(GearsError, "Given key is not in the current shard");
            return NULL;
        }
    }

    PyObject* obj = NULL;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, len);
    RedisGears_LockHanlderAcquire(rctx);
    RedisModuleKey *key = RedisModule_OpenKey(rctx,keyNameStr, REDISMODULE_WRITE);
    // Check if the key is empty or 
    bool empty = RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY;
    if(!empty && (RedisModule_ModuleTypeGetType(key) != ai_tensorType)) {
        PyErr_SetString(GearsError, "ERR Key is already set with non tensor values.");
        goto clean_up;
    }

    // Set a shllow copy of the tensor.
    RedisModule_ModuleTypeSetValue(key, ai_tensorType, RedisAI_TensorGetShallowCopy(pyt->t));
    Py_INCREF(Py_None);
    obj = Py_None;
    
clean_up:
    RedisModule_CloseKey(key);
    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeString(rctx, keyNameStr);
    RedisModule_FreeThreadSafeContext(rctx);
    return obj;
}

static PyObject* msetTensorsInKeyspace(PyObject *cls, PyObject *args) {
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to msetTensorInKeyspace");
        return NULL;
    }
    // Input should be a Dictionary<string, tensor>.
    PyObject* py_tensorDict = PyTuple_GetItem(args, 0);
    if(!PyDict_Check(py_tensorDict)){
        PyErr_SetString(GearsError, "Given argument is not of type PyDict");
        return NULL;
    }

    PyObject* obj = NULL;
    bool is_cluster = RedisGears_IsClusterMode();
    size_t len = PyDict_Size(py_tensorDict);
    array_new_on_stack(RedisModuleKey*, STACK_BUFFER_SIZE, keys);
    array_new_on_stack(RAI_Tensor*, STACK_BUFFER_SIZE, tensorList);
    array_new_on_stack(const char*, STACK_BUFFER_SIZE, keyNameList);

    Py_ssize_t i = 0;
    PyObject *key, *value;
    // Validate each key-value pair for <string, tensor> type.
    while (PyDict_Next(py_tensorDict, &i, &key, &value)) {
        if(!PyUnicode_Check(key)){
            PyErr_SetString(GearsError, "key name argument must be a string");
            goto clean_up;
        }
        if(!PyObject_IsInstance(value, (PyObject*)&PyTensorType)){
            PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
            goto clean_up;         
        }
        tensorList = array_append(tensorList, ((PyTensor*)value)->t);
        size_t keylen;
        const char* keyNameCStr = PyUnicode_AsUTF8AndSize(key, &keylen);
        if(is_cluster){
            const char* keyNodeId = RedisGears_GetNodeIdByKey(keyNameCStr);
            if(!RedisGears_ClusterIsMyId(keyNodeId)){
                PyErr_SetString(GearsError, "Given key is not in the current shard");
                goto clean_up;
            }
        }
        keyNameList = array_append(keyNameList, keyNameCStr);
    }
   
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(rctx);
    for(size_t i = 0; i < len; i++) {
        const char *keyNameCStr = keyNameList[i];
        RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, strlen(keyNameCStr));
        RedisModuleKey *key = RedisModule_OpenKey(rctx,keyNameStr, REDISMODULE_WRITE);
        keys = array_append(keys, key);
        // Check if the key is empty or 
        bool empty = RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY;
        if(!empty && (RedisModule_ModuleTypeGetType(key) != ai_tensorType)) {
            RedisModule_FreeString(rctx, keyNameStr);
            PyErr_SetString(GearsError, "ERR Key is already set with non tensor values.");
            goto clean_up;
        }
        RedisModule_FreeString(rctx, keyNameStr);
    }
    // We are ok to modify keyspace.
    for(size_t i = 0; i < len; i++) {
        RedisModule_ModuleTypeSetValue(keys[i], ai_tensorType, RedisAI_TensorGetShallowCopy(tensorList[i]));
    }

    Py_INCREF(Py_None);
    obj = Py_None;

clean_up:
    array_free_ex(keys, RedisModule_CloseKey(*(RedisModuleKey**)ptr));
    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    array_free(tensorList);
    array_free(keyNameList);
    return obj;
}

static PyObject* getTensorFromKey(PyObject *cls, PyObject *args) {
     verifyRedisAILoaded();
    // Input validation: 1 argument. args[0]: string.
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to getTensorFromKey");
        return NULL;
    }
    PyObject* keyName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(keyName)){
        PyErr_SetString(GearsError, "key name argument must be a string");
        return NULL;
    }

    size_t len;
    const char* keyNameCStr = PyUnicode_AsUTF8AndSize(keyName, &len);
    if(RedisGears_IsClusterMode()){
        const char* keyNodeId = RedisGears_GetNodeIdByKey(keyNameCStr);
        if(!RedisGears_ClusterIsMyId(keyNodeId)){
            PyErr_SetString(GearsError, "Given key is not in the current shard");
        return NULL;
        }
    }
    PyObject* obj = NULL;
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, len);
    RedisGears_LockHanlderAcquire(rctx);
    RedisModuleKey *key = RedisModule_OpenKey(rctx,keyNameStr, REDISMODULE_READ);
    // Empty key.
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        PyErr_SetString(GearsError, "ERR tensor key is empty");
        goto clean_up;
    }
    // No tensor key.
    if (RedisModule_ModuleTypeGetType(key) != ai_tensorType) {
        PyErr_SetString(GearsError, "ERR key is not a tensor");
        goto clean_up;
    }
    // Read tensor and clean up.
    RAI_Tensor *tensor = RedisModule_ModuleTypeGetValue(key);
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = RedisAI_TensorGetShallowCopy(tensor);
    obj = (PyObject*)pyt;

clean_up:
    RedisModule_CloseKey(key);
    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeString(rctx, keyNameStr);
    RedisModule_FreeThreadSafeContext(rctx);
    return obj;
}

static PyObject* mgetTensorsFromKeyspace(PyObject *cls, PyObject *args) {
     verifyRedisAILoaded();
    // Input validation: 1 argument. args[0]: List<String>.
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to getTensorsFromKeyspace");
        return NULL;
    }
    PyObject* keys_iter = PyObject_GetIter(PyTuple_GetItem(args, 0));
    if(!keys_iter){
        PyErr_SetString(GearsError, "keys argument must be iterable");
        return NULL;
    }

    PyObject* obj = NULL;
    array_new_on_stack(const char*, STACK_BUFFER_SIZE, keyNameList);
    array_new_on_stack(RedisModuleKey*, STACK_BUFFER_SIZE, keys);
    bool is_cluster = RedisGears_IsClusterMode();
    PyObject* keyName = NULL;
    while((keyName = PyIter_Next(keys_iter)) != NULL) {
        // Check for string.
        if(!PyUnicode_Check(keyName)) {
            PyErr_SetString(GearsError, "key name argument must be a string");
            goto clean_up;
        }
        size_t keylen;
        const char* keyNameCStr = PyUnicode_AsUTF8AndSize(keyName, &keylen);
        // In cluster, verify all keys are in the local shard.
        if(is_cluster){
            const char* keyNodeId = RedisGears_GetNodeIdByKey(keyNameCStr);
            if(!RedisGears_ClusterIsMyId(keyNodeId)){
                PyErr_SetString(GearsError, "Given key is not in the current shard");
                goto clean_up;
            }
        }
        keyNameList = array_append(keyNameList, keyNameCStr);
    }

    size_t len = array_len(keyNameList);
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(rctx);
    for(size_t i = 0; i < len; i++) {
        // Open key.
        const char *keyNameCStr = keyNameList[i];
        RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, strlen(keyNameCStr));
        RedisModuleKey *key = RedisModule_OpenKey(rctx,keyNameStr, REDISMODULE_READ);
        keys = array_append(keys, key);
        // Empty key.
        if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
            RedisModule_FreeString(rctx, keyNameStr);
            PyErr_SetString(GearsError, "ERR tensor key is empty");
            goto clean_up;
        }
        // No tensor key.
        if (RedisModule_ModuleTypeGetType(key) != ai_tensorType) {
            RedisModule_FreeString(rctx, keyNameStr);
            PyErr_SetString(GearsError, "ERR key is not a tensor");
            goto clean_up;
        }
        // Success. Set key and release string.
        RedisModule_FreeString(rctx, keyNameStr);
    }
    
    PyObject* tensorsList = PyList_New(0);
    // Read tensors.
    for(size_t i = 0; i < len; i++) {
        RAI_Tensor *tensor = RedisModule_ModuleTypeGetValue(keys[i]);
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy(tensor);
        PyList_Append(tensorsList, (PyObject*)pyt);
        GearsPyDecRef((PyObject*)pyt);
    }
    obj = tensorsList;

clean_up:

    array_free_ex(keys, RedisModule_CloseKey(*(RedisModuleKey**)ptr));
    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    array_free(keyNameList);
    GearsPyDecRef(keys_iter);
    return obj;
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
    if (pyg->g) RedisAI_ModelRunCtxFree(pyg->g);
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
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to createModelRunner");
        return NULL;
    }
    PyObject* keyName = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(keyName)){
        PyErr_SetString(GearsError, "key argument must be a string");
        return NULL;
    }
    const char* keyNameStr = PyUnicode_AsUTF8AndSize(keyName, NULL);
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_MODULE){
        RedisModule_FreeString(ctx, keyRedisStr);
        RedisModule_CloseKey(key);
        RedisGears_LockHanlderRelease(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        PyErr_SetString(GearsError, "given key do not contain RedisAI model");
        return NULL;
    }
    // todo:
    // It might be another module key, we need api from RedisAI to verify
    // it really an RedisAI model
    RAI_Model *g = RedisModule_ModuleTypeGetValue(key);
    RAI_ModelRunCtx* runCtx = RedisAI_ModelRunCtxCreate(g);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    PyGraphRunner* pyg = PyObject_New(PyGraphRunner, &PyGraphRunnerType);
    pyg->g = runCtx;

    return (PyObject*)pyg;
}

static PyObject* modelRunnerAddInput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 3){
        PyErr_SetString(GearsError, "Wrong number of arguments given to modelRunnerAddInput");
        return NULL;
    }
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyg, (PyObject*)&PyGraphRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyGraphRunner");
        return NULL;
    }
	if(pyg->g == NULL){
		PyErr_SetString(GearsError, "PyGraphRunner is invalid");
		return NULL;
	}
    PyObject* inputName = PyTuple_GetItem(args, 1);
    if(!PyUnicode_Check(inputName)){
        PyErr_SetString(GearsError, "input name argument must be a string");
        return NULL;
    }
    const char* inputNameStr = PyUnicode_AsUTF8AndSize(inputName, NULL);
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 2);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensorType");
        return NULL;
    }
    RedisAI_ModelRunCtxAddInput(pyg->g, inputNameStr, pyt->t);
    return PyLong_FromLong(1);
}

static PyObject* modelRunnerAddOutput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "Wrong number of arguments given to modelRunnerAddOutput");
        return NULL;
    }
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyg, (PyObject*)&PyGraphRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyGraphRunner");
        return NULL;
    }
	if(pyg->g == NULL){
		PyErr_SetString(GearsError, "PyGraphRunner is invalid");
		return NULL;
	}
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
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to modelRunnerRun");
        return NULL;
    }
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pyg, (PyObject*)&PyGraphRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyGraphRunner");
        return NULL;
    }
	if(pyg->g == NULL){
		PyErr_SetString(GearsError, "PyGraphRunner is invalid");
		return NULL;
	}
    RAI_Error* err;
    RedisAI_InitError(&err);
    RAI_ModelRunCtx *mctx = pyg->g;
    pyg->g = NULL;
    PyThreadState* _save = PyEval_SaveThread();
    RedisAI_ModelRun(&mctx, 1, err);
    PyEval_RestoreThread(_save);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
		RedisAI_ModelRunCtxFree(mctx);
        return NULL;
    }
    RedisAI_FreeError(err);
    PyObject* tensorList = PyList_New(0);
    for(size_t i = 0 ; i < RedisAI_ModelRunCtxNumOutputs(mctx) ; ++i){
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ModelRunCtxOutputTensor(mctx, i));
        PyList_Append(tensorList, (PyObject*)pyt);
        GearsPyDecRef((PyObject*)pyt);
    }
    RedisAI_ModelRunCtxFree(mctx);
    return tensorList;
}

static void FinishAsyncModelRun(RAI_OnFinishCtx *onFinishCtx, void *private_data) {

	RAI_Error *error;
	RedisAI_InitError(&error);
	RAI_ModelRunCtx* mctx = RedisAI_GetAsModelRunCtx(onFinishCtx, error);

	RedisGearsPy_LOCK
	PyObject* future = private_data;
	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, future);

	if (RedisAI_GetErrorCode(error) != RedisAI_ErrorCode_OK) {
		const char *errStr = RedisAI_GetError(error);
		PyObject* pyErr = PyUnicode_FromStringAndSize(errStr, strlen(errStr));
		PyTuple_SetItem(pArgs, 1, pyErr);
		PyObject_CallObject(setFutureExceptionFunction, pArgs);
		goto finish;
	}

    PyObject* tensorList = PyList_New(0);
	for(size_t i = 0 ; i < RedisAI_ModelRunCtxNumOutputs(mctx) ; ++i){
		PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
		pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ModelRunCtxOutputTensor(mctx, i));
		PyList_Append(tensorList, (PyObject*)pyt);
		GearsPyDecRef((PyObject*)pyt);
	}

	PyTuple_SetItem(pArgs, 1, tensorList);
	PyObject* r = PyObject_CallObject(setFutureResultsFunction, pArgs);
	if(!r){
		char* err = getPyError();
		RedisModule_Log(NULL, "warning", "Error happened when releasing execution future, error='%s'", err);
		RG_FREE(err);
	}else{
		GearsPyDecRef(r);
	}

	finish:
	RedisAI_FreeError(error);
	RedisAI_ModelRunCtxFree(mctx);
	GearsPyDecRef(pArgs);
	RedisGearsPy_UNLOCK
}

static PyObject* modelRunnerRunAsync(PyObject *cls, PyObject *args){
	verifyRedisAILoaded();
	if (RedisAI_ModelRunAsync == NULL) {
		PyErr_SetString(GearsError, "Asynchronous run is not supported in RedisAI version");
		return NULL;
	}
	if(PyTuple_Size(args) != 1){
		PyErr_SetString(GearsError, "Wrong number of arguments given to modelRunnerRunAsync");
		return NULL;
	}
	PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
	if(!PyObject_IsInstance((PyObject*)pyg, (PyObject*)&PyGraphRunnerType)){
		PyErr_SetString(GearsError, "Given argument is not of type PyGraphRunner");
		return NULL;
	}
	if(pyg->g == NULL){
		PyErr_SetString(GearsError, "PyGraphRunner is invalid");
		return NULL;
	}
	PyObject* pArgs = PyTuple_New(0);
	PyObject* future = PyObject_CallObject(createFutureFunction, pArgs);
	GearsPyDecRef(pArgs);

	RedisAI_ModelRunAsync(pyg->g, FinishAsyncModelRun, future);
	pyg->g = NULL;
	Py_INCREF(future);
	return future;
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
    if (pys->s) RedisAI_ScriptRunCtxFree(pys->s);
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
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "Wrong number of arguments given to createScriptRunner");
        return NULL;
    }
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
    RedisGears_LockHanlderAcquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);

    if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_MODULE){
        RedisModule_FreeString(ctx, keyRedisStr);
        RedisModule_CloseKey(key);
        RedisGears_LockHanlderRelease(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        PyErr_SetString(GearsError, "given key do not contain RedisAI script");
        return NULL;
    }

    // todo:
    // It might be another module key, we need api from RedisAI to verify
    // it really an RedisAI model

    RAI_Script *s = RedisModule_ModuleTypeGetValue(key);

    const char* fnNameStr = PyUnicode_AsUTF8AndSize(fnName, NULL);

    RAI_ScriptRunCtx* runCtx = RedisAI_ScriptRunCtxCreate(s, fnNameStr);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    PyTorchScriptRunner* pys = PyObject_New(PyTorchScriptRunner, &PyTorchScriptRunnerType);
    pys->s = runCtx;

    return (PyObject*)pys;
}

static PyObject* scriptRunnerAddInput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "Wrong number of arguments given to scriptRunnerAddInput");
        return NULL;
    }
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pys, (PyObject*)&PyTorchScriptRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTorchScriptRunner");
        return NULL;
    }
    if(pys->s == NULL){
        PyErr_SetString(GearsError, "PyTorchScriptRunner is invalid");
        return NULL;
    }
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 1);
    if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
        return NULL;
    }
    RAI_Error* err;
    RedisAI_InitError(&err);
    RedisAI_ScriptRunCtxAddInput(pys->s, pyt->t, err);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
        return NULL;
    }
    RedisAI_FreeError(err);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* scriptRunnerAddInputList(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();

    if(PyTuple_Size(args) != 2){
        PyErr_SetString(GearsError, "Wrong number of arguments given to scriptRunnerAddInput");
        return NULL;
    }
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pys, (PyObject*)&PyTorchScriptRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTorchScriptRunner");
        return NULL;
    }
    if(pys->s == NULL){
        PyErr_SetString(GearsError, "PyTorchScriptRunner is invalid");
        return NULL;
    }
    PyObject* py_tensors_iter = PyObject_GetIter(PyTuple_GetItem(args, 1));
    if(!py_tensors_iter){
        PyErr_SetString(GearsError, "Tensors argument must be iterable");
        return NULL;
    }

    PyObject* obj = NULL;
    array_new_on_stack(RAI_Tensor*, STACK_BUFFER_SIZE, tensorList);
    RAI_Error* err;
    RedisAI_InitError(&err);
    PyTensor* pyt;
    while((pyt = (PyTensor*)PyIter_Next(py_tensors_iter)) != NULL) {
        if(!PyObject_IsInstance((PyObject*)pyt, (PyObject*)&PyTensorType)){
            PyErr_SetString(GearsError, "Given argument is not of type PyTensor");
            goto clean_up;
        }
        tensorList = array_append(tensorList, pyt->t);
    }

    RedisAI_ScriptRunCtxAddInputList(pys->s, tensorList, array_len(tensorList), err);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        goto clean_up;
    }
    Py_INCREF(Py_None);
    obj = Py_None;
clean_up:

    GearsPyDecRef(py_tensors_iter);
    array_free(tensorList);
    RedisAI_FreeError(err);
    return obj;
}

static PyObject* scriptRunnerAddOutput(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to scriptRunnerAddOutput");
        return NULL;
    }
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pys, (PyObject*)&PyTorchScriptRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTorchScriptRunner");
        return NULL;
    }
	if(pys->s == NULL){
		PyErr_SetString(GearsError, "PyTorchScriptRunner is invalid");
		return NULL;
	}
    RedisAI_ScriptRunCtxAddOutput(pys->s);
    return PyLong_FromLong(1);
}

static PyObject* scriptRunnerRun(PyObject *cls, PyObject *args){
    verifyRedisAILoaded();
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Wrong number of arguments given to scriptRunnerRun");
        return NULL;
    }
    PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
    if(!PyObject_IsInstance((PyObject*)pys, (PyObject*)&PyTorchScriptRunnerType)){
        PyErr_SetString(GearsError, "Given argument is not of type PyTorchScriptRunner");
        return NULL;
    }
	if(pys->s == NULL){
		PyErr_SetString(GearsError, "PyTorchScriptRunner is invalid");
		return NULL;
	}
    RAI_Error* err;
    RedisAI_InitError(&err);
	RAI_ScriptRunCtx *sctx = pys->s;
	pys->s = NULL;
    PyThreadState* _save = PyEval_SaveThread();
    RedisAI_ScriptRun(sctx, err);
    PyEval_RestoreThread(_save);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
        RedisAI_ScriptRunCtxFree(sctx);
        return NULL;
    }
    RedisAI_FreeError(err);
    PyObject* tensorList = PyList_New(0);
    for(size_t i = 0 ; i < RedisAI_ScriptRunCtxNumOutputs(sctx) ; ++i){
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ScriptRunCtxOutputTensor(sctx, i));
        PyList_Append(tensorList, (PyObject*)pyt);
        GearsPyDecRef((PyObject*)pyt);
    }
	RedisAI_ScriptRunCtxFree(sctx);
    return tensorList;
}

static void FinishAsyncScriptRun(RAI_OnFinishCtx *onFinishCtx, void *private_data) {

	RAI_Error *error;
	RedisAI_InitError(&error);
	RAI_ScriptRunCtx* sctx = RedisAI_GetAsScriptRunCtx(onFinishCtx, error);

	RedisGearsPy_LOCK
	PyObject* future = private_data;
	PyObject* pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, future);

	if (RedisAI_GetErrorCode(error) != RedisAI_ErrorCode_OK) {
		const char *errStr = RedisAI_GetError(error);
		PyObject* pyErr = PyUnicode_FromStringAndSize(errStr, strlen(errStr));
		PyTuple_SetItem(pArgs, 1, pyErr);
		PyObject_CallObject(setFutureExceptionFunction, pArgs);
		goto finish;
	}

    PyObject* tensorList = PyList_New(0);
	for(size_t i = 0 ; i < RedisAI_ScriptRunCtxNumOutputs(sctx) ; ++i){
		PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
		pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ScriptRunCtxOutputTensor(sctx, i));
		PyList_Append(tensorList, (PyObject*)pyt);
		GearsPyDecRef((PyObject*)pyt);
	}

	PyTuple_SetItem(pArgs, 1, tensorList);
	PyObject* r = PyObject_CallObject(setFutureResultsFunction, pArgs);
	if(!r){
		char* err = getPyError();
		RedisModule_Log(NULL, "warning", "Error happened when releasing execution future, error='%s'", err);
		RG_FREE(err);
	}else{
		GearsPyDecRef(r);
	}

	finish:
	RedisAI_FreeError(error);
	RedisAI_ScriptRunCtxFree(sctx);
	GearsPyDecRef(pArgs);
	RedisGearsPy_UNLOCK
}

static PyObject* scriptRunnerRunAsync(PyObject *cls, PyObject *args){
	verifyRedisAILoaded();
	if (RedisAI_ScriptRunAsync == NULL) {
		PyErr_SetString(GearsError, "Asynchronous run is not supported in RedisAI version");
		return NULL;
	}
	if(PyTuple_Size(args) != 1){
		PyErr_SetString(GearsError, "Wrong number of arguments given to scriptRunnerRunAsync");
		return NULL;
	}
	PyTorchScriptRunner* pys = (PyTorchScriptRunner*)PyTuple_GetItem(args, 0);
	if(!PyObject_IsInstance((PyObject*)pys, (PyObject*)&PyTorchScriptRunnerType)){
		PyErr_SetString(GearsError, "Given argument is not of type PyTorchScriptRunner");
		return NULL;
	}
	if(pys->s == NULL){
		PyErr_SetString(GearsError, "PyTorchScriptRunner is invalid");
		return NULL;
	}
	PyObject* pArgs = PyTuple_New(0);
	PyObject* future = PyObject_CallObject(createFutureFunction, pArgs);
	GearsPyDecRef(pArgs);

	RedisAI_ScriptRunAsync(pys->s, FinishAsyncScriptRun, future);
	pys->s = NULL;
	Py_INCREF(future);
	return future;
}

// LCOV_EXCL_STOP

#define TIME_EVENT_ENCVER 2
#define TIME_EVENT_ENCVER_WITH_VERSIONED_SESSION_PYCALLBACK 2

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
    PythonExecutionCtx pectx = PythonExecutionCtx_New(td->session, NULL);
    RedisGearsPy_Lock(&pectx);
    PyObject* pArgs = PyTuple_New(0);
    PyObject_CallObject(td->callback, pArgs);
    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error occured on TimeEvent_Callback, error=%s", error);
        RG_FREE(error);
        td->status =TE_STATUS_ERR;
    }else{
        td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    }
    GearsPyDecRef(pArgs);
    RedisGearsPy_Unlock(&pectx);
}

static void *TimeEvent_RDBLoad(RedisModuleIO *rdb, int encver){
    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = RedisModule_LoadUnsigned(rdb);

    int version = 0;
    if(encver >= TIME_EVENT_ENCVER_WITH_VERSIONED_SESSION_PYCALLBACK){
        version = RedisModule_LoadUnsigned(rdb);
    }

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
    td->session = PythonSessionCtx_Deserialize(&br, version, &err, true);
    if(!td->session){
        RedisModule_Log(staticCtx, "warning", "Could not deserialize TimeEven Session, error='%s'", err);
    }
    RedisModule_Free(serializedSession);

    version = 0;
    if(encver >= TIME_EVENT_ENCVER_WITH_VERSIONED_SESSION_PYCALLBACK){
        version = RedisModule_LoadUnsigned(rdb);
    }

    size_t len;
    char* buff = RedisModule_LoadStringBuffer(rdb, &len);
    Gears_Buffer b = {
            .cap = len,
            .size = len,
            .buff = buff,
    };
    Gears_BufferReader reader;
    Gears_BufferReaderInit(&reader, &b);
    td->callback = RedisGearsPy_PyCallbackDeserialize(NULL, &reader, version, NULL);
    RedisModule_Assert(td->callback);

    // change callback global
    PythonExecutionCtx pectx = PythonExecutionCtx_New(td->session, NULL);
    RedisGearsPy_Lock(&pectx);
    PyFunctionObject* callback_func = (PyFunctionObject*)td->callback;
    PyDict_Merge(td->session->globalsDict, callback_func->func_globals, 0);
    GearsPyDecRef(callback_func->func_globals);
    callback_func->func_globals = td->session->globalsDict;
    Py_INCREF(callback_func->func_globals);
    RedisGearsPy_Unlock(&pectx);

    RedisModule_Free(buff);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    RedisModule_FreeThreadSafeContext(ctx);
    return td;
}

static void TimeEvent_RDBSave(RedisModuleIO *rdb, void *value){
    TimerData* td = value;
    RedisModule_SaveUnsigned(rdb, td->period);
    RedisModule_SaveUnsigned(rdb, PY_SESSION_TYPE_VERSION);
    Gears_Buffer* b = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, b);
    PythonSessionCtx_Serialize(NULL, td->session, &bw, NULL);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferClear(b);
    RedisModule_SaveUnsigned(rdb, PY_OBJECT_TYPE_VERSION);
    int res = RedisGearsPy_PyCallbackSerialize(NULL, td->callback, &bw, NULL);
    RedisModule_Assert(res == REDISMODULE_OK);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferFree(b);
}

static void TimeEvent_Free(void *value){
    TimerData* td = value;
    PythonExecutionCtx pectx = PythonExecutionCtx_New(td->session, NULL);
    RedisGearsPy_Lock(&pectx);
    GearsPyDecRef(td->callback);
    RedisGearsPy_Unlock(&pectx);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_StopTimer(ctx, td->id, NULL);
    PythonSessionCtx_Free(td->session);
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
    if(!ptctx->currSession){
        PyErr_SetString(GearsError, "can not create time event on a python created thread");
        return NULL;
    }
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
    td->session = PythonSessionCtx_ShallowCopy(ptctx->currSession);
    Py_INCREF(callback);

    RedisGears_LockHanlderAcquire(ctx);

    if(keyNameStr){
        RedisModuleKey* key = RedisModule_OpenKey(ctx, keyNameStr, REDISMODULE_WRITE);
        if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
            TimeEvent_Free(td);
            RedisGears_LockHanlderRelease(ctx);
            RedisModule_FreeThreadSafeContext(ctx);
            return Py_False;
        }
        RedisModule_ModuleTypeSetValue(key, TimeEventType, td);
    }

    td->id = RedisModule_CreateTimer(ctx, period * 1000, TimeEvent_Callback, td);

    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    return Py_True;
}

static PyObject* flatError(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "not enough arguments for time flat error");
        return NULL;
    }
    PyObject* msg = PyTuple_GetItem(args, 0);
    PyErr_SetObject(GearsFlatError, msg);

    return NULL;
}

static PyObject* isAsyncAllow(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyObject* ret = Py_False;
    if(ptctx->currEctx){
        RunFlags runFlags = RedisGears_GetRunFlags(ptctx->currEctx);
        if(!(runFlags & RFNoAsync)){
            ret = Py_True;
        }
    }

    Py_INCREF(ret);
    return ret;
}

static PyObject* overrideReply(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    CommandCtx* commandCtx = getCommandCtx(ptctx);
    if(!commandCtx){
        PyErr_SetString(GearsError, "Can not get command ctx");
        return NULL;
    }

    if(PyTuple_Size(args) != 1){
        PyErr_SetString(GearsError, "Override reply must get a single argument");
        return NULL;
    }

    PyObject* reply = PyTuple_GetItem(args, 0);

    PythonRecord* record = (PythonRecord*)PyObjRecordCreate();
    record->obj = reply;

    Py_INCREF(record->obj);

    char* err = NULL;

    if(RedisGears_CommandCtxOverrideReply(commandCtx, &record->base, &err) != REDISMODULE_OK){
        PyErr_SetString(GearsError, err);
        RG_FREE(err);
        RedisGears_FreeRecord(&record->base);
        return NULL;
    }

    Py_INCREF(Py_None);

    return Py_None;
}

static PyObject* getCommand(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    CommandCtx* commandCtx = getCommandCtx(ptctx);
    if(!commandCtx){
        PyErr_SetString(GearsError, "Can not get command ctx");
        return NULL;
    }

    // we must take the lock, it is not safe to access the command args without the lock because redis might
    // change them under our noise
    RedisGears_LockHanlderAcquire(staticCtx);

    size_t len;
    RedisModuleString** argv = RedisGears_CommandCtxGetCommand(commandCtx, &len);

    PyObject *list = PyList_New(0);

    for(size_t i = 0 ; i < len ; ++i){
        size_t strLen;
        const char* arg = RedisModule_StringPtrLen(argv[i], &strLen);
        PyObject* pyArg = PyUnicode_FromStringAndSize(arg, strLen);
        PyList_Append(list, pyArg);
    }

    RedisGears_LockHanlderRelease(staticCtx);

    return list;
}

static PyObject* callNext(PyObject *cls, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    RedisGears_LockHanlderAcquire(staticCtx);

    CommandReaderTriggerCtx* crtCtx = getCommandReaderTriggerCtx(ptctx);;

    if(!crtCtx){
        PyErr_SetString(GearsError, "Can not get CommandHook ctx");
        RedisGears_LockHanlderRelease(staticCtx);
        return NULL;
    }

    RedisModuleString** arguments = createArgs(args);

    RedisModuleCallReply* reply = RedisGears_CommandReaderTriggerCtxNext(crtCtx, arguments, array_len(arguments));

    PyObject* res = createReply(reply);

    array_free_ex(arguments, RedisModule_FreeString(NULL, *(RedisModuleString**)ptr));

    RedisGears_LockHanlderRelease(staticCtx);
    return res;
}

PyMethodDef EmbRedisGearsMethods[] = {
    {"gearsCtx", gearsCtx, METH_VARARGS, "creating an empty gears context"},
    {"registerGearsThread", registerGearsThread, METH_VARARGS, "Register a thread to be a RedisGears thread"},
    {"getGearsSession", getGearsSession, METH_VARARGS, "get the current gears session"},
    {"setGearsSession", setGearsSession, METH_VARARGS, "set the current gears session"},
    {"isInAtomicBlock", isInAtomicBlock, METH_VARARGS, "return true if currently inside atomic block"},
    {"gearsFutureCtx", gearsFutureCtx, METH_VARARGS, "creating future object to block the execution"},
    {"atomicCtx", atomicCtx, METH_VARARGS, "creating a atomic ctx for atomic block"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "execute a redis command and return the result"},
    {"log", (PyCFunction)RedisLog, METH_VARARGS|METH_KEYWORDS, "write a message into the redis log file"},
    {"config_get", RedisConfigGet, METH_VARARGS, "write a message into the redis log file"},
    {"getMyHashTag", getMyHashTag, METH_VARARGS, "return hash tag of the current node or None if not running on cluster"},
    {"registerTimeEvent", gearsTimeEvent, METH_VARARGS, "register a function to be called on each time period"},
    {"callNext", callNext, METH_VARARGS, "call the next command registration or the original command (will raise error when used outside on CommandHook scope)"},
    {"getCommand", getCommand, METH_VARARGS, "return the current running command, raise error if command is not available"},
    {"overrideReply", overrideReply, METH_VARARGS, "override the reply with the given python value, raise error if there is no command to override its reply"},
    {"isAsyncAllow", isAsyncAllow, METH_VARARGS, "return true iff async await is allow"},
    {"flatError", flatError, METH_VARARGS, "return flat error object that will be return to the user without extracting the trace"},
    {NULL, NULL, 0, NULL}
};

PyMethodDef EmbRedisAIMethods[] = {
    {"createTensorFromValues", createTensorFromValues, METH_VARARGS, "creating a tensor object from values"},
    {"createTensorFromBlob", createTensorFromBlob, METH_VARARGS, "creating a tensor object from blob"},
    {"setTensorInKey", setTensorInKey, METH_VARARGS, "set a tensor in keyspace"},
    {"msetTensorsInKeyspace", msetTensorsInKeyspace, METH_VARARGS, "set multiple tensors in keyspace"},
    {"getTensorFromKey", getTensorFromKey, METH_VARARGS, "get a tensor from keyspace"},
    {"mgetTensorsFromKeyspace", mgetTensorsFromKeyspace, METH_VARARGS, "get multiple tensors from keyspace"},
    {"createModelRunner", createModelRunner, METH_VARARGS, "open TF graph by key name"},
    {"modelRunnerAddInput", modelRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"modelRunnerAddOutput", modelRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"modelRunnerRun", modelRunnerRun, METH_VARARGS, "run graph runner"},
	{"modelRunnerRunAsync", modelRunnerRunAsync, METH_VARARGS, "run graph runner async"},
    {"createScriptRunner", createScriptRunner, METH_VARARGS, "open a torch script by key name"},
    {"scriptRunnerAddInput", scriptRunnerAddInput, METH_VARARGS, "add input to torch script runner"},
    {"scriptRunnerAddInputList", scriptRunnerAddInputList, METH_VARARGS, "add a list of tensor as input to torch script runner"},
    {"scriptRunnerAddOutput", scriptRunnerAddOutput, METH_VARARGS, "add output to torch script runner"},
    {"scriptRunnerRun", scriptRunnerRun, METH_VARARGS, "run torch script runner"},
    {"scriptRunnerRunAsync", scriptRunnerRunAsync, METH_VARARGS, "run torch script runner async"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
    {"tensorGetDataAsBlob", tensorGetDataAsBlob, METH_VARARGS, "getting the tensor data as a string blob"},
    {"tensorGetDims", tensorGetDims, METH_VARARGS, "return tuple of the tensor dims"},
    {"createDAGRunner", createDAGRunner, METH_VARARGS, "create an empty DAG runner"},
    {NULL, NULL, 0, NULL}
};

static int RedisGearsPy_FreeInterpreter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
//    RedisGearsPy_Lock(NULL);
//	Py_Finalize();
//	RedisModule_ReplyWithSimpleString(ctx, "OK");
//	return REDISMODULE_OK;
    RedisModule_ReplyWithCString(ctx, "Free interpreter is not longer supported");
    return REDISMODULE_OK;
}

static int RedisGearsPy_ExecuteRemote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_ReplyWithCString(ctx, "Execute remote is not longer supported");
    return REDISMODULE_OK;
}

static void RedisGearsPy_GetRequirementsList(const char** requirementsList, RedisModuleString **argv, int argc){
    for(size_t i = 0 ; i < argc ; ++i){
        requirementsList[i] = RedisModule_StringPtrLen(argv[i], NULL);
    }
}

typedef struct BackgroundDepsInstallCtx{
    PythonSessionCtx* session;
    PythonSessionCtx* oldSession;
    RedisModuleBlockedClient *bc;
    char* script;
    DoneCallbackFunction doneFunction;
    bool isBlocking;
}BackgroundDepsInstallCtx;

static Gears_threadpool installDepsPool = NULL;

static void RedisGearsPy_ExectionRunningCallback(ExecutionPlan* ep, void* privateData) {
    // privateData is the blocked client
    RedisModule_BlockedClientMeasureTimeStart(privateData);
}

static void RedisGearsPy_ExectionHoldingCallback(ExecutionPlan* ep, void* privateData) {
    // privateData is the blocked client
    RedisModule_BlockedClientMeasureTimeEnd(privateData);
}

static void RedisGearsPy_RegisterOnDone(char **errors, size_t len, void *pd){
    RedisModuleBlockedClient* bc = pd;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
    if (len == 0) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        RedisModule_ReplyWithArray(ctx, len);
        for (size_t i = 0 ; i < len ; ++i) {
            RedisModule_ReplyWithError(ctx, errors[i]);
        }
    }
    RedisModule_UnblockClient(bc, bc);
    RedisModule_FreeThreadSafeContext(ctx);
}

static int RedisGearsPy_InnerExecute(RedisModuleCtx* rctx, BackgroundDepsInstallCtx* bdiCtx){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    ptctx->currentCtx = rctx;
    ptctx->createdExecution = NULL;

    PythonExecutionCtx pectx = PythonExecutionCtx_New(bdiCtx->session, NULL);
    RedisGearsPy_Lock(&pectx);

    if (bdiCtx->session->isInstallationNeeded) {
        /* Requirements was installed, reset modules catch. */
        PyInterpreterState *interp = PyThreadState_GET()->interp;
        PyDict_Clear(interp->modules);
        PyDict_Merge(interp->modules, initialModulesDict, 1);
    }

    PyObject *v = PyRun_StringFlags(bdiCtx->script, Py_file_input, ptctx->currSession->globalsDict, ptctx->currSession->globalsDict, NULL);

    if(!v){
        char* err = getPyError();
        if(!err){
            RedisModule_ReplyWithError(rctx, "failed running the given script");
        }else{
            RedisModule_ReplyWithError(rctx, err);
            RG_FREE(err);
        }

        if(ptctx->createdExecution){
            // error occured, we need to abort the created execution.
            int res = RedisGears_AbortExecution(ptctx->createdExecution);
            RedisModule_Assert(res == REDISMODULE_OK);
            RedisGears_DropExecution(ptctx->createdExecution);
        }

        RedisGearsPy_Unlock(&pectx);

        ptctx->createdExecution = NULL;
        ptctx->currentCtx = NULL;
        return REDISMODULE_ERR;
    }

    if (array_len(bdiCtx->session->registrations) > 0){
        // we have registrations, lets register them!
        RedisModuleBlockedClient* bc = bdiCtx->bc;
        bdiCtx->bc = NULL; // we are taking ownership of the blocked client
        if(!bc){
            bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 0);
        }
        char *err = NULL;
        if (RedisGears_Register(bdiCtx->session->srctx, RedisGearsPy_RegisterOnDone, bc, &err) != REDISMODULE_OK) {
            RedisModule_AbortBlock(bc);
            RedisModule_ReplyWithError(ptctx->currentCtx, err);
        }
        bdiCtx->session->srctx = NULL;
    } else {
        if(ptctx->createdExecution){
            RedisGearsPy_AddSessionRequirementsToDict(bdiCtx->session); // add the requirements to dictionary
            bdiCtx->session->fullSerialization = true; // mark the session to be fully serialized with requirements
            if(bdiCtx->isBlocking){
                RedisModuleBlockedClient* bc = bdiCtx->bc;
                bdiCtx->bc = NULL; // we are taking ownership of the blocked client
                if(!bc){
                    bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 0);
                }
                RedisGears_AddOnDoneCallback(ptctx->createdExecution, bdiCtx->doneFunction, bc);
                // on async executions we will set running and holding callbacks to update slowlog stats
                // but we do it only if we have the relevant api from Redis
                if (RedisModule_BlockedClientMeasureTimeStart && RedisModule_BlockedClientMeasureTimeStart) {
                    RedisGears_AddOnRunningCallback(ptctx->createdExecution, RedisGearsPy_ExectionRunningCallback, bc);
                    RedisGears_AddOnHoldingCallback(ptctx->createdExecution, RedisGearsPy_ExectionHoldingCallback, bc);
                }
            }else{
                const char* id = RedisGears_GetId(ptctx->createdExecution);
                RedisModule_ReplyWithStringBuffer(rctx, id, strlen(id));
            }
        }else{
            RedisModule_ReplyWithSimpleString(ptctx->currentCtx, "OK");
        }
        RedisGears_SessionRegisterCtxFree(bdiCtx->session->srctx);
        bdiCtx->session->srctx = NULL;
    }
    RedisGearsPy_Unlock(&pectx);

    ptctx->createdExecution = NULL;
    ptctx->currentCtx = NULL;
    return REDISMODULE_OK;
}

static ExecutionPlan* RedisGearsPy_DistributeRequirements(PythonRequirementCtx** requirements, RedisGears_OnExecutionDoneCallback doneCallback, void* pd, char** err){
    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader, err);
    if(!fep){
        return NULL;
    }
    RedisGears_SetMaxIdleTime(fep, pythonConfig.installReqMaxIdleTime);
    RGM_Map(fep, RedisGearsPy_InstallRequirementsMapper, PythonSessionRequirements_Dup(requirements));
    RGM_Collect(fep);
    ExecutionPlan* ep = RGM_Run(fep, ExecutionModeAsync, NULL, doneCallback, pd, err);
    RedisGears_FreeFlatExecution(fep);
    return ep;
}

static void RedisGearsPy_AddSessionRequirementsToDict(PythonSessionCtx* session){
    for(size_t i = 0 ; i < array_len(session->requirements) ; ++i){
        PythonRequirementCtx *req = session->requirements[i];
        if (!req->isInRequirementsDict) {
            req->refCount++; // pushing to requirements dictionary
            req->isInRequirementsDict = true;
            PythonRequirementCtx *oldReq = Gears_dictFetchValue(RequirementsDict, req->installName);
            if (oldReq) {
                Gears_dictDelete(RequirementsDict, oldReq->installName);
                oldReq->isInRequirementsDict = false;
                PythonRequirementCtx_Free(oldReq);
            }
            Gears_dictAdd(RequirementsDict, req->installName, req);
        }
    }
}

static void RedisGearsPy_DownloadWheelsAndDistribute(void* ctx){
    BackgroundDepsInstallCtx* bdiCtx = ctx;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bdiCtx->bc);
    if(!PythonSessionCtx_DownloadWheels(bdiCtx->session)){
        RedisModule_ReplyWithError(rctx, "Could not satisfy requirements (look at redis log file for more information)");
        RedisGears_LockHanlderAcquire(rctx);
        goto done;
    }

    RedisGears_LockHanlderAcquire(rctx);
    for(size_t i = 0 ; i < array_len(bdiCtx->session->requirements) ; ++i){
        PythonRequirementCtx *req = bdiCtx->session->requirements[i];
        if(!PythonRequirementCtx_InstallRequirement(req)){
            RedisModule_ReplyWithError(rctx, "Failed installing requirements on shard, please check shard logs for more details.");
            goto done;
        }
    }

    char *err;
    if (RedisGears_PutUsedSession(bdiCtx->session->srctx, bdiCtx->session, &err) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Failed serializing session");
        goto done;
    }
    // we gave the session to srctx so we need another copy of it.
    bdiCtx->session = PythonSessionCtx_ShallowCopy(bdiCtx->session);

    if (bdiCtx->oldSession) {
        RedisGears_AddSessionToUnlink(bdiCtx->session->srctx, bdiCtx->oldSession->sessionId);
        PythonSessionCtx_RunWithRegistrationsLock(bdiCtx->oldSession, {
            for (size_t i = 0 ; i < array_len(bdiCtx->oldSession->registrations) ; ++i) {
                RedisGears_AddRegistrationToUnregister(bdiCtx->session->srctx, bdiCtx->oldSession->registrations[i]);
            }
        });
    }

    if (RedisGearsPy_InnerExecute(rctx, bdiCtx) != REDISMODULE_OK) {
        goto done;
    }

done:
    // free bdiCtx
    if (bdiCtx->session->srctx) {
        RedisGears_SessionRegisterCtxFree(bdiCtx->session->srctx);
        bdiCtx->session->srctx = NULL;
    }
    PythonSessionCtx_Free(bdiCtx->session);
    if (bdiCtx->oldSession) PythonSessionCtx_Free(bdiCtx->oldSession);
    RG_FREE(bdiCtx->script);
    if(bdiCtx->bc){
        RedisModule_UnblockClient(bdiCtx->bc, NULL);
    }
    RG_FREE(bdiCtx);

    RedisGears_LockHanlderRelease(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void RedisGearsPy_BackgroundExecute(PythonSessionCtx* session,
                                    PythonSessionCtx* oldSession,
                                    RedisModuleBlockedClient *bc,
                                    const char* script,
                                    DoneCallbackFunction doneFunction,
                                    bool isBlocking){

    BackgroundDepsInstallCtx* bdiCtx = RG_ALLOC(sizeof(*bdiCtx));
    *bdiCtx = (BackgroundDepsInstallCtx){
            .session = session,
            .oldSession = oldSession,
            .bc = bc,
            .script = RG_STRDUP(script),
            .doneFunction = doneFunction,
            .isBlocking = isBlocking,
    };

    Gears_thpool_add_work(installDepsPool, RedisGearsPy_DownloadWheelsAndDistribute, bdiCtx);
}

#define CLUSTER_ERROR "ERRCLUSTER"
#define VERIFY_CLUSTER_INITIALIZE(c) if(!RedisGears_ClusterIsInitialized()) return RedisModule_ReplyWithError(c, CLUSTER_ERROR" Uninitialized cluster state")

static int RedisGearsPy_VerifySessionId(const char *id) {
    for (; *id ; ++id) {
        if (*id >= 'a' && *id <= 'z') {
            continue;
        }
        if (*id >= 'A' && *id <= 'Z') {
            continue;
        }
        if (*id >= '0' && *id <= '9') {
            continue;
        }
        if (*id == '_') {
            continue;
        }
        if (*id == '-') {
            continue;
        }
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define ID_SIZE 40
    char id[ID_SIZE + 1] = {0}; // buffer to generate session id

    int ctxFlags = RedisModule_GetContextFlags(ctx);

    if(ctxFlags & (REDISMODULE_CTX_FLAGS_LUA|REDISMODULE_CTX_FLAGS_MULTI|REDISMODULE_CTX_FLAGS_DENY_BLOCKING)){
        RedisModule_ReplyWithError(ctx, "Can not run gears inside a multi exec, lua, or when blocking is not allowed");
        return REDISMODULE_OK;
    }

    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    VERIFY_CLUSTER_INITIALIZE(ctx);

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
    char *sessionName = NULL;
    char *replaceWith = NULL;
    char *sessionDesc = NULL;
    bool isBlocking = true;
    bool upgrade = false;
    bool forceRequirementsReinstallation = false;
    size_t currArg = 2;
    for(; currArg < argc ; ++currArg) {
        const char* option = RedisModule_StringPtrLen(argv[currArg], NULL);
        if(strcasecmp(option, "UNBLOCKING") == 0){
            isBlocking = false;
            continue;
        }
        if(strcasecmp(option, "UPGRADE") == 0){
            upgrade = true;
            continue;
        }
        if(strcasecmp(option, "FORCE_REINSTALL_REQUIREMENTS") == 0){
            forceRequirementsReinstallation = true;
            continue;
        }
        if(strcasecmp(option, "REPLACE_WITH") == 0){
            if (++currArg >= argc) {
                RedisModule_ReplyWithError(ctx, "REPLACE_WITH is missing");
                return REDISMODULE_OK;
            }
            replaceWith = (char*)RedisModule_StringPtrLen(argv[currArg], NULL);
            continue;
        }
        if(strcasecmp(option, "ID") == 0){
            if (++currArg >= argc) {
                RedisModule_ReplyWithError(ctx, "ID is missing");
                return REDISMODULE_OK;
            }
            sessionName = (char*)RedisModule_StringPtrLen(argv[currArg], NULL);
            if (RedisGearsPy_VerifySessionId(sessionName) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "ID must be compose of: ['A'-'Z' | 'a'-'z' | '0' - '9' | '-' | '_']");
                return REDISMODULE_OK;
            }
            continue;
        }
        if(strcasecmp(option, "DESCRIPTION") == 0){
            if (++currArg >= argc) {
                RedisModule_ReplyWithError(ctx, "DESCRIPTION is missing");
                return REDISMODULE_OK;
            }
            sessionDesc = (char*)RedisModule_StringPtrLen(argv[currArg], NULL);
            continue;
        }
        break;
    }

    size_t reqLen = (argc > currArg) ? argc - currArg - 1 : 0;
    const char* requirementsList[reqLen];
    if (currArg < argc) {
        // last arguments must be requirements
        const char* requirements = RedisModule_StringPtrLen(argv[currArg++], NULL);
        if(strcasecmp(requirements, "REQUIREMENTS") == 0){
            RedisGearsPy_GetRequirementsList(requirementsList, argv + currArg, reqLen);
        }else{
            char* msg;
            RedisGears_ASprintf(&msg, "Unknown arguments were given: %s", requirements);
            RedisModule_ReplyWithError(ctx, msg);
            RG_FREE(msg);
            return REDISMODULE_OK;
        }
    }

    if (!sessionName) {
        RedisModule_GetRandomHexChars(id, ID_SIZE);
        sessionName = id;
    }

    PythonSessionCtx* oldSession = PythonSessionCtx_Get(sessionName);;
    if (replaceWith) {
        if (oldSession) {
            char* msg;
            RedisGears_ASprintf(&msg, "Can not replace an existing session %s with %s", sessionName, replaceWith);
            RedisModule_ReplyWithError(ctx, msg);
            RG_FREE(msg);
            PythonSessionCtx_Free(oldSession);
            return REDISMODULE_OK;
        }
        upgrade = true;
        oldSession = PythonSessionCtx_Get(replaceWith);
        if (!oldSession) {
            char* msg;
            RedisGears_ASprintf(&msg, "Can not replace with %s, session does not exists.", replaceWith);
            RedisModule_ReplyWithError(ctx, msg);
            RG_FREE(msg);
            return REDISMODULE_OK;
        }
    }
    if (oldSession) {
        if (!upgrade) {
            char* msg;
            RedisGears_ASprintf(&msg, "Session %s already exists", sessionName);
            RedisModule_ReplyWithError(ctx, msg);
            RG_FREE(msg);
            PythonSessionCtx_Free(oldSession);
            return REDISMODULE_OK;
        }
    }

    PythonSessionCtx* session = PythonSessionCtx_Create(sessionName, sessionDesc, requirementsList, reqLen, forceRequirementsReinstallation);
    if(!session){
        if (oldSession) PythonSessionCtx_Free(oldSession);
        RedisModule_ReplyWithError(ctx, "Could not satisfy requirements, look at the log file for more information.");
        return REDISMODULE_OK;
    }

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(session->isInstallationNeeded){
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        RedisGearsPy_BackgroundExecute(session, oldSession, bc, script, ptctx->doneFunction, isBlocking);
        return REDISMODULE_OK;
    }

    char *err;
    if (RedisGears_PutUsedSession(session->srctx, session, &err) != REDISMODULE_OK) {
        if (oldSession) PythonSessionCtx_Free(oldSession);
        RedisGears_SessionRegisterCtxFree(session->srctx);
        session->srctx = NULL;
        PythonSessionCtx_Free(session);
        RedisModule_ReplyWithError(ctx, "Failed serializing session");
        RG_FREE(err);
        return REDISMODULE_OK;
    }
    // we gave the session to srctx so we need another copy of it.
    session = PythonSessionCtx_ShallowCopy(session);

    if (oldSession) {
        RedisGears_AddSessionToUnlink(session->srctx, oldSession->sessionId);
        PythonSessionCtx_RunWithRegistrationsLock(oldSession, {
            for (size_t i = 0 ; i < array_len(oldSession->registrations) ; ++i) {
                RedisGears_AddRegistrationToUnregister(session->srctx, oldSession->registrations[i]);
            }
        });
    }

    BackgroundDepsInstallCtx bdiCtx = (BackgroundDepsInstallCtx){
            .session = session,
            .oldSession = oldSession,
            .bc = NULL,
            .script = (char*)script,
            .doneFunction = ptctx->doneFunction,
            .isBlocking = isBlocking,
    };

    RedisGearsPy_InnerExecute(ctx, &bdiCtx);

    // free bdiCtx
    if (bdiCtx.session->srctx) {
        RedisGears_SessionRegisterCtxFree(bdiCtx.session->srctx);
        bdiCtx.session->srctx = NULL;
    }
    PythonSessionCtx_Free(bdiCtx.session);
    if (bdiCtx.oldSession) PythonSessionCtx_Free(bdiCtx.oldSession);

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
    PyObject *pMsg = NULL;
    if (pType == GearsFlatError) {
        // flat error, return the message only!
        pMsg = PyObject_Str(pValue);
    }
    if (!pMsg) {
        PyObject *pModuleName = PyUnicode_FromString("traceback");
        PyObject *pModule = PyImport_Import(pModuleName);
        if(pythonConfig.attemptTraceback && pTraceback != NULL && pModule != NULL){
            PyObject *pFunc = PyObject_GetAttrString(pModule, "format_exception");
            if(pFunc != NULL){
                if(PyCallable_Check(pFunc)){
                    PyObject *pCall = PyObject_CallFunctionObjArgs(pFunc, pType, pValue, pTraceback, NULL);
                    if(pCall){
                        pMsg = PyObject_Str(pCall);
                        GearsPyDecRef(pCall);
                    }
                }
                GearsPyDecRef(pFunc);
            }
            GearsPyDecRef(pModule);
        }
        GearsPyDecRef(pModuleName);
    }
    if(!pMsg){
        PyObject *pStrFormat = PyUnicode_FromString("Error type: %s, Value: %s");
        PyObject* pStrType = PyObject_Str(pType);
        PyObject* pStrValue = PyObject_Str(pValue);
        PyObject *pArgs = PyTuple_New(2);
        PyTuple_SetItem(pArgs, 0, pStrType);
        PyTuple_SetItem(pArgs, 1, pStrValue);
        pMsg = PyUnicode_Format(pStrFormat, pArgs);
        GearsPyDecRef(pArgs);
        GearsPyDecRef(pStrFormat);
    }
    char *msg = (char*)PyUnicode_AsUTF8AndSize(pMsg, NULL);
    char* err =  RG_STRDUP(msg);
    GearsPyDecRef(pMsg);
    if(pType){
        GearsPyDecRef(pType);
    }
    if(pValue){
        GearsPyDecRef(pValue);
    }
    if(pTraceback){
        GearsPyDecRef(pTraceback);
    }
    return err;
}

void fetchPyError(ExecutionCtx* rctx) {
    RedisGears_SetError(rctx, getPyError());
}

PyObject* RedisGearsPy_PyCallbackHandleCoroutine(ExecutionCtx* rctx, PyObject* coro, PythonThreadCtx* ptctx){
    // object is a coroutine, we need to hold and pass it to the event loop.
   char* err = NULL;
   Record* asyncRecord = RedisGears_AsyncRecordCreate(rctx, &err);

   if(!asyncRecord){
       // failed creating async record, will return an error.
       PyErr_SetString(GearsError, err);
       RG_FREE(err);
       return NULL;
   }

   PyFuture* pyfuture = PyObject_New(PyFuture, &PyFutureType);
   pyfuture->asyncRecord = asyncRecord;
   pyfuture->continueType = ContinueType_Default;

   Py_INCREF(pyfuture);

   ptctx->pyfutureCreated = (PyObject*)pyfuture;

   Py_INCREF(ptctx->pyfutureCreated);

   PyObject* pArgs = PyTuple_New(4);
   PyTuple_SetItem(pArgs, 0, coro);
   PyTuple_SetItem(pArgs, 1, (PyObject*)pyfuture);
   PyTuple_SetItem(pArgs, 2, PyLong_FromLong(0));

   /* Create session object */
   PyExecutionSession* pyExSes = PyObject_New(PyExecutionSession, &PyExecutionSessionType);
   pyExSes->s = PythonSessionCtx_ShallowCopy(ptctx->currSession);
   pyExSes->crtCtx = RedisGears_GetCommandReaderTriggerCtx(rctx);
   pyExSes->cmdCtx = RedisGears_CommandCtxGet(rctx);
   if(pyExSes->crtCtx){
       pyExSes->crtCtx = RedisGears_CommandReaderTriggerCtxGetShallowCopy(pyExSes->crtCtx);
   }
   if(pyExSes->cmdCtx){
       pyExSes->cmdCtx = RedisGears_CommandCtxGetShallowCopy(pyExSes->cmdCtx);
   }
   PyTuple_SetItem(pArgs, 3, (PyObject*)pyExSes);

   PyObject* nn = PyObject_CallObject(runCoroutineFunction, pArgs);
   GearsPyDecRef(pArgs);

   if(!nn){
       char* err = getPyError();
       RedisModule_Log(staticCtx, "warning", "Error when runnong coroutine, error='%s'", err);
       RG_FREE(err);
       GearsPyDecRef((PyObject*)pyfuture);
       return NULL;
   }

   GearsPyDecRef(nn);

   return (PyObject*)pyfuture;
}

int RedisGearsPy_PyCallbackForEach(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // stop profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(ret && PyCoro_CheckExact(ret)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        ret = RedisGearsPy_PyCallbackHandleCoroutine(rctx, ret, ptctx);
    }

    if(ptctx->pyfutureCreated && ret == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)ret;
        future->continueType = ContinueType_Foreach;
        // no need to free the original record because the async record took the ownership on it

        // we need to free the future as we do not return it
        GearsPyDecRef(ret);
        RedisGearsPy_Unlock(&pectx);
        return RedisGears_StepHold;
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         */
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, RedisGears_GetDummyRecord());
        f->asyncRecord = NULL;
    }

    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        return RedisGears_StepSuccess;
    }

    if(ret != Py_None){
        Py_INCREF(Py_None);
    	GearsPyDecRef(ret);
    }

    RedisGearsPy_Unlock(&pectx);
    return RedisGears_StepSuccess;
}

static Record* RedisGearsPy_PyCallbackAccumulateByKey(ExecutionCtx* rctx, char* key, Record *accumulate, Record *r, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

	PyObject* pArgs = PyTuple_New(3);
	PyObject* callback = arg;
	PyObject* currObj = PyObjRecordGet(r);
	PyObject* keyPyStr = PyUnicode_FromString(key);
	PyObjRecordSet(r, NULL);
	PyObject* oldAccumulateObj = Py_None;
	if(!accumulate){
		accumulate = PyObjRecordCreate();
		Py_INCREF(oldAccumulateObj);
	}else{
		oldAccumulateObj = PyObjRecordGet(accumulate);
	}
	PyObjRecordSet(accumulate, NULL);
	PyTuple_SetItem(pArgs, 0, keyPyStr);
	PyTuple_SetItem(pArgs, 1, oldAccumulateObj);
	PyTuple_SetItem(pArgs, 2, currObj);
	if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
	PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
	if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
	GearsPyDecRef(pArgs);

	PythonThreadCtx* ptctx = GetPythonThreadCtx();

	if(newAccumulateObj && PyCoro_CheckExact(newAccumulateObj)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        newAccumulateObj = RedisGearsPy_PyCallbackHandleCoroutine(rctx, newAccumulateObj, ptctx);
    }

    if(ptctx->pyfutureCreated && newAccumulateObj == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)newAccumulateObj;
        future->continueType = ContinueType_Default;
        GearsPyDecRef(newAccumulateObj);
        RedisGearsPy_Unlock(&pectx);
        RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
        return RedisGears_GetDummyRecord();
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         *
         * Accumulateby can not be discarded with DummyRecord,
         * We will discard it with python record pointing to None
         */
        Record* discardRecord = PyObjRecordCreate();
        Py_INCREF(Py_None);
        PyObjRecordSet(discardRecord, Py_None);
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, discardRecord);
        f->asyncRecord = NULL;
    }

	if(!newAccumulateObj){
	    fetchPyError(rctx);

	    RedisGearsPy_Unlock(&pectx);
		RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
		return NULL;
	}

	PyObjRecordSet(accumulate, newAccumulateObj);

	RedisGearsPy_Unlock(&pectx);
    RedisGears_FreeRecord(r);
	return accumulate;
}

static Record* RedisGearsPy_PyCallbackAccumulate(ExecutionCtx* rctx, Record *accumulate, Record *r, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pArgs = PyTuple_New(2);
    PyObject* callback = arg;
    PyObject* currObj = PyObjRecordGet(r);
    PyObjRecordSet(r, NULL);
    RedisGears_FreeRecord(r);
    PyObject* oldAccumulateObj = Py_None;
    if(!accumulate){
        accumulate = PyObjRecordCreate();
        Py_INCREF(oldAccumulateObj);
    }else{
        oldAccumulateObj = PyObjRecordGet(accumulate);
    }
    PyObjRecordSet(accumulate, NULL);
    PyTuple_SetItem(pArgs, 0, oldAccumulateObj);
    PyTuple_SetItem(pArgs, 1, currObj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(newAccumulateObj && PyCoro_CheckExact(newAccumulateObj)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        newAccumulateObj = RedisGearsPy_PyCallbackHandleCoroutine(rctx, newAccumulateObj, ptctx);
    }

    if(ptctx->pyfutureCreated && newAccumulateObj == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)newAccumulateObj;
        future->continueType = ContinueType_Default;
        GearsPyDecRef(newAccumulateObj);
        RedisGears_FreeRecord(accumulate);
        RedisGearsPy_Unlock(&pectx);
        return RedisGears_GetDummyRecord();
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         *
         * Accumulate can not be discarded with DummyRecord,
         * We will discard it with python record pointing to None
         */
        Record* discardRecord = PyObjRecordCreate();
        Py_INCREF(Py_None);
        PyObjRecordSet(discardRecord, Py_None);
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, discardRecord);
        f->asyncRecord = NULL;
    }

    if(!newAccumulateObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        RedisGears_FreeRecord(accumulate);
        return NULL;
    }

    PyObjRecordSet(accumulate, newAccumulateObj);

    RedisGearsPy_Unlock(&pectx);
    return accumulate;
}

static Record* RedisGearsPy_PyCallbackMapper(ExecutionCtx* rctx, Record *record, void* arg){
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = PyObjRecordGet(record);
    PyObjRecordSet(record, NULL); // pass ownership of oldObj to NULL
    PyTuple_SetItem(pArgs, 0, oldObj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(newObj && PyCoro_CheckExact(newObj)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        newObj = RedisGearsPy_PyCallbackHandleCoroutine(rctx, newObj, ptctx);
    }

    if(ptctx->pyfutureCreated && newObj == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)newObj;
        future->continueType = ContinueType_Default;
        GearsPyDecRef(newObj);
        RedisGears_FreeRecord(record);
        RedisGearsPy_Unlock(&pectx);
        return RedisGears_GetDummyRecord();
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         */
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, RedisGears_GetDummyRecord());
        f->asyncRecord = NULL;
    }

    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        RedisGears_FreeRecord(record);
        return NULL;
    }

    PyObjRecordSet(record, newObj);

    RedisGearsPy_Unlock(&pectx);
    return record;
}

static Record* RedisGearsPy_PyCallbackFlatMapper(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = PyObjRecordGet(record);
    PyObjRecordSet(record, NULL);
    PyTuple_SetItem(pArgs, 0, oldObj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(newObj && PyCoro_CheckExact(newObj)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        newObj = RedisGearsPy_PyCallbackHandleCoroutine(rctx, newObj, ptctx);
    }

    if(ptctx->pyfutureCreated && newObj == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)newObj;
        future->continueType = ContinueType_Flat;

        RedisGears_FreeRecord(record);
        GearsPyDecRef(newObj);
        RedisGearsPy_Unlock(&pectx);
        return RedisGears_GetDummyRecord();
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         */
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, RedisGears_GetDummyRecord());
        f->asyncRecord = NULL;
    }

    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
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
        GearsPyDecRef(newObj);
    }else{
        PyObjRecordSet(record, newObj);
    }

    RedisGearsPy_Unlock(&pectx);
    return record;
}

static int RedisGearsPy_PyCallbackFilter(ExecutionCtx* rctx, Record *record, void* arg){
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // stop profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(ret && PyCoro_CheckExact(ret)){
        // object is a coroutine, we need to hold and pass it to the event loop.
        ret = RedisGearsPy_PyCallbackHandleCoroutine(rctx, ret, ptctx);
    }

    if(ptctx->pyfutureCreated && ret == ptctx->pyfutureCreated){
        PyFuture* future = (PyFuture*)ret;
        future->continueType = ContinueType_Filter;
        // no need to free the original record because the async record took the ownership on it

        // we need to free the future as we do not return it
        GearsPyDecRef(ret);
        RedisGearsPy_Unlock(&pectx);
        return RedisGears_StepHold;
    }

    if(ptctx->pyfutureCreated){
        /**
         * Async record created but not returned lets disscard it
         */
        PyFuture* f = (PyFuture*)ptctx->pyfutureCreated;
        RedisGears_AsyncRecordContinue(f->asyncRecord, RedisGears_GetDummyRecord());
        f->asyncRecord = NULL;
    }

    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        return RedisGears_StepFailed;
    }

    bool ret1 = PyObject_IsTrue(ret);

    GearsPyDecRef(ret);

    RedisGearsPy_Unlock(&pectx);
    return ret1? RedisGears_StepSuccess : RedisGears_StepFailed;
}

static char* RedisGearsPy_PyCallbackExtractor(ExecutionCtx* rctx, Record *record, void* arg, size_t* len){
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* obj = PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* ret = PyObject_CallObject(extractor, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    if(ret && PyCoro_CheckExact(ret)){
        GearsPyDecRef(ret);
        RedisGears_SetError(rctx, RG_STRDUP("coroutine are not allow on extractor"));
        RedisGearsPy_Unlock(&pectx);
        return "";
    }

    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        return "";
    }
    PyObject* retStr;
    if(!PyUnicode_Check(ret)){
        retStr = PyObject_Repr(ret);
        GearsPyDecRef(ret);
    }else{
        retStr = ret;
    }
    const char* retCStr = PyUnicode_AsUTF8AndSize(retStr, len);
    char* retValue = RG_ALLOC(*len + 1);
    memcpy(retValue, retCStr, *len);
    retValue[*len] = '\0';
    GearsPyDecRef(retStr);
    //GearsPyDecRef(retStr); todo: we should uncomment it after we will pass bool
    //                         that will tell the extractor to free the memory!!

    RedisGearsPy_Unlock(&pectx);
    return retValue;
}

static Record* RedisGearsPy_PyCallbackReducer(ExecutionCtx* rctx, char* key, size_t keyLen, Record *records, void* arg){
    RedisModule_Assert(RedisGears_RecordGetType(records) == RedisGears_GetListRecordType());

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* obj = PyList_New(0);
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(records) ; ++i){
        Record* r = RedisGears_ListRecordGet(records, i);
        RedisModule_Assert(RedisGears_RecordGetType(r) == pythonRecordType);
        PyObject* element = PyObjRecordGet(r);
        PyList_Append(obj, element);
    }
    PyObject* reducer = arg;
    PyObject* pArgs = PyTuple_New(2);
    PyObject* keyPyObj = PyUnicode_FromString(key);
    PyTuple_SetItem(pArgs, 0, keyPyObj);
    PyTuple_SetItem(pArgs, 1, obj);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStartFunction, "O", sctx->profiler);
    }
    PyObject* ret = PyObject_CallObject(reducer, pArgs);
    if (RedisGears_ProfileEnabled()) {
        // start profiling
        PyObject_CallFunction(profileStopFunction, "O", sctx->profiler);
    }
    GearsPyDecRef(pArgs);

    if(ret && PyCoro_CheckExact(ret)){
        GearsPyDecRef(ret);
        RedisGears_SetError(rctx, RG_STRDUP("coroutine are not allow on reduce"));
        RedisGearsPy_Unlock(&pectx);
        RedisGears_FreeRecord(records);
        return NULL;
    }

    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_Unlock(&pectx);
        RedisGears_FreeRecord(records);
        return NULL;
    }
    Record* retRecord = PyObjRecordCreate();
    PyObjRecordSet(retRecord, ret);

    RedisGearsPy_Unlock(&pectx);
    RedisGears_FreeRecord(records);
    return retRecord;
}

static Record* RedisGearsPy_ToPyRecordMapperInternal(ExecutionCtx* rctx, Record *record, void* arg){
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
    if(RedisGears_RecordGetType(record) == RedisGears_GetStringRecordType()){
        str = RedisGears_StringRecordGet(record, &len);
        // try to first decode it as string, if fails create a byte array.
        obj = PyUnicode_FromStringAndSize(str, len);
        if(!obj){
            PyErr_Clear();
            obj = PyByteArray_FromStringAndSize(str, len);
        }
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetLongRecordType()){
        longNum = RedisGears_LongRecordGet(record);
        obj = PyLong_FromLong(longNum);
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetDoubleRecordType()){
        doubleNum = RedisGears_DoubleRecordGet(record);
        obj = PyLong_FromDouble(doubleNum);
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetKeyRecordType()){
        key = RedisGears_KeyRecordGetKey(record, NULL);
        obj = PyDict_New();
        temp = PyUnicode_FromString(key);
        PyDict_SetItemString(obj, "key", temp);
        GearsPyDecRef(temp);
        tempRecord = RedisGears_KeyRecordGetVal(record);
        if(tempRecord){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(rctx, tempRecord, arg);
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyDict_SetItemString(obj, "value", PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }else{
            Py_INCREF(Py_None);
            PyDict_SetItemString(obj, "value", Py_None);
        }
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetListRecordType()){
        len = RedisGears_ListRecordLen(record);
        obj = PyList_New(0);
        for(size_t i = 0 ; i < len ; ++i){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(rctx, RedisGears_ListRecordGet(record, i), arg);
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyList_Append(obj, PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetHashSetRecordType()){
        keys = RedisGears_HashSetRecordGetAllKeys(record);
        obj = PyDict_New();
        for(size_t i = 0 ; i < array_len(keys) ; ++i){
            key = keys[i];
            temp = PyUnicode_FromString(key);
            tempRecord = RedisGears_HashSetRecordGet(record, key);
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(rctx, tempRecord, arg);
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyDict_SetItem(obj, temp, PyObjRecordGet(tempRecord));
            GearsPyDecRef(temp);
            RedisGears_FreeRecord(tempRecord);
        }
        array_free(keys);
    }else if(RedisGears_RecordGetType(record) == pythonRecordType){
        obj = PyObjRecordGet(record);
        Py_INCREF(obj);
    }else if(RedisGears_RecordGetType(record) == RedisGears_GetErrorRecordType()){
        str = RedisGears_StringRecordGet(record, &len);
        RedisGears_SetError(rctx, RG_STRDUP(str));

        Py_INCREF(Py_None);
        obj = Py_None;
    }else{
        RedisModule_Assert(false);
    }
    PyObjRecordSet(res, obj);
    return res;
}

#define IMPORT_REQ_INTERAL_COMMAND "RG.PYIMPORTRETINTERNAL"

static Record* RedisGearsPy_InstallRequirementsMapper(ExecutionCtx* rctx, Record *record, void* arg){
    PythonRequirementCtx** reqs = arg;

    RedisGears_FreeRecord(record);

    // first lets send the requirements to the slave/aof
    char* err = NULL;
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    RedisGears_BWWriteLong(&bw, PY_SESSION_REQ_VERSION);
    if(PythonSessionRequirements_Serialize(NULL, reqs, &bw, &err) != REDISMODULE_OK){
        if(!err){
            err = RG_STRDUP("Failed serialize requirement to slave/aof");
        }
        RedisGears_SetError(rctx, err);
        Gears_BufferFree(buff);
        return NULL;
    }

    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);
    RedisGears_LockHanlderAcquire(ctx);
    RedisModule_Replicate(ctx, IMPORT_REQ_INTERAL_COMMAND, "b", buff->buff, buff->size);
    RedisGears_LockHanlderRelease(ctx);

    Gears_BufferFree(buff);

    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        if(!PythonRequirementCtx_InstallRequirement(reqs[i])){
            RedisGears_SetError(rctx, RG_STRDUP("Failed install requirement on shard, check shard log for more info."));
            return NULL;
        }
    }

    return RedisGears_StringRecordCreate(RG_STRDUP("Done"), strlen("Done"));
}

static Record* RedisGearsPy_ToPyRecordMapper(ExecutionCtx* rctx, Record *record, void* arg){

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    Record* res = RedisGearsPy_ToPyRecordMapperInternal(rctx, record, arg);
    RedisGearsPy_Unlock(&pectx);

    RedisGears_FreeRecord(record);

    return res;
}

static void* RedisGearsPy_PyObjectDup(FlatExecutionPlan* fep, void* arg){
    RedisGearsPy_LOCK
    PyObject* obj = arg;
    Py_INCREF(obj);
    RedisGearsPy_UNLOCK
    return arg;
}

static void RedisGearsPy_PyObjectFree(FlatExecutionPlan* fep, void* arg){
    RedisGearsPy_LOCK
    PyObject* obj = arg;
    GearsPyDecRef(obj);
    RedisGearsPy_UNLOCK
}

static char* RedisGearsPy_PyObjectToString(FlatExecutionPlan* fep, void* arg){
    char* objCstr = NULL;
    RedisGearsPy_LOCK
    PyObject* obj = arg;
    PyObject *objStr = PyObject_Str(obj);
    const char* objTempCstr = PyUnicode_AsUTF8AndSize(objStr, NULL);
    objCstr = RG_STRDUP(objTempCstr);
    GearsPyDecRef(objStr);
    RedisGearsPy_UNLOCK
    return objCstr;
}

int RedisGearsPy_PyObjectSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    RedisGearsPy_LOCK
    PyObject* obj = arg;
    PyObject* objStr = PyMarshal_WriteObjectToString(obj, Py_MARSHAL_VERSION);
    if(!objStr){
        *err = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error occured on RedisGearsPy_PyObjectSerialize, error=%s", *err);
        RedisGearsPy_UNLOCK
        return REDISMODULE_ERR;
    }
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(objStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    GearsPyDecRef(objStr);
    RedisGearsPy_UNLOCK
    return REDISMODULE_OK;
}

void* RedisGearsPy_PyObjectDeserialize(Gears_BufferReader* br){
    RedisGearsPy_LOCK
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject* obj = PyMarshal_ReadObjectFromString(data, len);
    RedisGearsPy_UNLOCK
    return obj;
}

static int RedisGearsPy_PyCallbackSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    RedisGearsPy_LOCK
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
        RedisGears_ASprintf(err, "Error occured when serialized a python callback, callback=%s error=%s", nameCStr, internalErr);
        RG_FREE(internalErr);
        RedisGearsPy_UNLOCK
        return REDISMODULE_ERR;
    }
    GearsPyDecRef(args);
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(serializedStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    GearsPyDecRef(serializedStr);
    RedisGearsPy_UNLOCK
    return REDISMODULE_OK;
}

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > PY_OBJECT_TYPE_VERSION){
        *err = RG_STRDUP("unsupported python callback version");
        return NULL;
    }
    RedisGearsPy_LOCK

    if(fep){
        /* set requested_base_globals, cloud pickle was modified to look at this
         * variable and if set, use is as the global dictionary for the deserialized
         * functions. */
        PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
        PyDict_SetItemString(pyGlobals, "requested_base_globals", sctx->globalsDict);
    }

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
            RedisGears_ASprintf(err, "Error occured when deserialized a python callback, error=%s",  error);
            RedisModule_Log(staticCtx, "warning", "%s", *err);
        }else{
            RedisModule_Log(staticCtx, "warning", "Error occured when deserialized a python callback, error=%s",  error);
        }
        RG_FREE(error);
        GearsPyDecRef(args);
        RedisGearsPy_UNLOCK
        return NULL;
    }
    GearsPyDecRef(args);

    /* restore requested_base_globals */
    Py_INCREF(Py_None);
    PyDict_SetItemString(pyGlobals, "requested_base_globals", Py_None);

    RedisGearsPy_UNLOCK
    return callback;
}

long long totalAllocated = 0;
long long currAllocated = 0;
long long peakAllocated = 0;

static void* RedisGearsPy_AllocInternal(void* ctx, size_t size, bool useCalloc){
    void* allocated_address = NULL;
    if(size == 0){
        size = 1;
    }
    // 16-byte aligned is required
    int offset = 15 + sizeof(void *);
    size = size + offset;
    if(useCalloc){
        allocated_address = RG_CALLOC(1, size);
    }else{
        allocated_address = RG_ALLOC(size);
    }
    size_t mallocSize = RedisModule_MallocSize(allocated_address);
    totalAllocated += mallocSize;
    currAllocated += mallocSize;
    if(currAllocated > peakAllocated){
        peakAllocated = currAllocated;
    }
    void **aligned_address = (void **)(((size_t)(allocated_address) + offset) & (~15));
    aligned_address[-1] = allocated_address;
    return aligned_address;
}

static void* RedisGearsPy_Alloc(void* ctx, size_t size){
    return RedisGearsPy_AllocInternal(ctx, size, false);
}

static void* RedisGearsPy_Calloc(void* ctx, size_t n_elements, size_t size){
    return RedisGearsPy_AllocInternal(ctx, n_elements * size, true);
}

static void RedisGearsPy_Free(void* ctx, void * p){
    if(!p){
        return;
    }
    void *allocated_address = ((void **)p)[-1];
    size_t mallocSize = RedisModule_MallocSize(allocated_address);
    currAllocated -= mallocSize;
    RG_FREE(allocated_address);
}

static void* RedisGearsPy_Relloc(void* ctx, void * p, size_t size){
	if(!p){
		return RedisGearsPy_Alloc(ctx, size);
	}
	void *allocated_address = ((void **)p)[-1];
	size_t mallocSize = RedisModule_MallocSize(allocated_address);
	size_t dataSize = mallocSize - (p - allocated_address);
	if (size <= dataSize) {
	    // we have enough space, we can return p
	    return p;
	}
	void* new_add = RedisGearsPy_AllocInternal(ctx, size, false);
	memcpy(new_add, p, dataSize);
	RedisGearsPy_Free(ctx, p);
	return new_add;
}

static void RedisGearsPy_SendReqMetaData(RedisModuleCtx *ctx, PythonRequirementCtx* req){
    RedisModule_ReplyWithArray(ctx, 18);

    RedisModule_ReplyWithCString(ctx, "GearReqVersion");
    RedisModule_ReplyWithLongLong(ctx, PY_REQ_VERSION);

    RedisModule_ReplyWithCString(ctx, "Name");
    RedisModule_ReplyWithCString(ctx, req->installName);

    RedisModule_ReplyWithCString(ctx, "IsDownloaded");
    RedisModule_ReplyWithCString(ctx, req->isDownloaded ? "yes" : "no");

    RedisModule_ReplyWithCString(ctx, "IsInstalled");
    RedisModule_ReplyWithCString(ctx, req->isInstalled ? "yes" : "no");

    RedisModule_ReplyWithCString(ctx, "CompiledOs");
    RedisModule_ReplyWithCString(ctx, RedisGears_GetCompiledOs());

    RedisModule_ReplyWithCString(ctx, "Wheels");
    if(req->isDownloaded){
        RedisModule_ReplyWithArray(ctx, array_len(req->wheels));
        for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
            RedisModule_ReplyWithCString(ctx, req->wheels[i]);
        }
    }else{
        RedisModule_ReplyWithCString(ctx, "Requirement was not yet downloaded so wheels are not available");
    }

    RedisModule_ReplyWithCString(ctx, "Path");
    RedisModule_ReplyWithCString(ctx, req->basePath);

    RedisModule_ReplyWithCString(ctx, "RefCount");
    RedisModule_ReplyWithLongLong(ctx, req->refCount);

    RedisModule_ReplyWithCString(ctx, "IsInReqDictionary");
    RedisModule_ReplyWithCString(ctx, req->isInRequirementsDict ? "yes" : "no");
}


static void RedisGearsPy_DoneImportRequirement(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient* bc = privateData;

    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bc);

    size_t nErrors = RedisGears_GetErrorsLen(ep);

    if(nErrors > 0){
        Record* errorRecord = RedisGears_GetError(ep, 0);
        size_t errorLen;
        const char* errorStr = RedisGears_StringRecordGet(errorRecord, &errorLen);
        RedisModule_ReplyWithError(rctx, errorStr);
    }else{
        RedisModule_ReplyWithCString(rctx, "OK");
    }

    RedisModule_UnblockClient(bc, NULL);

    RedisGears_DropExecution(ep);

    RedisModule_FreeThreadSafeContext(rctx);
}

static int RedisGearsPy_ImportRequirementInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        RedisModule_Log(staticCtx, "warning", "On RedisGearsPy_ImportRequirementInternal, got bad arguments");
        RedisModule_ReplyWithError(ctx, "On RedisGearsPy_ImportRequirementInternal, got bad arguments");
        return REDISMODULE_OK;
    }

    size_t len;
    const char* data = RedisModule_StringPtrLen(argv[1], &len);
    Gears_Buffer buff = {
            .buff = (char*)data,
            .size = len,
            .cap = len,
    };

    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);

    int version = RedisGears_BRReadLong(&br);
    if(version == LONG_READ_ERROR){
        RedisModule_Log(staticCtx, "warning", "On RedisGearsPy_ImportRequirementInternal, failed deserialize requirements version");
        RedisModule_ReplyWithError(ctx, "On RedisGearsPy_ImportRequirementInternal, failed deserialize requirements version");
        return REDISMODULE_OK;
    }

    char* err = NULL;

    // fep is not used here
    PythonRequirementCtx** reqs = PythonSessionRequirements_Deserialize(NULL, &br, version, &err);

    if(!reqs){
        if(!err){
            err = RG_STRDUP("On RedisGearsPy_ImportRequirementInternal, failed deserialize requirements");
        }
        RedisModule_Log(staticCtx, "warning", "%s", err);
        RedisModule_ReplyWithError(ctx, err);
        return REDISMODULE_OK;
    }

    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        if(!PythonRequirementCtx_InstallRequirement(reqs[i])){
            RedisModule_Log(staticCtx, "warning", "On RedisGearsPy_ImportRequirementInternal, Failed install requirement on shard, check shard log for more info.");
            RedisModule_ReplyWithError(ctx, "On RedisGearsPy_ImportRequirementInternal, Failed install requirement on shard, check shard log for more info.");
            return REDISMODULE_OK;
        }

        // we are holding a shared ref to req lets free it
        PythonRequirementCtx_Free(reqs[i]);
    }

    array_free(reqs);

    return REDISMODULE_OK;
}

static void RedisGearsPy_DumpSingleSession(RedisModuleCtx *ctx, PythonSessionCtx* s, int verbose){
    RedisModule_ReplyWithArray(ctx, 16);
    RedisModule_ReplyWithCString(ctx, "ID");
    RedisModule_ReplyWithCString(ctx, s->sessionId);
    RedisModule_ReplyWithCString(ctx, "sessionDescription");
    if (s->sessionDesc) {
        RedisModule_ReplyWithCString(ctx, s->sessionDesc);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithCString(ctx, "refCount");
    RedisModule_ReplyWithLongLong(ctx, s->refCount);
    RedisModule_ReplyWithCString(ctx, "linked");
    RedisModule_ReplyWithCString(ctx, s->linkedTo == LinkedTo_None ? "None" : s->linkedTo == LinkedTo_PrimaryDict ? "primary" : "temprary");
    RedisModule_ReplyWithCString(ctx, "dead");
    RedisModule_ReplyWithCString(ctx, s->deadNode ? "true" : "false");
    RedisModule_ReplyWithCString(ctx, "requirementInstallationNeeded");
    RedisModule_ReplyWithLongLong(ctx, s->isInstallationNeeded);
    RedisModule_ReplyWithCString(ctx, "requirements");
    RedisModule_ReplyWithArray(ctx, array_len(s->requirements));
    for(size_t i = 0 ; i < array_len(s->requirements) ; ++i) {
        PythonRequirementCtx* req = s->requirements[i];
        if (verbose) {
            RedisGearsPy_SendReqMetaData(ctx, req);
        } else {
            RedisModule_ReplyWithCString(ctx, req->installName);
        }
    }
    RedisModule_ReplyWithCString(ctx, "registrations");

    PythonSessionCtx_RunWithRegistrationsLock(s, {
        RedisModule_ReplyWithArray(ctx, array_len(s->registrations));
        for(size_t i = 0 ; i < array_len(s->registrations) ; ++i) {
            const char *registrationId = s->registrations[i];
            if (verbose) {
                FlatExecutionPlan *fep = RedisGears_GetFepById(registrationId);
                if (!fep) {
                    RedisModule_ReplyWithCString(ctx, registrationId);
                } else {
                    RedisGears_DumpRegistration(ctx, fep, REGISTRATION_DUMP_NO_PD);
                }
            } else {
                RedisModule_ReplyWithCString(ctx, registrationId);
            }
        }
    });
}

static int RedisGearsPy_DumpSessions(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    pthread_mutex_lock(&PySessionsLock);
    int verbose = 0;
    int ts = 0;
    int tmp = 0;
    if (argc > 1) {
        // dump sessions by name
        size_t currArg = 1;
        for(; currArg < argc ; ++currArg) {
            const char* option = RedisModule_StringPtrLen(argv[currArg], NULL);
            if(strcasecmp(option, "VERBOSE") == 0){
                verbose = 1;
                continue;
            }
            if(strcasecmp(option, "DEAD") == 0){
                ts = 1;
                continue;
            }
            break;
        }

        if (currArg < argc) {
            const char* lastOption = RedisModule_StringPtrLen(argv[currArg++], NULL);
            if(strcasecmp(lastOption, "SESSIONS") == 0){
                RedisModule_ReplyWithArray(ctx, argc - currArg);
                for (size_t i = currArg ; i < argc ; ++i) {
                    const char* sessionName = RedisModule_StringPtrLen(argv[i], NULL);
                    // we can fetch session directly because we take the PySessionsLock
                    PythonSessionCtx* s = RedisModule_DictGetC(SessionsDict, (char*)sessionName, strlen(sessionName), NULL);
                    if (!s) {
                        char* msg;
                        RedisGears_ASprintf(&msg, "Session %s does not exists", sessionName);
                        RedisModule_ReplyWithCString(ctx, msg);
                        RG_FREE(msg);
                    } else {
                        RedisGearsPy_DumpSingleSession(ctx, s, verbose);
                    }
                }
            } else {
                char* msg;
                RedisGears_ASprintf(&msg, "Unknown option %s", lastOption);
                RedisModule_ReplyWithError(ctx, msg);
                RG_FREE(msg);
            }
            pthread_mutex_unlock(&PySessionsLock);
            return REDISMODULE_OK;
        }
        if (ts) {
            RedisModule_ReplyWithArray(ctx, Gears_listLength(DeadSessionsList));
            Gears_listIter *iter = Gears_listGetIterator(DeadSessionsList, AL_START_HEAD);
            Gears_listNode *n = NULL;
            while((n = Gears_listNext(iter))) {
                PythonSessionCtx* s = Gears_listNodeValue(n);
                RedisGearsPy_DumpSingleSession(ctx, s, verbose);
            }
            Gears_listReleaseIterator(iter);
            pthread_mutex_unlock(&PySessionsLock);
            return REDISMODULE_OK;
        }
    }
    // dump all sessions
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(SessionsDict, "^", NULL, 0);
    char* key;
    size_t keyLen;
    PythonSessionCtx* s;
    RedisModule_ReplyWithArray(ctx, RedisModule_DictSize(SessionsDict));
    while((key = RedisModule_DictNextC(iter, &keyLen, (void**)&s))){
        RedisGearsPy_DumpSingleSession(ctx, s, verbose);
    }
    RedisModule_DictIteratorStop(iter);
    pthread_mutex_unlock(&PySessionsLock);
    return REDISMODULE_OK;
}

static int RedisGearsPy_ProfileReset(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if (argc != 1){
        return RedisModule_WrongArity(ctx);
    }

    const char* strId = RedisModule_StringPtrLen(argv[0], NULL);

    PythonSessionCtx* s = PythonSessionCtx_Get(strId);
    if (!s) {
        RedisModule_ReplyWithError(ctx, "session does not exists");
        return REDISMODULE_OK;
    }

    PythonExecutionCtx pectx = PythonExecutionCtx_New(s, NULL);
    RedisGearsPy_Lock(&pectx);
    GearsPyDecRef(s->profiler);
    s->profiler = PyObject_CallFunction(profileCreateFunction, NULL);
    RedisGearsPy_Unlock(&pectx);
    PythonSessionCtx_Free(s);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

static int RedisGearsPy_ProfileStats(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if (argc < 0 || argc > 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* strId = RedisModule_StringPtrLen(argv[0], NULL);
    const char* order = "tottime";
    if (argc == 3) {
        order = RedisModule_StringPtrLen(argv[1], NULL);
    }

    PythonSessionCtx* s = PythonSessionCtx_Get(strId);
    if (!s) {
        RedisModule_ReplyWithError(ctx, "session does not exists");
        return REDISMODULE_OK;
    }

    PythonExecutionCtx pectx = PythonExecutionCtx_New(s, NULL);
    RedisGearsPy_Lock(&pectx);

    PyObject* res = PyObject_CallFunction(profileGetInfoFunction, "Os", s->profiler, order);
    if (!res) {
        char* error = getPyError();
        if (!error) {
            error = RG_STRDUP("Failed getting profile information");
        }
        RedisModule_ReplyWithError(ctx, error);
        RG_FREE(error);
        RedisGearsPy_Unlock(&pectx);
        PythonSessionCtx_Free(s);
        return REDISMODULE_OK;
    }
    PyObject* statsPyStr = PyObject_Str(res);
    size_t len;
    const char* statsCStr = PyUnicode_AsUTF8AndSize(statsPyStr, &len);
    RedisModule_ReplyWithStringBuffer(ctx, statsCStr, len);
    GearsPyDecRef(statsPyStr);
    GearsPyDecRef(res);

    RedisGearsPy_Unlock(&pectx);
    PythonSessionCtx_Free(s);

    return REDISMODULE_OK;
}

static int RedisGearsPy_Profile(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 2) {
		return RedisModule_WrongArity(ctx);
	}
	const char* subCommand = RedisModule_StringPtrLen(argv[1], NULL);
	if (strcasecmp(subCommand, "stats") == 0) {
		return RedisGearsPy_ProfileStats(ctx, argv + 2, argc - 2);
	} else if (strcasecmp(subCommand, "reset") == 0) {
		return RedisGearsPy_ProfileReset(ctx, argv + 2, argc - 2);
	}

	RedisModule_ReplyWithError(ctx, "Wrong subcommand given to RG.PYPROFILE");
	return REDISMODULE_OK;
}

static int RedisGearsPy_DumpRequirements(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){

    RedisModule_ReplyWithArray(ctx, Gears_dictSize(RequirementsDict));
    Gears_dictIterator *iter = Gears_dictGetIterator(RequirementsDict);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        PythonRequirementCtx* req = Gears_dictGetVal(entry);
        RedisGearsPy_SendReqMetaData(ctx, req);
    }
    Gears_dictReleaseIterator(iter);

    return REDISMODULE_OK;
}

static int RedisGearsPy_ImportRequirement(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    VERIFY_CLUSTER_INITIALIZE(ctx);

    Gears_Buffer* buff = Gears_BufferCreate();

    for(size_t i = 1 ; i < argc ; i++){
        size_t len;
        const char* d = RedisModule_StringPtrLen(argv[i], &len);
        Gears_BufferAdd(buff, d, len);
    }

    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, buff);

    char* err = NULL;
    PythonRequirementCtx* req = PythonRequirement_Deserialize(&br, &err);
    if(!req){
        if(!err){
            err = RG_STRDUP("Failed deserialize requirement");
        }
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
        Gears_BufferFree(buff);
        return REDISMODULE_OK;
    }

    Gears_BufferFree(buff);

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    PythonRequirementCtx** reqs = array_new(PythonRequirementCtx*, 1);
    reqs = array_append(reqs, req);
    ExecutionPlan* ep = RedisGearsPy_DistributeRequirements(reqs, RedisGearsPy_DoneImportRequirement, bc, &err);
    if(!ep){
        // error here leave us in a state where we have the requirement but others
        // do not, user will see the error and will have to retry.
        if(!err){
            err = RG_STRDUP("Failed create distribute requirements execution on rg.importreq");
        }
        RedisModule_AbortBlock(bc);
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
    }

    // We are holding a shared ref to the requirement, we need to free it.
    // Notice that this will not cause the requirement to be freed.
    PythonRequirementCtx_Free(req);
    array_free(reqs);

    return REDISMODULE_OK;
}

static int RedisGearsPy_ExportRequirement(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* reqName = RedisModule_StringPtrLen(argv[1], NULL);

    // Notice that this increase the requirement ref count so we must free it as the end.
    PythonRequirementCtx* req = PythonRequirementCtx_Get(reqName);

    if(!req){
        RedisModule_ReplyWithError(ctx, "requirement does not exists");
        return REDISMODULE_OK;
    }

    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    char* err = NULL;
    if(PythonRequirement_Serialize(req, &bw, &err) != REDISMODULE_OK){
        PythonRequirementCtx_Free(req);
        Gears_BufferFree(buff);
        if(!err){
            err = RG_STRDUP("Failed serializing requirement");
        }
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 2);

    RedisGearsPy_SendReqMetaData(ctx, req);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

#define BULK_LEN (1024*1024*10); // 10 MB
    int maxBulKLen = BULK_LEN;

    size_t currPos = 0 ;
    size_t arrayLen = 0;
    while(currPos < buff->size){
        size_t currLen = MIN(maxBulKLen, (buff->size - currPos));
        RedisModule_ReplyWithStringBuffer(ctx, buff->buff + currPos, currLen);
        currPos += currLen;
        ++arrayLen;
    }

    RedisModule_ReplySetArrayLength(ctx, arrayLen);

    Gears_BufferFree(buff);
    PythonRequirementCtx_Free(req);
    return REDISMODULE_OK;
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
    RedisModule_Assert(sctx);

    PythonExecutionCtx pectx = PythonExecutionCtx_New(sctx, rctx);
    RedisGearsPy_Lock(&pectx);

    PyObject* pyRecord = NULL;
    if(!pyCtx->generator){
        PyObject* pArgs = PyTuple_New(0);
        PyObject* callback = pyCtx->callback;
        pyRecord = PyObject_CallObject(callback, pArgs);
        GearsPyDecRef(pArgs);
        if(!pyRecord){
            fetchPyError(rctx);
            RedisGearsPy_Unlock(&pectx);
            pyCtx->isDone = true;
            return NULL;
        }
        if(PyGen_Check(pyRecord)) {
            pyCtx->generator = PyObject_GetIter(pyRecord);
            GearsPyDecRef(pyRecord);
            pyRecord = PyIter_Next(pyCtx->generator);
        }
    }else{
        pyRecord = PyIter_Next(pyCtx->generator);
    }
    if(!pyRecord){
        fetchPyError(rctx);
        RedisGearsPy_Unlock(&pectx);
        pyCtx->isDone = true;
        return NULL;
    }
    RedisGearsPy_Unlock(&pectx);
    Record* record = PyObjRecordCreate();
    PyObjRecordSet(record, pyRecord);
    return record;
}

static void PythonReader_Free(void* ctx){
    PythonReaderCtx* pyCtx = ctx;
    PyObject* callback = pyCtx->callback;
    RedisGearsPy_LOCK
    GearsPyDecRef(callback);
    if(pyCtx->generator){
        GearsPyDecRef(pyCtx->generator);
    }
    RedisGearsPy_UNLOCK
    RG_FREE(pyCtx);
}

static int PythonReader_Serialize(ExecutionCtx* ectx, void* ctx, Gears_BufferWriter* bw){
    PythonReaderCtx* pyCtx = ctx;
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    char* err = NULL;
    int ret = RedisGearsPy_PyCallbackSerialize(RedisGears_GetFep(ep), pyCtx->callback, bw, &err);
    if(ret != REDISMODULE_OK){
        RedisGears_SetError(ectx, err);
    }
    return ret;
}

static int PythonReader_Deserialize(ExecutionCtx* ectx, void* ctx, Gears_BufferReader* br){
    PythonReaderCtx* pyCtx = ctx;
    // this serialized data reached from another shard (not rdb) so its save to assume the version is PY_OBJECT_TYPE_VERSION
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    char* err = NULL;
    pyCtx->callback = RedisGearsPy_PyCallbackDeserialize(RedisGears_GetFep(ep), br, PY_OBJECT_TYPE_VERSION, &err);
    if(!pyCtx->callback){
        RedisGears_SetError(ectx, err);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static Reader* PythonReader_Create(void* arg){
    PyObject* callback = arg;
    if(callback){
        RedisGearsPy_LOCK
        Py_INCREF(callback);
        RedisGearsPy_UNLOCK
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

static char* PYINSTALL_DIR;
static char* PYENV_DIR;
static char* PYENV_HOME_DIR;
static char* PYENV_BIN_DIR;
static char* PYENV_ACTIVATE;
static char* PYENV_ACTIVATE_SCRIPT;

static bool PathExist(const char* path) {
    DIR* dir = opendir(path);
    if (dir) {
        closedir(dir);
        return true;
    }
    return false;
}

static bool PyEnvExist() {
    return PathExist(PYENV_DIR);
}

static void InitializeGlobalPaths(){
    const char* moduleDataDir = getenv("modulesdatadir");
    if(moduleDataDir){
        // modulesdatadir env var exists, we are running on redis enterprise and we need to run on modules directory
        RedisGears_ASprintf(&PYINSTALL_DIR, "%s/%s/%d/deps/", moduleDataDir, "rg", RedisGears_GetVersion());
        RedisGears_ASprintf(&PYENV_DIR, "%s/gears_python/python3_%s/", PYINSTALL_DIR, RedisGears_GetVersionStr());
    }else{
        // try build path first if its exists
#ifdef CPYTHON_PATH
        RedisGears_ASprintf(&PYINSTALL_DIR, "%s/", CPYTHON_PATH);

        // we create it temporary to check if exists
        RedisGears_ASprintf(&PYENV_DIR, "%s/python3_%s/", PYINSTALL_DIR, RedisGears_GetVersionStr());

        if(!PyEnvExist()){
            RG_FREE(PYINSTALL_DIR);
            RedisGears_ASprintf(&PYINSTALL_DIR, "%s/", pythonConfig.pythonInstallationDir);
        }

        RG_FREE(PYENV_DIR);
#else
        RedisGears_ASprintf(&PYINSTALL_DIR, "%s/", pythonConfig.pythonInstallationDir);
#endif

        RedisGears_ASprintf(&PYENV_DIR, "%s/python3_%s/", PYINSTALL_DIR, RedisGears_GetVersionStr());
    }
    RedisGears_ASprintf(&PYENV_HOME_DIR, "%s/.venv/", PYENV_DIR);
    RedisGears_ASprintf(&PYENV_BIN_DIR, "%s/bin", PYENV_HOME_DIR);
    RedisGears_ASprintf(&PYENV_ACTIVATE, "%s/activate_this.py", PYENV_BIN_DIR);
    RedisGears_ASprintf(&PYENV_ACTIVATE_SCRIPT, "%s/activate", PYENV_BIN_DIR);
}

static void PrintGlobalPaths(RedisModuleCtx* ctx){
    RedisModule_Log(staticCtx, "notice", "PYENV_DIR: %s", PYENV_DIR);
    RedisModule_Log(staticCtx, "notice", "PYENV_HOME_DIR: %s", PYENV_HOME_DIR);
    RedisModule_Log(staticCtx, "notice", "PYENV_BIN_DIR: %s", PYENV_BIN_DIR);
    RedisModule_Log(staticCtx, "notice", "PYENV_ACTIVATE: %s", PYENV_ACTIVATE);
    RedisModule_Log(staticCtx, "notice", "PYENV_ACTIVATE_SCRIPT: %s", PYENV_ACTIVATE_SCRIPT);
}

static int RedisGears_InstallDeps(RedisModuleCtx *ctx) {
#define SHA_256_SIZE            64
#define TMP_DEPS_FILE_PATH_FMT  "/tmp/deps.%s.%s.tgz"
#define TMP_DEPS_FILE_DIR_FMT   "/tmp/deps.%s.%s/"
#define LOCAL_VENV_FMT          PYENV_DIR"/%s"

    const char *no_deps = getenv("GEARS_NO_DEPS");
    bool skip_deps_install = (no_deps && !strcmp(no_deps, "1")) || !pythonConfig.downloadDeps;
    if(!skip_deps_install && RedisGears_IsEnterprise()){
        skip_deps_install = !pythonConfig.foreceDownloadDepsOnEnterprise;
    }
    const char* shardUid = RedisGears_GetShardIdentifier();
    if (!PyEnvExist()){
        if (skip_deps_install) {
            RedisModule_Log(staticCtx, "warning", "No Python installation found and auto install is not enabled, aborting.");
            return REDISMODULE_ERR;
        }
        const char* expectedSha256 = pythonConfig.gearsPythonSha256;

        RedisGears_ExecuteCommand(ctx, "notice", "rm -rf "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);

        RedisGears_ExecuteCommand(ctx, "notice", "curl -o "TMP_DEPS_FILE_PATH_FMT" %s", shardUid, expectedSha256, pythonConfig.gearsPythonUrl);

        char* sha256Command;
        RedisGears_ASprintf(&sha256Command, "sha256sum "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);
        FILE* f = popen(sha256Command, "r");
        RG_FREE(sha256Command);
        char sha256[SHA_256_SIZE];
        if(fscanf(f, "%64s", sha256) != 1){
            RedisModule_Log(staticCtx, "warning", "Failed to calculate sha25 on file "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);
            pclose(f);
            return REDISMODULE_ERR;
        }
        pclose(f);

        if(strcmp(expectedSha256, sha256) != 0){
            RedisModule_Log(staticCtx, "warning", "Failed on sha 256 comparison");
            return REDISMODULE_ERR;
        }

        RedisGears_ExecuteCommand(ctx, "notice", "rm -rf "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256);
        RedisGears_ExecuteCommand(ctx, "notice", "mkdir -p "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256);

        RedisGears_ExecuteCommand(ctx, "notice", "tar -xvzf "TMP_DEPS_FILE_PATH_FMT" -C "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256, shardUid, expectedSha256);

        RedisGears_ExecuteCommand(ctx, "notice", "mkdir -p %s", PYINSTALL_DIR);
        RedisGears_ExecuteCommand(ctx, "notice", "mv "TMP_DEPS_FILE_DIR_FMT"/python3_%s/ %s", shardUid, expectedSha256, RedisGears_GetVersionStr(), PYINSTALL_DIR);
    }else{
        RedisModule_Log(staticCtx, "notice", "Found python installation under: %s", PYENV_DIR);
    }
    if(pythonConfig.createVenv){
        const char* moduleDataDir = getenv("modulesdatadir");
        if(moduleDataDir){
            RedisGears_ASprintf(&venvDir, "%s/%s/.venv-%s", moduleDataDir, "rg", shardUid);
        } else {
            RedisGears_ASprintf(&venvDir, "%s/.venv-%s", pythonConfig.pythonInstallationDir, shardUid);
        }

        DIR* dir = opendir(venvDir);
        if(!dir){
            RedisGears_ExecuteCommand(ctx, "notice", "mkdir -p %s", venvDir);
            setenv("VIRTUALENV_OVERRIDE_APP_DATA", venvDir, 1);
            int rc = RedisGears_ExecuteCommand(ctx, "notice", "/bin/bash -c \"%s/bin/python3 -m virtualenv %s\"", PYENV_DIR, venvDir);
            if (rc) {
                RedisModule_Log(staticCtx, "warning", "Failed to construct virtualenv");
                RedisGears_ExecuteCommand(ctx, "notice", "rm -rf %s", venvDir);
                return REDISMODULE_ERR;
            }
        }else{
            RedisModule_Log(staticCtx, "notice", "Found venv installation under: %s", venvDir);
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
    RedisModule_Assert(dir);
    closedir(dir);
    char* activateScript = NULL;
    char* activateCommand = NULL;
    RedisGears_ASprintf(&activateScript, "%s/bin/activate_this.py", venvDir);
    RedisGears_ASprintf(&activateCommand, "exec(open('%s').read(), {'__file__': '%s'})", activateScript, activateScript);
    RedisModule_Log(staticCtx, "notice", "Initializing Python environment with: %s", activateCommand);
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
        RedisGears_RecordSendReply(rgl, rctx);
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
        if(len > 1){
            if(str[0] == '+'){
                RedisModule_ReplyWithSimpleString(rctx, str + 1);
                return REDISMODULE_OK;
            }else if(str[0] == '-'){
                RedisModule_ReplyWithError(rctx, str + 1);
                return REDISMODULE_OK;
            }
        }
        RedisModule_ReplyWithStringBuffer(rctx, str, len);
    }else if(PyBytes_Check(obj)) {
        size_t len;
        char* str;
        PyBytes_AsStringAndSize(obj, &str, &len);
        RedisModule_ReplyWithStringBuffer(rctx, str, len);
    }else{
        RedisModule_ReplyWithStringBuffer(rctx, "PY RECORD", strlen("PY RECORD"));
    }
    return REDISMODULE_OK;
}

static int PythonRecord_Serialize(ExecutionCtx* ectx, Gears_BufferWriter* bw, Record* base){
    PythonRecord* r = (PythonRecord*)base;
    char* err = NULL;
    if(RedisGearsPy_PyObjectSerialize(r->obj, bw, &err) != REDISMODULE_OK){
        RedisGears_SetError(ectx, err);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static void PythonRecord_Free(Record* base){
    PythonRecord* record = (PythonRecord*)base;
    if(record->obj && record->obj != Py_None){
        RedisGearsPy_LOCK
        GearsPyDecRef(record->obj);
        RedisGearsPy_UNLOCK
    }
}

static Record* PythonRecord_Deserialize(ExecutionCtx* ectx, Gears_BufferReader* br){
    Record* r = PyObjRecordCreate();
    PyObject* obj = RedisGearsPy_PyObjectDeserialize(br);
    PyObjRecordSet(r, obj);
    return r;
}

#define REDISGEARSPY_REQ_DATATYPE_NAME "GEAR_REQ0"
#define REDISGEARSPY_REQ_DATATYPE_VERSION 1

static void RedisGearsPy_ClearRequirements(){
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
        Gears_dictDelete(RequirementsDict, reqs[i]->installName);
        reqs[i]->isInRequirementsDict = false;
        PythonRequirementCtx_Free(reqs[i]);
    }

    array_free(reqs);
}

static void RedisGearsPy_ClearAllData(){
    RedisGearsPy_ClearRequirements();
    PythonSessionCtx** sessions = array_new(PythonSessionCtx*, 10);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(SessionsDict, "^", NULL, 0);
    char* key;
    size_t keyLen;
    PythonSessionCtx* s;
    while((key = RedisModule_DictNextC(iter, &keyLen, (void**)&s))){
        sessions = array_append(sessions, s);
    }
    RedisModule_DictIteratorStop(iter);

    for (size_t i = 0 ; i < array_len(sessions) ; ++i) {
        PythonSessionCtx_Unlink(sessions[i]);
    }
    array_free(sessions);

    RedisModule_FreeDict(staticCtx, SessionsDict);
    SessionsDict = RedisModule_CreateDict(staticCtx);
}

static void RedisGearsPy_OnLoadedEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data){
    if(subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
            subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
            subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START){
        RedisGearsPy_ClearAllData();
    }
}

static int RedisGearsPy_LoadRegistrations(RedisModuleIO *rdb, int encver, int when){
    if(encver > REDISGEARSPY_REQ_DATATYPE_VERSION){
        RedisModule_LogIOError(rdb, "warning", "Give requirement version is not supported, please upgrade to newer RedisGears.");
        return REDISMODULE_ERR;
    }

    RedisGearsPy_ClearAllData();

    char* err = NULL;
    char *data = NULL;
    PythonRequirementCtx* req = NULL;
    while(RedisModule_LoadUnsigned(rdb)){
        size_t len;
        char *data = RedisModule_LoadStringBuffer(rdb, &len);
        Gears_Buffer buff = {
                .buff = data,
                .size = len,
                .cap = len,
        };
        Gears_BufferReader br;
        Gears_BufferReaderInit(&br, &buff);
        req = PythonRequirement_Deserialize(&br, &err);
        if(!req){
            goto error;
        }

        // now we also need to install the requirement
        if(!PythonRequirementCtx_InstallRequirement(req)){
            err = RG_STRDUP("Failed install requirement on shard, check shard log for more info.");
            goto error;
        }

        RedisModule_Free(data);
    }
    return REDISMODULE_OK;

error:
    RedisModule_LogIOError(rdb, "warning", "Failed deserialize requirements (%s)", err ? err : "unknown error");
    if(err){
        RG_FREE(err);
    }
    if(data){
        RedisModule_Free(data);
    }
    if(req){
        PythonRequirementCtx_Free(req);
    }
    RedisGearsPy_ClearRequirements();
    return REDISMODULE_ERR;
}

static void RedisGearsPy_SaveRegistrations(RedisModuleIO *rdb, int when){
    Gears_dictIterator *iter = Gears_dictGetIterator(RequirementsDict);
    Gears_dictEntry *entry = NULL;
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    while((entry = Gears_dictNext(iter))){
        PythonRequirementCtx* req = Gears_dictGetVal(entry);
        Gears_BufferClear(buff);
        char* err = NULL;
        if(PythonRequirement_Serialize(req, &bw, &err) != REDISMODULE_OK){
            RedisModule_LogIOError(rdb, "warning", "Failed serializing requirement %s (%s)", req->installName, err ? err : "Unknow error");
            RG_FREE(err);
        }else{
            // indicate there is another requirement
            RedisModule_SaveUnsigned(rdb, 1);
            RedisModule_SaveStringBuffer(rdb, buff->buff, buff->size);
        }
    }
    // indicate we are done
    RedisModule_SaveUnsigned(rdb, 0);
    Gears_dictReleaseIterator(iter);
    Gears_BufferFree(buff);
}

static int RedisGearsPy_CreateRequirementsDataType(RedisModuleCtx* ctx){
    RedisModuleTypeMethods methods = {
            .version = REDISMODULE_TYPE_METHOD_VERSION,
            .rdb_load = NULL,
            .rdb_save = NULL,
            .aof_rewrite = NULL,
            .mem_usage = NULL,
            .digest = NULL,
            .free = NULL,
            .aux_load = RedisGearsPy_LoadRegistrations,
            .aux_save = RedisGearsPy_SaveRegistrations,
            .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
        };

    RedisModuleType* GearsPyRequirementDT = RedisModule_CreateDataType(ctx, REDISGEARSPY_REQ_DATATYPE_NAME, REDISGEARSPY_REQ_DATATYPE_VERSION, &methods);
    return GearsPyRequirementDT ? REDISMODULE_OK : REDISMODULE_ERR;
}

static void PythonThreadCtx_Destructor(void *p) {
    RG_FREE(p);
}

static int PythonConfig_ToLong(const char* val, long long* res){
    RedisModuleString* valRedisStr = RedisModule_CreateString(NULL, val, strlen(val));
    int ret = RedisModule_StringToLongLong(valRedisStr, res);
    RedisModule_FreeString(NULL, valRedisStr);
    return ret;
}

#define CONFIG_FLAG_GT_ZERO 0x01
#define CONFIG_FLAG_IGNORE_DEFAULT 0x02

extern char GearsPythonUrl[];
extern char GearsPythonSha256[];

static int PythonConfig_GetIntVal(const char* val, int defaultVal, const char* newVal, int* res, int flags){
    const char *valStr = newVal;
    if(!valStr){
        valStr = RedisGears_GetConfig(val);
    }
    if(!valStr){
        if(flags & CONFIG_FLAG_IGNORE_DEFAULT){
            return REDISMODULE_ERR;
        }
        *res = defaultVal;
        return REDISMODULE_OK;
    }
    long long longVal;
    if(PythonConfig_ToLong(valStr, &longVal) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Failed parsing %s config, value must be a number.", val);
        return REDISMODULE_ERR;
    }

    if(flags & CONFIG_FLAG_GT_ZERO){
        if(longVal <= 0){
            RedisModule_Log(staticCtx, "warning", "Failed parsing %s config, value must be greater then zero.", val);
            return REDISMODULE_ERR;
        }
    }

    *res = longVal;
    return REDISMODULE_OK;
}

static int PythonConfig_GetBoolVal(const char* val, int defaultVal, const char* newVal, int* res, int flags){
    const char *valStr = newVal;
    if(!valStr){
        valStr = RedisGears_GetConfig(val);
    }
    if(!valStr){
        if(flags & CONFIG_FLAG_IGNORE_DEFAULT){
            return REDISMODULE_ERR;
        }
        *res = defaultVal;
        return REDISMODULE_OK;
    }
    long long longVal;
    if(PythonConfig_ToLong(valStr, &longVal) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Failed parsing %s config value.", val);
        return REDISMODULE_ERR;
    }
    *res = (longVal != 0);
    return REDISMODULE_OK;
}

static int PythonConfig_GetStrVal(const char* val, const char* defaultVal, const char* newVal, char** res, int flags){
    const char *valStr = newVal;
    if(!valStr){
        valStr = RedisGears_GetConfig(val);
    }
    if(!valStr){
        if(flags & CONFIG_FLAG_IGNORE_DEFAULT){
            return REDISMODULE_ERR;
        }
        valStr = defaultVal;
    }
    *res = RG_STRDUP(valStr);
    return REDISMODULE_OK;
}

#define CreateVenvConfigName "CreateVenv"
#define DownloadDepsConfigName "DownloadDeps"
#define ForeceDownloadDepsOnEnterpriseConfigName "ForeceDownloadDepsOnEnterprise"
#define PythonAttemptTracebackConfigName "PythonAttemptTraceback"
#define PythonOverrideAllocatorsConfigName "OverridePythonAllocators"
#define PythonInstallReqMaxIdleTimeConfigName "PythonInstallReqMaxIdleTime"
#define GearsPythonUrlConfigName "DependenciesUrl"
#define GearsPythonSha256ConfigName "DependenciesSha256"
#define PythonInstallationDirConfigName "PythonInstallationDir"

typedef enum PythonConfigType{
    PythonConfigType_Bool, PythonConfigType_Int, PythonConfigType_Str
}PythonConfigType;

typedef struct PythonConfigValDef{
    const char* name;
    PythonConfigType type;
    bool configurableAtRuntime;
    union{
        int defaultBoolVal;
        int defaultIntVal;
        const char* defaultStrVal;
    };
    void* ptr;
    int flags;
}PythonConfigValDef;

PythonConfigValDef configDefs[] = {
        {
                .name = CreateVenvConfigName,
                .type = PythonConfigType_Bool,
                .configurableAtRuntime = false,
                .defaultBoolVal = 0,
                .ptr = &pythonConfig.createVenv,
        },
        {
                .name = DownloadDepsConfigName,
                .type = PythonConfigType_Bool,
                .configurableAtRuntime = false,
                .defaultBoolVal = 0,
                .ptr = &pythonConfig.downloadDeps,
        },
        {
                .name = ForeceDownloadDepsOnEnterpriseConfigName,
                .type = PythonConfigType_Bool,
                .configurableAtRuntime = false,
                .defaultBoolVal = 0,
                .ptr = &pythonConfig.foreceDownloadDepsOnEnterprise,
        },
        {
                .name = PythonAttemptTracebackConfigName,
                .type = PythonConfigType_Bool,
                .configurableAtRuntime = true,
                .defaultBoolVal = 1,
                .ptr = &pythonConfig.attemptTraceback,
        },
        {
                .name = PythonOverrideAllocatorsConfigName,
                .type = PythonConfigType_Bool,
                .configurableAtRuntime = false,
                .defaultBoolVal = 1,
                .ptr = &pythonConfig.overrideAllocators,
        },
        {
                .name = PythonInstallReqMaxIdleTimeConfigName,
                .type = PythonConfigType_Int,
                .configurableAtRuntime = true,
                .defaultIntVal = 30000,
                .ptr = &pythonConfig.installReqMaxIdleTime,
                .flags = CONFIG_FLAG_GT_ZERO,
        },
        {
                .name = GearsPythonUrlConfigName,
                .type = PythonConfigType_Str,
                .configurableAtRuntime = false,
                .defaultStrVal = GearsPythonUrl,
                .ptr = &pythonConfig.gearsPythonUrl,
        },
        {
                .name = GearsPythonSha256ConfigName,
                .type = PythonConfigType_Str,
                .configurableAtRuntime = false,
                .defaultStrVal = GearsPythonSha256,
                .ptr = &pythonConfig.gearsPythonSha256,
        },
        {
                .name = PythonInstallationDirConfigName,
                .type = PythonConfigType_Str,
                .configurableAtRuntime = false,
                .defaultStrVal = "/var/opt/redislabs/modules/rg",
                .ptr = &pythonConfig.pythonInstallationDir,
        },
        {
                .name = NULL,
        },
};

#define TRY_LOAD_CONFIG(v) if(v != REDISMODULE_OK) return REDISMODULE_ERR

static int PythonConfig_InitConfig(RedisModuleCtx *ctx){

    for(PythonConfigValDef* def = configDefs ; def->name ; ++def){
        switch(def->type){
        case PythonConfigType_Bool:
            TRY_LOAD_CONFIG(PythonConfig_GetBoolVal(def->name, def->defaultBoolVal, NULL, def->ptr, def->flags));
            break;
        case PythonConfigType_Int:
            TRY_LOAD_CONFIG(PythonConfig_GetIntVal(def->name, def->defaultIntVal, NULL, def->ptr, def->flags));
            break;
        case PythonConfigType_Str:
            TRY_LOAD_CONFIG(PythonConfig_GetStrVal(def->name, def->defaultStrVal, NULL, def->ptr, def->flags));
            break;
        default:
            RedisModule_Assert(false);
        }
    }

    RedisModule_Log(staticCtx, "notice", "Python config:");
    for(PythonConfigValDef* def = configDefs ; def->name ; ++def){
        switch(def->type){
        case PythonConfigType_Bool:
        case PythonConfigType_Int:
            RedisModule_Log(staticCtx, "notice", "\t%s : %d", def->name, *((int*)def->ptr));
            break;
        case PythonConfigType_Str:
            RedisModule_Log(staticCtx, "notice", "\t%s : %s", def->name, *((char**)def->ptr));
            break;
        default:
            RedisModule_Assert(false);
        }
    }
    return REDISMODULE_OK;
}

static void* Python_SaveThread(){
    if(!RedisGearsPy_IsLockAcquired()){
        return NULL;
    }

    return PyEval_SaveThread();
}

static void Python_RestorThread(void* s){
    if(!s){
        return;
    }

    PyEval_RestoreThread(s);
}

static int Python_GetConfig(const char* key, RedisModuleCtx* ctx){
    for(PythonConfigValDef* def = configDefs ; def->name ; ++def){
        if(strcmp(def->name, key) == 0){
            switch(def->type){
            case PythonConfigType_Bool:
            case PythonConfigType_Int:
                RedisModule_ReplyWithLongLong(ctx, *(int*)def->ptr);
                break;
            case PythonConfigType_Str:
                RedisModule_ReplyWithCString(ctx, *(char**)def->ptr);
                break;
            default:
                RedisModule_Assert(false);
            }
            return REDISMODULE_OK;
        }
    }
    return REDISMODULE_ERR;
}

static int Python_BeforeConfigChange(const char* key, const char* val, char** err){
    for(PythonConfigValDef* def = configDefs ; def->name ; ++def){
        if(strcmp(def->name, key) == 0){
            if(!def->configurableAtRuntime){
                RedisGears_ASprintf(err, "(error) %s is not configurable at runtime", key);
                return REDISMODULE_ERR;
            }
            switch(def->type){
            case PythonConfigType_Bool:
                TRY_LOAD_CONFIG(PythonConfig_GetBoolVal(def->name, 0, val, def->ptr, def->flags | CONFIG_FLAG_IGNORE_DEFAULT));
                break;
            case PythonConfigType_Int:
                TRY_LOAD_CONFIG(PythonConfig_GetIntVal(def->name, 0, val, def->ptr, def->flags | CONFIG_FLAG_IGNORE_DEFAULT));
                break;
            case PythonConfigType_Str:
                RG_FREE(*(char**)def->ptr); // free old pointer
                TRY_LOAD_CONFIG(PythonConfig_GetStrVal(def->name, NULL, val, def->ptr, def->flags | CONFIG_FLAG_IGNORE_DEFAULT));
                break;
            default:
                RedisModule_Assert(false);
            }
        }
    }
    return REDISMODULE_OK;
}

static int Python_SerializeSession(void* session, Gears_BufferWriter *bw, char **err){
    RedisGears_BWWriteLong(bw, PY_SESSION_TYPE_VERSION);
    return PythonSessionCtx_SerializeInternals(NULL, session, bw, true, err);
}

static void Python_SetCurrSession(void *s, bool onlyFree){
    if (onlyFree) {
        PythonSessionCtx_Free(s);
        return;
    }
    if (!s) {
        RedisGearsPy_AddSessionRequirementsToDict(currentSession);

        PythonSessionCtx_Set(currentSession);
        PythonSessionCtx_Free(currentSession);
        currentSession = NULL;
    } else {
        currentSession = s;
    }
}

static void* Python_DeserializeSession(Gears_BufferReader *br, char **err) {
    long version = RedisGears_BRReadLong(br);
    return PythonSessionCtx_Deserialize(br, version, err, false);
}

static void Python_UnlinkSession(const char* sessionId) {
    PythonSessionCtx* s = PythonSessionCtx_Get(sessionId);
    if (s) {
        PythonSessionCtx_Unlink(s);
        PythonSessionCtx_Free(s);
    }
}

static void Python_Info(RedisModuleInfoCtx *ctx, int for_crash_report) {
    if (RedisModule_InfoAddSection(ctx, "python_stats") == REDISMODULE_OK) {
        RedisModule_InfoAddFieldULongLong(ctx, "TotalAllocated", totalAllocated);
        RedisModule_InfoAddFieldULongLong(ctx, "PeakAllocated", peakAllocated);
        RedisModule_InfoAddFieldULongLong(ctx, "CurrAllocated", currAllocated);
    }

    if (RedisModule_InfoAddSection(ctx, "python_requirements") == REDISMODULE_OK) {
        Gears_dictIterator *iter = Gears_dictGetIterator(RequirementsDict);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            PythonRequirementCtx* req = Gears_dictGetVal(entry);
            RedisModule_InfoBeginDictField(ctx, req->installName);
            RedisModule_InfoAddFieldCString(ctx, "IsDownloaded", req->isDownloaded ? "yes" : "no");
            RedisModule_InfoAddFieldCString(ctx, "IsInstalled", req->isInstalled ? "yes" : "no");
            RedisModule_InfoAddFieldCString(ctx, "CompiledOs", (char*)RedisGears_GetCompiledOs());
            char* wheelsStr = RedisGears_ArrToStr((void**)req->wheels, array_len(req->wheels), PythonStringArray_ToStr, '|');
            RedisModule_InfoAddFieldCString(ctx, "Wheels", wheelsStr);
            RG_FREE(wheelsStr);
            RedisModule_InfoAddFieldCString(ctx, "Path", req->basePath);
            RedisModule_InfoAddFieldULongLong(ctx, "RefCount", req->refCount);
            RedisModule_InfoAddFieldCString(ctx, "IsInReqDictionary", req->isInRequirementsDict ? "yes" : "no");
            RedisModule_InfoEndDictField(ctx);
        }
        Gears_dictReleaseIterator(iter);
    }

    if (RedisModule_InfoAddSection(ctx, "python_sessions") == REDISMODULE_OK) {
        pthread_mutex_lock(&PySessionsLock);
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(SessionsDict, "^", NULL, 0);
        char* key;
        size_t keyLen;
        PythonSessionCtx* s;
        while((key = RedisModule_DictNextC(iter, &keyLen, (void**)&s))){
            char* str = PythonSessionCtx_ToString(s);
            RedisModule_InfoAddFieldCString(ctx, s->sessionId, str);
            RG_FREE(str);
        }
        RedisModule_DictIteratorStop(iter);

        Gears_listIter *tsIter = Gears_listGetIterator(DeadSessionsList, AL_START_HEAD);
        Gears_listNode *n = NULL;
        while ((n = Gears_listNext(tsIter))) {
            PythonSessionCtx* s = Gears_listNodeValue(n);
            char* str = PythonSessionCtx_ToString(s);
            RedisModule_InfoAddFieldCString(ctx, s->sessionId, str);
            RG_FREE(str);
        }
        Gears_listReleaseIterator(tsIter);

        pthread_mutex_unlock(&PySessionsLock);
    }
}

__attribute__ ((visibility ("default"))) 
int RedisGears_OnLoad(RedisModuleCtx *ctx){

    pluginCtx = RedisGears_InitAsGearPlugin(ctx, REDISGEARSPYTHON_PLUGIN_NAME, REDISGEARSPYTHON_PLUGIN_VERSION);
    if (!pluginCtx) {
        RedisModule_Log(ctx, "warning", "Failed initialize RedisGears API");
        return REDISMODULE_ERR;
    }

    RedisGears_RegisterLoadingEvent(RedisGearsPy_OnLoadedEvent);

    pthread_mutex_init(&PySessionsLock, NULL);

    redisVersion = RedisGears_GetRedisVersion();

    RedisGears_PluginSetInfoCallback(pluginCtx, Python_Info);
    RedisGears_PluginSetUnlinkSessionCallback(pluginCtx, Python_UnlinkSession);
    RedisGears_PluginSetDeserializeSessionCallback(pluginCtx, Python_DeserializeSession);
    RedisGears_PluginSetSerializeSessionCallback(pluginCtx, Python_SerializeSession);
    RedisGears_PluginSetSetCurrSessionCallback(pluginCtx, Python_SetCurrSession);

    if(RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)){
        staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
    }else{
        staticCtx = RedisModule_GetThreadSafeContext(NULL);
    }

    RedisModule_Log(staticCtx, "Notice", "Initializing " REDISGEARSPYTHON_PLUGIN_NAME " version %d.%d.%d", REDISGEARSPYTHON_VERSION_MAJOR, REDISGEARSPYTHON_VERSION_MINOR, REDISGEARSPYTHON_VERSION_PATCH);

    RedisGears_AddLockStateHandler(Python_SaveThread, Python_RestorThread);
    RedisGears_AddConfigHooks(Python_BeforeConfigChange, NULL, Python_GetConfig);

    if(PythonConfig_InitConfig(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Failed initialize python configuration");
        return REDISMODULE_ERR;
    }

    installDepsPool = Gears_thpool_init(1);
    InitializeGlobalPaths();
    PrintGlobalPaths(ctx);
    if(RedisGears_InstallDeps(ctx) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "Failed installing python plugin");
        return REDISMODULE_ERR;
    }

    RedisGearsPy_CreateRequirementsDataType(ctx);

    int err = pthread_key_create(&pythonThreadCtxKey, PythonThreadCtx_Destructor);
    if(err){
        return REDISMODULE_ERR;
    }

    SessionsDict = RedisModule_CreateDict(ctx);
    DeadSessionsList = Gears_listCreate();
    RequirementsDict = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    if (pythonConfig.overrideAllocators) {
        PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &allocator);
        PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &allocator);
        PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &allocator);
    }
    char* arg = "Embeded";
    size_t len = strlen(arg);
    wchar_t *pyHome = Py_DecodeLocale(PYENV_DIR, NULL);
    Py_SetPythonHome(pyHome);
    PyMem_RawFree(pyHome);

    EmbRedisGears.m_methods = EmbRedisGearsMethods;
    EmbRedisGears.m_size = sizeof(EmbRedisGearsMethods) / sizeof(*EmbRedisGearsMethods);
    PyImport_AppendInittab("redisgears", &PyInit_RedisGears);

    EmbRedisAI.m_methods = EmbRedisAIMethods;
    EmbRedisAI.m_size = sizeof(EmbRedisAIMethods) / sizeof(*EmbRedisAIMethods);
    PyImport_AppendInittab("redisAI", &PyInit_RedisAI);

    Py_Initialize();
    if(pythonConfig.createVenv){
        // lets activate the virtual env we are operate in
        RedisGears_SetupPyEnv(ctx);
    }
    PyEval_InitThreads();
    wchar_t* arg2 = Py_DecodeLocale(arg, &len);
    PySys_SetArgv(1, &arg2);
    PyMem_RawFree(arg2);
    PyTensorType.tp_new = PyType_GenericNew;
    PyGraphRunnerType.tp_new = PyType_GenericNew;
    PyDAGRunnerType.tp_new = PyType_GenericNew;
    PyFlatExecutionType.tp_new = PyType_GenericNew;
    PyExecutionSessionType.tp_new = PyType_GenericNew;
    PyAtomicType.tp_new = PyType_GenericNew;

    PyDAGRunnerType.tp_methods = PyDAGRunnerMethods;
    PyFlatExecutionType.tp_methods = PyFlatExecutionMethods;
    PyAtomicType.tp_methods = PyAtomicMethods;
    PyFutureType.tp_methods = PyFutureMethods;

    if (PyType_Ready(&PyTensorType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyTensorType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyGraphRunnerType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyGraphRunnerType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyTorchScriptRunnerType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyGraphRunnerType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyDAGRunnerType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyGraphRunnerType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyExecutionSessionType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyExecutionSessionType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyFlatExecutionType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyFlatExecutionType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyAtomicType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyAtomicType not ready");
        return REDISMODULE_ERR;
    }

    if (PyType_Ready(&PyFutureType) < 0){
        RedisModule_Log(staticCtx, "warning", "PyFutureType not ready");
        return REDISMODULE_ERR;
    }

    PyObject *pName = PyUnicode_FromString("redisgears");
    PyObject* redisGearsModule = PyImport_Import(pName);
    GearsPyDecRef(pName);

    pName = PyUnicode_FromString("redisAI");
    PyObject* redisAIModule = PyImport_Import(pName);
    GearsPyDecRef(pName);

    Py_INCREF(&PyTensorType);
    Py_INCREF(&PyGraphRunnerType);
    Py_INCREF(&PyTorchScriptRunnerType);
    Py_INCREF(&PyDAGRunnerType);
    Py_INCREF(&PyExecutionSessionType);
    Py_INCREF(&PyFlatExecutionType);
    Py_INCREF(&PyAtomicType);
    Py_INCREF(&PyFutureType);

    PyModule_AddObject(redisAIModule, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(redisAIModule, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);
    PyModule_AddObject(redisAIModule, "PyTorchScriptRunner", (PyObject *)&PyTorchScriptRunnerType);
    PyModule_AddObject(redisAIModule, "PyDAGRunner", (PyObject *)&PyDAGRunnerType);
    PyModule_AddObject(redisGearsModule, "PyExecutionSession", (PyObject *)&PyExecutionSessionType);
    PyModule_AddObject(redisGearsModule, "PyFlatExecution", (PyObject *)&PyFlatExecutionType);
    PyModule_AddObject(redisGearsModule, "PyAtomic", (PyObject *)&PyAtomicType);
    PyModule_AddObject(redisGearsModule, "PyFuture", (PyObject *)&PyFutureType);
    GearsError = PyErr_NewException("gears.error", NULL, NULL);
    Py_INCREF(GearsError);
    PyModule_AddObject(redisGearsModule, "GearsError", GearsError);

    ForceStoppedError = PyErr_NewException("gears.forcestopped", NULL, NULL);
    Py_INCREF(ForceStoppedError);
    PyModule_AddObject(redisGearsModule, "GearsForceStopped", ForceStoppedError);

    GearsFlatError = PyErr_NewException("gears.flat_error", NULL, NULL);
    Py_INCREF(GearsFlatError);
    PyModule_AddObject(redisGearsModule, "GearsFlatError", GearsFlatError);

    char* script = RG_ALLOC(cloudpickle_py_len + 1);
    memcpy(script, cloudpickle_py, cloudpickle_py_len);
    script[cloudpickle_py_len] = '\0';
    PyRun_SimpleString(script);
    RG_FREE(script);

    script = RG_ALLOC(GearsBuilder_py_len + 1);
    memcpy(script, GearsBuilder_py, GearsBuilder_py_len);
    script[GearsBuilder_py_len] = '\0';
    PyRun_SimpleString(script);
    RG_FREE(script);

    // todo: fix, this is not really catching any errors
    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error occured on RedisGearsPy_Init, error=%s", error);
        RG_FREE(error);
        return REDISMODULE_ERR;
    }

    if(PyErr_Occurred()){
        char* error = getPyError();
        RedisModule_Log(staticCtx, "warning", "Error occured on RedisGearsPy_Init, error=%s", error);
        RG_FREE(error);
        return REDISMODULE_ERR;
    }

    runCoroutineFunction = PyDict_GetItemString(pyGlobals, "runCoroutine");
    createFutureFunction = PyDict_GetItemString(pyGlobals, "createFuture");
    setFutureResultsFunction = PyDict_GetItemString(pyGlobals, "setFutureResults");
    setFutureExceptionFunction = PyDict_GetItemString(pyGlobals, "setFutureException");

    profileCreateFunction = PyDict_GetItemString(pyGlobals, "profilerCreate");
    profileStartFunction = PyDict_GetItemString(pyGlobals, "profileStart");
    profileStopFunction = PyDict_GetItemString(pyGlobals, "profileStop");
    profileGetInfoFunction = PyDict_GetItemString(pyGlobals, "profileGetInfo");


    pythonRecordType = RedisGears_RecordTypeCreate("PythonRecord", sizeof(PythonRecord),
                                                   PythonRecord_SendReply,
                                                   PythonRecord_Serialize,
                                                   PythonRecord_Deserialize,
                                                   PythonRecord_Free);

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType",
                                                    PY_OBJECT_TYPE_VERSION,
                                                    RedisGearsPy_PyObjectFree,
                                                    RedisGearsPy_PyObjectDup,
                                                    RedisGearsPy_PyCallbackSerialize,
                                                    RedisGearsPy_PyCallbackDeserialize,
                                                    RedisGearsPy_PyObjectToString,
                                                    NULL);

    ArgType* pySessionType = RedisGears_CreateType("PySessionType",
                                                    PY_SESSION_TYPE_VERSION,
                                                    PythonSessionCtx_FreeWithFep,
                                                    PythonSessionCtx_ShallowCopyWithFep,
                                                    PythonSessionCtx_Serialize,
                                                    PythonSessionCtx_DeserializeWithFep,
                                                    PythonSessionCtx_ToStringWithFep,
                                                    PythonSessionCtx_OnFepDeserialized);

    ArgType* pySessionRequirementsType = RedisGears_CreateType("PySessionRequirementsType",
                                                                PY_SESSION_REQ_VERSION,
                                                                PythonSessionRequirements_Free,
                                                                PythonSessionRequirements_DupWithFep,
                                                                PythonSessionRequirements_Serialize,
                                                                PythonSessionRequirements_Deserialize,
                                                                PythonSessionRequirements_ToString,
                                                                NULL);

    RedisGears_RegisterFlatExecutionPrivateDataType(pySessionType);

    RGM_RegisterMap(RedisGearsPy_InstallRequirementsMapper, pySessionRequirementsType);

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
        RedisModule_Log(staticCtx, "warning", "could not register command timer datatype");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexecute", RedisGearsPy_Execute, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyexecute");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexecuteremote", RedisGearsPy_ExecuteRemote, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyexecuteremote");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyfreeinterpreter", RedisGearsPy_FreeInterpreter, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(staticCtx, "warning", "could not register command rg.pyexecute");
		return REDISMODULE_ERR;
	}

    if (RedisModule_CreateCommand(ctx, "rg.pystats", RedisGearsPy_Stats, "readonly", 0, 0, 0) != REDISMODULE_OK) {
    	RedisModule_Log(staticCtx, "warning", "could not register command rg.pystats");
		return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexportreq", RedisGearsPy_ExportRequirement, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyexportreq");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyimportreq", RedisGearsPy_ImportRequirement, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyimportreq");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pydumpreqs", RedisGearsPy_DumpRequirements, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pydumpreqs");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyprofile", RedisGearsPy_Profile, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyprofilestats");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pydumpsessions", RedisGearsPy_DumpSessions, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pydumpsessions");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, IMPORT_REQ_INTERAL_COMMAND, RedisGearsPy_ImportRequirementInternal, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(staticCtx, "warning", "could not register command rg.pyimportreq");
        return REDISMODULE_ERR;
    }

    initialModulesDict = PyDict_Copy(PyThreadState_GET()->interp->modules);

    PyEval_SaveThread();

    return REDISMODULE_OK;
}

void RedisGearsPy_Clean() {
    if(!RequirementsDict){
        return;
    }

    RedisGearsPy_ClearRequirements();
}
