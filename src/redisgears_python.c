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
#include "utils/thpool.h"
#include "utils/buffer.h"
#include <pthread.h>
#include "cluster.h"


#define PY_OBJECT_TYPE_VERSION 1

#define PY_SESSION_TYPE_VERSION 1
#define PY_SESSION_TYPE_WITH_REQ_NAMES_ONLY 1

#define PY_REQ_VERSION 1
#define PY_REQ_VERSION_WITH_OS_VERSION 1
#define PY_SESSION_REQ_VERSION 1

#define SUB_INTERPRETER_TYPE "subInterpreterType"

#define STACK_BUFFER_SIZE 100

static PyObject* pyGlobals;
PyObject* GearsError;
PyObject* ForceStoppedError;

RecordType* pythonRecordType;

RedisModuleType* GearsPyRequirementDT;

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

#define PYTHON_ERROR "error running python code"

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err);
static void TimeEvent_Free(void *value);
static int RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw, char** err);
static PythonThreadCtx* GetPythonThreadCtx();

static long long CurrSessionId = 0;

Gears_dict* SessionsDict = NULL;

static char* venvDir = NULL;

typedef struct PythonRequirementCtx{
    size_t refCount;
    char* basePath;
    char* installName;
    char** wheels;
    volatile bool isInstalled;
    volatile bool isDownloaded;
    pthread_mutex_t installationLock;
}PythonRequirementCtx;

Gears_dict* RequirementsDict = NULL;

typedef struct PythonSessionCtx{
    size_t refCount;
    char sessionId[ID_LEN];
    char sessionIdStr[STR_ID_LEN];
    PyObject* globalsDict;
    PythonRequirementCtx** requirements;
    bool isInstallationNeeded;
}PythonSessionCtx;

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
        if(GearsConfig_CreateVenv()){
            exitCode = ExecCommand(NULL, "/bin/bash -c \"source %s/bin/activate;cd '%s';python -m pip wheel '%s'\"", venvDir, req->basePath, req->installName);
        }else{
            exitCode = ExecCommand(NULL, "/bin/bash -c \"cd '%s';%s/bin/python3 -m pip wheel '%s'\"", req->basePath, venvDir, req->installName);
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
    if(req->isInstalled){
        goto done;
    }
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
    if(GearsConfig_CreateVenv()){
        exitCode = ExecCommand(NULL, "/bin/bash -c \"source %s/bin/activate; cd '%s'; python -m pip install --no-index --disable-pip-version-check %s\"", venvDir, req->basePath, filesInDir);
    }else{
        exitCode = ExecCommand(NULL, "/bin/bash -c \"cd '%s'; %s/bin/python3 -m pip install --no-index --disable-pip-version-check %s\"", req->basePath, venvDir, filesInDir);
    }
    array_free(filesInDir);
    if(exitCode == 0){
        req->isInstalled = true;
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
    RedisModule_Log(NULL, "warning", "Fatal!!!, failed verifying basePath of requirment. name:'%s', basePath:'%s'", req->installName, req->basePath);
    RedisModule_Assert(false);
}
static void PythonRequirementCtx_Free(PythonRequirementCtx* reqCtx){
    if(--reqCtx->refCount){
        return;
    }

    PythonRequirementCtx_VerifyBasePath(reqCtx);
    ExecCommand(NULL, "rm -rf '%s'", reqCtx->basePath);
    Gears_dictDelete(RequirementsDict, reqCtx->installName);
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

static char* PythonRequirementCtx_WheelToStr(void* wheel){
    char* str;
    rg_asprintf(&str, "'%s'", (char*)wheel);
    return str;
}

static char* PythonRequirementCtx_ToStr(void* val){
    PythonRequirementCtx* req = val;
    char* res;
    char* wheelsStr = ArrToStr((void**)req->wheels, array_len(req->wheels), PythonRequirementCtx_WheelToStr);
    rg_asprintf(&res, "{'name':'%s', 'basePath':'%s', 'wheels':%s}", req->installName, req->basePath, wheelsStr);
    RG_FREE(wheelsStr);
    return res;
}

static char* PythonSessionRequirements_ToString(void* arg){
    PythonRequirementCtx** req = arg;
    return ArrToStr((void**)req, array_len(req), PythonRequirementCtx_ToStr);
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

static int PythonSessionRequirements_Serialize(void* arg, Gears_BufferWriter* bw, char** err){
    PythonRequirementCtx** reqs = arg;
    RedisGears_BWWriteLong(bw, array_len(reqs));
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        if(PythonRequirement_Serialize(reqs[i], bw, err) != REDISMODULE_OK){
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

static void PythonSessionRequirements_Free(void* arg){
    PythonRequirementCtx** reqs = arg;
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        PythonRequirementCtx_Free(reqs[i]);
    }
    array_free(reqs);
}

static PythonRequirementCtx* PythonRequirementCtx_ShellowCopy(PythonRequirementCtx* req){
    ++req->refCount;
    return req;
}

static void* PythonSessionRequirements_Dup(void* arg){
    PythonRequirementCtx** reqs = arg;
    PythonRequirementCtx** res = array_new(PythonRequirementCtx*, 10);
    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        res = array_append(res, PythonRequirementCtx_ShellowCopy(reqs[i]));
    }
    return res;
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
    int exitCode = ExecCommand(NULL, "mkdir -p '%s/%s'", venvDir, requirement);
    if(exitCode != 0){
        return NULL;
    }

    PythonRequirementCtx* ret = RG_ALLOC(sizeof(*ret));
    ret->installName = RG_STRDUP(requirement);
    rg_asprintf(&ret->basePath, "%s/%s", venvDir, ret->installName);
    ret->wheels = array_new(char*, 10);
    // refCount is starting from 2, one hold by RequirementsDict and once by the caller.
    // currently we basically never delete requirments so we will know not to reinstall them
    // to save time
    ret->refCount = 2;
    pthread_mutex_init(&ret->installationLock, NULL);

    Gears_dictAdd(RequirementsDict, ret->installName, ret);

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
        if(strcmp(os, RedisGears_GetCompiledOs()) != 0){
            rg_asprintf(err, "Requirement was compiled on different os (compiled_os = %s, current_os = %s)", os, RedisGears_GetCompiledOs());
            goto done;
        }
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

    req = PythonRequirementCtx_Get(name);
    if(!req){
        req = PythonRequirementCtx_Create(name);
    }

    pthread_mutex_lock(&req->installationLock);
    if(!req->isDownloaded){
        for(size_t i = 0 ; i < array_len(wheelsData) ; ++i){
            char* filePath;
            rg_asprintf(&filePath, "%s/%s", req->basePath, wheelsData[i].fileName);

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
    RedisGears_BWWriteLong(bw, array_len(req->wheels));
    for(size_t i = 0 ; i < array_len(req->wheels) ; ++i){
        char* wheel = req->wheels[i];
        char* filePath;
        rg_asprintf(&filePath, "%s/%s", req->basePath, wheel);

        FILE *f = fopen(filePath, "rb");
        if(!f){
            RG_FREE(filePath);
            rg_asprintf(err, "Could not open file %s", filePath);
            RedisModule_Log(NULL, "warning", "%s", *err);
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
            RedisModule_Log(NULL, "warning", "%s", *err);
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
        PyDict_Clear(session->globalsDict);
        Py_DECREF(session->globalsDict);
        RedisGearsPy_Unlock(old);
        RG_FREE(session);
    }
}

static PythonSessionCtx* PythonSessionCtx_Get(char* id){
    return Gears_dictFetchValue(SessionsDict, id);
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

static PythonSessionCtx* PythonSessionCtx_CreateWithId(char* id, const char** requirementsList, size_t requirementsListLen){
    // creating a new global dict for this session
    void* old = RedisGearsPy_Lock(NULL);

    PyObject* globalDict = PyDict_Copy(pyGlobals);

    PythonSessionCtx* session = RG_ALLOC(sizeof(*session));
    *session = (PythonSessionCtx){
            .refCount = 1,
            .globalsDict = globalDict,
            .isInstallationNeeded = false,
    };
    SetId(NULL, session->sessionId, session->sessionIdStr, &CurrSessionId);

    session->requirements = array_new(PythonRequirementCtx*, 10);

    Gears_dictAdd(SessionsDict, session->sessionId, session);

    for(size_t i = 0 ; i < requirementsListLen ; ++i){
        PythonRequirementCtx* req = PythonRequirementCtx_Get(requirementsList[i]);

        if(!req){
            req = PythonRequirementCtx_Create(requirementsList[i]);
            session->isInstallationNeeded = true;

            if(!req){
                PythonSessionCtx_Free(session);
                session = NULL;
                break;
            }
        }

        if(!req->isInstalled){
            // it could be that the requirement already exits but not yet installed
            // in this case we will move the execution to background installation also.
            session->isInstallationNeeded = true;
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
        RedisGears_BWWriteString(bw, session->requirements[i]->installName);
    }

    return REDISMODULE_OK;
}

static void* PythonSessionCtx_Deserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > PY_SESSION_TYPE_VERSION){
        *err = RG_STRDUP("unsupported session version");
        return NULL;
    }
    size_t len;
    char* id = RedisGears_BRReadBuffer(br, &len);

    PythonSessionCtx* s = PythonSessionCtx_Get(id);
    bool sessionExists;
    if(!s){
        s = PythonSessionCtx_CreateWithId(id, NULL, 0);
        if(!s){
            *err = RG_STRDUP("Could not create session");
            return NULL;
        }
        sessionExists = false;
    }else{
        s = PythonSessionCtx_ShellowCopy(s);
        sessionExists = true;
    }
    size_t requirementsLen = RedisGears_BRReadLong(br);
    for(size_t i = 0 ; i < requirementsLen ; ++i){
        PythonRequirementCtx* req;
        if(version >= PY_SESSION_TYPE_WITH_REQ_NAMES_ONLY){
            const char* reqName = RedisGears_BRReadString(br);
            req = PythonRequirementCtx_Get(reqName);
            if(!req){
                rg_asprintf(err, "session missing requirement (%s)", reqName);
                PythonSessionCtx_Free(s);
                return NULL;
            }
        }else{
            // we set the req version to 0, only at gears v1.0.0 we added the serialized
            // requirement to the session so we need to read it and install it here
            RedisModule_Log(NULL, "notice", "Loading an old rdb registrations that comes with a requirement. the requirent will also be installed.");
            req = PythonRequirementCtx_Deserialize(br, 0, err);
            if(!req){
                if(!*err){
                    *err = RG_STRDUP("Failed deserializing requirement");
                }
                PythonSessionCtx_Free(s);
                return NULL;
            }

            if(!PythonRequirementCtx_InstallRequirement(req)){
                PythonSessionCtx_Free(s);
                *err = RG_STRDUP("Failed install requirement on shard, check shard log for more info.");
                return NULL;
            }
        }

        if(!sessionExists){
            s->requirements = array_append(s->requirements , req);
        }
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
    RedisModule_Assert(sctx);

    PyObject* callback = arg;

    void* old = RedisGearsPy_Lock(sctx);

    RedisModule_Assert(PyFunction_Check(callback));

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
        RedisModule_Assert(oldState == PyGILState_UNLOCKED);
    }
    ++ptctx->lockCounter;
    return oldSession;
}

void RedisGearsPy_Unlock(PythonSessionCtx* prevSession){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    RedisModule_Assert(ptctx);
    RedisModule_Assert(ptctx->lockCounter > 0);
    ptctx->currSession = prevSession;
    if(--ptctx->lockCounter == 0){
        RedisModule_Assert(!prevSession);
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
        RedisModule_Assert(RedisGears_RecordGetType(r) == pythonRecordType);
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
            RedisModule_Assert(false);
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

    // Declare and initialize variables for arguments processing.
    size_t argLen;
    const char* argumentCStr = NULL;
    RedisModuleString* argumentRedisStr = NULL;
    RedisModuleString** arguments = array_new(RedisModuleString*, 10);
    for(int i = 1 ; i < PyTuple_Size(args) ; ++i){
        PyObject* argument = PyTuple_GetItem(args, i);
        if(PyByteArray_Check(argument)) {
            // Argument is bytearray.
            argLen = PyByteArray_Size(argument);
            argumentCStr = PyByteArray_AsString(argument);
            argumentRedisStr = RedisModule_CreateString(rctx, argumentCStr, argLen);
        } else if(PyBytes_Check(argument)) {
            // Argument is bytes.
            argLen = PyBytes_Size(argument);
            argumentCStr = PyBytes_AsString(argument);
            argumentRedisStr = RedisModule_CreateString(rctx, argumentCStr, argLen);
        } else {
            // Argument is string.
            PyObject* argumentStr = PyObject_Str(argument);
            argumentCStr = PyUnicode_AsUTF8AndSize(argumentStr, &argLen);
            argumentRedisStr = RedisModule_CreateString(rctx, argumentCStr, argLen);
            // Decrease ref-count after done processing the argument.
            Py_DECREF(argumentStr);
        }
        arguments = array_append(arguments, argumentRedisStr);
    }

    PyObject* res = NULL;
    RedisModuleCallReply *reply = RedisModule_Call(rctx, commandStr, "!v", arguments, array_len(arguments));
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

    array_free_ex(arguments, RedisModule_FreeString(rctx, *(RedisModuleString**)ptr));

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
            Py_DECREF(flatList);
            return NULL;
        }

        PyList_Append(flatList, pyVal);
    }
    return flatList;
}

static bool verifyOrLoadRedisAI(){
    return globals.redisAILoaded;
}

#define verifyRedisAILoaded() \
    if(!verifyOrLoadRedisAI()){ \
        PyErr_SetString(GearsError, "RedisAI is not loaded, it is not possible to use AI interface."); \
        return NULL;\
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
    const char* blob;
    bool free_blob = false;

    // Collect dims.
    while((currDim = PyIter_Next(dimsIter)) != NULL){
        if(!PyLong_Check(currDim)){
            PyErr_SetString(GearsError, "dims arguments must be long");
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            goto clean_up;
        }
        if(PyErr_Occurred()){
            Py_DECREF(currDim);
            Py_DECREF(dimsIter);
            goto clean_up;
        }
        dims = array_append(dims, PyLong_AsLong(currDim));
        Py_DECREF(currDim);
    }
    Py_DECREF(dimsIter);

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

    if(Cluster_IsClusterMode()){
        const char* keyNodeId = Cluster_GetNodeIdByKey(keyNameCStr);
        if(!Cluster_IsMyId(keyNodeId)){
            PyErr_SetString(GearsError, "Given key is not in the current shard");
            return NULL;
        }
    }

    PyObject* obj = NULL;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, len);
    LockHandler_Acquire(rctx);
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
    LockHandler_Release(rctx);
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
    bool is_cluster = Cluster_IsClusterMode();
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
            const char* keyNodeId = Cluster_GetNodeIdByKey(keyNameCStr);
            if(!Cluster_IsMyId(keyNodeId)){
                PyErr_SetString(GearsError, "Given key is not in the current shard");
                goto clean_up;
            }
        }
        keyNameList = array_append(keyNameList, keyNameCStr);
    }
   
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);
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
    LockHandler_Release(rctx);
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
    if(Cluster_IsClusterMode()){
        const char* keyNodeId = Cluster_GetNodeIdByKey(keyNameCStr);
        if(!Cluster_IsMyId(keyNodeId)){
            PyErr_SetString(GearsError, "Given key is not in the current shard");
        return NULL;
        }
    }
    PyObject* obj = NULL;
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleString *keyNameStr = RedisModule_CreateString(rctx, keyNameCStr, len);
    LockHandler_Acquire(rctx);
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
    LockHandler_Release(rctx);
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
    bool is_cluster = Cluster_IsClusterMode();
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
            const char* keyNodeId = Cluster_GetNodeIdByKey(keyNameCStr);
            if(!Cluster_IsMyId(keyNodeId)){
                PyErr_SetString(GearsError, "Given key is not in the current shard");
                goto clean_up;
            }
        }
        keyNameList = array_append(keyNameList, keyNameCStr);
    }

    size_t len = array_len(keyNameList);
    RedisModuleType *ai_tensorType = RedisAI_TensorRedisType();
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);
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
    }
    obj = tensorsList;

clean_up:

    array_free_ex(keys, RedisModule_CloseKey(*(RedisModuleKey**)ptr));
    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    array_free(keyNameList);
    Py_DECREF(keys_iter);
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
    LockHandler_Acquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_MODULE){
        RedisModule_FreeString(ctx, keyRedisStr);
        RedisModule_CloseKey(key);
        LockHandler_Release(ctx);
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
    LockHandler_Release(ctx);
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
    RAI_Error* err;
    RedisAI_InitError(&err);
    PyThreadState* _save = PyEval_SaveThread();
    RedisAI_ModelRun(&pyg->g, 1, err);
    PyEval_RestoreThread(_save);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
        return NULL;
    }
    RedisAI_FreeError(err);
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
    LockHandler_Acquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);

    if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_MODULE){
        RedisModule_FreeString(ctx, keyRedisStr);
        RedisModule_CloseKey(key);
        LockHandler_Release(ctx);
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
    LockHandler_Release(ctx);
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

    Py_DECREF(py_tensors_iter);
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
    RAI_Error* err;
    RedisAI_InitError(&err);
    PyThreadState* _save = PyEval_SaveThread();
    RedisAI_ScriptRun(pys->s, err);
    PyEval_RestoreThread(_save);
    if (RedisAI_GetErrorCode(err) != RedisAI_ErrorCode_OK) {
        PyErr_SetString(GearsError, RedisAI_GetError(err));
        RedisAI_FreeError(err);
        return NULL;
    }
    RedisAI_FreeError(err);
    PyObject* tensorList = PyList_New(0);
    for(size_t i = 0 ; i < RedisAI_ScriptRunCtxNumOutputs(pys->s) ; ++i){
        PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
        pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ScriptRunCtxOutputTensor(pys->s, i));
        PyList_Append(tensorList, (PyObject*)pyt);
        Py_DECREF(pyt);
    }
    return tensorList;
}

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
    td->session = PythonSessionCtx_Deserialize(NULL, &br, version, &err);
    if(!td->session){
        RedisModule_Log(NULL, "warning", "Could not deserialize TimeEven Session, error='%s'", err);
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
    RedisModule_SaveUnsigned(rdb, PY_SESSION_TYPE_VERSION);
    Gears_Buffer* b = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, b);
    PythonSessionCtx_Serialize(td->session, &bw, NULL);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferClear(b);
    RedisModule_SaveUnsigned(rdb, PY_OBJECT_TYPE_VERSION);
    int res = RedisGearsPy_PyCallbackSerialize(td->callback, &bw, NULL);
    RedisModule_Assert(res == REDISMODULE_OK);
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
    {"setTensorInKey", setTensorInKey, METH_VARARGS, "set a tensor in keyspace"},
    {"msetTensorsInKeyspace", msetTensorsInKeyspace, METH_VARARGS, "set multiple tensors in keyspace"},
    {"getTensorFromKey", getTensorFromKey, METH_VARARGS, "get a tensor from keyspace"},
    {"mgetTensorsFromKeyspace", mgetTensorsFromKeyspace, METH_VARARGS, "get multiple tensors from keyspace"},
    {"createModelRunner", createModelRunner, METH_VARARGS, "open TF graph by key name"},
    {"modelRunnerAddInput", modelRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"modelRunnerAddOutput", modelRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"modelRunnerRun", modelRunnerRun, METH_VARARGS, "run graph runner"},
    {"createScriptRunner", createScriptRunner, METH_VARARGS, "open a torch script by key name"},
    {"scriptRunnerAddInput", scriptRunnerAddInput, METH_VARARGS, "add input to torch script runner"},
    {"scriptRunnerAddInputList", scriptRunnerAddInputList, METH_VARARGS, "add a list of tensor as input to torch script runner"},
    {"scriptRunnerAddOutput", scriptRunnerAddOutput, METH_VARARGS, "add output to torch script runner"},
    {"scriptRunnerRun", scriptRunnerRun, METH_VARARGS, "run torch script runner"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
    {"tensorGetDataAsBlob", tensorGetDataAsBlob, METH_VARARGS, "getting the tensor data as a string blob"},
    {"tensorGetDims", tensorGetDims, METH_VARARGS, "return tuple of the tensor dims"},
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
    RedisModuleBlockedClient *bc;
    char* script;
    DoneCallbackFunction doneFunction;
    bool isBlocking;
}BackgroundDepsInstallCtx;

static Gears_threadpool installDepsPool = NULL;

static void RedisGearsPy_InnerExecute(RedisModuleCtx* rctx, BackgroundDepsInstallCtx* bdiCtx){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    ptctx->currentCtx = rctx;
    ptctx->createdExecution = NULL;

    void* old = RedisGearsPy_Lock(bdiCtx->session);
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

        RedisGearsPy_Unlock(old);

        ptctx->createdExecution = NULL;
        ptctx->currentCtx = NULL;
        return;
    }

    if(ptctx->createdExecution){
        if(bdiCtx->isBlocking){
            RedisModuleBlockedClient* bc = bdiCtx->bc;
            bdiCtx->bc = NULL; // we are taking ownership of the blocked client
            if(!bc){
                bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 0);
            }
            RedisGears_AddOnDoneCallback(ptctx->createdExecution, bdiCtx->doneFunction, bc);
        }else{
            const char* id = RedisGears_GetId(ptctx->createdExecution);
            RedisModule_ReplyWithStringBuffer(rctx, id, strlen(id));
        }
    }else{
        RedisModule_ReplyWithSimpleString(ptctx->currentCtx, "OK");
    }
    RedisGearsPy_Unlock(old);

    ptctx->createdExecution = NULL;
    ptctx->currentCtx = NULL;
}

static void RedisGears_OnRequirementInstallationDone(ExecutionPlan* ep, void* privateData){
    BackgroundDepsInstallCtx* bdiCtx = privateData;

    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bdiCtx->bc);

    size_t nErrors = RedisGears_GetErrorsLen(ep);

    if(nErrors > 0){
        Record* errorRecord = RedisGears_GetError(ep, 0);
        size_t errorLen;
        const char* errorStr = RedisGears_StringRecordGet(errorRecord, &errorLen);
        RedisModule_ReplyWithError(rctx, errorStr);
    }else{
        RedisGearsPy_InnerExecute(rctx, bdiCtx);
    }

    RedisGears_DropExecution(ep);

    // free bdiCtx
    PythonSessionCtx_Free(bdiCtx->session);
    RG_FREE(bdiCtx->script);
    if(bdiCtx->bc){
        RedisModule_UnblockClient(bdiCtx->bc, NULL);
    }
    RG_FREE(bdiCtx);

    RedisModule_FreeThreadSafeContext(rctx);
}

static ExecutionPlan* RedisGearsPy_DistributeRequirements(PythonRequirementCtx** requirements, RedisGears_OnExecutionDoneCallback doneCallback, void* pd, char** err){
    FlatExecutionPlan* fep = RGM_CreateCtx(ShardIDReader);
    RedisGears_SetMaxIdleTime(fep, GearsConfig_PythonInstallReqMaxIdleTime());
    RGM_Map(fep, RedisGearsPy_InstallRequirementsMapper, PythonSessionRequirements_Dup(requirements));
    RGM_Collect(fep);
    ExecutionPlan* ep = RGM_Run(fep, ExecutionModeAsync, NULL, doneCallback, pd, err);
    RedisGears_FreeFlatExecution(fep);
    return ep;
}

static void RedisGearsPy_DownloadWheelsAndDistribute(void* ctx){
    BackgroundDepsInstallCtx* bdiCtx = ctx;
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(bdiCtx->bc);
    if(!PythonSessionCtx_DownloadWheels(bdiCtx->session)){
        RedisModule_ReplyWithError(rctx, "Could not satisfy requirments (look at redis log file for more information)");
        LockHandler_Acquire(rctx);
        goto error;
    }

    LockHandler_Acquire(rctx);
    char* err = NULL;
    ExecutionPlan* ep = RedisGearsPy_DistributeRequirements(bdiCtx->session->requirements, RedisGears_OnRequirementInstallationDone, bdiCtx, &err);
    if(!ep){
        RedisModule_ReplyWithError(ctx, err);
        RG_FREE(err);
        goto error;
    }

    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return;

error:
    // free bdiCtx

    PythonSessionCtx_Free(bdiCtx->session);
    RG_FREE(bdiCtx->script);
    if(bdiCtx->bc){
        RedisModule_UnblockClient(bdiCtx->bc, NULL);
    }
    RG_FREE(bdiCtx);

    LockHandler_Release(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void RedisGearsPy_BackgroundExecute(PythonSessionCtx* session,
                                    RedisModuleBlockedClient *bc,
                                    const char* script,
                                    DoneCallbackFunction doneFunction,
                                    bool isBlocking){

    BackgroundDepsInstallCtx* bdiCtx = RG_ALLOC(sizeof(*bdiCtx));
    *bdiCtx = (BackgroundDepsInstallCtx){
            .session = session,
            .bc = bc,
            .script = RG_STRDUP(script),
            .doneFunction = doneFunction,
            .isBlocking = isBlocking,
    };

    Gears_thpool_add_work(installDepsPool, RedisGearsPy_DownloadWheelsAndDistribute, bdiCtx);
}

int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    int ctxFlags = RedisModule_GetContextFlags(ctx);

    if(ctxFlags & (REDISMODULE_CTX_FLAGS_LUA|REDISMODULE_CTX_FLAGS_MULTI)){
        RedisModule_ReplyWithError(ctx, "Can not run gear inside multi exec or lua");
        return REDISMODULE_OK;
    }

    if(argc < 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
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
        }else{
            RedisModule_ReplyWithError(ctx, "Extra unkown arguments was given.");
            return REDISMODULE_OK;
        }
    }

    PythonSessionCtx* session = PythonSessionCtx_Create(requirementsList, reqLen);
    if(!session){
        RedisModule_ReplyWithError(ctx, "Could not satisfy requirments, look at the log file for more information.");
        return REDISMODULE_OK;
    }

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    if(session->isInstallationNeeded){
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        RedisGearsPy_BackgroundExecute(session, bc, script, ptctx->doneFunction, isBlocking);
        return REDISMODULE_OK;
    }

    BackgroundDepsInstallCtx bdiCtx = (BackgroundDepsInstallCtx){
            .session = session,
            .bc = NULL,
            .script = (char*)script,
            .doneFunction = ptctx->doneFunction,
            .isBlocking = isBlocking,
    };

    RedisGearsPy_InnerExecute(ctx, &bdiCtx);

    PythonSessionCtx_Free(session);

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
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(RedisGears_RecordGetType(record) == pythonRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

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
    RedisModule_Assert(RedisGears_RecordGetType(records) == listRecordType);

    PythonSessionCtx* sctx = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisModule_Assert(sctx);

    void* old = RedisGearsPy_Lock(sctx);

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
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
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
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
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
            RedisModule_Assert(RedisGears_RecordGetType(tempRecord) == pythonRecordType);
            PyDict_SetItem(obj, temp, PyObjRecordGet(tempRecord));
            Py_DECREF(temp);
            RedisGears_FreeRecord(tempRecord);
        }
        array_free(keys);
    }else if(RedisGears_RecordGetType(record) == pythonRecordType){
        obj = PyObjRecordGet(record);
        Py_INCREF(obj);
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
    if(PythonSessionRequirements_Serialize(reqs, &bw, &err) != REDISMODULE_OK){
        if(!err){
            err = RG_STRDUP("Failed serialize requirement to slave/aof");
        }
        RedisGears_SetError(rctx, err);
        Gears_BufferFree(buff);
        return NULL;
    }

    RedisModuleCtx* ctx = RedisGears_GetRedisModuleCtx(rctx);
    LockHandler_Acquire(ctx);
    RedisModule_Replicate(ctx, IMPORT_REQ_INTERAL_COMMAND, "b", buff->buff, buff->size);
    LockHandler_Release(ctx);

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

static void* RedisGearsPy_PyCallbackDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > PY_OBJECT_TYPE_VERSION){
        *err = RG_STRDUP("unsupported python callback version");
        return NULL;
    }
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
            RedisModule_Log(NULL, "warning", "%s", *err);
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

static void RedisGearsPy_SendReqMetaData(RedisModuleCtx *ctx, PythonRequirementCtx* req){
    RedisModule_ReplyWithArray(ctx, 12);

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
        RedisModule_Log(ctx, "warning", "On RedisGearsPy_ImportRequirementInternal, got bad arguments");
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
        RedisModule_Log(ctx, "warning", "On RedisGearsPy_ImportRequirementInternal, failed deserialize requirements version");
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
        RedisModule_Log(ctx, "warning", "%s", err);
        RedisModule_ReplyWithError(ctx, err);
        return REDISMODULE_OK;
    }

    for(size_t i = 0 ; i < array_len(reqs) ; ++i){
        if(!PythonRequirementCtx_InstallRequirement(reqs[i])){
            RedisModule_Log(ctx, "warning", "On RedisGearsPy_ImportRequirementInternal, Failed install requirement on shard, check shard log for more info.");
            RedisModule_ReplyWithError(ctx, "On RedisGearsPy_ImportRequirementInternal, Failed install requirement on shard, check shard log for more info.");
            return REDISMODULE_OK;
        }

        // we are holding a shared ref to req lets free it
        PythonRequirementCtx_Free(reqs[i]);
    }

    array_free(reqs);

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
    RedisModule_Assert(res == REDISMODULE_OK);
}

static void PythonReader_Deserialize(FlatExecutionPlan* fep, void* ctx, Gears_BufferReader* br){
    PythonReaderCtx* pyCtx = ctx;
    // this serialized data reached from another shard (not rdb) so its save to assume the version is PY_OBJECT_TYPE_VERSION
    pyCtx->callback = RedisGearsPy_PyCallbackDeserialize(fep, br, PY_OBJECT_TYPE_VERSION, NULL);
    RedisModule_Assert(pyCtx->callback);
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
        rg_asprintf(&PYINSTALL_DIR, "%s/%s/%d/deps/", moduleDataDir, REDISGEARS_MODULE_NAME, REDISGEARS_MODULE_VERSION);
    }else{
        // try build path first if its exists
#ifdef CPYTHON_PATH
        rg_asprintf(&PYINSTALL_DIR, "%s/", CPYTHON_PATH);

        // we create it temporary to check if exists
        rg_asprintf(&PYENV_DIR, "%s/python3_%s/", PYINSTALL_DIR, REDISGEARS_VERSION_STR);

        if(!PyEnvExist()){
            RG_FREE(PYINSTALL_DIR);
            rg_asprintf(&PYINSTALL_DIR, "%s/", GearsConfig_GetPythonInstallationDir());
        }

        RG_FREE(PYENV_DIR);
#else
        rg_asprintf(&PYINSTALL_DIR, "%s/", GearsConfig_GetPythonInstallationDir());
#endif

    }
    rg_asprintf(&PYENV_DIR, "%s/python3_%s/", PYINSTALL_DIR, REDISGEARS_VERSION_STR);
    rg_asprintf(&PYENV_HOME_DIR, "%s/.venv/", PYENV_DIR);
    rg_asprintf(&PYENV_BIN_DIR, "%s/bin", PYENV_HOME_DIR);
    rg_asprintf(&PYENV_ACTIVATE, "%s/activate_this.py", PYENV_BIN_DIR);
    rg_asprintf(&PYENV_ACTIVATE_SCRIPT, "%s/activate", PYENV_BIN_DIR);
}

static void PrintGlobalPaths(RedisModuleCtx* ctx){
    RedisModule_Log(ctx, "notice", "PYENV_DIR: %s", PYENV_DIR);
    RedisModule_Log(ctx, "notice", "PYENV_HOME_DIR: %s", PYENV_HOME_DIR);
    RedisModule_Log(ctx, "notice", "PYENV_BIN_DIR: %s", PYENV_BIN_DIR);
    RedisModule_Log(ctx, "notice", "PYENV_ACTIVATE: %s", PYENV_ACTIVATE);
    RedisModule_Log(ctx, "notice", "PYENV_ACTIVATE_SCRIPT: %s", PYENV_ACTIVATE_SCRIPT);
}

static int RedisGears_InstallDeps(RedisModuleCtx *ctx) {
#define SHA_256_SIZE            64
#define TMP_DEPS_FILE_PATH_FMT  "/tmp/deps.%s.%s.tgz"
#define TMP_DEPS_FILE_DIR_FMT   "/tmp/deps.%s.%s/"
#define LOCAL_VENV_FMT          PYENV_DIR"/%s"

    const char *no_deps = getenv("GEARS_NO_DEPS");
    bool skip_deps_install = (no_deps && !strcmp(no_deps, "1")) || !GearsConfig_DownloadDeps();
    if(!skip_deps_install && IsEnterprise()){
        skip_deps_install = !GearsConfig_ForceDownloadDepsOnEnterprise();
    }
    const char* shardUid = GetShardUniqueId();
    if (!PyEnvExist()){
        if (skip_deps_install) {
            RedisModule_Log(ctx, "warning", "No Python installation found and auto install is not enabled, aborting.");
            return REDISMODULE_ERR;
        }
        const char* expectedSha256 = GearsConfig_GetDependenciesSha256();

        ExecCommand(ctx, "rm -rf "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);

        ExecCommand(ctx, "curl -o "TMP_DEPS_FILE_PATH_FMT" %s", shardUid, expectedSha256, GearsConfig_GetDependenciesUrl());

        char* sha256Command;
        rg_asprintf(&sha256Command, "sha256sum "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);
        FILE* f = popen(sha256Command, "r");
        RG_FREE(sha256Command);
        char sha256[SHA_256_SIZE];
        if(fscanf(f, "%64s", sha256) != 1){
            RedisModule_Log(ctx, "warning", "Failed to calculate sha25 on file "TMP_DEPS_FILE_PATH_FMT, shardUid, expectedSha256);
            pclose(f);
            return REDISMODULE_ERR;
        }
        pclose(f);

        if(strcmp(expectedSha256, sha256) != 0){
            RedisModule_Log(ctx, "warning", "Failed on sha 256 comparison");
            return REDISMODULE_ERR;
        }

        ExecCommand(ctx, "rm -rf "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256);
        ExecCommand(ctx, "mkdir -p "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256);

        ExecCommand(ctx, "tar -xvzf "TMP_DEPS_FILE_PATH_FMT" -C "TMP_DEPS_FILE_DIR_FMT, shardUid, expectedSha256, shardUid, expectedSha256);

        ExecCommand(ctx, "mkdir -p %s", PYINSTALL_DIR);
        ExecCommand(ctx, "mv "TMP_DEPS_FILE_DIR_FMT"/python3_%s/ %s", shardUid, expectedSha256, REDISGEARS_VERSION_STR, PYINSTALL_DIR);
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
    RedisModule_Assert(dir);
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
        PythonRequirementCtx_Free(reqs[i]);
    }

    array_free(reqs);
}

static int RedisGearsPy_LoadRegistrations(RedisModuleIO *rdb, int encver, int when){
    if(encver > REDISGEARSPY_REQ_DATATYPE_VERSION){
        RedisModule_LogIOError(rdb, "warning", "Give requirement version is not supported, please upgrade to newer RedisGears.");
        return REDISMODULE_ERR;
    }

    // first we need to clear all existing requirements
    RedisGearsPy_ClearRequirements();

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

        // we hold a sharded ref to the requirement, lets free it.
        PythonRequirementCtx_Free(req);
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

int RedisGearsPy_Init(RedisModuleCtx *ctx){
    installDepsPool = Gears_thpool_init(1);
    InitializeGlobalPaths();
    PrintGlobalPaths(ctx);
    if(RedisGears_InstallDeps(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Failed installing python dependencies");
        return REDISMODULE_ERR;
    }

    RedisGearsPy_CreateRequirementsDataType(ctx);

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

    PyObject *pName = PyUnicode_FromString("redisgears");
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
                                                    PY_OBJECT_TYPE_VERSION,
                                                    RedisGearsPy_PyObjectFree,
                                                    RedisGearsPy_PyObjectDup,
                                                    RedisGearsPy_PyCallbackSerialize,
                                                    RedisGearsPy_PyCallbackDeserialize,
                                                    RedisGearsPy_PyObjectToString);

    ArgType* pySessionType = RedisGears_CreateType("PySessionType",
                                                    PY_SESSION_TYPE_VERSION,
                                                    PythonSessionCtx_Free,
                                                    PythonSessionCtx_ShellowCopy,
                                                    PythonSessionCtx_Serialize,
                                                    PythonSessionCtx_Deserialize,
                                                    PythonSessionCtx_ToString);

    ArgType* pySessionRequirementsType = RedisGears_CreateType("PySessionRequirementsType",
                                                                PY_SESSION_REQ_VERSION,
                                                                PythonSessionRequirements_Free,
                                                                PythonSessionRequirements_Dup,
                                                                PythonSessionRequirements_Serialize,
                                                                PythonSessionRequirements_Deserialize,
                                                                PythonSessionRequirements_ToString);

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

    if (RedisModule_CreateCommand(ctx, "rg.pyexportreq", RedisGearsPy_ExportRequirement, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyexportreq");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyimportreq", RedisGearsPy_ImportRequirement, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyimportreq");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pydumpreqs", RedisGearsPy_DumpRequirements, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pydumpreqs");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, IMPORT_REQ_INTERAL_COMMAND, RedisGearsPy_ImportRequirementInternal, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyimportreq");
        return REDISMODULE_ERR;
    }

    PyEval_SaveThread();

    return REDISMODULE_OK;
}

void RedisGearsPy_Clean() {
    if(!RequirementsDict){
        return;
    }

    RedisGearsPy_ClearRequirements();
}
