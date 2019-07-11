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
#include "GearsBuilder.auto.h"
#include "cloudpickle.auto.h"

#define SUB_INTERPRETER_TYPE "subInterpreterType"

/* TODO: this needs to be exported from RAI via an API */
typedef struct RAI_Error {
 int code;
 char* detail;
 char* detail_oneline;
} RAI_Error;

static PyObject* pyGlobals;
PyObject* GearsError;

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
 * Sub-interpreter maybe shared between multiple python runners (registered, time events and normal execution).
 * This is why it need to be refcounted and owned by multiple owners.
 * We free sub-interpreter once its refcount reach zero
 */
typedef struct PythonSubInterpreter{
    size_t refCount;
    PyThreadState* subInterpreter;
}PythonSubInterpreter;

/*
 * Thread spacific data
 */
typedef struct PythonThreadCtx{
    int lockCounter;
    RedisModuleCtx* currentCtx;
    PythonSubInterpreter* subInterpreter;
    bool blockingExecute;
    bool executionTriggered;
    bool timeEventRegistered;
    DoneCallbackFunction doneFunction;
}PythonThreadCtx;

/* default onDone function */
static void onDone(ExecutionPlan* ep, void* privateData);

/* the main interpreter */
static PythonSubInterpreter* MainInterpreter;

/* callback that get gears remote builder and run it, used for python client */
static PyObject *runGearsRemoteBuilderCallback;

#define PYTHON_ERROR "error running python code"

static void RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw);
static void* RedisGearsPy_PyCallbackDeserialize(Gears_BufferReader* br);
static void TimeEvent_Free(void *value);
static void RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw);

static PythonThreadCtx* GetPythonThreadCtx(){
    PythonThreadCtx* ptctx = pthread_getspecific(pythonThreadCtxKey);
    if(!ptctx){
        ptctx = RG_ALLOC(sizeof(*ptctx));
        *ptctx = (PythonThreadCtx){
                .lockCounter = 0,
                .currentCtx = NULL,
                .subInterpreter = NULL,
                .blockingExecute = true,
                .executionTriggered = false,
                .timeEventRegistered = false,
                .doneFunction = onDone,
        };
        pthread_setspecific(pythonThreadCtxKey, ptctx);
    }
    return ptctx;
}

/**
 * This functions override the PyGILState_Ensure and PyGILState_Release of the interpreter.
 * Those function are not working well with Sub-interpreters yet we do not want to deny
 * the user from using them. We override them with our own implementation that operate as follow:
 * 1. If the GIL is already acquired by the thread we continue the run (with the same Sub-interpreter)
 * 2. If the GIL is not yet acquired we acquired it and run with the main interpreter.
 *
 * Notice that this is not a perfect solution. If an extension written in C open other threads,
 * Then those threads, when acquiring the GIL, will be run on the main interpreter while the original
 * thread runs on the Sub-interpreter.
 *
 * We believe that this solution will be good enough for most use-cases, but we are still operate under
 * the best effort approach.
 */
PyGILState_STATE PyGILState_Ensure(void){
    RedisGearsPy_RestoreThread(NULL);
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    return ptctx->lockCounter == 1 ? PyGILState_UNLOCKED : PyGILState_LOCKED;
}

void PyGILState_Release(PyGILState_STATE oldstate){
    RedisGearsPy_SaveThread();
}

bool RedisGearsPy_IsLockAcquired(){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    return ptctx->lockCounter > 0;
}

void RedisGearsPy_RestoreThread(PythonSubInterpreter* interpreter){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    if(ptctx->lockCounter == 0){
        if(interpreter){
            PyEval_RestoreThread(interpreter->subInterpreter);
        }else{
            PyEval_RestoreThread(MainInterpreter->subInterpreter);
        }
    }
    ++ptctx->lockCounter;
}

void RedisGearsPy_SaveThread(){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    assert(ptctx);
    assert(ptctx->lockCounter > 0);
    if(--ptctx->lockCounter == 0){
        PyEval_SaveThread();
    }
}

static PythonSubInterpreter* RedisGearsPy_SubInterpreterNew(){
    PyThreadState * subInterpreter = Py_NewInterpreter();
    PythonSubInterpreter* interp = RG_ALLOC(sizeof(*interp));
    *interp = (PythonSubInterpreter){
        .refCount = 1,
        .subInterpreter = subInterpreter,
    };
    PyThreadState_Swap(interp->subInterpreter);
    return interp;
}

static PythonSubInterpreter* RedisGearsPy_SubInterpreterShallowCopy(PythonSubInterpreter* subInterpreter){
    RedisGearsPy_RestoreThread(subInterpreter);
    subInterpreter->refCount++;
    RedisGearsPy_SaveThread();
    return subInterpreter;
}

static void RedisGearsPy_FreeSubInterpreter(void* PD){
    PythonSubInterpreter* subInterpreter = PD;
    assert(subInterpreter);

    RedisGearsPy_RestoreThread(subInterpreter);

    assert(subInterpreter->refCount > 0);

    if(--subInterpreter->refCount == 0){
        Py_EndInterpreter(subInterpreter->subInterpreter);
        RG_FREE(subInterpreter);
        PyThreadState_Swap(MainInterpreter->subInterpreter);
    }

    RedisGearsPy_SaveThread();
}

static void* RedisGearsPy_SubInterpreterDup(void* arg){
    PythonSubInterpreter* subInterpreter = arg;
    return RedisGearsPy_SubInterpreterShallowCopy(subInterpreter);
}

static void RedisGearsPy_SubInterpreterSerialize(void* arg, Gears_BufferWriter* bw){
    // do nothing, we do not serialize subinterpreter, we will create new subinterpreter
    // when deserialize will be called.
}

static void* RedisGearsPy_SubInterpreterDeserialize(Gears_BufferReader* br){
    RedisGearsPy_RestoreThread(MainInterpreter);

    PythonSubInterpreter* subInterpreter = RedisGearsPy_SubInterpreterNew();

    RedisGearsPy_SaveThread();

    return subInterpreter;
}

static char* RedisGearsPy_SubInterpreterToString(void* arg){
    // todo: maybe add information about the subinterpreter !?
    return RG_STRDUP("subinterpreter ToStr");
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
        assert(RedisGears_RecordGetType(r) == PY_RECORD);
        PyObject* obj = RG_PyObjRecordGet(r);
        Gears_BufferWriter bw;
        Gears_BufferWriterInit(&bw, buff);
        RedisGearsPy_PyCallbackSerialize(obj, &bw);
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

static PyObject* run(PyObject *self, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    char* defaultRegexStr = "*";
    char* regexStr = defaultRegexStr;
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
    }else{
        if(PyTuple_Size(args) > 0){
            PyObject* regex = PyTuple_GetItem(args, 0);
            if(!PyUnicode_Check(regex)){
                PyErr_SetString(GearsError, "reader argument must be a string");
                return NULL;
            }
            regexStr = (char*)PyUnicode_AsUTF8AndSize(regex, NULL);
        }
        if(strcmp(RedisGears_GetReader(pfep->fep), "StreamReader") == 0){
                arg = RedisGears_StreamReaderCtxCreate(regexStr, "0-0");
        }else{
            arg = RG_STRDUP(regexStr);
        }
    }
    RedisGears_SetFlatExecutionPrivateData(pfep->fep, SUB_INTERPRETER_TYPE,
                                           RedisGearsPy_SubInterpreterShallowCopy(ptctx->subInterpreter));
    ExecutionPlan* ep = RGM_Run(pfep->fep, arg, NULL, NULL);
    ptctx->executionTriggered = true;
    if(!ptctx->currentCtx){
        RedisGears_RegisterExecutionDoneCallback(ep, dropExecutionOnDone);
    }
    else if(!ptctx->blockingExecute){
        const char* id = RedisGears_GetId(ep);
        RedisModule_ReplyWithStringBuffer(ptctx->currentCtx, id, strlen(id));
    }else{
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(ptctx->currentCtx, NULL, NULL, NULL, 1000000);
        RedisGears_RegisterExecutionDoneCallback(ep, ptctx->doneFunction);
        RedisGears_SetPrivateData(ep, bc, NULL);
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* registerExecution(PyObject *self, PyObject *args){
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    PyObject* regex = NULL;
    if(PyTuple_Size(args) > 0){
        regex = PyTuple_GetItem(args, 0);
    }
    char* defaultRegexStr = "*";
    const char* regexStr = defaultRegexStr;
    if(regex){
        if(PyUnicode_Check(regex)){
            regexStr = PyUnicode_AsUTF8AndSize(regex, NULL);
        }else{
            PyErr_SetString(GearsError, "register argument must be a string");
            return NULL;
        }
    }
    RedisGears_SetFlatExecutionPrivateData(pfep->fep, SUB_INTERPRETER_TYPE,
                                           RedisGearsPy_SubInterpreterShallowCopy(ptctx->subInterpreter));
    int status = RGM_Register(pfep->fep, RG_STRDUP(regexStr));
    ptctx->executionTriggered = true;
    if(!ptctx->currentCtx){
        Py_INCREF(Py_None);
        return Py_None;
    }
    if(status){
        RedisModule_ReplyWithSimpleString(ptctx->currentCtx, "OK");
    }else{
        RedisModule_ReplyWithError(ptctx->currentCtx, "Registration Failed");
    }
    Py_INCREF(Py_None);
    return Py_None;
}

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
    {"run", run, METH_VARARGS, "start the execution"},
    {"register", registerExecution, METH_VARARGS, "register the execution on an event"},
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

static PyObject* executeCommand(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) < 1){
        return PyList_New(0);
    }
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(rctx);

    RedisModule_AutoMemory(rctx);

    PyObject* command = PyTuple_GetItem(args, 0);
    if(!PyUnicode_Check(command)){
        PyErr_SetString(GearsError, "the given command must be a string");
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

    RedisModuleCallReply *reply = RedisModule_Call(rctx, commandStr, "v", argements, array_len(argements));

    PyObject* res = replyToPyList(reply);

    if(reply){
        RedisModule_FreeCallReply(reply);
    }
    array_free(argements);

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
    TimeEventStatus status;
    PythonSubInterpreter* subInterpreter;
}TimerData;

#define RG_TIME_EVENT_TYPE "rg_timeev"

RedisModuleType *TimeEventType;

static void TimeEvent_Callback(RedisModuleCtx *ctx, void *data){
    TimerData* td = data;
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    ptctx->subInterpreter = td->subInterpreter;
    RedisGearsPy_RestoreThread(ptctx->subInterpreter);
    PyObject* pArgs = PyTuple_New(0);
    PyObject_CallObject(td->callback, pArgs);
    if(PyErr_Occurred()){
        PyErr_Print();
        td->status =TE_STATUS_ERR;
    }else{
        td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    }
    Py_DECREF(pArgs);
    RedisGearsPy_SaveThread();
}

static void *TimeEvent_RDBLoad(RedisModuleIO *rdb, int encver){
    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = RedisModule_LoadUnsigned(rdb);
    size_t len;
    char* buff = RedisModule_LoadStringBuffer(rdb, &len);
    Gears_Buffer b = {
            .cap = len,
            .size = len,
            .buff = buff,
    };
    Gears_BufferReader reader;
    Gears_BufferReaderInit(&reader, &b);
    td->callback = RedisGearsPy_PyCallbackDeserialize(&reader);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    RedisGearsPy_RestoreThread(NULL);
    td->subInterpreter = RedisGearsPy_SubInterpreterNew();
    RedisGearsPy_SaveThread();
    RedisModule_FreeThreadSafeContext(ctx);
    return td;
}

static void TimeEvent_RDBSave(RedisModuleIO *rdb, void *value){
    TimerData* td = value;
    RedisModule_SaveUnsigned(rdb, td->period);
    Gears_Buffer* b = Gears_BufferNew(100);
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, b);
    RedisGearsPy_PyCallbackSerialize(td->callback, &bw);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Gears_BufferFree(b);
}

static void TimeEvent_Free(void *value){
    TimerData* td = value;
    RedisGearsPy_RestoreThread(td->subInterpreter);
    Py_DECREF(td->callback);
    RedisGearsPy_FreeSubInterpreter(td->subInterpreter);
    RedisGearsPy_SaveThread();
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
    td->subInterpreter = RedisGearsPy_SubInterpreterShallowCopy(ptctx->subInterpreter);
    ptctx->timeEventRegistered = true;
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
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "execute a redis command and return the result"},
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
    RedisGearsPy_RestoreThread(NULL);
	Py_Finalize();
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

static int RedisGearsPy_ExecuteRemote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
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

    PyObject * grb = RedisGearsPy_PyCallbackDeserialize(&br);

    Gears_BufferFree(buff);

    if(!grb){
        RedisModule_ReplyWithError(ctx, "could not deserialize GearsRemoteBuilder");
        return REDISMODULE_OK;
    }

    RedisGearsPy_RestoreThread(NULL);

    PythonThreadCtx* ptctx = GetPythonThreadCtx();

    DoneCallbackFunction oldDoneFunction = ptctx->doneFunction;
    ptctx->doneFunction = onDoneSerializeResults;

    ptctx->currentCtx = ctx;

    ptctx->executionTriggered = false;

    // create sub-interpreter
    ptctx->subInterpreter = RedisGearsPy_SubInterpreterNew();

    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, grb);
    PyObject* globalsDict = PyDict_Copy(pyGlobals);
    PyTuple_SetItem(pArgs, 1, globalsDict);
    PyObject* v = PyObject_CallObject(runGearsRemoteBuilderCallback, pArgs);
    Py_DECREF(pArgs);

    ptctx->doneFunction = oldDoneFunction;

    if(!v){
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        if(!pvalue){
            RedisModule_ReplyWithError(ctx, "failed running the given script");
        }else{
            PyObject* errStr = PyObject_Str(pvalue);
            size_t s;
            const char *pStrErrorMessage = PyUnicode_AsUTF8AndSize(errStr, &s);
            RedisModule_ReplyWithError(ctx, pStrErrorMessage);
            Py_DECREF(errStr);
            Py_DECREF(ptype);
            Py_DECREF(pvalue);
            if(ptraceback){
                Py_DECREF(ptraceback);
            }
        }
        RedisGearsPy_FreeSubInterpreter(ptctx->subInterpreter);
        PyThreadState_Swap(MainInterpreter->subInterpreter);
        RedisGearsPy_SaveThread();
        return REDISMODULE_OK;
    }

    if(ptctx->timeEventRegistered){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }else if(!ptctx->executionTriggered){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    RedisGearsPy_FreeSubInterpreter(ptctx->subInterpreter);
    PyThreadState_Swap(MainInterpreter->subInterpreter);
    RedisGearsPy_SaveThread();

    ptctx->executionTriggered = false;
    ptctx->blockingExecute = true;
    ptctx->currentCtx = NULL;

    return REDISMODULE_OK;
}

int RedisGearsPy_Execute(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2 || argc > 3){
        return RedisModule_WrongArity(ctx);
    }

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
    PythonThreadCtx* ptctx = GetPythonThreadCtx();
    ptctx->currentCtx = ctx;
    ptctx->blockingExecute = true;
    if(argc == 3){
        const char* block = RedisModule_StringPtrLen(argv[2], NULL);
        if(strcasecmp(block, "UNBLOCKING") == 0){
            ptctx->blockingExecute = false;
        }
    }

    ptctx->executionTriggered = false;

    RedisGearsPy_RestoreThread(NULL);

    ptctx->subInterpreter = RedisGearsPy_SubInterpreterNew();

    PyObject *v;

    PyObject* globalsDict = PyDict_Copy(pyGlobals);
    v = PyRun_StringFlags(script, Py_file_input, globalsDict, globalsDict, NULL);
    Py_DECREF(globalsDict);

    if(!v){
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        if(!pvalue){
            RedisModule_ReplyWithError(ctx, "failed running the given script");
        }else{
            PyObject* errStr = PyObject_Str(pvalue);
            size_t s;
            const char *pStrErrorMessage = PyUnicode_AsUTF8AndSize(errStr, &s);
            RedisModule_ReplyWithError(ctx, pStrErrorMessage);
            Py_DECREF(errStr);
            Py_DECREF(ptype);
            Py_DECREF(pvalue);
            if(ptraceback){
                Py_DECREF(ptraceback);
            }
        }
        RedisGearsPy_FreeSubInterpreter(ptctx->subInterpreter);
        RedisGearsPy_SaveThread();
        return REDISMODULE_OK;
    }

    if(ptctx->timeEventRegistered){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }else if(!ptctx->executionTriggered){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    RedisGearsPy_FreeSubInterpreter(ptctx->subInterpreter);
    PyThreadState_Swap(MainInterpreter->subInterpreter);
    RedisGearsPy_SaveThread();

    ptctx->executionTriggered = false;
    ptctx->blockingExecute = true;
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

void fetchPyError(ExecutionCtx* rctx) {
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
                pStrTraceback = PyObject_Str(pCall);
                Py_DECREF(pCall);
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
    RedisGears_SetError(rctx, RG_STRDUP(strTraceback));
    Py_DECREF(pStrTraceback);
    Py_DECREF(pModuleName);
    Py_DECREF(pType);
    Py_DECREF(pValue);
    if(pTraceback){
        Py_DECREF(pTraceback);
    }
}

void RedisGearsPy_PyCallbackForEach(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == PY_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);


    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
        return;
    }
    if(ret != Py_None){
    	Py_DECREF(ret);
    }

    RedisGearsPy_SaveThread();
}

static Record* RedisGearsPy_PyCallbackAccumulateByKey(ExecutionCtx* rctx, char* key, Record *accumulate, Record *r, void* arg){

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
	RedisGearsPy_RestoreThread(subInterpreter);

	PyObject* pArgs = PyTuple_New(3);
	PyObject* callback = arg;
	PyObject* currObj = RG_PyObjRecordGet(r);
	PyObject* keyPyStr = PyUnicode_FromString(key);
	RG_PyObjRecordSet(r, NULL);
	PyObject* oldAccumulateObj = Py_None;
	Py_INCREF(oldAccumulateObj);
	if(!accumulate){
		accumulate = RG_PyObjRecordCreate();
	}else{
		oldAccumulateObj = RG_PyObjRecordGet(accumulate);
	}
	PyTuple_SetItem(pArgs, 0, keyPyStr);
	PyTuple_SetItem(pArgs, 1, oldAccumulateObj);
	PyTuple_SetItem(pArgs, 2, currObj);
	PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
	Py_DECREF(pArgs);
	if(!newAccumulateObj){
	    fetchPyError(rctx);

	    RedisGearsPy_SaveThread();
		RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
		return NULL;
	}
	RG_PyObjRecordSet(accumulate, newAccumulateObj);

	RedisGearsPy_SaveThread();
    RedisGears_FreeRecord(r);
	return accumulate;
}

static Record* RedisGearsPy_PyCallbackAccumulate(ExecutionCtx* rctx, Record *accumulate, Record *r, void* arg){

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* pArgs = PyTuple_New(2);
    PyObject* callback = arg;
    PyObject* currObj = RG_PyObjRecordGet(r);
    RG_PyObjRecordSet(r, NULL);
    PyObject* oldAccumulateObj = Py_None;
    Py_INCREF(oldAccumulateObj);
    if(!accumulate){
        accumulate = RG_PyObjRecordCreate();
    }else{
        oldAccumulateObj = RG_PyObjRecordGet(accumulate);
    }
    PyTuple_SetItem(pArgs, 0, oldAccumulateObj);
    PyTuple_SetItem(pArgs, 1, currObj);
    PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newAccumulateObj){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
        RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
        return NULL;
    }
    RG_PyObjRecordSet(accumulate, newAccumulateObj);

    RedisGearsPy_SaveThread();

    RedisGears_FreeRecord(r);
    return accumulate;
}

static Record* RedisGearsPy_PyCallbackMapper(ExecutionCtx* rctx, Record *record, void* arg){
    assert(RedisGears_RecordGetType(record) == PY_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RG_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
        RedisGears_FreeRecord(record);
        return NULL;
    }
    RG_PyObjRecordSet(record, newObj);

    RedisGearsPy_SaveThread();
    return record;
}

static Record* RedisGearsPy_PyCallbackFlatMapper(ExecutionCtx* rctx, Record *record, void* arg){
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == PY_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RG_PyObjRecordGet(record);
    RG_PyObjRecordSet(record, NULL);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
        RedisGears_FreeRecord(record);
        return NULL;
    }
    if(PyList_Check(newObj)){
        RedisGears_FreeRecord(record);
        size_t len = PyList_Size(newObj);
        record = RedisGears_ListRecordCreate(len);
        for(size_t i = 0 ; i < len ; ++i){
            PyObject* temp = PyList_GetItem(newObj, i);
            Record* pyRecord = RG_PyObjRecordCreate();
            Py_INCREF(temp);
            RG_PyObjRecordSet(pyRecord, temp);
            RedisGears_ListRecordAdd(record, pyRecord);
        }
        Py_DECREF(newObj);
    }else{
        RG_PyObjRecordSet(record, newObj);
    }

    RedisGearsPy_SaveThread();
    return record;
}

static bool RedisGearsPy_PyCallbackFilter(ExecutionCtx* rctx, Record *record, void* arg){
    assert(RedisGears_RecordGetType(record) == PY_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
        return false;
    }
    bool ret1 = PyObject_IsTrue(ret);

    RedisGearsPy_SaveThread();
    return ret1;
}

static char* RedisGearsPy_PyCallbackExtractor(ExecutionCtx* rctx, Record *record, void* arg, size_t* len){
    assert(RedisGears_RecordGetType(record) == PY_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(extractor, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        fetchPyError(rctx);

        RedisGearsPy_SaveThread();
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

    RedisGearsPy_SaveThread();
    return retValue;
}

static Record* RedisGearsPy_PyCallbackReducer(ExecutionCtx* rctx, char* key, size_t keyLen, Record *records, void* arg){
    assert(RedisGears_RecordGetType(records) == LIST_RECORD);

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* obj = PyList_New(0);
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(records) ; ++i){
        Record* r = RedisGears_ListRecordGet(records, i);
        assert(RedisGears_RecordGetType(r) == PY_RECORD);
        PyObject* element = RG_PyObjRecordGet(r);
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

        RedisGearsPy_SaveThread();
        RedisGears_FreeRecord(records);
        return NULL;
    }
    Record* retRecord = RG_PyObjRecordCreate();
    RG_PyObjRecordSet(retRecord, ret);

    RedisGearsPy_SaveThread();
    RedisGears_FreeRecord(records);
    return retRecord;
}

static Record* RedisGearsPy_ToPyRecordMapperInternal(Record *record, void* arg){
    Record* res = RG_PyObjRecordCreate();
    Record* tempRecord;
    PyObject* obj;
    PyObject* temp;
    char* str;
    long longNum;
    double doubleNum;
    char* key;
    char** keys;
    size_t len;
    switch(RedisGears_RecordGetType(record)){
    case STRING_RECORD:
        str = RedisGears_StringRecordGet(record, &len);
        // try to first decode it as string, if fails create a byte array.
        obj = PyUnicode_FromStringAndSize(str, len);
        if(!obj){
            PyErr_Clear();
            obj = PyByteArray_FromStringAndSize(str, len);
        }
        break;
    case LONG_RECORD:
        longNum = RedisGears_LongRecordGet(record);
        obj = PyLong_FromLong(longNum);
        break;
    case DOUBLE_RECORD:
        doubleNum = RedisGears_DoubleRecordGet(record);
        obj = PyLong_FromDouble(doubleNum);
        break;
    case KEY_RECORD:
        key = RedisGears_KeyRecordGetKey(record, NULL);
        obj = PyDict_New();
        temp = PyUnicode_FromString(key);
        PyDict_SetItemString(obj, "key", temp);
        Py_DECREF(temp);
        tempRecord = RedisGears_KeyRecordGetVal(record);
        if(tempRecord){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(tempRecord, arg);
            assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
            PyDict_SetItemString(obj, "value", RG_PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }else{
            Py_INCREF(Py_None);
            PyDict_SetItemString(obj, "value", Py_None);
        }

        break;
    case LIST_RECORD:
        len = RedisGears_ListRecordLen(record);
        obj = PyList_New(0);
        for(size_t i = 0 ; i < len ; ++i){
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(RedisGears_ListRecordGet(record, i), arg);
            assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
            PyList_Append(obj, RG_PyObjRecordGet(tempRecord));
            RedisGears_FreeRecord(tempRecord);
        }
        break;
    case HASH_SET_RECORD:
        keys = RedisGears_HashSetRecordGetAllKeys(record, &len);
        obj = PyDict_New();
        for(size_t i = 0 ; i < len ; ++i){
            key = keys[i];
            temp = PyUnicode_FromString(key);
            tempRecord = RedisGears_HashSetRecordGet(record, key);
            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(tempRecord, arg);
            assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
            PyDict_SetItem(obj, temp, RG_PyObjRecordGet(tempRecord));
            Py_DECREF(temp);
            RedisGears_FreeRecord(tempRecord);
        }
        RedisGears_HashSetRecordFreeKeysArray(keys);
        break;
    case PY_RECORD:
        obj = RG_PyObjRecordGet(record);
        Py_INCREF(obj);
        break;
    default:
        assert(false);
    }
    RG_PyObjRecordSet(res, obj);
    return res;
}

static Record* RedisGearsPy_ToPyRecordMapper(ExecutionCtx* rctx, Record *record, void* arg){
    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    Record* res = RedisGearsPy_ToPyRecordMapperInternal(record, arg);
    RedisGearsPy_SaveThread();

    RedisGears_FreeRecord(record);

    return res;
}

static void* RedisGearsPy_PyObjectDup(void* arg){
    RedisGearsPy_RestoreThread(NULL);
    PyObject* obj = arg;
    Py_INCREF(obj);
    RedisGearsPy_SaveThread();
    return arg;
}

static void RedisGearsPy_PyObjectFree(void* arg){
    RedisGearsPy_RestoreThread(NULL);
    PyObject* obj = arg;
    Py_DECREF(obj);
    RedisGearsPy_SaveThread();
}

static char* RedisGearsPy_PyObjectToString(void* arg){
    char* objCstr = NULL;
    RedisGearsPy_RestoreThread(NULL);
    PyObject* obj = arg;
    PyObject *objStr = PyObject_Str(obj);
    const char* objTempCstr = PyUnicode_AsUTF8AndSize(objStr, NULL);
    objCstr = RG_STRDUP(objTempCstr);
    Py_DECREF(objStr);
    RedisGearsPy_SaveThread();
    return objCstr;
}

void RedisGearsPy_PyObjectSerialize(void* arg, Gears_BufferWriter* bw){
    RedisGearsPy_RestoreThread(NULL);
    PyObject* obj = arg;
    PyObject* objStr = PyMarshal_WriteObjectToString(obj, Py_MARSHAL_VERSION);
    if(!objStr){
        PyErr_Print();
        assert(false);
    }
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(objStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(objStr);
    RedisGearsPy_SaveThread();
    return;
}

void* RedisGearsPy_PyObjectDeserialize(Gears_BufferReader* br){
    RedisGearsPy_RestoreThread(NULL);
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject* obj = PyMarshal_ReadObjectFromString(data, len);
    RedisGearsPy_SaveThread();
    return obj;
}

static void RedisGearsPy_PyCallbackSerialize(void* arg, Gears_BufferWriter* bw){
    RedisGearsPy_RestoreThread(NULL);
    PyObject* callback = arg;
    PyObject *pickleFunction = PyDict_GetItemString(pyGlobals, "dumps");
    PyObject *args = PyTuple_New(1);
    Py_INCREF(callback);
    PyTuple_SetItem(args, 0, callback);
    PyObject * serializedStr = PyObject_CallObject(pickleFunction, args);
    if(!serializedStr || PyErr_Occurred()){
        PyErr_Print();
        assert(false);
    }
    Py_DECREF(args);
    size_t len;
    char* objStrCstr;
    PyBytes_AsStringAndSize(serializedStr, &objStrCstr, &len);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(serializedStr);
    RedisGearsPy_SaveThread();
    return;
}

static void* RedisGearsPy_PyCallbackDeserialize(Gears_BufferReader* br){
    RedisGearsPy_RestoreThread(NULL);
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject *dataStr = PyBytes_FromStringAndSize(data, len);
    PyObject *loadFunction = PyDict_GetItemString(pyGlobals, "loads");
    PyObject *args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, dataStr);
    PyObject * callback = PyObject_CallObject(loadFunction, args);
    if(!callback || PyErr_Occurred()){
        PyErr_Print();
        assert(false);
    }
    Py_DECREF(args);
    RedisGearsPy_SaveThread();
    return callback;
}

long long totalAllocated = 0;
long long currAllocated = 0;
long long peakAllocated = 0;

typedef struct pymem{
	size_t size;
	char data[];
}pymem;

static void* RedisGearsPy_Alloc(void* ctx, size_t size){
	pymem* m = RG_ALLOC(sizeof(pymem) + size);
	m->size = size;
	totalAllocated += size;
	currAllocated += size;
	if(currAllocated > peakAllocated){
		peakAllocated = currAllocated;
	}
	return m->data;
}

static void* RedisGearsPy_Calloc(void* ctx, size_t n_elements, size_t size){
    return RedisGearsPy_Alloc(ctx, n_elements * size);
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
}PythonReaderCtx;

static Record* PythonReader_Next(ExecutionCtx* rctx, void* ctx){
    PythonReaderCtx* pyCtx = ctx;

    PythonSubInterpreter* subInterpreter = RedisGears_GetFlatExecutionPrivateData(rctx);
    RedisGearsPy_RestoreThread(subInterpreter);

    PyObject* pyRecord = NULL;
    if(!pyCtx->generator){
        PyObject* pArgs = PyTuple_New(0);
        PyObject* callback = pyCtx->callback;
        pyRecord = PyObject_CallObject(callback, pArgs);
        if(PyGen_Check(pyRecord)) {
            pyCtx->generator = PyObject_GetIter(pyRecord);
            pyRecord = PyIter_Next(pyCtx->generator);
        }
    }else{
        pyRecord = PyIter_Next(pyCtx->generator);
    }
    if(pyRecord == Py_None || pyRecord == NULL){
        RedisGearsPy_SaveThread();
        return NULL;
    }
    RedisGearsPy_SaveThread();
    Record* record = RG_PyObjRecordCreate();
    RG_PyObjRecordSet(record, pyRecord);
    return record;
}

static void PythonReader_Free(void* ctx){
    PythonReaderCtx* pyCtx = ctx;
    PyObject* callback = pyCtx->callback;
    RedisGearsPy_RestoreThread(NULL);
    Py_DECREF(callback);
    if(pyCtx->generator){
        Py_DECREF(pyCtx->generator);
    }
    RedisGearsPy_SaveThread();
    RG_FREE(pyCtx);
}

static void PythonReader_Serialize(void* ctx, Gears_BufferWriter* bw){
    PythonReaderCtx* pyCtx = ctx;
    RedisGearsPy_PyCallbackSerialize(pyCtx->callback, bw);
}

static void PythonReader_Deserialize(void* ctx, Gears_BufferReader* br){
    PythonReaderCtx* pyCtx = ctx;
    pyCtx->callback = RedisGearsPy_PyCallbackDeserialize(br);
}

static Reader* PythonReader_Create(void* arg){
    PyObject* callback = arg;
    if(callback){
        RedisGearsPy_RestoreThread(NULL);
        Py_INCREF(callback);
        RedisGearsPy_SaveThread();
    }
    PythonReaderCtx* pyCtx = RG_ALLOC(sizeof(*pyCtx));
    pyCtx->callback = callback;
    pyCtx->generator = NULL;
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

#define PYENV_DIR "/opt/redislabs/lib/modules/python3"
#define PYENV_HOME_DIR PYENV_DIR "/.venv/bin"
#define PYENV_ACTIVATE PYENV_HOME_DIR "/activate_this.py"

bool PyEnvExist() {
    DIR* dir = opendir(PYENV_DIR);
    if (dir) {
        closedir(dir);
        return true;
    }
    return false;
}

int RedisGears_SetupPyEnv(RedisModuleCtx *ctx) {
    if (!PyEnvExist()) return 0;
    RedisModule_Log(ctx, "notice", "Initializing Python environment with: " "exec(open('" PYENV_ACTIVATE "').read(), {'__file__': '" PYENV_ACTIVATE "'})");
    PyRun_SimpleString("exec(open('" PYENV_ACTIVATE "').read(), {'__file__': '" PYENV_ACTIVATE "'})");
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

int RedisGearsPy_Init(RedisModuleCtx *ctx){
    int err = pthread_key_create(&pythonThreadCtxKey, NULL);
    if(err){
        return REDISMODULE_ERR;
    }

    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &allocator);
    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &allocator);
    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &allocator);
	char* arg = "Embeded";
	size_t len = strlen(arg);
    char* progName = (char*)GearsConfig_GetPythonHomeDir();
    if (PyEnvExist()) progName = PYENV_HOME_DIR;
    Py_SetProgramName((wchar_t *)progName);

    EmbRedisGears.m_methods = EmbRedisGearsMethods;
    EmbRedisGears.m_size = sizeof(EmbRedisGearsMethods) / sizeof(*EmbRedisGearsMethods);
    PyImport_AppendInittab("redisgears", &PyInit_RedisGears);

    EmbRedisAI.m_methods = EmbRedisAIMethods;
    EmbRedisAI.m_size = sizeof(EmbRedisAIMethods) / sizeof(*EmbRedisAIMethods);
    PyImport_AppendInittab("redisAI", &PyInit_RedisAI);

    Py_Initialize();
    RedisGears_SetupPyEnv(ctx);
    PyEval_InitThreads();
    wchar_t* arg2 = Py_DecodeLocale(arg, &len);
    PySys_SetArgv(1, &arg2);
    PyMem_RawFree(arg2);
    PyTensorType.tp_new = PyType_GenericNew;
    PyGraphRunnerType.tp_new = PyType_GenericNew;
    PyFlatExecutionType.tp_new = PyType_GenericNew;

    PyFlatExecutionType.tp_methods = PyFlatExecutionMethods;

    if (PyType_Ready(&PyTensorType) < 0){
        RedisModule_Log(ctx, "warning", "PyTensorType not ready");
    }

    if (PyType_Ready(&PyGraphRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
    }

    if (PyType_Ready(&PyTorchScriptRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
    }

    if (PyType_Ready(&PyFlatExecutionType) < 0){
        RedisModule_Log(ctx, "warning", "PyFlatExecutionType not ready");
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

    PyModule_AddObject(redisAIModule, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(redisAIModule, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);
    PyModule_AddObject(redisAIModule, "PyTorchScriptRunner", (PyObject *)&PyTorchScriptRunnerType);
    PyModule_AddObject(redisGearsModule, "PyFlatExecution", (PyObject *)&PyFlatExecutionType);

    GearsError = PyErr_NewException("spam.error", NULL, NULL);
    Py_INCREF(GearsError);
    PyModule_AddObject(redisGearsModule, "GearsError", GearsError);

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
        PyErr_Print();
        return REDISMODULE_ERR;
    }

    if(PyErr_Occurred()){
        PyErr_Print();
        return REDISMODULE_ERR;
    }

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType",
                                                    RedisGearsPy_PyObjectFree,
                                                    RedisGearsPy_PyObjectDup,
                                                    RedisGearsPy_PyCallbackSerialize,
                                                    RedisGearsPy_PyCallbackDeserialize,
                                                    RedisGearsPy_PyObjectToString);
    ArgType* subInterpreterType = RedisGears_CreateType(SUB_INTERPRETER_TYPE,
                                                        RedisGearsPy_FreeSubInterpreter,
                                                        RedisGearsPy_SubInterpreterDup,
                                                        RedisGearsPy_SubInterpreterSerialize,
                                                        RedisGearsPy_SubInterpreterDeserialize,
                                                        RedisGearsPy_SubInterpreterToString);

    RedisGears_RegisterFlatExecutionPrivateDataType(subInterpreterType);

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

    PyThreadState* mainInterpreter = PyEval_SaveThread();
    MainInterpreter = RG_ALLOC(sizeof(*MainInterpreter));
    *MainInterpreter = (PythonSubInterpreter){
            .refCount = 1,
            .subInterpreter = mainInterpreter,
    };

    return REDISMODULE_OK;
}
