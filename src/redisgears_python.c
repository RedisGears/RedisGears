#include <Python.h>
#include "record.h"
#include "redisai.h"
#include "globals.h"
#include "commands.h"
#include "config.h"
#include <marshal.h>
#include <assert.h>
#include <redisgears.h>
#include <redisgears_memory.h>
#include <redisgears_python.h>
#include "utils/arr_rm_alloc.h"
#include "utils/dict.h"
#include "lock_handler.h"
#include "GearsBuilder.auto.h"
#include "cloudpickle.auto.h"

static PyObject* pFunc;
static PyObject* pyGlobals;
PyObject* GearsError;

static RedisModuleCtx* currentCtx = NULL;
static bool blockingExecute = true;
static bool executionTriggered = false;

static PyThreadState *_save;

#define PYTHON_ERROR "error running python code"

static void RedisGearsPy_PyCallbackSerialize(void* arg, BufferWriter* bw);
static void* RedisGearsPy_PyCallbackDeserialize(BufferReader* br);
static void TimeEvent_Free(void *value);

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
    RSM_Map(pfep->fep, RedisGearsPy_PyCallbackMapper, callback);
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
    RSM_Filter(pfep->fep, RedisGearsPy_PyCallbackFilter, callback);
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
	RSM_AccumulateBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackAccumulateByKey, accumulator);
	RSM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
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
    RSM_GroupBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackReducer, reducer);
    RSM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    Py_INCREF(self);
    return self;
}

static PyObject* collect(PyObject *self, PyObject *args){
    if(PyTuple_Size(args) != 0){
        PyErr_SetString(GearsError, "wrong number of args to collect function");
        return NULL;
    }
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    RSM_Collect(pfep->fep);
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
    RSM_ForEach(pfep->fep, RedisGearsPy_PyCallbackForEach, callback);
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
    RSM_Repartition(pfep->fep, RedisGearsPy_PyCallbackExtractor, callback);
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
    RSM_FlatMap(pfep->fep, RedisGearsPy_PyCallbackFlatMapper, callback);
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
    if(!PyObject_TypeCheck(len, &PyInt_Type)){
        PyErr_SetString(GearsError, "limit argument must be a number");
        return NULL;
    }
    long lenLong = PyInt_AsLong(len);
    long offsetLong = 0;
    if(PyTuple_Size(args) > 1){
        PyObject* offset= PyTuple_GetItem(args, 1);
        if(!PyObject_TypeCheck(offset, &PyInt_Type)){
            PyErr_SetString(GearsError, "limit argument must be a number");
            return NULL;
        }
        offsetLong = PyInt_AsLong(offset);
    }
    RSM_Limit(pfep->fep, (size_t)offsetLong, (size_t)lenLong);
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
    RSM_Accumulate(pfep->fep, RedisGearsPy_PyCallbackAccumulate, callback);
    Py_INCREF(self);
    return self;
}

static void onDone(ExecutionPlan* ep, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    Command_ReturnResults(ep, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ep);
    RedisModule_FreeThreadSafeContext(rctx);
}

static void dropExecutionOnDone(ExecutionPlan* ep, void* privateData){
    RedisGears_DropExecution(ep);
}

static PyObject* run(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    char* regexStr = "*";
    if(PyTuple_Size(args) > 0){
        PyObject* regex = PyTuple_GetItem(args, 0);
        regexStr = PyString_AsString(regex);
    }
    ExecutionPlan* ep = RSM_Run(pfep->fep, RG_STRDUP(regexStr), NULL, NULL);
    executionTriggered = true;
    if(!currentCtx){
        RedisGears_RegisterExecutionDoneCallback(ep, dropExecutionOnDone);
    }
    else if(!blockingExecute){
        const char* id = RedisGears_GetId(ep);
        RedisModule_ReplyWithStringBuffer(currentCtx, id, strlen(id));
    }else{
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(currentCtx, NULL, NULL, NULL, 1000000);
        RedisGears_RegisterExecutionDoneCallback(ep, onDone);
        RedisGears_SetPrivateData(ep, bc, NULL);
    }
    return Py_None;
}

static PyObject* registerExecution(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    PyObject* regex = PyTuple_GetItem(args, 0);
    char* regexStr = "*";
    if(regex){
        regexStr = PyString_AsString(regex);
    }
    int status = RSM_Register(pfep->fep, RG_STRDUP(regexStr));
    executionTriggered = true;
    if(!currentCtx){
        return Py_None;
    }
    if(status){
        RedisModule_ReplyWithSimpleString(currentCtx, "OK");
    }else{
        RedisModule_ReplyWithError(currentCtx, "Registration Failed");
    }
    return Py_None;
}

PyMethodDef PyFlatExecutionMethods[] = {
    {"map", map, METH_VARARGS, "map operation on each record"},
    {"filter", filter, METH_VARARGS, "filter operation on each record"},
    {"batchgroupby", groupby, METH_VARARGS, "batch groupby operation on each record"},
	{"groupby", accumulateby, METH_VARARGS, "groupby operation on each record"},
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
    return PyString_FromString("PyFlatExecution");
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
    char* readerStr = "KeysReader"; // default reader
    if(PyTuple_Size(args) > 0){
        PyObject* reader = PyTuple_GetItem(args, 0);
        readerStr = PyString_AsString(reader);
    }
    PyFlatExecution* pyfep = PyObject_New(PyFlatExecution, &PyFlatExecutionType);
    pyfep->fep = RedisGears_CreateCtx(readerStr);
    if(!pyfep->fep){
        Py_DecRef((PyObject*)pyfep);
        PyErr_SetString(GearsError, "the given reader are not exists");
        return NULL;
    }
    RSM_Map(pyfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    return (PyObject*)pyfep;
}

static PyObject* saveGlobals(PyObject *cls, PyObject *args){
    pyGlobals = PyEval_GetGlobals();
    Py_INCREF(pyGlobals);
    // main var are not serialize, we want all the user define functions to
    // be serialize so we specify the module on which the user run as not_main!!
    PyDict_SetItemString(pyGlobals, "__name__", PyString_FromString("not_main"));
    return PyLong_FromLong(1);
}

static PyObject* replyToPyList(RedisModuleCallReply *reply){
    if(!reply){
        return Py_None;
    }
    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
        PyObject* ret = PyList_New(0);
        for(size_t i = 0 ; i < RedisModule_CallReplyLength(reply) ; ++i){
            RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply, i);
            PyList_Append(ret, replyToPyList(subReply));
        }
        return ret;
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING ||
            RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
        return PyString_FromStringAndSize(replyStr, len);
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER){
        long long val = RedisModule_CallReplyInteger(reply);
        return PyLong_FromLongLong(val);
    }

    return Py_None;
}

static PyObject* executeCommand(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) < 1){
        return PyList_New(0);
    }
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    PyThreadState *_save = PyEval_SaveThread();
    LockHandler_Acquire(rctx);
    PyEval_RestoreThread(_save);

    RedisModule_AutoMemory(rctx);

    PyObject* command = PyTuple_GetItem(args, 0);
    char* commandStr = PyString_AsString(command);

    RedisModuleString** argements = array_new(RedisModuleString*, 10);
    for(int i = 1 ; i < PyTuple_Size(args) ; ++i){
        PyObject* argument = PyTuple_GetItem(args, i);
        PyObject* argumentStr = PyObject_Str(argument);
        char* argumentCStr = PyString_AsString(argumentStr);
        size_t argLen = PyString_Size(argumentStr);
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

    LockHandler_Realse(rctx);
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

static PyObject* tensorToFlatList(PyObject *cls, PyObject *args){
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

static PyObject* createTensor(PyObject *cls, PyObject *args){
    assert(globals.redisAILoaded);
    PyObject* typeName = PyTuple_GetItem(args, 0);
    char* typeNameStr = PyString_AsString(typeName);
    PyObject* pyDims = PyTuple_GetItem(args, 1);
    long long* dims = array_new(long long, 10);
    double* values = array_new(long long, 1000);
    size_t ndims = getDimsRecursive(pyDims, &dims);
    RAI_Tensor* t = RedisAI_TensorCreate(typeNameStr, dims, ndims);
    getAllValues(pyDims, &values);
    for(long long i = 0 ; i < array_len(values) ; ++i){
        RedisAI_TensorSetValueFromDouble(t, i, values[i]);
    }
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    array_free(dims);
    array_free(values);
    return (PyObject*)pyt;
}

typedef struct PyGraphRunner{
   PyObject_HEAD
   RAI_ModelRunCtx* g;
} PyGraphRunner;

static PyObject* PyGraph_ToStr(PyObject *obj){
    return PyString_FromString("PyGraphRunner to str");
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

static PyObject* creatGraphRunner(PyObject *cls, PyObject *args){
    assert(globals.redisAILoaded);
    PyObject* keyName = PyTuple_GetItem(args, 0);
    char* keyNameStr = PyString_AsString(keyName);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    LockHandler_Acquire(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    // todo: check for type, add api for this
    RAI_Model *g = RedisModule_ModuleTypeGetValue(key);
    RAI_ModelRunCtx* runCtx = RedisAI_ModelRunCtxCreate(g);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    LockHandler_Realse(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    PyGraphRunner* pyg = PyObject_New(PyGraphRunner, &PyGraphRunnerType);
    pyg->g = runCtx;

    return (PyObject*)pyg;
}

static PyObject* graphRunnerAddInput(PyObject *cls, PyObject *args){
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    PyObject* inputName = PyTuple_GetItem(args, 1);
    char* inputNameStr = PyString_AsString(inputName);
    PyTensor* pyt = (PyTensor*)PyTuple_GetItem(args, 2);
    RedisAI_ModelRunCtxAddInput(pyg->g, inputNameStr, pyt->t);
    return PyLong_FromLong(1);
}

static PyObject* graphRunnerAddOutput(PyObject *cls, PyObject *args){
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    PyObject* outputName = PyTuple_GetItem(args, 1);
    char* outputNameStr = PyString_AsString(outputName);
    RedisAI_ModelRunCtxAddOutput(pyg->g, outputNameStr);
    return PyLong_FromLong(1);
}

static PyObject* graphRunnerRun(PyObject *cls, PyObject *args){
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    RedisAI_ModelRun(pyg->g);
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = RedisAI_TensorGetShallowCopy(RedisAI_ModelRunCtxOutputTensor(pyg->g, 0));
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
}TimerData;

#define RG_TIME_EVENT_TYPE "rg_timeev"

RedisModuleType *TimeEventType;

static void TimeEvent_Callback(RedisModuleCtx *ctx, void *data){
    TimerData* td = data;
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pArgs = PyTuple_New(0);
    PyObject_CallObject(td->callback, pArgs);
    if(PyErr_Occurred()){
        PyErr_Print();
        td->status =TE_STATUS_ERR;
    }else{
        td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    }
    Py_DECREF(pArgs);
    PyGILState_Release(state);
}

static void *TimeEvent_RDBLoad(RedisModuleIO *rdb, int encver){
    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = RedisModule_LoadUnsigned(rdb);
    size_t len;
    char* buff = RedisModule_LoadStringBuffer(rdb, &len);
    Buffer b = {
            .cap = len,
            .size = len,
            .buff = buff,
    };
    BufferReader reader;
    BufferReader_Init(&reader, &b);
    td->callback = RedisGearsPy_PyCallbackDeserialize(&reader);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    td->id = RedisModule_CreateTimer(ctx, td->period * 1000, TimeEvent_Callback, td);
    RedisModule_FreeThreadSafeContext(ctx);
    return td;
}

static void TimeEvent_RDBSave(RedisModuleIO *rdb, void *value){
    TimerData* td = value;
    RedisModule_SaveUnsigned(rdb, td->period);
    Buffer* b = Buffer_New(100);
    BufferWriter bw;
    BufferWriter_Init(&bw, b);
    RedisGearsPy_PyCallbackSerialize(td->callback, &bw);
    RedisModule_SaveStringBuffer(rdb, b->buff, b->size);
    Buffer_Free(b);
}

static void TimeEvent_Free(void *value){
    TimerData* td = value;
    PyGILState_STATE state = PyGILState_Ensure();
    Py_DECREF(td->callback);
    PyGILState_Release(state);
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
        return Py_False;
    }
    PyObject* timeInSec = PyTuple_GetItem(args, 0);
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);
    RedisModuleString* keyNameStr = NULL;
    if(PyTuple_Size(args) == 3){
        PyObject* keyName = PyTuple_GetItem(args, 2);
        if(PyObject_TypeCheck(keyName, &PyString_Type)){
            char* keyNameCStr = PyString_AsString(keyName);
            keyNameStr = RedisModule_CreateString(ctx, keyNameCStr, strlen(keyNameCStr));
        }
    }
    if(!PyObject_TypeCheck(timeInSec, &PyInt_Type)) {
        return Py_False;
    }
    long period = PyInt_AsLong(timeInSec);
    PyObject* callback = PyTuple_GetItem(args, 1);
    TimerData* td = RG_ALLOC(sizeof(*td));
    td->status = TE_STATUS_RUNNING;
    td->period = period;
    td->callback = callback;
    Py_INCREF(callback);

    // avoiding deadlock
    PyThreadState *_save = PyEval_SaveThread();
    LockHandler_Acquire(ctx);
    PyEval_RestoreThread(_save);

    if(keyNameStr){
        RedisModuleKey* key = RedisModule_OpenKey(ctx, keyNameStr, REDISMODULE_WRITE);
        if(RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
            TimeEvent_Free(td);
            LockHandler_Realse(ctx);
            RedisModule_FreeThreadSafeContext(ctx);
            return Py_False;
        }
        RedisModule_ModuleTypeSetValue(key, TimeEventType, td);
    }

    td->id = RedisModule_CreateTimer(ctx, period * 1000, TimeEvent_Callback, td);

    LockHandler_Realse(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    return Py_True;
}

PyMethodDef EmbMethods[] = {
    {"gearsCtx", gearsCtx, METH_VARARGS, "creating an empty gears context"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "execute a redis command and return the result"},
    {"createTensor", createTensor, METH_VARARGS, "creating a tensor object"},
    {"createGraphRunner", creatGraphRunner, METH_VARARGS, "open TF graph by key name"},
    {"graphRunnerAddInput", graphRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"graphRunnerAddOutput", graphRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"graphRunnerRun", graphRunnerRun, METH_VARARGS, "run graph runner"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
    {"registerTimeEvent", gearsTimeEvent, METH_VARARGS, "register a function to be called on each time period"},
    {NULL, NULL, 0, NULL}
};

static int RedisGearsPy_FreeInterpreter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	PyGILState_STATE state = PyGILState_Ensure();
	Py_Finalize();
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

static int RedisGearsPy_Execut(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 2 || argc > 3){
        return RedisModule_WrongArity(ctx);
    }

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
    currentCtx = ctx;
    if(argc == 3){
        const char* block = RedisModule_StringPtrLen(argv[2], NULL);
        if(strcasecmp(block, "UNBLOCKING") == 0){
            blockingExecute = false;
        }
    }

    executionTriggered = false;

    PyEval_RestoreThread(_save);

    PyObject *v;

    v = PyRun_StringFlags(script, Py_file_input, pyGlobals, pyGlobals, NULL);

    if(!v){
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        if(!pvalue){
            RedisModule_ReplyWithError(ctx, "failed running the given script");
        }else{
            PyObject* errStr = PyObject_Str(pvalue);
            char *pStrErrorMessage = PyString_AsString(errStr);
            RedisModule_ReplyWithError(ctx, pStrErrorMessage);
            Py_DECREF(errStr);
            Py_DECREF(ptype);
            Py_DECREF(pvalue);
            if(ptraceback){
                Py_DECREF(ptraceback);
            }
        }
        _save = PyEval_SaveThread();
        return REDISMODULE_OK;
    }
    _save = PyEval_SaveThread();

    if(!executionTriggered){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    executionTriggered = false;
    blockingExecute = true;
    currentCtx = NULL;

    return REDISMODULE_OK;
}

#define FETCH_PY_ERROR \
    PyObject *ptype, *pvalue, *ptraceback;\
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);\
    PyObject* errStr = PyObject_Str(pvalue);\
    char *pStrErrorMessage = PyString_AsString(errStr);\
    *err = RG_STRDUP(pStrErrorMessage);\
    Py_DECREF(errStr);\
    Py_DECREF(ptype);\
    Py_DECREF(pvalue);\
    if(ptraceback){\
        Py_DECREF(ptraceback);\
    }

void RedisGearsPy_PyCallbackForEach(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        FETCH_PY_ERROR
        PyGILState_Release(state);
        return;
    }
    if(ret != Py_None){
    	Py_DECREF(ret);
    }
    PyGILState_Release(state);
}

static Record* RedisGearsPy_PyCallbackAccumulateByKey(RedisModuleCtx* rctx, char* key, Record *accumulate, Record *r, void* arg, char** err){
	PyGILState_STATE state = PyGILState_Ensure();
	PyObject* pArgs = PyTuple_New(3);
	PyObject* callback = arg;
	PyObject* currObj = RG_PyObjRecordGet(r);
	PyObject* keyPyStr = PyString_FromString(key);
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
	    FETCH_PY_ERROR
	    RedisGears_FreeRecord(accumulate);
		RedisGears_FreeRecord(r);
		PyGILState_Release(state);
		return NULL;
	}
	RG_PyObjRecordSet(accumulate, newAccumulateObj);
	RedisGears_FreeRecord(r);
	PyGILState_Release(state);
	return accumulate;
}

static Record* RedisGearsPy_PyCallbackAccumulate(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
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
        FETCH_PY_ERROR
        RedisGears_FreeRecord(accumulate);
        RedisGears_FreeRecord(r);
        PyGILState_Release(state);
        return NULL;
    }
    RG_PyObjRecordSet(accumulate, newAccumulateObj);
    RedisGears_FreeRecord(r);
    PyGILState_Release(state);
    return accumulate;
}

static Record* RedisGearsPy_PyCallbackMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RG_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        FETCH_PY_ERROR
        RedisGears_FreeRecord(record);
        PyGILState_Release(state);
        return NULL;
    }
    RG_PyObjRecordSet(record, newObj);
    PyGILState_Release(state);
    return record;
}

static Record* RedisGearsPy_PyCallbackFlatMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RedisGears_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RG_PyObjRecordGet(record);
    RG_PyObjRecordSet(record, NULL);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        FETCH_PY_ERROR
        RedisGears_FreeRecord(record);
        PyGILState_Release(state);
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
    PyGILState_Release(state);
    return record;
}

static bool RedisGearsPy_PyCallbackFilter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RedisGears_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        FETCH_PY_ERROR
        PyGILState_Release(state);
        return false;
    }
    bool ret1 = PyObject_IsTrue(ret);
    PyGILState_Release(state);
    return ret1;
}

static char* RedisGearsPy_PyCallbackExtractor(RedisModuleCtx* rctx, Record *record, void* arg, size_t* len, char** err){
	PyGILState_STATE state = PyGILState_Ensure();
    assert(RedisGears_RecordGetType(record) == PY_RECORD);
    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* obj = RG_PyObjRecordGet(record);
    Py_INCREF(obj);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(extractor, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        FETCH_PY_ERROR
        PyGILState_Release(state);
        return "";
    }
    PyObject* retStr;
    if(!PyObject_TypeCheck(ret, &PyBaseString_Type)){
        retStr = PyObject_Repr(ret);
        Py_DECREF(ret);
    }else{
        retStr = ret;
    }
    char* retCStr = PyString_AsString(retStr);
    *len = strlen(retCStr);
    char* retValue = RG_ALLOC(*len + 1);
    memcpy(retValue, retCStr, *len);
    retValue[*len] = '\0';
    Py_DECREF(retStr);
    //Py_DECREF(retStr); todo: we should uncomment it after we will pass bool
    //                         that will tell the extractor to free the memory!!
    PyGILState_Release(state);
    return retValue;
}

static Record* RedisGearsPy_PyCallbackReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RedisGears_RecordGetType(records) == LIST_RECORD);
    PyObject* obj = PyList_New(0);
    for(size_t i = 0 ; i < RedisGears_ListRecordLen(records) ; ++i){
        Record* r = RedisGears_ListRecordGet(records, i);
        assert(RedisGears_RecordGetType(r) == PY_RECORD);
        PyObject* element = RG_PyObjRecordGet(r);
        PyList_Append(obj, element);
    }
    PyObject* reducer = arg;
    PyObject* pArgs = PyTuple_New(2);
    PyObject* keyPyObj = PyString_FromString(key);
    PyTuple_SetItem(pArgs, 0, keyPyObj);
    PyTuple_SetItem(pArgs, 1, obj);
    PyObject* ret = PyObject_CallObject(reducer, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        FETCH_PY_ERROR
        RedisGears_FreeRecord(records);
        PyGILState_Release(state);
        return NULL;
    }
    Record* retRecord = RG_PyObjRecordCreate();
    RG_PyObjRecordSet(retRecord, ret);
    RedisGears_FreeRecord(records);
    PyGILState_Release(state);
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
        obj = PyString_FromStringAndSize(str, len);
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
        temp = PyString_FromString(key);
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
            temp = PyString_FromString(key);
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

static Record* RedisGearsPy_ToPyRecordMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    Record* res = RedisGearsPy_ToPyRecordMapperInternal(record, arg);
    RedisGears_FreeRecord(record);
    PyGILState_Release(state);
    return res;
}

static void* RedisGearsPy_PyObjectDup(void* arg){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* obj = arg;
    Py_INCREF(obj);
    PyGILState_Release(state);
    return arg;
}

static void RedisGearsPy_PyObjectFree(void* arg){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* obj = arg;
    Py_DECREF(obj);
    PyGILState_Release(state);
}

void RedisGearsPy_PyObjectSerialize(void* arg, BufferWriter* bw){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* obj = arg;
    PyObject* objStr = PyMarshal_WriteObjectToString(obj, Py_MARSHAL_VERSION);
    if(!objStr){
        PyErr_Print();
        assert(false);
    }
    size_t len = PyString_Size(objStr);
    char* objStrCstr  = PyString_AsString(objStr);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(objStr);
    PyGILState_Release(state);
    return;
}

void* RedisGearsPy_PyObjectDeserialize(BufferReader* br){
    PyGILState_STATE state = PyGILState_Ensure();
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject* obj = PyMarshal_ReadObjectFromString(data, len);
    PyGILState_Release(state);
    return obj;
}

static void RedisGearsPy_PyCallbackSerialize(void* arg, BufferWriter* bw){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* callback = arg;
    PyObject *pickleFunction = PyDict_GetItemString(pyGlobals, "dumps");
    PyObject *args = PyTuple_New(1);
    Py_INCREF(callback);
    PyTuple_SetItem(args, 0, callback);
    PyObject * serializedStr = PyObject_CallObject(pickleFunction, args);
    if(!serializedStr || PyErr_Occurred()){
        PyErr_Print();
        exit(1);
    }
    Py_DECREF(args);
    size_t len = PyString_Size(serializedStr);
    char* objStrCstr  = PyString_AsString(serializedStr);
    RedisGears_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(serializedStr);
    PyGILState_Release(state);
    return;
}

static void* RedisGearsPy_PyCallbackDeserialize(BufferReader* br){
    PyGILState_STATE state = PyGILState_Ensure();
    size_t len;
    char* data = RedisGears_BRReadBuffer(br, &len);
    PyObject *dataStr = PyString_FromStringAndSize(data, len);
    PyObject *loadFunction = PyDict_GetItemString(pyGlobals, "loads");
    PyObject *args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, dataStr);
    PyObject * callback = PyObject_CallObject(loadFunction, args);
    if(!callback || PyErr_Occurred()){
        PyErr_Print();
        exit(1);
    }
    Py_DECREF(args);
    PyGILState_Release(state);
    return callback;
}

void Py_SetAllocFunction(void *(*alloc)(size_t));
void Py_SetReallocFunction(void *(*realloc)(void *, size_t));
void Py_SetFreeFunction(void (*free)(void *));

long long totalAllocated = 0;
long long currAllocated = 0;
long long peakAllocated = 0;

typedef struct pymem{
	size_t size;
	char data[];
}pymem;

static void* RedisGearsPy_Alloc(size_t size){
	pymem* m = RG_ALLOC(sizeof(pymem) + size);
	m->size = size;
	totalAllocated += size;
	currAllocated += size;
	if(currAllocated > peakAllocated){
		peakAllocated = currAllocated;
	}
	return m->data;
}

static void* RedisGearsPy_Relloc(void * p, size_t size){
	if(!p){
		return RedisGearsPy_Alloc(size);
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

static void RedisGearsPy_Free(void * p){
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

int RedisGearsPy_Init(RedisModuleCtx *ctx){
	Py_SetAllocFunction(RedisGearsPy_Alloc);
	Py_SetReallocFunction(RedisGearsPy_Relloc);
	Py_SetFreeFunction(RedisGearsPy_Free);
	char* arg = "Embeded";
	char* progName = (char*)GearsCOnfig_GetPythonHomeDir();
    Py_SetProgramName(progName);
    Py_Initialize();
    PyEval_InitThreads();
    PySys_SetArgv(1, &arg);
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

    if (PyType_Ready(&PyFlatExecutionType) < 0){
        RedisModule_Log(ctx, "warning", "PyFlatExecutionType not ready");
    }

    PyObject* m = Py_InitModule("redisgears", EmbMethods);

    Py_INCREF(&PyTensorType);
    Py_INCREF(&PyGraphRunnerType);
    Py_INCREF(&PyFlatExecutionType);

    PyModule_AddObject(m, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(m, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);
    PyModule_AddObject(m, "PyFlatExecution", (PyObject *)&PyFlatExecutionType);

    GearsError = PyErr_NewException("spam.error", NULL, NULL);
    Py_INCREF(GearsError);
    PyModule_AddObject(m, "GearsError", GearsError);

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

    PyObject* pName = PyString_FromString("types");
    PyObject* pModule = PyImport_Import(pName);
    pFunc = PyObject_GetAttrString(pModule, "FunctionType");
    Py_DECREF(pName);

    if(PyErr_Occurred()){
        PyErr_Print();
        return REDISMODULE_ERR;
    }

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType", RedisGearsPy_PyObjectFree, RedisGearsPy_PyObjectDup, RedisGearsPy_PyCallbackSerialize, RedisGearsPy_PyCallbackDeserialize);

    RSM_RegisterForEach(RedisGearsPy_PyCallbackForEach, pyCallbackType);
    RSM_RegisterFilter(RedisGearsPy_PyCallbackFilter, pyCallbackType);
    RSM_RegisterMap(RedisGearsPy_ToPyRecordMapper, NULL);
    RSM_RegisterMap(RedisGearsPy_PyCallbackFlatMapper, pyCallbackType);
    RSM_RegisterMap(RedisGearsPy_PyCallbackMapper, pyCallbackType);
    RSM_RegisterAccumulator(RedisGearsPy_PyCallbackAccumulate, pyCallbackType);
    RSM_RegisterAccumulatorByKey(RedisGearsPy_PyCallbackAccumulateByKey, pyCallbackType);
    RSM_RegisterGroupByExtractor(RedisGearsPy_PyCallbackExtractor, pyCallbackType);
    RSM_RegisterReducer(RedisGearsPy_PyCallbackReducer, pyCallbackType);

    if(TimeEvent_RegisterType(ctx) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "could not register command timer datatype");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyexecute", RedisGearsPy_Execut, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyexecute");
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

    _save = PyEval_SaveThread();

    return REDISMODULE_OK;
}
