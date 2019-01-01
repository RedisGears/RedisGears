#include <Python.h>
#include "record.h"
#include "redisdl.h"
#include "globals.h"
#include "commands.h"
#include <marshal.h>
#include <assert.h>
#include <redisgears.h>
#include <redisgears_memory.h>
#include <redisgears_python.h>
#include "utils/arr_rm_alloc.h"

static PyObject* pFunc;
static PyObject* pyGlobals;

static RedisModuleCtx* currentCtx = NULL;
static bool blockingExecute = true;

#define PYTHON_ERROR "error running python code"

typedef struct PyFlatExecution{
   PyObject_HEAD
   FlatExecutionPlan* fep;
} PyFlatExecution;

static PyObject* map(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    Py_INCREF(callback);
    RSM_Map(pfep->fep, RedisGearsPy_PyCallbackMapper, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* filter(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    Py_INCREF(callback);
    RSM_Filter(pfep->fep, RedisGearsPy_PyCallbackFilter, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* groupby(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 2){
        //todo: print error
        return Py_None;
    }
    PyObject* extractor = PyTuple_GetItem(args, 0);
    PyObject* reducer = PyTuple_GetItem(args, 1);
    Py_INCREF(extractor);
    Py_INCREF(reducer);
    RSM_GroupBy(pfep->fep, RedisGearsPy_PyCallbackExtractor, extractor, RedisGearsPy_PyCallbackReducer, reducer);
    RSM_Map(pfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    Py_INCREF(self);
    return self;
}

static PyObject* collect(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    RSM_Collect(pfep->fep);
    Py_INCREF(self);
    return self;
}

static PyObject* foreach(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    Py_INCREF(callback);
    RSM_ForEach(pfep->fep, RedisGearsPy_PyCallbackForEach, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* repartition(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    Py_INCREF(callback);
    RSM_Repartition(pfep->fep, RedisGearsPy_PyCallbackExtractor, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* flatmap(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
    Py_INCREF(callback);
    RSM_FlatMap(pfep->fep, RedisGearsPy_PyCallbackFlatMapper, callback);
    Py_INCREF(self);
    return self;
}

static PyObject* limit(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) < 1){
        //todo: print error
        return Py_None;
    }
    PyObject* len = PyTuple_GetItem(args, 0);
    long lenLong = PyInt_AsLong(len);
    long offsetLong = 0;
    if(PyTuple_Size(args) > 1){
        PyObject* offset= PyTuple_GetItem(args, 1);
        offsetLong = PyInt_AsLong(offset);
    }
    RSM_Limit(pfep->fep, (size_t)offsetLong, (size_t)lenLong);
    Py_INCREF(self);
    return self;
}

static PyObject* accumulate(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    if(PyTuple_Size(args) != 1){
        //todo: print error
        return Py_None;
    }
    PyObject* callback = PyTuple_GetItem(args, 0);
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

static PyObject* run(PyObject *self, PyObject *args){
    PyFlatExecution* pfep = (PyFlatExecution*)self;
    char* regexStr = "*";
    if(PyTuple_Size(args) > 0){
        PyObject* regex = PyTuple_GetItem(args, 0);
        regexStr = PyString_AsString(regex);
    }
    ExecutionPlan* ep = RSM_Run(pfep->fep, RG_STRDUP(regexStr), NULL, NULL);
    if(!blockingExecute){
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
    int status = RSM_Register(pfep->fep, regexStr);
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
    {"groupby", groupby, METH_VARARGS, "groupby operation on each record"},
    {"collect", collect, METH_VARARGS, "collect all the records to the initiator"},
    {"foreach", foreach, METH_VARARGS, "collect all the records to the initiator"},
    {"repartition", repartition, METH_VARARGS, "repartition the records according to the extracted data"},
    {"flatmap", flatmap, METH_VARARGS, "flat map a the records"},
    {"limit", limit, METH_VARARGS, "limit the results to a give size and offset"},
    {"accumulate", accumulate, METH_VARARGS, "accumulate the records to a single record"},
    {"run", run, METH_VARARGS, "start the execution"},
    {"register", registerExecution, METH_VARARGS, "register the execution on key space notification"},
    {NULL, NULL, 0, NULL}
};

static PyObject *PyFlatExecution_ToStr(PyObject * pyObj){
    return PyString_FromString("PyFlatExecution");
}

static void PyFlatExecution_Destruct(PyObject *pyObj){
    PyFlatExecution* pfep = (PyFlatExecution*)pyObj;
    RedisGears_FreeFlatExecution(pfep->fep);
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
    RSM_Map(pyfep->fep, RedisGearsPy_ToPyRecordMapper, NULL);
    return (PyObject*)pyfep;
}

static PyObject* saveGlobals(PyObject *cls, PyObject *args){
    pyGlobals = PyEval_GetGlobals();
    Py_INCREF(pyGlobals);
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
        return PyString_FromString(replyStr);
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
    PyEval_ReleaseLock();
    RedisModule_ThreadSafeContextLock(rctx);
    PyEval_AcquireLock();

    RedisModule_AutoMemory(rctx);

    PyObject* command = PyTuple_GetItem(args, 0);
    char* commandStr = PyString_AsString(command);

    RedisModuleString** argements = array_new(RedisModuleString*, 10);
    for(int i = 1 ; i < PyTuple_Size(args) ; ++i){
        PyObject* argument = PyTuple_GetItem(args, i);
        char* argumentStr = PyString_AsString(argument);
        RedisModuleString* argumentRedisStr = RedisModule_CreateString(rctx, argumentStr, strlen(argumentStr));
        argements = array_append(argements, argumentRedisStr);
    }

    RedisModuleCallReply *reply = RedisModule_Call(rctx, commandStr, "v", argements, array_len(argements));

    PyObject* res = replyToPyList(reply);

    if(reply){
        RedisModule_FreeCallReply(reply);
    }
    array_free(argements);

    RedisModule_ThreadSafeContextUnlock(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return res;
}

typedef struct PyTensor{
   PyObject_HEAD
   RDL_Tensor* t;
} PyTensor;

static PyObject *PyTensor_ToFlatList(PyTensor * pyt){
    int ndims = RedisDL_TensorNumDims(pyt->t);
    PyObject* dims = PyList_New(0);
    long long len = 0;
    long long totalElements = 1;
    for(int i = 0 ; i < ndims ; ++i){
        totalElements *= RedisDL_TensorDim(pyt->t, i);
    }
    PyObject* elements = PyList_New(0);
    for(long long j = 0 ; j < totalElements ; ++j){
        double val;
        RedisDL_TensorGetValueAsDouble(pyt->t, j, &val);
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
    RedisDL_TensorFree(pyt->t);
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
    assert(globals.redisDLLoaded);
    PyObject* typeName = PyTuple_GetItem(args, 0);
    char* typeNameStr = PyString_AsString(typeName);
    PyObject* pyDims = PyTuple_GetItem(args, 1);
    long long* dims = array_new(long long, 10);
    double* values = array_new(long long, 1000);
    size_t ndims = getDimsRecursive(pyDims, &dims);
    RDL_Tensor* t = RedisDL_TensorCreate(typeNameStr, dims, ndims);
    getAllValues(pyDims, &values);
    for(long long i = 0 ; i < array_len(values) ; ++i){
        RedisDL_TensorSetValueFromDouble(t, i, values[i]);
    }
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = t;
    array_free(dims);
    array_free(values);
    return (PyObject*)pyt;
}

typedef struct PyGraphRunner{
   PyObject_HEAD
   RDL_GraphRunCtx* g;
} PyGraphRunner;

static PyObject* PyGraph_ToStr(PyObject *obj){
    return PyString_FromString("PyGraphRunner to str");
}

static void PyGraphRunner_Destruct(PyObject *pyObj){
    PyGraphRunner* pyg = (PyGraphRunner*)pyObj;
    RedisDL_RunCtxFree(pyg->g);
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
    assert(globals.redisDLLoaded);
    PyObject* keyName = PyTuple_GetItem(args, 0);
    char* keyNameStr = PyString_AsString(keyName);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_ThreadSafeContextLock(ctx);

    RedisModuleString* keyRedisStr = RedisModule_CreateString(ctx, keyNameStr, strlen(keyNameStr));
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyRedisStr, REDISMODULE_READ);
    // todo: check for type, add api for this
    RDL_Graph *g = RedisModule_ModuleTypeGetValue(key);
    RDL_GraphRunCtx* runCtx = RedisDL_RunCtxCreate(g);

    RedisModule_FreeString(ctx, keyRedisStr);
    RedisModule_CloseKey(key);
    RedisModule_ThreadSafeContextUnlock(ctx);
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
    RedisDL_RunCtxAddInput(pyg->g, inputNameStr, pyt->t);
    return PyLong_FromLong(1);
}

static PyObject* graphRunnerAddOutput(PyObject *cls, PyObject *args){
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    PyObject* outputName = PyTuple_GetItem(args, 1);
    char* outputNameStr = PyString_AsString(outputName);
    RedisDL_RunCtxAddOutput(pyg->g, outputNameStr);
    return PyLong_FromLong(1);
}

static PyObject* graphRunnerRun(PyObject *cls, PyObject *args){
    PyGraphRunner* pyg = (PyGraphRunner*)PyTuple_GetItem(args, 0);
    RedisDL_GraphRun(pyg->g);
    PyTensor* pyt = PyObject_New(PyTensor, &PyTensorType);
    pyt->t = RedisDL_TensorGetShallowCopy(RedisDL_RunCtxOutputTensor(pyg->g, 0));
    return (PyObject*)pyt;
}

PyMethodDef EmbMethods[] = {
    {"gearsCtx", gearsCtx, METH_VARARGS, "creating an empty gears context"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "saving the given key to a give value"},
    {"createTensor", createTensor, METH_VARARGS, "creating a tensor object"},
    {"createGraphRunner", creatGraphRunner, METH_VARARGS, "open TF graph by key name"},
    {"graphRunnerAddInput", graphRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"graphRunnerAddOutput", graphRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"graphRunnerRun", graphRunnerRun, METH_VARARGS, "run graph runner"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
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


    PyGILState_STATE state = PyGILState_Ensure();
    if(PyRun_SimpleString(script) == -1){
        PyErr_Print();
        RedisModule_ReplyWithError(ctx, "failed running the given script");
        PyGILState_Release(state);
        return REDISMODULE_OK;
    }
    PyGILState_Release(state);

    blockingExecute = true;

    return REDISMODULE_OK;
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
    PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    PyGILState_Release(state);
}

static Record* RedisGearsPy_PyCallbackAccumulate(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pArgs = PyTuple_New(2);
    PyObject* callback = arg;
    PyObject* currObj = RG_PyObjRecordGet(r);
    RG_PyObjRecordSet(r, NULL);
    PyObject* oldAccumulateObj = Py_None;
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
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
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
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
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
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
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
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
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
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
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
    Py_DECREF(retStr);
    //Py_DECREF(retStr); todo: we should uncomment it after we will pass bool
    //                         that will tell the extractor to free the memory!!
    PyGILState_Release(state);
    return retCStr;
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
        PyErr_Print();
        RedisGears_FreeRecord(records);
        *err = RG_STRDUP(PYTHON_ERROR);
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

        tempRecord = RedisGearsPy_ToPyRecordMapperInternal(RedisGears_KeyRecordGetVal(record), arg);
        assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
        PyDict_SetItemString(obj, "value", RG_PyObjRecordGet(tempRecord));
        RedisGears_FreeRecord(tempRecord);

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
    PyObject* callbackCode = PyObject_GetAttrString(callback, "func_code");
    RedisGearsPy_PyObjectSerialize(callbackCode, bw);
    PyGILState_Release(state);
    return;
}

static void* RedisGearsPy_PyCallbackDeserialize(BufferReader* br){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* callbackCode = RedisGearsPy_PyObjectDeserialize(br);
    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, callbackCode);
    PyTuple_SetItem(pArgs, 1, pyGlobals);
    Py_INCREF(pyGlobals);
    PyObject* callback = PyObject_CallObject(pFunc, pArgs);
    Py_DECREF(pArgs);
    PyGILState_Release(state);
    return callback;
}

void Py_SetAllocFunction(void *(*alloc)(size_t));
void Py_SetReallocFunction(void *(*realloc)(void *, size_t));
void Py_SetFreeFunction(void (*free)(void *));

int RedisGearsPy_Init(RedisModuleCtx *ctx){
	Py_SetAllocFunction(RG_ALLOC);
	Py_SetReallocFunction(RG_REALLOC);
	Py_SetFreeFunction(RG_FREE);
    Py_SetProgramName("/usr/bin/python");
    Py_Initialize();
    PyEval_InitThreads();
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

    PyRun_SimpleString("import redisgears\n"
                       "from redisgears import gearsCtx\n"
                       "globals()['str'] = str\n"
                       "redisgears._saveGlobals()\n");

    PyObject* pName = PyString_FromString("types");
    PyObject* pModule = PyImport_Import(pName);
    pFunc = PyObject_GetAttrString(pModule, "FunctionType");

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType", RedisGearsPy_PyObjectFree, RedisGearsPy_PyObjectDup, RedisGearsPy_PyCallbackSerialize, RedisGearsPy_PyCallbackDeserialize);

    RSM_RegisterForEach(RedisGearsPy_PyCallbackForEach, pyCallbackType);
    RSM_RegisterFilter(RedisGearsPy_PyCallbackFilter, pyCallbackType);
    RSM_RegisterMap(RedisGearsPy_ToPyRecordMapper, NULL);
    RSM_RegisterMap(RedisGearsPy_PyCallbackFlatMapper, pyCallbackType);
    RSM_RegisterMap(RedisGearsPy_PyCallbackMapper, pyCallbackType);
    RSM_RegisterAccumulator(RedisGearsPy_PyCallbackAccumulate, pyCallbackType);
    RSM_RegisterGroupByExtractor(RedisGearsPy_PyCallbackExtractor, pyCallbackType);
    RSM_RegisterReducer(RedisGearsPy_PyCallbackReducer, pyCallbackType);

    if (RedisModule_CreateCommand(ctx, "rg.pyexecute", RedisGearsPy_Execut, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.pyexecute");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.pyfreeinterpreter", RedisGearsPy_FreeInterpreter, "readonly", 0, 0, 0) != REDISMODULE_OK) {
		RedisModule_Log(ctx, "warning", "could not register command rg.pyexecute");
		return REDISMODULE_ERR;
	}

    PyEval_ReleaseLock();

    return true;
}
