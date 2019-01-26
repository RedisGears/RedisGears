#include <Python.h>
#include "record.h"
#include "redisdl.h"
#include "globals.h"
#include "commands.h"
#include "config.h"
#include <marshal.h>
#include <assert.h>
#include <redisgears.h>
#include <redisgears_memory.h>
#include <redisgears_python.h>
#include <execution_plan.h>
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

static PyObject* accumulateby(PyObject *self, PyObject *args){
	PyFlatExecution* pfep = (PyFlatExecution*)self;
	if(PyTuple_Size(args) != 2){
		//todo: print error
		return Py_None;
	}
	PyObject* extractor = PyTuple_GetItem(args, 0);
	PyObject* accumulator = PyTuple_GetItem(args, 1);
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
    {"batchgroupby", groupby, METH_VARARGS, "batch groupby operation on each record"},
	{"groupby", accumulateby, METH_VARARGS, "groupby operation on each record"},
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

typedef struct PyRecord{
   PyObject_HEAD
   Record r;
} PyRecord;

static PyObject* PyRecord_GetItem(PyObject *self, PyObject *obj){
    return PyString_FromString("PyRecord");
}

static Py_ssize_t PyRecord_Length(PyObject* obj){

}

PyMethodDef PyRecordMethods[] = {
//    {"__getitem__", PyRecord_GetItem, METH_VARARGS, "return an item from the record"},
    {NULL, NULL, 0, NULL}
};

static PyObject *PyRecord_ToStr(PyObject * pyObj){
    return PyString_FromString("PyRecord");
}

static void PyRecord_Destruct(PyObject *pyObj){
    PyRecord* pr = (PyRecord*)pyObj;
    RG_DisposeRecord(&pr->r);
    Py_TYPE(pyObj)->tp_free((PyObject*)pyObj);
}

PyMappingMethods PyRecordMap = {
        .mp_length = PyRecord_Length,
        .mp_subscript = PyRecord_GetItem,
        .mp_ass_subscript = 0,
};

static PyTypeObject PyRecordType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "redisgears.PyRecord",       /* tp_name */
    sizeof(PyRecord),          /* tp_basicsize */
    0,                         /* tp_itemsize */
    PyRecord_Destruct,         /* tp_dealloc */
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
    PyRecord_ToStr,            /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
    "PyRecord",         /* tp_doc */
};

typedef enum OwnershipType{
    TAKE_OWNERSHIP, SHARE_OWENERSHIP, LEAVE_OWNERSHIP
}OwnershipType;

static inline PyObject* PyRecord_ToPyRecord(Record* r, OwnershipType ownershipType){
    if(RedisGears_RecordGetType(r) == PY_RECORD){
        PyObject* p = RG_PyObjRecordGet(r);
        if(ownershipType == TAKE_OWNERSHIP){
            RG_PyObjRecordSet(r, NULL);
            RedisGears_FreeRecord(r);
        }else if(ownershipType == SHARE_OWENERSHIP){
            Py_INCREF(p);
        }
        return p;
    }
    PyRecord* nullPr = (PyRecord*)NULL;
    size_t offset = (size_t)&nullPr->r;
    PyRecord* pr = (PyRecord*)(((char*)r) - offset);
    if(ownershipType == SHARE_OWENERSHIP){
        Py_INCREF(pr);
    }
    return (PyObject*)pr;
}

static inline Record* PyRecord_ToRecord(PyObject* p, OwnershipType ownershipType){
    if(ownershipType == SHARE_OWENERSHIP){
        Py_INCREF(p);
    }
    if(PyObject_TypeCheck(p, &PyRecordType)){
        return &((PyRecord*)p)->r;
    }
    Record* r = RG_PyObjRecordCreate();
    RG_PyObjRecordSet(r, p);
    return r;
}

static PyObject* gearsCtx(PyObject *cls, PyObject *args){
    char* readerStr = "KeysReader"; // default reader
    if(PyTuple_Size(args) > 0){
        PyObject* reader = PyTuple_GetItem(args, 0);
        readerStr = PyString_AsString(reader);
    }
    PyFlatExecution* pyfep = PyObject_New(PyFlatExecution, &PyFlatExecutionType);
    pyfep->fep = RedisGears_CreateCtx(readerStr);
    FlatExecutionPlan_SetAllocator(pyfep->fep, PYTHON);
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
    PyThreadState *_save = PyEval_SaveThread();
    RedisModule_ThreadSafeContextLock(rctx);
    PyEval_RestoreThread(_save);

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
    RG_SetRecordAlocator(PYTHON);
    if(PyRun_SimpleString(script) == -1){
        PyErr_Print();
        RedisModule_ReplyWithError(ctx, "failed running the given script");
        PyGILState_Release(state);
        RG_SetRecordAlocator(DEFAULT);
        return REDISMODULE_OK;
    }
    PyGILState_Release(state);

    blockingExecute = true;

    RG_SetRecordAlocator(DEFAULT);
    return REDISMODULE_OK;
}

void RedisGearsPy_PyCallbackForEach(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pr = PyRecord_ToPyRecord(record, SHARE_OWENERSHIP);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyTuple_SetItem(pArgs, 0, pr);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(ret != Py_None){
    	Py_DECREF(ret);
    }
    PyGILState_Release(state);
}

static Record* RedisGearsPy_PyCallbackAccumulateByKey(RedisModuleCtx* rctx, char* key, Record *accumulate, Record *r, void* arg, char** err){
	PyGILState_STATE state = PyGILState_Ensure();
	PyObject* pArgs = PyTuple_New(3);
	PyObject* callback = arg;
	PyObject* currObj = PyRecord_ToPyRecord(r, TAKE_OWNERSHIP);
	PyObject* keyPyStr = PyString_FromString(key);
	PyObject* oldAccumulateObj = Py_None;
	if(accumulate){
        oldAccumulateObj = PyRecord_ToPyRecord(accumulate, TAKE_OWNERSHIP);
	}
	PyTuple_SetItem(pArgs, 0, keyPyStr);
	PyTuple_SetItem(pArgs, 1, oldAccumulateObj);
	PyTuple_SetItem(pArgs, 2, currObj);
	PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
	Py_DECREF(pArgs);
	if(!newAccumulateObj){
		PyErr_Print();
		*err = RG_STRDUP(PYTHON_ERROR);
		PyGILState_Release(state);
		return NULL;
	}
	accumulate = PyRecord_ToRecord(newAccumulateObj, TAKE_OWNERSHIP);
	PyGILState_Release(state);
	return accumulate;
}

static Record* RedisGearsPy_PyCallbackAccumulate(RedisModuleCtx* rctx, Record *accumulate, Record *r, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pArgs = PyTuple_New(2);
    PyObject* callback = arg;
    PyObject* currObj = PyRecord_ToPyRecord(r, TAKE_OWNERSHIP);
    PyObject* oldAccumulateObj = Py_None;
    if(accumulate){
        oldAccumulateObj = PyRecord_ToPyRecord(accumulate, TAKE_OWNERSHIP);
    }
    PyTuple_SetItem(pArgs, 0, oldAccumulateObj);
    PyTuple_SetItem(pArgs, 1, currObj);
    PyObject* newAccumulateObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newAccumulateObj){
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return NULL;
    }
    accumulate = PyRecord_ToRecord(newAccumulateObj, TAKE_OWNERSHIP);
    PyGILState_Release(state);
    return accumulate;
}

static Record* RedisGearsPy_PyCallbackMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pr = PyRecord_ToPyRecord(record, TAKE_OWNERSHIP);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyTuple_SetItem(pArgs, 0, pr);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return NULL;
    }
    Record* res = PyRecord_ToRecord(newObj, TAKE_OWNERSHIP);
    PyGILState_Release(state);
    return res;
}

static Record* RedisGearsPy_PyCallbackFlatMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pr = PyRecord_ToPyRecord(record, TAKE_OWNERSHIP);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyTuple_SetItem(pArgs, 0, pr);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!newObj){
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return NULL;
    }
    if(PyList_Check(newObj)){
        size_t len = PyList_Size(newObj);
        record = RedisGears_ListRecordCreate(len);
        for(size_t i = 0 ; i < len ; ++i){
            PyObject* temp = PyList_GetItem(newObj, i);
            Record* pyRecord = PyRecord_ToRecord(temp, SHARE_OWENERSHIP);
            RedisGears_ListRecordAdd(record, pyRecord);
        }
        Py_DECREF(newObj);
    }else{
        record = PyRecord_ToRecord(newObj, TAKE_OWNERSHIP);
        RG_PyObjRecordSet(record, newObj);
    }
    PyGILState_Release(state);
    return record;
}

static bool RedisGearsPy_PyCallbackFilter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* pr = PyRecord_ToPyRecord(record, SHARE_OWENERSHIP);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    Py_INCREF(pr);
    PyTuple_SetItem(pArgs, 0, pr);
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
    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* pr = PyRecord_ToPyRecord(record, SHARE_OWENERSHIP);
    PyTuple_SetItem(pArgs, 0, pr);
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
    char* retValue = RG_ALLOC(*len + 1);
    memcpy(retValue, retCStr, *len);
    retValue[*len] = '\0';
    Py_DECREF(retStr);
    PyGILState_Release(state);
    return retValue;
}

static Record* RedisGearsPy_PyCallbackReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RedisGears_RecordGetType(records) == LIST_RECORD);
    PyObject* obj = PyRecord_ToPyRecord(records, TAKE_OWNERSHIP);
    PyObject* reducer = arg;
    PyObject* pArgs = PyTuple_New(2);
    PyObject* keyPyObj = PyString_FromString(key);
    PyTuple_SetItem(pArgs, 0, keyPyObj);
    PyTuple_SetItem(pArgs, 1, obj);
    PyObject* ret = PyObject_CallObject(reducer, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        PyErr_Print();
        *err = RG_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return NULL;
    }
    Record* retRecord = PyRecord_ToRecord(ret, TAKE_OWNERSHIP);
    PyGILState_Release(state);
    return retRecord;
}

//static Record* RedisGearsPy_ToPyRecordMapperInternal(Record *record, void* arg){
//    Record* res = RG_PyObjRecordCreate();
//    Record* tempRecord;
//    PyObject* obj;
//    PyObject* temp;
//    char* str;
//    long longNum;
//    double doubleNum;
//    char* key;
//    char** keys;
//    size_t len;
//    switch(RedisGears_RecordGetType(record)){
//    case STRING_RECORD:
//        str = RedisGears_StringRecordGet(record, &len);
//        obj = PyString_FromStringAndSize(str, len);
//        break;
//    case LONG_RECORD:
//        longNum = RedisGears_LongRecordGet(record);
//        obj = PyLong_FromLong(longNum);
//        break;
//    case DOUBLE_RECORD:
//        doubleNum = RedisGears_DoubleRecordGet(record);
//        obj = PyLong_FromDouble(doubleNum);
//        break;
//    case KEY_RECORD:
//        key = RedisGears_KeyRecordGetKey(record, NULL);
//        obj = PyDict_New();
//        temp = PyString_FromString(key);
//        PyDict_SetItemString(obj, "key", temp);
//        Py_DECREF(temp);
//
//        tempRecord = RedisGearsPy_ToPyRecordMapperInternal(RedisGears_KeyRecordGetVal(record), arg);
//        assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
//        PyDict_SetItemString(obj, "value", RG_PyObjRecordGet(tempRecord));
//        RedisGears_FreeRecord(tempRecord);
//
//        break;
//    case LIST_RECORD:
//        len = RedisGears_ListRecordLen(record);
//        obj = PyList_New(0);
//        for(size_t i = 0 ; i < len ; ++i){
//            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(RedisGears_ListRecordGet(record, i), arg);
//            assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
//            PyList_Append(obj, RG_PyObjRecordGet(tempRecord));
//            RedisGears_FreeRecord(tempRecord);
//        }
//        break;
//    case HASH_SET_RECORD:
//        keys = RedisGears_HashSetRecordGetAllKeys(record, &len);
//        obj = PyDict_New();
//        for(size_t i = 0 ; i < len ; ++i){
//            key = keys[i];
//            temp = PyString_FromString(key);
//            tempRecord = RedisGears_HashSetRecordGet(record, key);
//            tempRecord = RedisGearsPy_ToPyRecordMapperInternal(tempRecord, arg);
//            assert(RedisGears_RecordGetType(tempRecord) == PY_RECORD);
//            PyDict_SetItem(obj, temp, RG_PyObjRecordGet(tempRecord));
//            Py_DECREF(temp);
//            RedisGears_FreeRecord(tempRecord);
//        }
//        RedisGears_HashSetRecordFreeKeysArray(keys);
//        break;
//    case PY_RECORD:
//        obj = RG_PyObjRecordGet(record);
//        Py_INCREF(obj);
//        break;
//    default:
//        assert(false);
//    }
//    RG_PyObjRecordSet(res, obj);
//    return res;
//}

//static Record* RedisGearsPy_ToPyRecordMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
//    PyGILState_STATE state = PyGILState_Ensure();
//    Record* res = RedisGearsPy_ToPyRecordMapperInternal(record, arg);
//    RedisGears_FreeRecord(record);
//    PyGILState_Release(state);
//    return res;
//}

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

Record* RedisGearsPy_AllocatePyRecord(){
    PyRecord* pr = PyObject_New(PyRecord, &PyRecordType);
    return PyRecord_ToRecord((PyObject*)pr, TAKE_OWNERSHIP);
}

void RedisGearsPy_DisposePyRecord(Record* r){
    PyObject* pr = PyRecord_ToPyRecord(r, TAKE_OWNERSHIP);
    Py_DECREF(pr);
}

int RedisGearsPy_Init(RedisModuleCtx *ctx){
	Py_SetAllocFunction(RedisGearsPy_Alloc);
	Py_SetReallocFunction(RedisGearsPy_Relloc);
	Py_SetFreeFunction(RedisGearsPy_Free);
    Py_SetProgramName((char*)GearsCOnfig_GetPythonHomeDir());
    Py_Initialize();
    PyEval_InitThreads();
    PyTensorType.tp_new = PyType_GenericNew;
    PyGraphRunnerType.tp_new = PyType_GenericNew;
    PyFlatExecutionType.tp_new = PyType_GenericNew;
    PyRecordType.tp_new = PyType_GenericNew;

    PyFlatExecutionType.tp_methods = PyFlatExecutionMethods;
    PyRecordType.tp_methods = PyRecordMethods;
    PyRecordType.tp_as_mapping = &PyRecordMap;

    if (PyType_Ready(&PyTensorType) < 0){
        RedisModule_Log(ctx, "warning", "PyTensorType not ready");
    }

    if (PyType_Ready(&PyGraphRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
    }

    if (PyType_Ready(&PyFlatExecutionType) < 0){
        RedisModule_Log(ctx, "warning", "PyFlatExecutionType not ready");
    }

    if (PyType_Ready(&PyRecordType) < 0){
        RedisModule_Log(ctx, "warning", "PyRecordType not ready");
    }

    PyObject* m = Py_InitModule("redisgears", EmbMethods);

    Py_INCREF(&PyTensorType);
    Py_INCREF(&PyGraphRunnerType);
    Py_INCREF(&PyFlatExecutionType);
    Py_INCREF(&PyRecordType);

    PyModule_AddObject(m, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(m, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);
    PyModule_AddObject(m, "PyFlatExecution", (PyObject *)&PyFlatExecutionType);
    PyModule_AddObject(m, "PyRecord", (PyObject *)&PyRecordType);

    PyRun_SimpleString("import redisgears\n"
                       "from redisgears import gearsCtx\n"
                       "globals()['str'] = str\n"
                       "redisgears._saveGlobals()\n");

    PyObject* pName = PyString_FromString("types");
    PyObject* pModule = PyImport_Import(pName);
    pFunc = PyObject_GetAttrString(pModule, "FunctionType");
    Py_DECREF(pName);

    ArgType* pyCallbackType = RedisGears_CreateType("PyObjectType", RedisGearsPy_PyObjectFree, RedisGearsPy_PyObjectDup, RedisGearsPy_PyCallbackSerialize, RedisGearsPy_PyCallbackDeserialize);

    RSM_RegisterForEach(RedisGearsPy_PyCallbackForEach, pyCallbackType);
    RSM_RegisterFilter(RedisGearsPy_PyCallbackFilter, pyCallbackType);
//    RSM_RegisterMap(RedisGearsPy_ToPyRecordMapper, NULL);
    RSM_RegisterMap(RedisGearsPy_PyCallbackFlatMapper, pyCallbackType);
    RSM_RegisterMap(RedisGearsPy_PyCallbackMapper, pyCallbackType);
    RSM_RegisterAccumulator(RedisGearsPy_PyCallbackAccumulate, pyCallbackType);
    RSM_RegisterAccumulatorByKey(RedisGearsPy_PyCallbackAccumulateByKey, pyCallbackType);
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

    if (RedisModule_CreateCommand(ctx, "rg.pystats", RedisGearsPy_Stats, "readonly", 0, 0, 0) != REDISMODULE_OK) {
    	RedisModule_Log(ctx, "warning", "could not register command rg.pystats");
		return REDISMODULE_ERR;
    }

    PyEval_ReleaseLock();

    return true;
}
