#include "redistar_python.h"
#include "redistar.h"
#include "redistar_memory.h"
#include "record.h"
#include "redisdl.h"
#include "globals.h"
#include <Python.h>
#include <marshal.h>
#include <assert.h>
#include "utils/arr_rm_alloc.h"

static PyObject* pFunc;
static PyObject* pyGlobals;

static RedisModuleCtx* currentCtx = NULL;

#define PYTHON_ERROR "error running python code"

static FlatExecutionPlan* createFep(PyObject* starCtx){
    size_t len;
    size_t offset;
    PyObject* name = PyObject_GetAttrString(starCtx, "name");
    char* nameStr = PyString_AsString(name);
    PyObject* reader = PyObject_GetAttrString(starCtx, "reader");
    char* readerStr = PyString_AsString(reader);
    Py_DECREF(name);
    // todo : expose execution name on python interface
	FlatExecutionPlan* rsctx = RediStar_CreateCtx(nameStr, readerStr);
	Py_DECREF(reader);
    if(!rsctx){
        return NULL;
    }
    RSM_Map(rsctx, RediStarPy_ToPyRecordMapper, NULL);

    PyObject* stepsKey = PyString_FromString("steps");
    PyObject* stepsList = PyObject_GetAttr(starCtx, stepsKey);
    Py_DECREF(stepsKey);

    for(size_t i = 0 ; i < PyList_Size(stepsList) ; ++i){
        PyObject* step = PyList_GetItem(stepsList, i);
        PyObject* stepType = PyTuple_GetItem(step, 0);
        PyObject* callback = PyTuple_GetItem(step, 1);
        PyObject* reducer = NULL;
        long type = PyLong_AsLong(stepType);
        switch(type){
        case 1: // mapping step
            RSM_Map(rsctx, RediStarPy_PyCallbackMapper, callback);
            break;
        case 2: // filter step
            RSM_Filter(rsctx, RediStarPy_PyCallbackFilter, callback);
            break;
        case 3: // groupby step
            reducer = PyTuple_GetItem(step, 2);
            Py_INCREF(reducer);
            RSM_GroupBy(rsctx, RediStarPy_PyCallbackExtractor, callback, RediStarPy_PyCallbackReducer, reducer);
            RSM_Map(rsctx, RediStarPy_ToPyRecordMapper, NULL);
            break;
        case 4:
            RSM_Collect(rsctx);
            break;
        case 5:
            RSM_Write(rsctx, RediStarPy_PyCallbackWriter, callback);
            break;
        case 6:
            RSM_Repartition(rsctx, RediStarPy_PyCallbackExtractor, callback);
            break;
        case 7:
            RSM_FlatMap(rsctx, RediStarPy_PyCallbackFlatMapper, callback);
            break;
        case 8:
            len = PyInt_AsLong(callback);
            reducer = PyTuple_GetItem(step, 2);
            offset = PyInt_AsLong(reducer);
            RSM_Limit(rsctx, offset, len);
            break;
        default:
            assert(false);
        }
    }
    return rsctx;
}

static PyObject* registerStream(PyObject *cls, PyObject *args){
    PyObject* starStreamCtx = PyTuple_GetItem(args, 0);
    PyObject* starCtx = PyObject_GetAttrString(starStreamCtx, "starCtx");
    PyObject* key = PyObject_GetAttrString(starStreamCtx, "key");
    char* keyStr = PyString_AsString(key);
    FlatExecutionPlan* fep = createFep(starCtx);

    if(!fep){
        RedisModule_ReplyWithError(currentCtx, "Flat  Execution with the given name already exists, pleas drop it first.");
        return PyLong_FromLong(1);
    }

    if(RSM_Register(fep, keyStr)){
        RedisModule_ReplyWithSimpleString(currentCtx, "OK");
    }else{
        RedisModule_ReplyWithError(currentCtx, "Registration Failed");
    }

    Py_DECREF(key);
    Py_DECREF(starCtx);
    Py_DECREF(starStreamCtx);

    return PyLong_FromLong(1);
}

static PyObject* run(PyObject *cls, PyObject *args){
    size_t len;
    size_t offset;
    PyObject* starCtx = PyTuple_GetItem(args, 0);
    PyObject* regex = PyObject_GetAttrString(starCtx, "regex");
    char* regexStr = PyString_AsString(regex);
    FlatExecutionPlan* fep = createFep(starCtx);

    if(!fep){
        RedisModule_ReplyWithError(currentCtx, "Flat  Execution with the given name already exists, pleas drop it first.");
        return PyLong_FromLong(1);
    }

    ExecutionPlan* ep = RSM_Run(fep, RS_STRDUP(regexStr), NULL, NULL);
    // todo: we should not return the reply to the user here,
    //       user might create multiple executions in a single script
    //       think what to do???
    const char* id = RediStar_GetId(ep);
    RedisModule_ReplyWithStringBuffer(currentCtx, id, strlen(id));

    Py_DECREF(regex);
    Py_DECREF(starCtx);

    return PyLong_FromLong(1);
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
    "redistar.PyTensor",       /* tp_name */
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
    "redistar.PyGraphRunner",             /* tp_name */
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
    {"run", run, METH_VARARGS, "start running the execution plan"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"executeCommand", executeCommand, METH_VARARGS, "saving the given key to a give value"},
    {"register", registerStream, METH_VARARGS, "register the stream"},
    {"createTensor", createTensor, METH_VARARGS, "creating a tensor object"},
    {"createGraphRunner", creatGraphRunner, METH_VARARGS, "open TF graph by key name"},
    {"graphRunnerAddInput", graphRunnerAddInput, METH_VARARGS, "add input to graph runner"},
    {"graphRunnerAddOutput", graphRunnerAddOutput, METH_VARARGS, "add output to graph runner"},
    {"graphRunnerRun", graphRunnerRun, METH_VARARGS, "run graph runner"},
    {"tensorToFlatList", tensorToFlatList, METH_VARARGS, "turning tensor into flat list"},
    {NULL, NULL, 0, NULL}
};

static int RediStarPy_Execut(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* script = RedisModule_StringPtrLen(argv[1], NULL);
    currentCtx = ctx;

    PyGILState_STATE state = PyGILState_Ensure();
    if(PyRun_SimpleString(script)){
        RedisModule_ReplyWithError(ctx, "failed running the given script");
        return REDISMODULE_OK;
    }
    PyGILState_Release(state);

    return REDISMODULE_OK;
}

void RediStarPy_PyCallbackWriter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RediStar_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RS_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject_CallObject(callback, pArgs);
    PyGILState_Release(state);
}

static Record* RediStarPy_PyCallbackMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RediStar_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RS_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    if(!newObj){
        PyErr_Print();
        *err = RS_STRDUP(PYTHON_ERROR);
        RediStar_FreeRecord(record);
        PyGILState_Release(state);
        return NULL;
    }
    Py_INCREF(newObj);
    Py_DECREF(oldObj);
    RS_PyObjRecordSet(record, newObj);
    PyGILState_Release(state);
    return record;
}

static Record* RediStarPy_PyCallbackFlatMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    // Call Python/C API functions...
    assert(RediStar_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* oldObj = RS_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, oldObj);
    PyObject* newObj = PyObject_CallObject(callback, pArgs);
    if(!newObj){
        PyErr_Print();
        *err = RS_STRDUP(PYTHON_ERROR);
        RediStar_FreeRecord(record);
        PyGILState_Release(state);
        return NULL;
    }
    if(PyList_Check(newObj)){
        RediStar_FreeRecord(record);
        size_t len = PyList_Size(newObj);
        record = RediStar_ListRecordCreate(len);
        for(size_t i = 0 ; i < len ; ++i){
            PyObject* temp = PyList_GetItem(newObj, i);
            Record* pyRecord = RS_PyObjRecordCreate();
            Py_INCREF(pyRecord);
            RS_PyObjRecordSet(pyRecord, temp);
            RediStar_ListRecordAdd(record, pyRecord);
        }
        Py_DECREF(newObj);
    }else{
        Py_INCREF(newObj);
        Py_DECREF(oldObj);
        RS_PyObjRecordSet(record, newObj);
    }
    PyGILState_Release(state);
    return record;
}

static bool RediStarPy_PyCallbackFilter(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RediStar_RecordGetType(record) == PY_RECORD);
    PyObject* pArgs = PyTuple_New(1);
    PyObject* callback = arg;
    PyObject* obj = RS_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(callback, pArgs);
    Py_DECREF(pArgs);
    if(!ret){
        PyErr_Print();
        *err = RS_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return false;
    }
    bool ret1 = PyObject_IsTrue(ret);
    PyGILState_Release(state);
    return ret1;
}

static char* RediStarPy_PyCallbackExtractor(RedisModuleCtx* rctx, Record *record, void* arg, size_t* len, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RediStar_RecordGetType(record) == PY_RECORD);
    PyObject* extractor = arg;
    PyObject* pArgs = PyTuple_New(1);
    PyObject* obj = RS_PyObjRecordGet(record);
    PyTuple_SetItem(pArgs, 0, obj);
    PyObject* ret = PyObject_CallObject(extractor, pArgs);
    Py_INCREF(obj);
    Py_DECREF(pArgs);
    if(!ret){
        PyErr_Print();
        *err = RS_STRDUP(PYTHON_ERROR);
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
    //Py_DECREF(retStr); todo: we should uncomment it after we will pass bool
    //                         that will tell the extractor to free the memory!!
    PyGILState_Release(state);
    return retCStr;
}

static Record* RediStarPy_PyCallbackReducer(RedisModuleCtx* rctx, char* key, size_t keyLen, Record *records, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    assert(RediStar_RecordGetType(records) == LIST_RECORD);
    PyObject* obj = PyList_New(0);
    for(size_t i = 0 ; i < RediStar_ListRecordLen(records) ; ++i){
        Record* r = RediStar_ListRecordGet(records, i);
        assert(RediStar_RecordGetType(r) == PY_RECORD);
        PyObject* element = RS_PyObjRecordGet(r);
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
        RediStar_FreeRecord(records);
        *err = RS_STRDUP(PYTHON_ERROR);
        PyGILState_Release(state);
        return NULL;
    }
    Record* retRecord = RS_PyObjRecordCreate();
    RS_PyObjRecordSet(retRecord, ret);
    RediStar_FreeRecord(records);
    PyGILState_Release(state);
    return retRecord;
}

static Record* RediStarPy_ToPyRecordMapperInternal(Record *record, void* arg){
    Record* res = RS_PyObjRecordCreate();
    Record* tempRecord;
    PyObject* obj;
    PyObject* temp;
    char* str;
    long longNum;
    double doubleNum;
    char* key;
    char** keys;
    size_t len;
    switch(RediStar_RecordGetType(record)){
    case STRING_RECORD:
        str = RediStar_StringRecordGet(record, &len);
        obj = PyString_FromStringAndSize(str, len);
        break;
    case LONG_RECORD:
        longNum = RediStar_LongRecordGet(record);
        obj = PyLong_FromLong(longNum);
        break;
    case DOUBLE_RECORD:
        doubleNum = RediStar_DoubleRecordGet(record);
        obj = PyLong_FromDouble(doubleNum);
        break;
    case KEY_RECORD:
        key = RediStar_KeyRecordGetKey(record, NULL);
        obj = PyDict_New();
        temp = PyString_FromString(key);
        PyDict_SetItemString(obj, "key", temp);

        tempRecord = RediStarPy_ToPyRecordMapperInternal(RediStar_KeyRecordGetVal(record), arg);
        assert(RediStar_RecordGetType(tempRecord) == PY_RECORD);
        PyDict_SetItemString(obj, "value", RS_PyObjRecordGet(tempRecord));
        RediStar_FreeRecord(tempRecord);

        break;
    case LIST_RECORD:
        len = RediStar_ListRecordLen(record);
        obj = PyList_New(0);
        for(size_t i = 0 ; i < len ; ++i){
            tempRecord = RediStarPy_ToPyRecordMapperInternal(RediStar_ListRecordGet(record, i), arg);
            assert(RediStar_RecordGetType(tempRecord) == PY_RECORD);
            PyList_Append(obj, RS_PyObjRecordGet(tempRecord));
            RediStar_FreeRecord(tempRecord);
        }
        break;
    case HASH_SET_RECORD:
        keys = RediStar_HashSetRecordGetAllKeys(record, &len);
        obj = PyDict_New();
        for(size_t i = 0 ; i < len ; ++i){
            key = keys[i];
            temp = PyString_FromString(key);
            tempRecord = RediStar_HashSetRecordGet(record, key);
            tempRecord = RediStarPy_ToPyRecordMapperInternal(tempRecord, arg);
            assert(RediStar_RecordGetType(tempRecord) == PY_RECORD);
            PyDict_SetItem(obj, temp, RS_PyObjRecordGet(tempRecord));
            RediStar_FreeRecord(tempRecord);
        }
        RediStar_HashSetRecordFreeKeysArray(keys);
        break;
    case PY_RECORD:
        obj = RS_PyObjRecordGet(record);
        break;
    default:
        assert(false);
    }
    RS_PyObjRecordSet(res, obj);
    return res;
}

static Record* RediStarPy_ToPyRecordMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err){
    PyGILState_STATE state = PyGILState_Ensure();
    Record* res = RediStarPy_ToPyRecordMapperInternal(record, arg);
    RediStar_FreeRecord(record);
    PyGILState_Release(state);
    return res;
}

static void RediStarPy_PyObjectFree(void* arg){
    PyObject* obj = arg;
    Py_DECREF(obj);
}

void RediStarPy_PyObjectSerialize(void* arg, BufferWriter* bw){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* obj = arg;
    PyObject* objStr = PyMarshal_WriteObjectToString(obj, Py_MARSHAL_VERSION);
    if(!objStr){
        PyErr_Print();
        assert(false);
    }
    size_t len = PyString_Size(objStr);
    char* objStrCstr  = PyString_AsString(objStr);
    RediStar_BWWriteBuffer(bw, objStrCstr, len);
    Py_DECREF(objStr);
    PyGILState_Release(state);
    return;
}

void* RediStarPy_PyObjectDeserialize(BufferReader* br){
    PyGILState_STATE state = PyGILState_Ensure();
    size_t len;
    char* data = RediStar_BRReadBuffer(br, &len);
    PyObject* obj = PyMarshal_ReadObjectFromString(data, len);
    PyGILState_Release(state);
    return obj;
}

static void RediStarPy_PyCallbackSerialize(void* arg, BufferWriter* bw){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* callback = arg;
    PyObject* callbackCode = PyObject_GetAttrString(callback, "func_code");
    RediStarPy_PyObjectSerialize(callbackCode, bw);
    Py_DECREF(callbackCode);
    PyGILState_Release(state);
    return;
}

static void* RediStarPy_PyCallbackDeserialize(BufferReader* br){
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject* callbackCode = RediStarPy_PyObjectDeserialize(br);
    PyObject* pArgs = PyTuple_New(2);
    PyTuple_SetItem(pArgs, 0, callbackCode);
    PyTuple_SetItem(pArgs, 1, pyGlobals);
    Py_INCREF(pyGlobals);
    PyObject* callback = PyObject_CallObject(pFunc, pArgs);
    Py_DECREF(pArgs);
    PyGILState_Release(state);
    return callback;
}

//  Script example
//
//    def filter(record):
//        return record.val == 'meir'
//    ctx = starCtx('*')
//    ctx.filter(filter)\n
//    ctx.returnResults()
int RediStarPy_Init(RedisModuleCtx *ctx){
    Py_SetProgramName("test");  /* optional but recommended */
    Py_Initialize();
    PyEval_InitThreads();
    PyTensorType.tp_new = PyType_GenericNew;
    PyGraphRunnerType.tp_new = PyType_GenericNew;

    if (PyType_Ready(&PyTensorType) < 0){
        RedisModule_Log(ctx, "warning", "PyTensorType not ready");
    }

    if (PyType_Ready(&PyGraphRunnerType) < 0){
        RedisModule_Log(ctx, "warning", "PyGraphRunnerType not ready");
    }

    PyObject* m = Py_InitModule("redistar", EmbMethods);

    Py_INCREF(&PyTensorType);
    Py_INCREF(&PyGraphRunnerType);

    PyModule_AddObject(m, "PyTensor", (PyObject *)&PyTensorType);
    PyModule_AddObject(m, "PyGraphRunner", (PyObject *)&PyGraphRunnerType);

    PyRun_SimpleString("import redistar\n"
                       "class starCtx:\n"
                       "    def __init__(self, name, reader='KeysReader'):\n"
    				   "        self.name = name\n"
                       "        self.reader = reader\n"
                       "        self.steps = []\n"
                       "    def map(self, mapFunc):\n"
                       "        self.steps.append((1, mapFunc))\n"
                       "        return self\n"
                       "    def filter(self, filterFunc):\n"
                       "        self.steps.append((2, filterFunc))\n"
                       "        return self\n"
                       "    def groupby(self, extractor, reducer):\n"
                       "        self.steps.append((3, extractor, reducer))\n"
                       "        return self\n"
    				   "    def collect(self):\n"
					   "        self.steps.append((4, None))\n"
					   "        return self\n"
                       "    def write(self, writeCallback):\n"
                       "        self.steps.append((5, writeCallback))\n"
                       "        return self\n"
                       "    def repartition(self, extractor):\n"
                       "        self.steps.append((6, extractor))\n"
                       "        return self\n"
                       "    def flatMap(self, flatMapFunc):\n"
                       "        self.steps.append((7, flatMapFunc))\n"
                       "        return self\n"
                       "    def limit(self, len, offset=0):\n"
                       "        if not isinstance(len, int):\n"
                       "            raise Exception('value given to limit is not int')\n"
                       "        if not isinstance(offset, int):\n"
                       "            raise Exception('value given to limit is not int')\n"
                       "        self.steps.append((8, len, offset))\n"
                       "        return self\n"
                       "    def run(self, regex='*'):\n"
                       "        self.regex = regex\n"
                       "        redistar.run(self)\n"
                       "class starStreamingCtx:\n"
                       "    def __init__(self, name, reader='KeysReader'):\n"
                       "        self.starCtx = starCtx(name, reader)\n"
                       "    def map(self, mapFunc):\n"
                       "        self.starCtx.map(mapFunc)\n"
                       "        return self\n"
                       "    def filter(self, filterFunc):\n"
                       "        self.starCtx.filter(filterFunc)\n"
                       "        return self\n"
                       "    def groupby(self, extractor, reducer):\n"
                       "        self.starCtx.groupby(extractor, reducer)\n"
                       "        return self\n"
                       "    def collect(self):\n"
                       "        self.starCtx.collect()\n"
                       "        return self\n"
                       "    def write(self, writeCallback):\n"
                       "        self.starCtx.write(writeCallback)\n"
                       "        return self\n"
                       "    def repartition(self, extractor):\n"
                       "        self.starCtx.repartition(extractor)\n"
                       "        return self\n"
                       "    def flatMap(self, flatMapFunc):\n"
                       "        self.starCtx.flatMap(flatMapFunc)\n"
                       "        return self\n"
                       "    def limit(self, len, offset=0):\n"
                       "        self.starCtx.limit(len, offset)\n"
                       "        return self\n"
                       "    def register(self, key):\n"
                       "        self.key = key\n"
                       "        redistar.register(self)\n"
                       "globals()['str'] = str\n"
                       "print 'PyTensor object : ' + str(redistar.PyTensor)\n"
                       "print 'PyGraphRunner object : ' + str(redistar.PyGraphRunner)\n"
                       "redistar._saveGlobals()\n");

    PyObject* pName = PyString_FromString("types");
    PyObject* pModule = PyImport_Import(pName);
    pFunc = PyObject_GetAttrString(pModule, "FunctionType");

    ArgType* pyCallbackType = RediStar_CreateType("PyObjectType", RediStarPy_PyObjectFree, RediStarPy_PyCallbackSerialize, RediStarPy_PyCallbackDeserialize);

    RSM_RegisterWriter(RediStarPy_PyCallbackWriter, pyCallbackType);
    RSM_RegisterFilter(RediStarPy_PyCallbackFilter, pyCallbackType);
    RSM_RegisterMap(RediStarPy_ToPyRecordMapper, NULL);
    RSM_RegisterMap(RediStarPy_PyCallbackFlatMapper, pyCallbackType);
    RSM_RegisterMap(RediStarPy_PyCallbackMapper, pyCallbackType);
    RSM_RegisterGroupByExtractor(RediStarPy_PyCallbackExtractor, pyCallbackType);
    RSM_RegisterReducer(RediStarPy_PyCallbackReducer, pyCallbackType);

    if (RedisModule_CreateCommand(ctx, "rs.pyexecute", RediStarPy_Execut, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command example");
        return REDISMODULE_ERR;
    }

    PyEval_ReleaseLock();

    return true;
}
