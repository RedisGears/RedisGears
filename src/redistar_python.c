#include "redistar_python.h"
#include "redistar.h"
#include "redistar_memory.h"
#include "record.h"
#include <Python.h>
#include <marshal.h>
#include <assert.h>

static PyObject* pFunc;
static PyObject* globals;

static RedisModuleCtx* currentCtx = NULL;

#define PYTHON_ERROR "error running python code"

static PyObject* run(PyObject *cls, PyObject *args){
    PyObject* self = PyTuple_GetItem(args, 0);
    PyObject* regexKey = PyString_FromString("regex");
    PyObject* regex = PyObject_GetAttr(self, regexKey);
    PyObject* nameKey = PyString_FromString("name");
	PyObject* name = PyObject_GetAttr(self, nameKey);
    Py_DECREF(regexKey);
    Py_DECREF(nameKey);
    char* regexStr = PyString_AsString(regex);
    char* nameStr = PyString_AsString(name);
    Py_DECREF(regex);
	Py_DECREF(name);
    // todo : expose execution name on python interface
    RediStarCtx* rsctx = RSM_CreateCtx(nameStr, KeysReader, RS_STRDUP(regexStr));
    RSM_Map(rsctx, RediStarPy_ToPyRecordMapper, NULL);

    PyObject* stepsKey = PyString_FromString("steps");
    PyObject* stepsList = PyObject_GetAttr(self, stepsKey);
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
        default:
            assert(false);
        }
    }

    if(!RSM_Run(rsctx, NULL, NULL)){
        RedisModule_ReplyWithError(currentCtx, "Execution with the given name already exists, pleas drop it first.");
        RediStar_DropExecution(rsctx, currentCtx);
    }else{
        RedisModule_ReplyWithSimpleString(currentCtx, "OK");
    }

    RediStar_FreeCtx(rsctx);
    return PyLong_FromLong(1);
}

static PyObject* saveGlobals(PyObject *cls, PyObject *args){
    globals = PyEval_GetGlobals();
    Py_INCREF(globals);
    return PyLong_FromLong(1);
}

static PyObject* saveKey(PyObject *cls, PyObject *args){
    if(PyTuple_Size(args) != 2){
        return PyBool_FromLong(0);
    }
    RedisModuleCtx* rctx = RedisModule_GetThreadSafeContext(NULL);
    PyObject* key = PyTuple_GetItem(args, 0);
    PyObject* val = PyTuple_GetItem(args, 1);
    char* keyStr = PyString_AsString(key);
    char* valStr = PyString_AsString(val);
    RedisModule_ThreadSafeContextLock(rctx);
    RedisModuleString* keyRedisStr = RedisModule_CreateString(rctx, keyStr, strlen(keyStr));
    RedisModuleKey* keyHandler = RedisModule_OpenKey(rctx, keyRedisStr, REDISMODULE_WRITE);
    int type = RedisModule_KeyType(keyHandler);
    if(type != REDISMODULE_KEYTYPE_EMPTY){
        RedisModule_DeleteKey(keyHandler);
    }
    RedisModuleString* valRedisStr = RedisModule_CreateString(rctx, valStr, strlen(valStr));
    RedisModule_StringSet(keyHandler, valRedisStr);
    RedisModule_ThreadSafeContextUnlock(rctx);
    RedisModule_FreeThreadSafeContext(rctx);
    return PyBool_FromLong(1);
}

PyMethodDef EmbMethods[] = {
    {"run", run, METH_VARARGS, "start running the execution plan"},
    {"_saveGlobals", saveGlobals, METH_VARARGS, "should not be use"},
    {"saveKey", saveKey, METH_VARARGS, "saving the given key to a give value"},
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
            Record* pyRecord = RS_PyObjRecordCreare();
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
    Py_DECREF(retStr);
    *len = strlen(retCStr);
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
    Record* retRecord = RS_PyObjRecordCreare();
    RS_PyObjRecordSet(retRecord, ret);
    RediStar_FreeRecord(records);
    PyGILState_Release(state);
    return retRecord;
}

static Record* RediStarPy_ToPyRecordMapperInternal(Record *record, void* arg){
    Record* res = RS_PyObjRecordCreare();
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
        str = RediStar_StringRecordGet(record);
        obj = PyString_FromString(str);
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
    PyTuple_SetItem(pArgs, 1, globals);
    Py_INCREF(globals);
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
    Py_InitModule("redistar", EmbMethods);

    PyRun_SimpleString("import redistar\n"
                       "class starCtx:\n"
                       "    def __init__(self, name, regex):\n"
                       "        self.regex = regex\n"
    				   "        self.name = name\n"
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
                       "    def run(self):\n"
                       "        redistar.run(self)\n"
                       "globals()['str'] = str\n"
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
