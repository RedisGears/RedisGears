#include "redistar.h"
#include <string.h>
#include <assert.h>
#include "redistar_memory.h"
#include "record.h"
#include "mgmt.h"
#include "redismodule.h"

typedef struct ResponseWriterCtx{
    size_t replySize;
}ResponseWriterCtx;

static void KeysWriter_Free(void* ctx){
    ResponseWriterCtx *writerCtx = ctx;
    RS_FREE(writerCtx);
}

static ArgType KeysWriterType = (ArgType){
    .type = "KeysWriterType",
    .free = KeysWriter_Free,
    .serialize = NULL,
    .deserialize = NULL,
};

static void KeysWriter_Start(RedisModuleCtx* rctx, void* ctx){
    RedisModule_ThreadSafeContextLock(rctx);
    RedisModule_ReplyWithArray(rctx, REDISMODULE_POSTPONED_ARRAY_LEN);
}

static void KeysWriter_WriteRecord(RedisModuleCtx* rctx, Record* record){
    size_t listLen;
    char* str;
#ifdef WITHPYTHON
    PyObject* obj;
#endif
    switch(RediStar_RecordGetType(record)){
    case STRING_RECORD:
        str = RediStar_StringRecordGet(record);
        RedisModule_ReplyWithStringBuffer(rctx, str, strlen(str));
        break;
    case LONG_RECORD:
        RedisModule_ReplyWithLongLong(rctx, RediStar_LongRecordGet(record));
        break;
    case DOUBLE_RECORD:
        RedisModule_ReplyWithDouble(rctx, RediStar_DoubleRecordGet(record));
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_ReplyWithStringBuffer(rctx, "KEY HANDLER RECORD", strlen("KEY HANDLER RECORD"));
        break;
    case KEY_RECORD:
        RedisModule_ReplyWithArray(rctx, 2);
        size_t keyLen;
        char* key = RediStar_KeyRecordGetKey(record, &keyLen);
        RedisModule_ReplyWithStringBuffer(rctx, key, keyLen);
        KeysWriter_WriteRecord(rctx, RediStar_KeyRecordGetVal(record));
        break;
    case LIST_RECORD:
        listLen = RediStar_ListRecordLen(record);
        RedisModule_ReplyWithArray(rctx, listLen);
        for(int i = 0 ; i < listLen ; ++i){
            KeysWriter_WriteRecord(rctx, RediStar_ListRecordGet(record, i));
        }
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        obj = RS_PyObjRecordGet(record);
        if(PyObject_TypeCheck(obj, &PyBaseString_Type)) {
            str = PyString_AsString(obj);
            RedisModule_ReplyWithStringBuffer(rctx, str, strlen(str));
        }else{
            RedisModule_ReplyWithStringBuffer(rctx, "PY RECORD", strlen("PY RECORD"));
        }
        break;
#endif
    default:
        assert(false);
    }
}

static void KeysWriter_Write(RedisModuleCtx* rctx, void* ctx, Record* record){
    ResponseWriterCtx *writerCtx = ctx;
    KeysWriter_WriteRecord(rctx, record);
    RediStar_FreeRecord(record);
    ++writerCtx->replySize;
}

static void KeysWriter_Done(RedisModuleCtx* rctx, void* ctx){
    ResponseWriterCtx *writerCtx = ctx;
    RedisModule_ReplySetArrayLength(rctx, writerCtx->replySize);
    RedisModule_ThreadSafeContextUnlock(rctx);
}

ArgType* GetKeysWriterArgType(){
    return &KeysWriterType;
}

Writer* ReplyWriter(void* arg){
    ResponseWriterCtx* ctx = RS_ALLOC(sizeof(*ctx));
    *ctx = (ResponseWriterCtx){
        .replySize = 0,
    };
    Writer* ret = RS_ALLOC(sizeof(*ret));
    *ret = (Writer){
        .ctx = ctx,
        .Start = KeysWriter_Start,
        .Write = KeysWriter_Write,
        .Done = KeysWriter_Done,
    };
    return ret;
}
