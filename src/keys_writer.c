#include "redistar.h"
#include <string.h>
#include <assert.h>
#include "redistar_memory.h"
#include "record.h"

typedef struct ResponseWriterCtx{
    RedisModuleCtx *rctx;
    size_t replySize;
}ResponseWriterCtx;

void KeysWriter_Start(void* ctx){
    ResponseWriterCtx *rctx = ctx;
    RedisModule_ReplyWithArray(rctx->rctx, REDISMODULE_POSTPONED_ARRAY_LEN);
}

static void KeysWriter_WriteRecord(void* ctx, Record* record){
    ResponseWriterCtx *rctx = ctx;
    size_t listLen;
    char* str;
#ifdef WITHPYTHON
    PyObject* obj;
#endif
    switch(RediStar_RecordGetType(record)){
    case STRING_RECORD:
        str = RediStar_StringRecordGet(record);
        RedisModule_ReplyWithStringBuffer(rctx->rctx, str, strlen(str));
        break;
    case LONG_RECORD:
        RedisModule_ReplyWithLongLong(rctx->rctx, RediStar_LongRecordGet(record));
        break;
    case DOUBLE_RECORD:
        RedisModule_ReplyWithDouble(rctx->rctx, RediStar_DoubleRecordGet(record));
        break;
    case KEY_HANDLER_RECORD:
        RedisModule_ReplyWithStringBuffer(rctx->rctx, "KEY HANDLER RECORD", strlen("KEY HANDLER RECORD"));
        break;
    case KEY_RECORD:
        RedisModule_ReplyWithArray(rctx->rctx, 2);
        size_t keyLen;
        char* key = RediStar_KeyRecordGetKey(record, &keyLen);
        RedisModule_ReplyWithStringBuffer(rctx->rctx, key, keyLen);
        KeysWriter_WriteRecord(ctx, RediStar_KeyRecordGetVal(record));
        break;
    case LIST_RECORD:
        listLen = RediStar_ListRecordLen(record);
        RedisModule_ReplyWithArray(rctx->rctx, listLen);
        for(int i = 0 ; i < listLen ; ++i){
            KeysWriter_WriteRecord(ctx, RediStar_ListRecordGet(record, i));
        }
        break;
#ifdef WITHPYTHON
    case PY_RECORD:
        obj = RS_PyObjRecordGet(record);
        if(PyObject_TypeCheck(obj, &PyBaseString_Type)) {
            str = PyString_AsString(obj);
            RedisModule_ReplyWithStringBuffer(rctx->rctx, str, strlen(str));
        }else{
            RedisModule_ReplyWithStringBuffer(rctx->rctx, "PY RECORD", strlen("PY RECORD"));
        }
        break;
#endif
    default:
        assert(false);
    }
}

void KeysWriter_Write(void* ctx, Record* record){
    ResponseWriterCtx *rctx = ctx;
    KeysWriter_WriteRecord(ctx, record);
    RediStar_FreeRecord(record);
    ++rctx->replySize;
}

void KeysWriter_Done(void* ctx){
    ResponseWriterCtx *rctx = ctx;
    RedisModule_ReplySetArrayLength(rctx->rctx, rctx->replySize);
}

void KeysWriter_Free(void* ctx){
    ResponseWriterCtx *rctx = ctx;
    RS_FREE(rctx);
}

Writer* ReplyWriter(void* arg){
    RedisModuleCtx *rctx = arg;
    ResponseWriterCtx* ctx = RS_ALLOC(sizeof(*ctx));
    *ctx = (ResponseWriterCtx){
        .rctx = rctx,
        .replySize = 0,
    };
    Writer* ret = RS_ALLOC(sizeof(*ret));
    *ret = (Writer){
        .ctx = ctx,
        .Start = KeysWriter_Start,
        .Write = KeysWriter_Write,
        .Done = KeysWriter_Done,
        .Free = KeysWriter_Free,
    };
    return ret;
}
