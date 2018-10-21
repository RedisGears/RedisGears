#include "redismodule.h"
#include "redistar.h"
#include <assert.h>
#include <string.h>

char* KeyRecordStrValueExtractor(Record *record, void* arg, size_t* len){
    assert(RediStar_RecordGetType(record) == KEY_RECORD);
    assert(RediStar_RecordGetType(RediStar_KeyRecordGetVal(record)) == STRING_RECORD);
    Record* strRecord = RediStar_KeyRecordGetVal(record);
    char* str = RediStar_StringRecordGet(strRecord);
    *len = strlen(str);
    return str;
}
