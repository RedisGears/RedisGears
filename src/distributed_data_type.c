#include "distributed_data_type.h"
#include "redisgears_memory.h"

#include <assert.h>

typedef struct DistributedDataTypeOperation{
    DataTypeOperationCallback OperationCallback;
    DataTypeAppliedOnClusterCallback appliedOnClusterCallback;
}DistributedDataTypeOperation;

static void DistributedDataType_OnMsgAproved(void* privateData, const char* msg, size_t len, void* additionalData){
    DistributedDataType* ddt = privateData;
    Gears_Buffer buff;
    buff.buff = (char*)msg;
    buff.size = len;
    buff.cap = len;
    Gears_BufferReader br;
    Gears_BufferReaderInit(&br, &buff);
    const char* opName = Gears_BufferReaderReadString(&br);

    DistributedDataTypeOperation* op = Gears_dictFetchValue(ddt->operations, opName);
    assert(op);

    size_t dataLen;
    const char* data = Gears_BufferReaderReadBuff(&br, &dataLen);

    op->OperationCallback(ddt->privateData, data, dataLen, additionalData);
}

DistributedDataType* DistributedDataType_Create(const char* name, void* privateData,
                                                SerializePrivateDataCallback serialize,
                                                DeserializePrivateDataCallback deserialize,
                                                FreePrivateDataCallback free){
    DistributedDataType* ddt = RG_ALLOC(sizeof(*ddt));
    ddt->serialize = serialize;
    ddt->deserialize = deserialize;
    ddt->free = free;
    ddt->privateData = privateData;
    ddt->operations = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    ddt->consensus = Consensus_Create(name, DistributedDataType_OnMsgAproved, ddt);
    return ddt;
}

void DistributedDataType_RegisterOperation(DistributedDataType* ddt,
                                           const char* opName,
                                           DataTypeOperationCallback OperationCallback,
                                           DataTypeAppliedOnClusterCallback appliedOnClusterCallback){
    DistributedDataTypeOperation* op = RG_ALLOC(sizeof(*op));
    *op = (DistributedDataTypeOperation){
            .OperationCallback = OperationCallback,
            .appliedOnClusterCallback = appliedOnClusterCallback,
    };
    Gears_dictAdd(ddt->operations, (char*)opName, op);
}

void DistributedDataType_ApplyOperation(DistributedDataType* ddt,
                                        const char* opName, const char* msg,
                                        size_t len, void* extraData){
    Gears_Buffer* buff = Gears_BufferCreate();
    Gears_BufferWriter bw;
    Gears_BufferWriterInit(&bw, buff);
    Gears_BufferWriterWriteString(&bw, opName);
    Gears_BufferWriterWriteBuff(&bw, msg, len);
    Consensus_Send(ddt->consensus, buff->buff, buff->size, extraData);
    Gears_BufferFree(buff);
}



