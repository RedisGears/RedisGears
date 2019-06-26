/*
 * distributed_data_type.h
 *
 *  Created on: Jun 17, 2019
 *      Author: root
 */

#ifndef CLUSTER_DISTRIBUTED_DATA_TYPE_H_
#define CLUSTER_DISTRIBUTED_DATA_TYPE_H_

#include "consensus.h"
#include "utils/dict.h"
#include "utils/buffer.h"
#include "redismodule.h"

typedef void (*DataTypeOperationCallback)(void* pd, const char* msg, size_t len, void* extraData);
typedef void (*DataTypeAppliedOnClusterCallback)(void* pd, void* extraData);
typedef void (*SerializePrivateDataCallback)(void* pd, Gears_BufferWriter* bw);
typedef void (*DeserializePrivateDataCallback)(void* pd, Gears_BufferReader* br);
typedef void (*FreePrivateDataCallback)(void* pd);

typedef struct DistributedDataType{
    SerializePrivateDataCallback serialize;
    DeserializePrivateDataCallback deserialize;
    FreePrivateDataCallback free;
    Gears_dict* operations;
    Consensus* consensus;
    void* privateData;
}DistributedDataType;


DistributedDataType* DistributedDataType_Create(const char* name, void* privateData,
                                                SerializePrivateDataCallback serialize,
                                                DeserializePrivateDataCallback deserialize,
                                                FreePrivateDataCallback free);

void DistributedDataType_RegisterOperation(DistributedDataType* ddt,
                                           const char* opName,
                                           DataTypeOperationCallback OperationCallback,
                                           DataTypeAppliedOnClusterCallback appliedOnClusterCallback);
#define DistributedDataType_Register(ddt, OperationCallback, appliedOnClusterCallback) \
    DistributedDataType_RegisterOperation(ddt, #OperationCallback, OperationCallback, appliedOnClusterCallback);

void DistributedDataType_ApplyOperation(DistributedDataType* ddt,
                                        const char* opName, const char* msg,
                                        size_t len, void* extraData);
#define DistributedDataType_Apply(ddt, callback, msg, len, extraData) \
        DistributedDataType_ApplyOperation(ddt, #callback, msg, len, extraData);


#endif /* CLUSTER_DISTRIBUTED_DATA_TYPE_H_ */
