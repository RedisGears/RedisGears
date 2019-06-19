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

typedef void (*DataTypeOperationCallback)(void* pd, char** argv, size_t* argc, void* extraData);
typedef void (*SerializePrivateDataCallback)(void* pd, Gears_BufferWriter* bw);
typedef void (*DeserializePrivateDataCallback)(void* pd, Gears_BufferReader* br);
typedef void (*FreePrivateDataCallback)(void* pd);

typedef struct DistributedDataType{
    Consensus* consensus;
    void* privateData;
    SerializePrivateDataCallback serialize;
    DeserializePrivateDataCallback deserialize;
    FreePrivateDataCallback free;
    Gears_dict* operations;
}DistributedDataType;


DistributedDataType* DistributedDataType_Create(void* privateData,
                                                SerializePrivateDataCallback serialize,
                                                DeserializePrivateDataCallback deserialize,
                                                FreePrivateDataCallback free);

void DistributedDataType_RegisterOperation(DistributedDataType* ddt, const char* opName, DataTypeOperationCallback callback);
#define DistributedDataType_Register(ddt, callback) \
    DistributedDataType_RegisterOperation(ddt, #callback, callback);

void DistributedDataType_ApplyOperation(DistributedDataType* ddt, const char* opName, char** argv, size_t* argc, void* extraData);
#define DistributedDataType_Apply(ddt, callback, argv, argc) \
        DistributedDataType_ApplyOperation(ddt, #callback, argv, argc);


#endif /* CLUSTER_DISTRIBUTED_DATA_TYPE_H_ */
