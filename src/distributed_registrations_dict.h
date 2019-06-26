/*
 * distributed_registered_dict.h
 *
 *  Created on: Jun 23, 2019
 *      Author: root
 */

#ifndef SRC_DISTRIBUTED_REGISTRATIONS_DICT_H_
#define SRC_DISTRIBUTED_REGISTRATIONS_DICT_H_

#include "redisgears.h"

int DistributedRegistrationsDict_Init();
void DistributedRegistrationsDict_Add(FlatExecutionPlan* fep, char* key);
void DistributedRegistrationsDict_Remove(const char* id, RedisModuleBlockedClient *bc);
void DistributedRegistrationsDict_Dump(RedisModuleCtx *ctx);


#endif /* SRC_DISTRIBUTED_REGISTRATIONS_DICT_H_ */
