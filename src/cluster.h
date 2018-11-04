/*
 * cluster.h
 *
 *  Created on: Nov 4, 2018
 *      Author: meir
 */

#ifndef SRC_CLUSTER_H_
#define SRC_CLUSTER_H_

#include "redismodule.h"
#include <stdbool.h>

bool Cluster_IsClusterMode();
size_t Cluster_GetSize();
void Cluster_Refresh();
char* Cluster_GetMyId();
char** Cluster_GetNodesList(size_t* len);
char* Cluster_GetNodeIdByKey(char* key);


#endif /* SRC_CLUSTER_H_ */
