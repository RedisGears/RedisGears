/*
 * mappers.h
 *
 *  Created on: May 6, 2019
 *      Author: root
 */

#ifndef SRC_MAPPERS_H_
#define SRC_MAPPERS_H_

#include "redisgears.h"

Record* GetValueMapper(RedisModuleCtx* rctx, Record *record, void* arg, char** err);


#endif /* SRC_MAPPERS_H_ */
