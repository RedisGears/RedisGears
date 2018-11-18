/*
 * triggers.h
 *
 *  Created on: 18 Nov 2018
 *      Author: root
 */

#ifndef SRC_TRIGGERS_H_
#define SRC_TRIGGERS_H_

#include "redistar.h"

int Triggers_Init();
int Trigger_OnKeyArriveTrigger(RedisModuleCtx* rctx, FlatExecutionPlan* fep);



#endif /* SRC_TRIGGERS_H_ */
