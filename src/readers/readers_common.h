#ifndef SRC_READERS_READERS_COMMON_H_
#define SRC_READERS_READERS_COMMON_H_

#include "../execution_plan.h"

#define totalDurationMS(ctx) DURATION2MS(ctx->totalRunDuration)
#define avgDurationMS(ctx) ((double)DURATION2MS(ctx->totalRunDuration) / (ctx->numSuccess + ctx->numFailures + ctx->numAborted))



#endif /* SRC_READERS_READERS_COMMON_H_ */
