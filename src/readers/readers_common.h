#ifndef SRC_READERS_READERS_COMMON_H_
#define SRC_READERS_READERS_COMMON_H_

#include "../execution_plan.h"

#define totalDurationMS(ctx) DURATION2MS(ctx->totalRunDuration)
#define avgDurationMS(ctx) ((ctx->numSuccess + ctx->numFailures + ctx->numAborted) == 0 ? 0 : ((double)DURATION2MS(ctx->totalRunDuration) / (ctx->numSuccess + ctx->numFailures + ctx->numAborted)))

#define resetStats(ctx) \
    do { \
    	ctx->numTriggered -= (ctx->numAborted + ctx->numSuccess + ctx->numFailures); \
        ctx->numAborted = 0; \
        ctx->numSuccess = 0; \
        ctx->numFailures = 0; \
        ctx->lastRunDuration = 0; \
        ctx->totalRunDuration = 0; \
        if(ctx->lastError){ \
            RG_FREE(ctx->lastError); \
        } \
        ctx->lastError = NULL; \
    } while(0)


#endif /* SRC_READERS_READERS_COMMON_H_ */
