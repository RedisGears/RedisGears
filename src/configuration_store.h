
#ifndef SRC_CONFIGURATION_STORE_H_
#define SRC_CONFIGURATION_STORE_H_

#include <stddef.h>
#include "redismodule.h"

typedef struct ConfigurationCtx ConfigurationCtx;

int ConfigurationStore_Initialize(RedisModuleCtx* ctx);

ConfigurationCtx* ConfigurationStore_CtxCreate(const char* val, size_t size);
const char* ConfigurationStore_CtxGetVal(ConfigurationCtx* ctx, size_t* size);
void ConfigurationStore_CtxFree(ConfigurationCtx* ctx);
void ConfigurationStore_Set(const char* key, ConfigurationCtx* ctx);
ConfigurationCtx* ConfigurationStore_Get(const char* key);

void ConfigurationStore_Save(RedisModuleIO *rdb, int when);
int ConfigurationStore_Load(RedisModuleIO *rdb, int encver, int when);

void ConfigurationStore_Clear();

#endif /* SRC_CONFIGURATION_STORE_H_ */
