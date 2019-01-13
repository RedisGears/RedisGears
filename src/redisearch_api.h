/*
 * redisearch_api.h
 *
 *  Created on: 8 Jan 2019
 *      Author: root
 */

#ifndef SRC_REDISEARCH_API_H_
#define SRC_REDISEARCH_API_H_

#define REDISEARCH_LOW_LEVEL_API_VERSION 1

#include "redismodule.h"

#define REDISEARCH_API_FUNC(x) (*x)

typedef int RediSeachStatus;

typedef struct IndexSpec IndexSpec;
typedef struct RSQueryNode QueryNode;
typedef struct indexIterator ResultsIterator;

#define INDEX_TYPE_TEXT 0
#define INDEX_TYPE_NUMERIC 1
#define INDEX_TYPE_GEO 2
#define INDEX_TYPE_TAG 3

#define PREFIX_SEARCH 1
#define EXECT_SEARCH 2

typedef union RediSearch_Val{
  const char* str;
  double doubleVal;
}RediSearch_Val;

typedef struct RediSearch_FieldVal{
  const char* fieldName;
  RediSearch_Val val;
}RediSearch_FieldVal;

typedef struct RediSearch_Field{
  const char* fieldName;
  int fieldType;
}RediSearch_Field;

int REDISEARCH_API_FUNC(RediSearch_CheckApiVersionCompatibility)(int currVersion);

/**
 * Create a new index spec with the give spec name.
 * The return value is the only way to access the spec, i.e, the spec are not save inside redis key space.
 */
IndexSpec* REDISEARCH_API_FUNC(RediSearch_CreateIndexSpec)(const char* specName, RediSearch_Field* fields, size_t len);

/**
 * Add a new field to the spec with the fiven name and type.
 * type must be one of the following:
 * 1. INDEX_TYPE_TEXT - full text search
 * 2. INDEX_TYPE_NUMERIC - numeric search
 * 3. INDEX_TYPE_GEO - geo search
 * 4. INDEX_TYPE_TAG - tag search (indexing the data as is with no tokenization process)
 */
RediSeachStatus REDISEARCH_API_FUNC(RediSearch_IndexSpecAddField)(IndexSpec* spec, RediSearch_Field* field);

/**
 * Adding a new document to the index. If the document already exists the field value in updated with the
 * new given value.
 */
RediSeachStatus REDISEARCH_API_FUNC(RediSearch_IndexSpecAddDocument)(IndexSpec* spec, const char* docId, RediSearch_FieldVal* vals, size_t len);

/**
 * Creating a union node
 */
QueryNode* REDISEARCH_API_FUNC(RediSearch_CreateUnionNode)(IndexSpec* spec, const char* fieldName);

/**
 * Adding a child to the union node
 */
void REDISEARCH_API_FUNC(RediSearch_UnionNodeAddChild)(QueryNode* unionNode, QueryNode* child);

/**
 * Creating a numeric filter node
 */
QueryNode* REDISEARCH_API_FUNC(RediSearch_CreateNumericNode)(IndexSpec* spec, const char* fieldName, double min, double max, int inclusiveMin, int inclusiveMax);

/**
 * Creating a tag filter node.
 * The field name is not optional and NULL value will not be accepted.
 */
QueryNode* REDISEARCH_API_FUNC(RediSearch_CreateTagNode)(IndexSpec* spec, const char* fieldName, char* tag, int flag);

/**
 * Creating a token node
 */
QueryNode* REDISEARCH_API_FUNC(RediSearch_CreateTokenNode)(IndexSpec* spec, const char* fieldName, const char* token, int flag);

/**
 * Return a results iterator on the given spec.
 * Return results which feats to the given query node
 */
ResultsIterator* REDISEARCH_API_FUNC(RediSearch_GetResultsIterator)(IndexSpec* spec, QueryNode* qn);

/**
 * Get the next result from the result iterator, the return value is the document id
 */
const char* REDISEARCH_API_FUNC(RediSearch_ResultsIteratorNext)(IndexSpec* spec, ResultsIterator* iter);

/**
 * Free the result iterator
 */
void REDISEARCH_API_FUNC(RediSearch_ResultsIteratorFree)(ResultsIterator* iter);


#define REDISEARCH_MODULE_INIT_FUNCTION(name) \
        if (RedisModule_GetApi("RediSearch_" #name, ((void **)&RediSearch_ ## name))) { \
            printf("could not initialize RediSearch_" #name "\r\n");\
            return REDISMODULE_ERR; \
        }

static int RediSearch_Initialize(){
  REDISEARCH_MODULE_INIT_FUNCTION(CheckApiVersionCompatibility);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateIndexSpec);
  REDISEARCH_MODULE_INIT_FUNCTION(IndexSpecAddField);
  REDISEARCH_MODULE_INIT_FUNCTION(IndexSpecAddDocument);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateUnionNode);
  REDISEARCH_MODULE_INIT_FUNCTION(UnionNodeAddChild);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateNumericNode);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateTagNode);
  REDISEARCH_MODULE_INIT_FUNCTION(CreateTokenNode);
  REDISEARCH_MODULE_INIT_FUNCTION(GetResultsIterator);
  REDISEARCH_MODULE_INIT_FUNCTION(ResultsIteratorNext);
  REDISEARCH_MODULE_INIT_FUNCTION(ResultsIteratorFree);
  return RediSearch_CheckApiVersionCompatibility(REDISEARCH_LOW_LEVEL_API_VERSION);
}

#endif /* SRC_REDISEARCH_API_H_ */
