#define __USE_GNU
#include <dlfcn.h>

#include "jni.h"       /* where everything is defined */
#include <stdbool.h>
#include <stdlib.h>
#include "redismodule.h"
#include "redisgears.h"
#include "version.h"
#include "utils/adlist.h"

#include <pthread.h>

#define JOBJECT_TYPE_VERSION 1
#define JSESSION_TYPE_VERSION 1

#define JVM_SESSION_TYPE_NAME "JVMSessionType"

typedef struct JVM_ThreadLocalData JVM_ThreadLocalData;
typedef struct JVM_ExecutionCtx JVM_ExecutionCtx;
typedef struct JVMRunSession JVMRunSession;
typedef struct JVMFlatExecutionSession JVMFlatExecutionSession;

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader, jstring descStr);
static void JVM_ARCreate(JNIEnv *env, jobject objectOrClass);
static void JVM_ARFree(JNIEnv *env, jobject objectOrClass);
static void JVM_ARSetResult(JNIEnv *env, jobject objectOrClass, jobject res);
static void JVM_ARSetError(JNIEnv *env, jobject objectOrClass, jstring error);
static void JVM_GBDestroy(JNIEnv *env, jobject objectOrClass);
static jobject JVM_GBMap(JNIEnv *env, jobject objectOrClass, jobject mapper);
static void JVM_GBRun(JNIEnv *env, jobject objectOrClass, jobject reader);
static jobject JVM_GBExecute(JNIEnv *env, jobject objectOrClass, jobjectArray command);
static jobject JVM_GBCallNext(JNIEnv *env, jobject objectOrClass, jobjectArray command);
static jobject JVM_GBGetCommand(JNIEnv *env, jobject objectOrClass);
static void JVM_GBOverriderReply(JNIEnv *env, jobject objectOrClass, jobject reply);
static jfloat JVM_GBGetMemoryRatio(JNIEnv *env, jobject objectOrClass);
static jboolean JVM_GBSetAvoidNotifications(JNIEnv *env, jobject objectOrClass, jboolean val);
static void JVM_GBAcquireRedisGil(JNIEnv *env, jobject objectOrClass);
static void JVM_GBReleaseRedisGil(JNIEnv *env, jobject objectOrClass);
static void JVM_GBLog(JNIEnv *env, jobject objectOrClass, jstring msg, jobject logLevel);
static jstring JVM_GBHashtag(JNIEnv *env, jobject objectOrClass);
static jstring JVM_GBConfigGet(JNIEnv *env, jobject objectOrClass, jstring key);
static jstring JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader, jobject jmode, jobject onRegistered, jobject onUnregistered);
static jobject JVM_GBAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator);
static jobject JVM_GBRepartition(JNIEnv *env, jobject objectOrClass, jobject extractor);
static jobject JVM_GBLocalAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator);
static jobject JVM_GBAccumulate(JNIEnv *env, jobject objectOrClass, jobject accumulator);
static jobject JVM_GBCollect(JNIEnv *env, jobject objectOrClass);
static jobject JVM_GBForeach(JNIEnv *env, jobject objectOrClass, jobject foreach);
static jobject JVM_GBFilter(JNIEnv *env, jobject objectOrClass, jobject filter);
static jobject JVM_GBFlatMap(JNIEnv *env, jobject objectOrClass, jobject mapper);
static jobject JVM_TurnToGlobal(JNIEnv *env, jobject local);
static char* JVM_GetException(JNIEnv *env);
static JVM_ThreadLocalData* JVM_GetThreadLocalData(JVM_ExecutionCtx* jectx);
static void JVM_ThreadLocalDataRestor(JVM_ThreadLocalData* jvm_ltd, JVM_ExecutionCtx* jectx);
static JVMRunSession* JVM_SessionCreate(const char* id, const char* mainClassName, const char* jarBytes, size_t len, char** err);
static void JVM_ThreadPoolWorkerHelper(JNIEnv *env, jobject objectOrClass, jlong ctx);
static void JVM_ClassLoaderFinalized(JNIEnv *env, jobject objectOrClass, jlong ctx);
static jstring JVM_GetSessionUpgradeData(JNIEnv *env, jobject objectOrClass, jstring sessionId);
static JVMFlatExecutionSession* JVM_FepSessionCreate(JNIEnv *env, JVMRunSession* s, char** err);
static int JVMRecord_SendReply(Record* base, RedisModuleCtx* rctx);

static RedisModuleCtx *staticCtx = NULL;

static char* shardUniqueId = NULL;
static char* workingDir = NULL;
static char* jarsDir = NULL;

int JVM_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg) {
  va_list args_copy;
  va_copy(args_copy, __arg);

  size_t needed = vsnprintf(NULL, 0, __fmt, __arg) + 1;
  *__ptr = JVM_ALLOC(needed);

  int res = vsprintf(*__ptr, __fmt, args_copy);

  va_end(args_copy);

  return res;
}

int JVM_asprintf(char **__ptr, const char *__restrict __fmt, ...) {
  va_list ap;
  va_start(ap, __fmt);

  int res = JVM_vasprintf(__ptr, __fmt, ap);

  va_end(ap);

  return res;
}

static const char* JVM_GetShardUniqueId() {
    if(!shardUniqueId){
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "logfile");
        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
        RedisModuleCallReply *uuidReply = RedisModule_CallReplyArrayElement(reply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(uuidReply) == REDISMODULE_REPLY_STRING);
        size_t len;
        const char* logFileName = RedisModule_CallReplyStringPtr(uuidReply, &len);
        const char* last = strrchr(logFileName, '/');
        if(last){
            len = len - (last - logFileName + 1);
            logFileName = last + 1;
        }
        shardUniqueId = JVM_ALLOC(len + 1);
        shardUniqueId[len] = '\0';
        memcpy(shardUniqueId, logFileName, len);
        RedisModule_FreeCallReply(reply);
        RedisModule_FreeThreadSafeContext(ctx);
    }
    return shardUniqueId;
}

static const char* JVM_GetWorkingDir() {
    if(!workingDir){
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "dir");
        RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
        RedisModuleCallReply *workingDirReply = RedisModule_CallReplyArrayElement(reply, 1);
        RedisModule_Assert(RedisModule_CallReplyType(workingDirReply) == REDISMODULE_REPLY_STRING);
        size_t len;
        const char* tempWorkingDir = RedisModule_CallReplyStringPtr(workingDirReply, &len);
        workingDir = JVM_ALLOC(len + 1);
        workingDir[len] = '\0';
        memcpy(workingDir, tempWorkingDir, len);
        RedisModule_FreeCallReply(reply);
        RedisModule_FreeThreadSafeContext(ctx);
    }
    return workingDir;
}

typedef struct JVMRecord{
    Record baseRecord;
    jobject obj;
}JVMRecord;

long long sessionsId = 0;

#define ASYNC_RECORD_TYPE int
#define ASYNC_RECORD_TYPE_DEFAULT 1
#define ASYNC_RECORD_TYPE_FILTER 2
#define ASYNC_RECORD_TYPE_FOREACH 3
#define ASYNC_RECORD_TYPE_FLATMAP 4

typedef struct JVMExecutionSession{
    jobject executionInputStream;
    jobject executionOutputStream;
}JVMExecutionSession;

typedef struct JVMRunSession{
    size_t refCount;
    char uuid[ID_LEN];
    char uuidStr[STR_ID_LEN];
    char* jarFilePath;
    char* mainClassName;
    uint version;
    jobject sessionClsLoader;
    JVM_listNode* tsNode;
}JVMRunSession;

typedef struct JVMFlatExecutionSession{
    jobject flatExecutionInputStream;
    jobject flatExecutionOutputStream;
    JVMRunSession* session;
}JVMFlatExecutionSession;

typedef struct JVM_ExecutionCtx{
    JVMRunSession* session;
    ExecutionCtx* eCtx;
    jobject createFuture;
    ASYNC_RECORD_TYPE asyncRecorType;
}JVM_ExecutionCtx;

#define JVM_ExecutionCtxInit(s, e) (JVM_ExecutionCtx){.session = s, .eCtx = e, .createFuture = NULL, .asyncRecorType = 0}

typedef struct JVM_ThreadLocalData{
    JNIEnv *env;
    RedisModuleCtx* rctx;
    bool isBlocked;
    bool allowBlock;
    JVMRunSession* currSession;
    ExecutionCtx* eCtx;
    jobject createFuture;
    ASYNC_RECORD_TYPE asyncRecorType;
}JVM_ThreadLocalData;

pthread_mutex_t JVMSessionsLock;
RedisModuleDict* JVMSessions = NULL;
JVM_list* JVMTSSessions = NULL;

pthread_key_t threadLocalData;

JavaVM *jvm = NULL;       /* denotes a Java VM */

jfieldID ptrFieldId = NULL;
jfieldID classLoaderPrtField = NULL;

jclass gearsObjectCls = NULL;
jclass gearsBooleanCls = NULL;
jclass gearsByteArrayCls = NULL;
jclass gearsStringCls = NULL;
jclass gearsClassCls = NULL;
jmethodID gearsClassGetNameMethodId = NULL;

jmethodID gearsGetBooleanValueMethodId = NULL;

jclass gearsLongCls = NULL;
jmethodID gearsLongValueOfMethodId = NULL;
jmethodID gearsLongValMethodId = NULL;

jclass futureRecordCls = NULL;
jfieldID futureRecordPtrFieldId = NULL;
jfieldID futureRecordTypeFieldId = NULL;

jclass gearsBuilderCls = NULL;
jmethodID gearsBuilderSerializeObjectMethodId = NULL;
jmethodID gearsBuilderDeserializeObjectMethodId = NULL;
jmethodID gearsBuilderOnUnpausedMethodId = NULL;
jmethodID gearsJNICallHelperMethodId = NULL;
jmethodID gearsGetStackTraceMethodId = NULL;
jmethodID gearsCleanCtxClassLoaderMethodId = NULL;
jmethodID gearsDumpHeapMethodId = NULL;
jmethodID gearsRunGCMethodId = NULL;
jmethodID gearsGetStatsMethodId = NULL;

jclass gearsObjectInputStreamCls = NULL;
jmethodID gearsObjectInputStreamGetMethodId = NULL;
jclass gearsObjectOutputStreamCls = NULL;
jmethodID gearsObjectOutputStreamGetMethodId = NULL;

jclass gearsClassLoaderCls = NULL;
jmethodID gearsClassLoaderNewMid = NULL;
jmethodID gearsClassLoaderShutDown = NULL;

jclass javaClassLoaderCls = NULL;
jmethodID javaLoadClassNewMid = NULL;

jclass gearsMappCls = NULL;
jmethodID gearsMapMethodId = NULL;

jclass gearsFlatMappCls = NULL;
jmethodID gearsFlatMapMethodId = NULL;

jclass gearsExtractorCls = NULL;
jmethodID gearsExtractorMethodId = NULL;

jclass gearsForeachCls = NULL;
jmethodID gearsForeachMethodId = NULL;

jclass gearsFilterCls = NULL;
jmethodID gearsFilterMethodId = NULL;

jclass gearsAccumulatorCls = NULL;
jmethodID gearsAccumulatorMethodId = NULL;

jclass gearsAccumulateByCls = NULL;
jmethodID gearsAccumulateByMethodId = NULL;

jclass gearsOnRegisteredCls = NULL;
jclass gearsOnUnregisteredCls = NULL;
jmethodID gearsOnRegisteredMethodId = NULL;
jmethodID gearsOnUnregisteredMethodId = NULL;

jclass baseRecordCls = NULL;
jmethodID recordToStr = NULL;

jclass hashRecordCls = NULL;
jmethodID hashRecordCtor = NULL;
jmethodID hashRecordSet = NULL;

jclass iterableCls = NULL;
jmethodID iteratorMethodId = NULL;

jclass iteratorCls = NULL;
jmethodID iteratorNextMethodId = NULL;
jmethodID iteratorHasNextMethodId = NULL;

jclass arrayCls = NULL;

jclass gearsBaseReaderCls = NULL;

jclass gearsKeyReaderCls = NULL;
jfieldID keysReaderPatternField = NULL;
jfieldID keysReaderNoscanField = NULL;
jfieldID keysReaderReadValuesField = NULL;
jfieldID keysReaderEventTypesField = NULL;
jfieldID keysReaderKeyTypesField = NULL;
jfieldID keysReaderCommandsField = NULL;

jclass gearsKeyReaderRecordCls = NULL;
jmethodID gearsKeyReaderRecordCtrMethodId = NULL;

jclass gearsExecutionModeCls = NULL;
jobject gearsExecutionModeAsync = NULL;
jobject gearsExecutionModeSync = NULL;
jobject gearsExecutionModeAsyncLocal = NULL;

jclass gearsStreamReaderCls = NULL;
jfieldID streamReaderPatternField = NULL;
jfieldID streamReaderStartIdField = NULL;
jfieldID streamReaderBatchSizeField = NULL;
jfieldID streamReaderDurationField = NULL;
jfieldID streamReaderFailurePolicyField = NULL;
jfieldID streamReaderRetryIntervalField = NULL;
jfieldID streamReaderTrimStreamField = NULL;

jclass gearsCommandReaderCls = NULL;
jclass gearsCommandOverriderCls = NULL;
jfieldID commandReaderTriggerField = NULL;
jfieldID commandOverriderCommandField = NULL;
jfieldID commandOverriderPrefixField = NULL;

jclass gearsStreamReaderFailedPolicyCls = NULL;
jclass gearsStreamReaderFailedPolicyContinueCls = NULL;
jclass gearsStreamReaderFailedPolicyAbortCls = NULL;
jclass gearsStreamReaderFailedPolicyRetryCls = NULL;

jclass gearsLogLevelCls = NULL;
jobject gearsLogLevelNotice = NULL;
jobject gearsLogLevelDebug = NULL;
jobject gearsLogLevelVerbose = NULL;
jobject gearsLogLevelWarning = NULL;

jclass exceptionCls = NULL;

RecordType* JVMRecordType = NULL;
ArgType* jvmSessionType = NULL;

JNINativeMethod futureRecordNativeMethod[] = {
        {
            .name = "createAsyncRecord",
            .signature = "()V",
            .fnPtr = JVM_ARCreate,
        },
        {
            .name = "asyncRecordFree",
            .signature = "()V",
            .fnPtr = JVM_ARFree,
        },
        {
            .name = "asyncRecordSetResult",
            .signature = "(Ljava/io/Serializable;)V",
            .fnPtr = JVM_ARSetResult,
        },
        {
            .name = "asyncRecordSetError",
            .signature = "(Ljava/lang/String;)V",
            .fnPtr = JVM_ARSetError,
        },
};

JNINativeMethod gearsBuilderNativeMethod[] = {
        {
            .name = "init",
            .signature = "(Ljava/lang/String;Ljava/lang/String;)V",
            .fnPtr = JVM_GBInit,
        },
        {
            .name = "destroy",
            .signature = "()V",
            .fnPtr = JVM_GBDestroy,
        },
        {
            .name = "map",
            .signature = "(Lgears/operations/MapOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBMap,
        },
        {
            .name = "flatMap",
            .signature = "(Lgears/operations/FlatMapOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBFlatMap,
        },
        {
            .name = "accumulateBy",
            .signature = "(Lgears/operations/ExtractorOperation;Lgears/operations/AccumulateByOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBAccumulateby,
        },
        {
            .name = "repartition",
            .signature = "(Lgears/operations/ExtractorOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBRepartition,
        },
        {
            .name = "localAccumulateBy",
            .signature = "(Lgears/operations/ExtractorOperation;Lgears/operations/AccumulateByOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBLocalAccumulateby,
        },
        {
            .name = "accumulate",
            .signature = "(Lgears/operations/AccumulateOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBAccumulate,
        },
        {
            .name = "foreach",
            .signature = "(Lgears/operations/ForeachOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBForeach,
        },
        {
            .name = "filter",
            .signature = "(Lgears/operations/FilterOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBFilter,
        },
        {
            .name = "collect",
            .signature = "()Lgears/GearsBuilder;",
            .fnPtr = JVM_GBCollect,
        },
        {
            .name = "innerRun",
            .signature = "(Lgears/readers/BaseReader;)V",
            .fnPtr = JVM_GBRun,
        },
        {
            .name = "innerRegister",
            .signature = "(Lgears/readers/BaseReader;Lgears/ExecutionMode;Lgears/operations/OnRegisteredOperation;Lgears/operations/OnUnregisteredOperation;)Ljava/lang/String;",
            .fnPtr = JVM_GBRegister,
        },
        {
            .name = "executeArray",
            .signature = "([Ljava/lang/String;)Ljava/lang/Object;",
            .fnPtr = JVM_GBExecute,
        },
        {
            .name = "callNextArray",
            .signature = "([Ljava/lang/String;)Ljava/lang/Object;",
            .fnPtr = JVM_GBCallNext,
        },
        {
            .name = "getCommand",
            .signature = "()[[B",
            .fnPtr = JVM_GBGetCommand,
        },
        {
            .name = "overrideReply",
            .signature = "(Ljava/lang/Object;)V",
            .fnPtr = JVM_GBOverriderReply,
        },
        {
            .name = "getMemoryRatio",
            .signature = "()F",
            .fnPtr = JVM_GBGetMemoryRatio,
        },
        {
            .name = "setAvoidNotifications",
            .signature = "(Z)Z",
            .fnPtr = JVM_GBSetAvoidNotifications,
        },
        {
            .name = "acquireRedisGil",
            .signature = "()V",
            .fnPtr = JVM_GBAcquireRedisGil,
        },
        {
            .name = "releaseRedisGil",
            .signature = "()V",
            .fnPtr = JVM_GBReleaseRedisGil,
        },
        {
            .name = "log",
            .signature = "(Ljava/lang/String;Lgears/LogLevel;)V",
            .fnPtr = JVM_GBLog,
        },
        {
            .name = "hashtag",
            .signature = "()Ljava/lang/String;",
            .fnPtr = JVM_GBHashtag,
        },
        {
            .name = "configGet",
            .signature = "(Ljava/lang/String;)Ljava/lang/String;",
            .fnPtr = JVM_GBConfigGet,
        },
        {
            .name = "jniTestHelper",
            .signature = "(J)V",
            .fnPtr = JVM_ThreadPoolWorkerHelper,
        },
        {
            .name = "classLoaderFinalized",
            .signature = "(J)V",
            .fnPtr = JVM_ClassLoaderFinalized,
        },
        {
            .name = "getSessionUpgradeData",
            .signature = "(Ljava/lang/String;)Ljava/lang/String;",
            .fnPtr = JVM_GetSessionUpgradeData,
        },

    };

static void JVM_SessionAdd(JVMRunSession* s){
    pthread_mutex_lock(&JVMSessionsLock);
    RedisModule_DictSetC(JVMSessions, s->uuid, ID_LEN, s);
    pthread_mutex_unlock(&JVMSessionsLock);
}

static void JVM_SessionDel(JVMRunSession* s){
    pthread_mutex_lock(&JVMSessionsLock);
    if(s->tsNode){
        JVM_listDelNode(JVMTSSessions, s->tsNode);
    }else{
        RedisModule_DictDelC(JVMSessions, (char*)s->uuid, ID_LEN, NULL);
    }
    pthread_mutex_unlock(&JVMSessionsLock);
}

static JVMRunSession* JVM_SessionGet(const char* uuid){
    pthread_mutex_lock(&JVMSessionsLock);
    JVMRunSession* ret = RedisModule_DictGetC(JVMSessions, (char*)uuid, ID_LEN, NULL);
    if(ret && ret->refCount == 0){
        // we do not return session with refcount 0;
        ret = NULL;
    }
    pthread_mutex_unlock(&JVMSessionsLock);
    return ret;
}

static JVMRunSession* JVM_SessionGetFromStrId(const char* id){
    char realId[ID_LEN] = {0};
    if(strlen(id) < REDISMODULE_NODE_ID_LEN + 2){
        return NULL;
    }
    if(id[REDISMODULE_NODE_ID_LEN] != '-'){
        return NULL;
    }
    memcpy(realId, id, REDISMODULE_NODE_ID_LEN);
    int match = sscanf(id + REDISMODULE_NODE_ID_LEN + 1, "%lld", (long long*)(&realId[REDISMODULE_NODE_ID_LEN]));
    if(match != 1){
        return NULL;
    }
    return JVM_SessionGet(realId);
}

static void JVM_SessionFreeMemory(JVMRunSession* s){
    RedisModule_Assert(!s->sessionClsLoader);
    RedisModule_Assert(s->refCount == 0);
    JVM_SessionDel(s);
    int ret = RedisGears_ExecuteCommand(NULL, "verbose", "rm -rf %s", s->jarFilePath);
    if(ret != 0){
        RedisModule_Log(NULL, "warning", "Failed deleting session jar %s", s->jarFilePath);
    }

    JVM_FREE(s->jarFilePath);
    JVM_FREE(s->mainClassName);
    JVM_FREE(s);
}

static void JVM_SessionFree(JVMRunSession* s){

    if(__atomic_sub_fetch(&s->refCount, 1, __ATOMIC_SEQ_CST) > 0){
        return;
    }

    if(s->sessionClsLoader){

        JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s, NULL);
        JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(&jectx);
        JNIEnv *env = jvm_ltd->env;

        (*env)->CallVoidMethod(env, s->sessionClsLoader, gearsClassLoaderShutDown);
        char* err = NULL;
        if((err = JVM_GetException(env))){
            RedisModule_Log(NULL, "warning", "Exception throw on closing class loader, error='%s'", err);
            JVM_FREE(err);
        }
        (*env)->DeleteGlobalRef(env, s->sessionClsLoader);

        s->sessionClsLoader = NULL;

        JVM_ThreadLocalDataRestor(jvm_ltd, &jectx);
    }else{
        JVM_SessionFreeMemory(s);
    }
}

static void JVM_FepSessionFree(void* arg){
    JVMFlatExecutionSession* fepSession = arg;
    JVM_SessionFree(fepSession->session);
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;
    if(fepSession->flatExecutionInputStream){
        (*env)->DeleteGlobalRef(env, fepSession->flatExecutionInputStream);
    }
    if(fepSession->flatExecutionOutputStream){
        (*env)->DeleteGlobalRef(env, fepSession->flatExecutionOutputStream);
    }
    JVM_FREE(fepSession);
}

static void* JVM_SessionDup(void* arg){
    JVMRunSession* s = arg;
    __atomic_add_fetch(&s->refCount, 1, __ATOMIC_SEQ_CST);
    return s;
}

static void* JVM_FepSessionDup(void* arg){
    JVMFlatExecutionSession* fepSession = arg;
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;
    char* err;
    JVMFlatExecutionSession* newFepSession = JVM_FepSessionCreate(env, JVM_SessionDup(fepSession->session), &err);
    if(err){
        RedisModule_Log(NULL, "warning", "%s", err);
        RedisModule_Assert(false);
    }
    return newFepSession;
}

static int JVM_SessionSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    JVMRunSession* s = arg;

    RedisGears_BWWriteBuffer(bw, s->mainClassName, strlen(s->mainClassName) + 1); // +1 for \0
    RedisGears_BWWriteLong(bw, s->version);
    RedisGears_BWWriteBuffer(bw, s->uuid, ID_LEN);

    FILE *f = fopen(s->jarFilePath, "rb");
    if(!f){
        JVM_asprintf(err, "Could not open jar file %s", s->jarFilePath);
        RedisModule_Log(NULL, "warning", "%s", *err);
        return REDISMODULE_ERR;
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  /* same as rewind(f); */

    char *data = JVM_ALLOC(fsize);
    size_t readData = fread(data, 1, fsize, f);
    if(readData != fsize){
        JVM_FREE(data);
        JVM_asprintf(err, "Could read data from file %s", s->jarFilePath);
        RedisModule_Log(NULL, "warning", "%s", *err);
        return REDISMODULE_ERR;
    }
    fclose(f);

    RedisGears_BWWriteBuffer(bw, data, fsize);

    JVM_FREE(data);

    return REDISMODULE_OK;
}

static int JVM_FepSessionSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    JVMFlatExecutionSession* fepSession = arg;
    return JVM_SessionSerialize(fep, fepSession->session, bw, err);
}

static void* JVM_SessionDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > JSESSION_TYPE_VERSION){
        *err = JVM_STRDUP("Missmatch jvm session version, update to newest JVM module");
        return NULL;
    }
    size_t mainClassNameLen;
    const char* mainClassName = RedisGears_BRReadBuffer(br, &mainClassNameLen);

    uint sessionVersion = RedisGears_BRReadLong(br);

    size_t idLen;
    const char* id = RedisGears_BRReadBuffer(br, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    size_t dataLen;
    const char* data = RedisGears_BRReadBuffer(br, &dataLen);

    JVMRunSession* s = JVM_SessionGet(id);
    // if sessionClsLoader is NULL the session is basically waiting to be free
    if(!s){
        s = JVM_SessionCreate(id, mainClassName, data, dataLen, err);
        s->version = sessionVersion;
    }else{
        s = JVM_SessionDup(s);
    }
    return s;
}

static void* JVM_FepSessionDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    JVMRunSession* s = JVM_SessionDeserialize(fep, br, version, err);
    if(!s){
        return NULL;
    }
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;
    return JVM_FepSessionCreate(env, s, err);
}

static char* JVM_FepSessionToString(void* arg){
    JVMFlatExecutionSession* fepSession = arg;
    char* res;
    JVM_asprintf(&res, "{'MainClassName':'%s', 'SessionId':'%s', 'JarFilePath', '%s'}",
            fepSession->session->mainClassName,
            fepSession->session->uuidStr,
            fepSession->session->jarFilePath);
    return res;
}

static JVMFlatExecutionSession* JVM_FepSessionCreate(JNIEnv *env, JVMRunSession* s, char** err){
    JVMFlatExecutionSession* fepSession = JVM_ALLOC(sizeof(*fepSession));
    fepSession->session = s;
    fepSession->flatExecutionInputStream = NULL;
    fepSession->flatExecutionOutputStream = NULL;

    jobject inputStream = (*env)->CallStaticObjectMethod(env, gearsObjectInputStreamCls, gearsObjectInputStreamGetMethodId, s->sessionClsLoader);

    if((*err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Fatal error, failed creating inputStream for flat execution. error='%s'", *err);
        JVM_FepSessionFree(fepSession);
        return NULL;
    }

    fepSession->flatExecutionInputStream = JVM_TurnToGlobal(env, inputStream);

    jobject outputStream = (*env)->CallStaticObjectMethod(env, gearsObjectOutputStreamCls, gearsObjectOutputStreamGetMethodId);

    if((*err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Fatal error, failed creating outputStream for flat execution. error='%s'", *err);
        JVM_FepSessionFree(fepSession);
        return NULL;
    }

    fepSession->flatExecutionOutputStream = JVM_TurnToGlobal(env, outputStream);

    return fepSession;
}

static JVMRunSession* JVM_SessionCreate(const char* id, const char* mainClassName, const char* jarBytes, size_t len, char** err){
    int ret = RedisGears_ExecuteCommand(NULL, "verbose", "mkdir -p %s/%s-jars", workingDir, shardUniqueId);
    if(ret != 0){
        *err = JVM_STRDUP("Failed create jar directory");
        return NULL;
    }

    JVMRunSession* s = JVM_ALLOC(sizeof(*s));
    RedisGears_GetShardUUID((char*)id, s->uuid, s->uuidStr, &sessionsId);

    s->mainClassName = JVM_STRDUP(mainClassName);

    s->version = 0;

    s->refCount = 1;

    s->sessionClsLoader = NULL;

    s->tsNode = NULL;

#define JAR_RANDOM_NAME 40
    char randomName[JAR_RANDOM_NAME + 1];
    RedisModule_GetRandomHexChars(randomName, JAR_RANDOM_NAME);
    randomName[JAR_RANDOM_NAME] = '\0';

    JVM_asprintf(&s->jarFilePath, "%s/%s-jars/%s.jar", workingDir, shardUniqueId, randomName);

    FILE *f = fopen(s->jarFilePath, "wb");
    if(!f){
        *err = JVM_STRDUP("Failed opening jar file");
        JVM_SessionFree(s);
        return NULL;
    }

    size_t dataWriten = fwrite(jarBytes, 1, len, f);
    if(dataWriten != len){
        *err = JVM_STRDUP("Failed write jar file");
        JVM_SessionFree(s);
        return NULL;
    }

    fclose(f);

    // Creating proper class loader
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s, NULL);
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(&jectx);
    JNIEnv *env = jvm_ltd->env;

    jstring jarPath = (*env)->NewStringUTF(env, s->jarFilePath);

    jobject clsLoader = (*env)->CallStaticObjectMethod(env, gearsClassLoaderCls, gearsClassLoaderNewMid, jarPath);

    (*env)->DeleteLocalRef(env, jarPath);

    if((*err = JVM_GetException(env))){
        JVM_SessionFree(s);
        JVM_ThreadLocalDataRestor(jvm_ltd, &jectx);
        return NULL;
    }

    s->sessionClsLoader = JVM_TurnToGlobal(env, clsLoader);

    (*env)->SetLongField(env, s->sessionClsLoader, classLoaderPrtField, (jlong)s);

    JVM_SessionAdd(s);

    JVM_ThreadLocalDataRestor(jvm_ltd, &jectx);

    return s;
}

static jclass JVM_FindClass(JNIEnv *env, const char* name){
    jclass clazz = (*env)->FindClass(env, name);
    if(!clazz || (*env)->ExceptionCheck(env)){
        if((*env)->ExceptionCheck(env)){
            (*env)->ExceptionDescribe(env);
        }
        RedisModule_Log(NULL, "warning", "Failed finding class %s", name);
        return NULL;
    }

    jclass gclazz  = JVM_TurnToGlobal(env, clazz);

    if(!gclazz || (*env)->ExceptionCheck(env)){
        if((*env)->ExceptionCheck(env)){
            (*env)->ExceptionDescribe(env);
        }
        RedisModule_Log(NULL, "warning", "Failed creating global reference for class %s", name);
        return NULL;
    }

    return gclazz;
}

static jmethodID JVM_InnerFindMethod(JNIEnv *env, jclass clazz, const char* name, const char* sig, bool isStatic){
    jmethodID method = NULL;
    if(isStatic){
        method = (*env)->GetStaticMethodID(env, clazz, name, sig);
    }else{
        method = (*env)->GetMethodID(env, clazz, name, sig);
    }
    if(!method || (*env)->ExceptionCheck(env)){
        if((*env)->ExceptionCheck(env)){
            (*env)->ExceptionDescribe(env);
        }
        RedisModule_Log(NULL, "warning", "Failed finding method, name='%s', sig='%s'", name, sig);
        return NULL;
    }
    return method;
}

#define JVM_FindStaticMethod(env, clazz, name, sig) JVM_InnerFindMethod(env, clazz, name, sig, true);
#define JVM_FindMethod(env, clazz, name, sig) JVM_InnerFindMethod(env, clazz, name, sig, false);

#define JVM_TryFindStaticMethod(env, clazz, name, sig, var) \
        var = JVM_FindStaticMethod(env, clazz, name, sig); \
        if(!var){ \
            return NULL; \
        }

#define JVM_TryFindMethod(env, clazz, name, sig, var) \
        var = JVM_FindMethod(env, clazz, name, sig); \
        if(!var){ \
            return NULL; \
        }

#define JVM_TryFindClass(env, name, var) \
        var = JVM_FindClass(env, name); \
        if(!var){ \
            return NULL; \
        }


static jfieldID JVM_FindField(JNIEnv *env, jclass clazz, const char* name, const char* sig){
    jfieldID fieldId = (*env)->GetFieldID(env, clazz, name, sig);
    if(!fieldId || (*env)->ExceptionCheck(env)){
        if((*env)->ExceptionCheck(env)){
            (*env)->ExceptionDescribe(env);
        }
        RedisModule_Log(NULL, "warning", "Failed finding field, name='%s', sig='%s'", name, sig);
        return NULL;
    }
    return fieldId;
}

#define JVM_TryFindField(env, clazz, name, sig, var) \
        var = JVM_FindField(env, clazz, name, sig); \
        if(!var){ \
            return NULL; \
        }

#define JVM_OPTIONS_CONFIG "JvmOptions"
#define JVM_PATH_CONFIG "JvmPath"

void test(){}

static JavaVMOption* JVM_GetJVMOptions(char** jvmOptionsString){
    JavaVMOption* options = array_new(JavaVMOption, 10);

    const char* moduleDataDir = getenv("modulesdatadir");
    if(moduleDataDir){
        JavaVMOption jniCheckOption;
        JVM_asprintf(&jniCheckOption.optionString, "-Djava.class.path=%s/rg/%d/deps/gears_jvm/gears_runtime/target/gear_runtime-jar-with-dependencies.jar", moduleDataDir, RedisGears_GetVersion());
        options = array_append(options, jniCheckOption);
    }

    JavaVMOption option;
    option.optionString = "-Xrs";
    options = array_append(options, option);

#ifdef VALGRIND
    JavaVMOption jniCheckOption;
    jniCheckOption.optionString = "-Xcheck:jni";
    options = array_append(options, jniCheckOption);
#endif

    *jvmOptionsString = (char*)RedisGears_GetConfig(JVM_OPTIONS_CONFIG);
    if(*jvmOptionsString){
        *jvmOptionsString = JVM_STRDUP(*jvmOptionsString);
        char* optionStr = *jvmOptionsString;
        while(optionStr && *optionStr != '\0'){
            while(*optionStr == ' '){
                ++optionStr;
            }
            if(*optionStr == '\0'){
                break;
            }
            JavaVMOption option;
            option.optionString = optionStr;
            options = array_append(options, option);
            optionStr = strstr(optionStr, " ");
            if(optionStr){
                *optionStr = '\0';
                optionStr++;
            }
        };
    }

    for(size_t i = 0 ; i < array_len(options) ; ++i){
        JavaVMOption* opt =  options + i;
        RedisModule_Log(NULL, "notice", "JVM Options: %s", opt->optionString);
    }

    return options;
}

static void JVM_ThreadLocalDataRestor(JVM_ThreadLocalData* jvm_ltd, JVM_ExecutionCtx* jectx){
    if(jvm_ltd->createFuture){
        (*(jvm_ltd->env))->DeleteGlobalRef(jvm_ltd->env, jvm_ltd->createFuture);
    }

    jvm_ltd->currSession = jectx->session;
    jvm_ltd->eCtx = jectx->eCtx;
    jvm_ltd->createFuture = jectx->createFuture;
    jvm_ltd->asyncRecorType = jectx->asyncRecorType;
}

typedef jint (JNICALL * CreateVM)(JavaVM **pvm, void **penv, void *args);

static JVM_ThreadLocalData* JVM_GetThreadLocalData(JVM_ExecutionCtx* jectx){
    JVM_ThreadLocalData* jvm_tld = pthread_getspecific(threadLocalData);
    if(!jvm_tld){
        jvm_tld = JVM_CALLOC(1, sizeof(*jvm_tld));
        if(!jvm){
            char* jvmOptionsString;
            JavaVMInitArgs vm_args; /* JDK/JRE 10 VM initialization arguments */
            JavaVMOption* options = JVM_GetJVMOptions(&jvmOptionsString);
            vm_args.version = JNI_VERSION_10;
            vm_args.nOptions = array_len(options);
            vm_args.options = options;
            vm_args.ignoreUnrecognized = false;
            /* load and initialize a Java VM, return a JNI interface
             * pointer in env */
            char *pathtojvm;

            const char* moduleDataDir = getenv("modulesdatadir");
            if(moduleDataDir){
                JVM_asprintf(&pathtojvm, "%s/rg/%d/deps/gears_jvm/bin/OpenJDK/jdk-11.0.9.1+1/lib/server/libjvm.so", moduleDataDir, RedisGears_GetVersion());
            }else{
                JVM_asprintf(&pathtojvm, "%s/lib/server/libjvm.so", RedisGears_GetConfig(JVM_PATH_CONFIG));
            }

            RedisModule_Log(NULL, "notice", "Loading jvm from %s", pathtojvm);

            void *handle = dlopen(pathtojvm, RTLD_NOW|RTLD_LOCAL);

            if (NULL == handle) {
                RedisModule_Log(NULL, "warning", "Failed open jvm");
                return NULL;
            }

            JVM_FREE(pathtojvm);

            CreateVM createVM = (CreateVM)dlsym(handle, "JNI_CreateJavaVM");

            if (NULL == createVM) {
                RedisModule_Log(NULL, "warning", "Failed getting JNI_CreateJavaVM symbol");
                return NULL;
            }

            jint jvmInitRes = createVM(&jvm, (void**)&jvm_tld->env, &vm_args);

            if(jvmInitRes != 0){
                RedisModule_Log(NULL, "warning", "Failed initializing the jvm");
                return NULL;
            }

            array_free(options);
            JVM_FREE(jvmOptionsString);

            // register native functions
            JVM_TryFindClass(jvm_tld->env, "java/lang/Object", gearsObjectCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/Boolean", gearsBooleanCls);

            JVM_TryFindClass(jvm_tld->env, "[B", gearsByteArrayCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/String", gearsStringCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/Class", gearsClassCls);
            JVM_TryFindMethod(jvm_tld->env, gearsClassCls, "getName", "()Ljava/lang/String;", gearsClassGetNameMethodId);

            JVM_TryFindMethod(jvm_tld->env, gearsBooleanCls, "booleanValue", "()Z", gearsGetBooleanValueMethodId);


            JVM_TryFindClass(jvm_tld->env, "java/lang/Long", gearsLongCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsLongCls, "valueOf", "(J)Ljava/lang/Long;", gearsLongValueOfMethodId);
            JVM_TryFindMethod(jvm_tld->env, gearsLongCls, "longValue", "()J", gearsLongValMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsBuilder", gearsBuilderCls);

            jint res = (*jvm_tld->env)->RegisterNatives(jvm_tld->env, gearsBuilderCls, gearsBuilderNativeMethod, sizeof(gearsBuilderNativeMethod)/sizeof(JNINativeMethod));

            if(res != JNI_OK){
                (*jvm_tld->env)->ExceptionDescribe(jvm_tld->env);
                RedisModule_Log(NULL, "warning", "could not initialize GearsBuilder natives");
                return NULL;
            }

            JVM_TryFindClass(jvm_tld->env, "gears/FutureRecord", futureRecordCls);

            res = (*jvm_tld->env)->RegisterNatives(jvm_tld->env, futureRecordCls, futureRecordNativeMethod, sizeof(futureRecordNativeMethod)/sizeof(JNINativeMethod));

            if(res != JNI_OK){
                (*jvm_tld->env)->ExceptionDescribe(jvm_tld->env);
                RedisModule_Log(NULL, "warning", "could not initialize futureRecord natives");
                return NULL;
            }

            JVM_TryFindClass(jvm_tld->env, "gears/GearsObjectInputStream", gearsObjectInputStreamCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsObjectInputStreamCls, "getGearsObjectInputStream", "(Ljava/lang/ClassLoader;)Lgears/GearsObjectInputStream;", gearsObjectInputStreamGetMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsObjectOutputStream", gearsObjectOutputStreamCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsObjectOutputStreamCls, "getGearsObjectOutputStream", "()Lgears/GearsObjectOutputStream;", gearsObjectOutputStreamGetMethodId);

            JVM_TryFindField(jvm_tld->env, gearsBuilderCls, "ptr", "J", ptrFieldId);

            JVM_TryFindField(jvm_tld->env, futureRecordCls, "nativeAsyncRecordPtr", "J", futureRecordPtrFieldId);
            JVM_TryFindField(jvm_tld->env, futureRecordCls, "futureRecordType", "I", futureRecordTypeFieldId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsClassLoader", gearsClassLoaderCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsClassLoaderCls, "getNew", "(Ljava/lang/String;)Ljava/net/URLClassLoader;", gearsClassLoaderNewMid);
            JVM_TryFindMethod(jvm_tld->env, gearsClassLoaderCls, "shutDown", "()V", gearsClassLoaderShutDown);
            JVM_TryFindField(jvm_tld->env, gearsClassLoaderCls, "ptr", "J", classLoaderPrtField);

            JVM_TryFindClass(jvm_tld->env, "java/lang/ClassLoader", javaClassLoaderCls);
            JVM_TryFindMethod(jvm_tld->env, javaClassLoaderCls, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;", javaLoadClassNewMid);

            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "serializeObject", "(Ljava/lang/Object;Lgears/GearsObjectOutputStream;Z)[B", gearsBuilderSerializeObjectMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "deserializeObject", "([BLgears/GearsObjectInputStream;Z)Ljava/lang/Object;", gearsBuilderDeserializeObjectMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "onUnpaused", "(Ljava/lang/ClassLoader;)V", gearsBuilderOnUnpausedMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "jniCallHelper", "(J)V", gearsJNICallHelperMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "getStackTrace", "(Ljava/lang/Throwable;)Ljava/lang/String;", gearsGetStackTraceMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "cleanCtxClassLoader", "()V", gearsCleanCtxClassLoaderMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "dumpHeap", "(Ljava/lang/String;Ljava/lang/String;)V", gearsDumpHeapMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "runGC", "()V", gearsRunGCMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "getStats", "()Ljava/lang/Object;", gearsGetStatsMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/MapOperation", gearsMappCls);
            JVM_TryFindMethod(jvm_tld->env, gearsMappCls, "map", "(Ljava/io/Serializable;)Ljava/io/Serializable;", gearsMapMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/FlatMapOperation", gearsFlatMappCls);
            JVM_TryFindMethod(jvm_tld->env, gearsFlatMappCls, "flatmap", "(Ljava/io/Serializable;)Ljava/lang/Iterable;", gearsFlatMapMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/ExtractorOperation", gearsExtractorCls);
            JVM_TryFindMethod(jvm_tld->env, gearsExtractorCls, "extract", "(Ljava/io/Serializable;)Ljava/lang/String;", gearsExtractorMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/ForeachOperation", gearsForeachCls);
            JVM_TryFindMethod(jvm_tld->env, gearsForeachCls, "foreach", "(Ljava/io/Serializable;)V", gearsForeachMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/FilterOperation", gearsFilterCls);
            JVM_TryFindMethod(jvm_tld->env, gearsFilterCls, "filter", "(Ljava/io/Serializable;)Z", gearsFilterMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/AccumulateOperation", gearsAccumulatorCls);
            JVM_TryFindMethod(jvm_tld->env, gearsAccumulatorCls, "accumulate", "(Ljava/io/Serializable;Ljava/io/Serializable;)Ljava/io/Serializable;", gearsAccumulatorMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/AccumulateByOperation", gearsAccumulateByCls);
            JVM_TryFindMethod(jvm_tld->env, gearsAccumulateByCls, "accumulateby", "(Ljava/lang/String;Ljava/io/Serializable;Ljava/io/Serializable;)Ljava/io/Serializable;", gearsAccumulateByMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/OnRegisteredOperation", gearsOnRegisteredCls);
            JVM_TryFindClass(jvm_tld->env, "gears/operations/OnUnregisteredOperation", gearsOnUnregisteredCls);
            JVM_TryFindMethod(jvm_tld->env, gearsOnRegisteredCls, "onRegistered", "(Ljava/lang/String;)V", gearsOnRegisteredMethodId);
            JVM_TryFindMethod(jvm_tld->env, gearsOnUnregisteredCls, "onUnregistered", "()V", gearsOnUnregisteredMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/records/BaseRecord", baseRecordCls);

            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "recordToString", "(Ljava/io/Serializable;)Ljava/lang/String;", recordToStr);

            JVM_TryFindClass(jvm_tld->env, "java/util/HashMap", hashRecordCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/Iterable", iterableCls);
            JVM_TryFindMethod(jvm_tld->env, iterableCls, "iterator", "()Ljava/util/Iterator;", iteratorMethodId);

            JVM_TryFindClass(jvm_tld->env, "java/util/Iterator", iteratorCls);
            JVM_TryFindMethod(jvm_tld->env, iteratorCls, "hasNext", "()Z", iteratorHasNextMethodId);
            JVM_TryFindMethod(jvm_tld->env, iteratorCls, "next", "()Ljava/lang/Object;", iteratorNextMethodId);

//            JVM_TryFindClass(jvm_tld->env, "java/lang/reglect/Array", arrayCls);

            JVM_TryFindMethod(jvm_tld->env, hashRecordCls, "<init>", "()V", hashRecordCtor);

            JVM_TryFindMethod(jvm_tld->env, hashRecordCls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;", hashRecordSet);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/BaseReader", gearsBaseReaderCls);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/KeysReader", gearsKeyReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "pattern", "Ljava/lang/String;", keysReaderPatternField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "noScan", "Z", keysReaderNoscanField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "readValues", "Z", keysReaderReadValuesField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "eventTypes", "[Ljava/lang/String;", keysReaderEventTypesField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "keyTypes", "[Ljava/lang/String;", keysReaderKeyTypesField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "commands", "[Ljava/lang/String;", keysReaderCommandsField);

            JVM_TryFindClass(jvm_tld->env, "gears/records/KeysReaderRecord", gearsKeyReaderRecordCls);
            JVM_TryFindMethod(jvm_tld->env, gearsKeyReaderRecordCls, "<init>", "(Ljava/lang/String;Ljava/lang/String;ZLjava/nio/ByteBuffer;)V", gearsKeyReaderRecordCtrMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/StreamReader", gearsStreamReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "pattern", "Ljava/lang/String;", streamReaderPatternField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "startId", "Ljava/lang/String;", streamReaderStartIdField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "batchSize", "I", streamReaderBatchSizeField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "duration", "I", streamReaderDurationField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "failurePolicy", "Lgears/readers/StreamReader$FailurePolicy;", streamReaderFailurePolicyField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "failureRertyInterval", "I", streamReaderRetryIntervalField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "trimStream", "Z", streamReaderTrimStreamField);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/CommandReader", gearsCommandReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsCommandReaderCls, "trigger", "Ljava/lang/String;", commandReaderTriggerField);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/CommandOverrider", gearsCommandOverriderCls);
            JVM_TryFindField(jvm_tld->env, gearsCommandOverriderCls, "command", "Ljava/lang/String;", commandOverriderCommandField);
            JVM_TryFindField(jvm_tld->env, gearsCommandOverriderCls, "prefix", "Ljava/lang/String;", commandOverriderPrefixField);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/StreamReader$FailurePolicy", gearsStreamReaderFailedPolicyCls);
            jfieldID temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsStreamReaderFailedPolicyCls, "CONTINUE", "Lgears/readers/StreamReader$FailurePolicy;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding FailedPolicy.CONTINUE enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyContinueCls = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsStreamReaderFailedPolicyCls, temp);
            if(!gearsStreamReaderFailedPolicyContinueCls){
                RedisModule_Log(NULL, "warning", "Failed loading FailedPolicy.CONTINUE enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyContinueCls = JVM_TurnToGlobal(jvm_tld->env, gearsStreamReaderFailedPolicyContinueCls);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsStreamReaderFailedPolicyCls, "ABORT", "Lgears/readers/StreamReader$FailurePolicy;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding FailedPolicy.ABORT enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyAbortCls = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsStreamReaderFailedPolicyCls, temp);
            if(!gearsStreamReaderFailedPolicyAbortCls){
                RedisModule_Log(NULL, "warning", "Failed loading FailedPolicy.ABORT enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyAbortCls = JVM_TurnToGlobal(jvm_tld->env, gearsStreamReaderFailedPolicyAbortCls);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsStreamReaderFailedPolicyCls, "RETRY", "Lgears/readers/StreamReader$FailurePolicy;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding FailedPolicy.RETRY enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyRetryCls = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsStreamReaderFailedPolicyCls, temp);
            if(!gearsStreamReaderFailedPolicyRetryCls){
                RedisModule_Log(NULL, "warning", "Failed loading FailedPolicy.RETRY enum");
                return NULL;
            }
            gearsStreamReaderFailedPolicyRetryCls = JVM_TurnToGlobal(jvm_tld->env, gearsStreamReaderFailedPolicyRetryCls);

            JVM_TryFindClass(jvm_tld->env, "gears/ExecutionMode", gearsExecutionModeCls);
            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "ASYNC", "Lgears/ExecutionMode;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding ExecutionPlan.ASYNC enum");
                return NULL;
            }
            gearsExecutionModeAsync = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsExecutionModeCls, temp);
            if(!gearsExecutionModeAsync){
                RedisModule_Log(NULL, "warning", "Failed loading ExecutionPlan.ASYNC enum");
                return NULL;
            }
            gearsExecutionModeAsync = JVM_TurnToGlobal(jvm_tld->env, gearsExecutionModeAsync);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "SYNC", "Lgears/ExecutionMode;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding ExecutionPlan.SYNC enum");
                return NULL;
            }
            gearsExecutionModeSync = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsExecutionModeCls, temp);
            if(!gearsExecutionModeSync){
                RedisModule_Log(NULL, "warning", "Failed loading ExecutionPlan.SYNC enum");
                return NULL;
            }
            gearsExecutionModeSync = JVM_TurnToGlobal(jvm_tld->env, gearsExecutionModeSync);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "ASYNC_LOCAL", "Lgears/ExecutionMode;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding ExecutionPlan.ASYNC_LOCAL enum");
                return NULL;
            }
            gearsExecutionModeAsyncLocal = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsExecutionModeCls, temp);
            if(!gearsExecutionModeAsyncLocal){
                RedisModule_Log(NULL, "warning", "Failed loading ExecutionPlan.ASYNC_LOCAL enum");
                return NULL;
            }
            gearsExecutionModeAsyncLocal = JVM_TurnToGlobal(jvm_tld->env, gearsExecutionModeAsyncLocal);

            JVM_TryFindClass(jvm_tld->env, "gears/LogLevel", gearsLogLevelCls);
            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsLogLevelCls, "NOTICE", "Lgears/LogLevel;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding LogLevel.NOTICE enum");
                return NULL;
            }
            gearsLogLevelNotice = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsLogLevelCls, temp);
            if(!gearsLogLevelNotice){
                RedisModule_Log(NULL, "warning", "Failed loading LogLevel.NOTICE enum");
                return NULL;
            }
            gearsLogLevelNotice = JVM_TurnToGlobal(jvm_tld->env, gearsLogLevelNotice);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsLogLevelCls, "DEBUG", "Lgears/LogLevel;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding LogLevel.DEBUG enum");
                return NULL;
            }
            gearsLogLevelDebug= (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsLogLevelCls, temp);
            if(!gearsLogLevelDebug){
                RedisModule_Log(NULL, "warning", "Failed loading LogLevel.DEBUG enum");
                return NULL;
            }
            gearsLogLevelDebug = JVM_TurnToGlobal(jvm_tld->env, gearsLogLevelDebug);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsLogLevelCls, "VERBOSE", "Lgears/LogLevel;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding LogLevel.VERBOSE enum");
                return NULL;
            }
            gearsLogLevelVerbose = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsLogLevelCls, temp);
            if(!gearsLogLevelVerbose){
                RedisModule_Log(NULL, "warning", "Failed loading LogLevel.VERBOSE enum");
                return NULL;
            }
            gearsLogLevelVerbose = JVM_TurnToGlobal(jvm_tld->env, gearsLogLevelVerbose);

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsLogLevelCls, "WARNING", "Lgears/LogLevel;");
            if(!temp){
                RedisModule_Log(NULL, "warning", "Failed finding LogLevel.WARNING enum");
                return NULL;
            }
            gearsLogLevelWarning = (*jvm_tld->env)->GetStaticObjectField(jvm_tld->env, gearsLogLevelCls, temp);
            if(!gearsLogLevelWarning){
                RedisModule_Log(NULL, "warning", "Failed loading LogLevel.WARNING enum");
                return NULL;
            }
            gearsLogLevelWarning = JVM_TurnToGlobal(jvm_tld->env, gearsLogLevelWarning);

            JVM_TryFindClass(jvm_tld->env, "java/lang/Exception", exceptionCls);

        }else{
            JavaVMAttachArgs args;
            args.version = JNI_VERSION_10; // choose your JNI version
            args.name = NULL; // you might want to give the java thread a name
            args.group = NULL; // you might want to assign the java thread to a ThreadGroup

            (*jvm)->AttachCurrentThread(jvm, (void**)&jvm_tld->env, &args);
        }

        pthread_setspecific(threadLocalData, jvm_tld);
    }
    if(jectx){
        JVMRunSession* oldSession = jvm_tld->currSession;
        ExecutionCtx* oldECtx = jvm_tld->eCtx;
        jobject oldCreateFuture = jvm_tld->createFuture;
        ASYNC_RECORD_TYPE oldAsyncRecorType = jvm_tld->asyncRecorType;

        jvm_tld->currSession = jectx->session;
        jvm_tld->eCtx = jectx->eCtx;
        jvm_tld->rctx = NULL;
        jvm_tld->isBlocked = false;
        jvm_tld->allowBlock = false;
        jvm_tld->createFuture = NULL;
        jvm_tld->asyncRecorType = 0;

        jectx->session = oldSession;
        jectx->eCtx = oldECtx;
        jectx->createFuture = oldCreateFuture;
        jectx->asyncRecorType = oldAsyncRecorType;
    }
    return jvm_tld;
}

static void JVM_PushFrame(JNIEnv *env){
    jint frame = (*env)->PushLocalFrame(env, 10);
    RedisModule_Assert(frame == 0);
}

static void JVM_PopFrame(JNIEnv *env){
    jobject localFrame = (*env)->PopLocalFrame(env, NULL);
    (*env)->DeleteLocalRef(env, localFrame);
}

static char* JVM_GetException(JNIEnv *env){
    jthrowable e = (*env)->ExceptionOccurred(env);
    if(!e){
        return NULL;
    }
//    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

    jstring message = (jstring)(*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsGetStackTraceMethodId, e);
    jthrowable e1 = (*env)->ExceptionOccurred(env);
    char* err = NULL;
    if(!e1){
        RedisModule_Assert(message);
        const char *mstr = (*env)->GetStringUTFChars(env, message, NULL);
        err = JVM_STRDUP(mstr);
        (*env)->ReleaseStringUTFChars(env, message, mstr);
        (*env)->DeleteLocalRef(env, message);
    }else{
        err = JVM_STRDUP("Could not extract excpetion data");
        (*env)->DeleteLocalRef(env, e1);
    }
    (*env)->DeleteLocalRef(env, e);
    for(size_t i = 0 ; i < strlen(err) ; ++i){
        if(err[i] == '\r' || err[i] == '\n'){
            err[i] = '|';
        }
    }
    RedisModule_Log(NULL, "verbose", "Error : %s", err);
    return err;
}

static jobject JVM_TurnToGlobal(JNIEnv *env, jobject local){
    jobject global = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    return global;
}

typedef struct JVM_ThreadPoolJob{
    void (*callback)(void*);
    void* arg;
}JVM_ThreadPoolJob;

typedef struct JVM_ThreadPool{
    pthread_cond_t cond;
    pthread_mutex_t lock;
    JVM_list* jobs;
}JVM_ThreadPool;

ExecutionThreadPool* jvmExecutionPool = NULL;

static jstring JVM_GetSessionUpgradeData(JNIEnv *env, jobject objectOrClass, jstring sessionId){
    if(!sessionId){
        (*env)->ThrowNew(env, exceptionCls, "NULL session id given");
        return NULL;
    }

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(ctx);

    const char* sessionIdStr = (*env)->GetStringUTFChars(env, sessionId, NULL);
    JVMRunSession* s = JVM_SessionGetFromStrId(sessionIdStr);
    (*env)->ReleaseStringUTFChars(env, sessionId, sessionIdStr);

    if(!s){
        goto error;
    }

    jstring clsNameJString = (*env)->NewStringUTF(env, s->mainClassName);

    jclass cls = (*env)->CallObjectMethod(env, s->sessionClsLoader, javaLoadClassNewMid, clsNameJString);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        goto error;
    }

    jmethodID upgradeMethodID = (*env)->GetStaticMethodID(env, cls, "getUpgradeData", "()Ljava/lang/String;");

    if((err = JVM_GetException(env))){
        goto error;
    }

    jstring result = (*env)->CallStaticObjectMethod(env, cls, upgradeMethodID);

    if((err = JVM_GetException(env))){
        goto error;
    }

    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    return result;

error:
    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
    (*env)->ThrowNew(env, exceptionCls, err);
    return NULL;
}

static void JVM_ClassLoaderFinalized(JNIEnv *env, jobject objectOrClass, jlong ctx){
    JVMRunSession* s = (JVMRunSession*)ctx;
    JVM_SessionFreeMemory(s);
}

static void JVM_ThreadPoolWorkerHelper(JNIEnv *env, jobject objectOrClass, jlong ctx){
    // here we are inside the jvm, we never get back.
    JVM_ThreadPool* pool = (void*)ctx;
    while(true){
        pthread_mutex_lock(&pool->lock);
        while(JVM_listLength(pool->jobs) == 0){
            pthread_cond_wait(&pool->cond, &pool->lock);
        }
        JVM_listNode* n = JVM_listFirst(pool->jobs);
        JVM_ThreadPoolJob* job = JVM_listNodeValue(n);
        JVM_listDelNode(pool->jobs, n);
        pthread_mutex_unlock(&pool->lock);
        job->callback(job->arg);
        char* err = NULL;
        if((err = JVM_GetException(env))){
            RedisModule_Log(NULL, "warning", "Excpetion raised but not catched, exception='%s'", err);
        }
        JVM_FREE(job);

        // clean the thread ctx class loader just in case
        (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsCleanCtxClassLoaderMethodId);
        if((err = JVM_GetException(env))){
            RedisModule_Log(NULL, "warning", "Failed cleaning thread ctx class loader, error='%s'", err);
        }
    }
}

static void* JVM_ThreadPoolWorker(void* poolCtx){
    // register the gears lock hanlder so gears can enforce
    // api usage that can only be used when redis GIL is taken.
    RedisGears_LockHanlderRegister();

    // we do not have session here and we just need the jvm env arg
    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsJNICallHelperMethodId, (jlong)poolCtx);

//    JVM_ThreadPool* pool = (void*)poolCtx;
//    while(true){
//        pthread_mutex_lock(&pool->lock);
//        while(JVM_listLength(pool->jobs) == 0){
//            pthread_cond_wait(&pool->cond, &pool->lock);
//        }
//        JVM_listNode* n = JVM_listFirst(pool->jobs);
//        JVM_ThreadPoolJob* job = JVM_listNodeValue(n);
//        JVM_listDelNode(pool->jobs, n);
//        pthread_mutex_unlock(&pool->lock);
//        job->callback(job->arg);
//        char* err = NULL;
//        if((err = JVM_GetException(env))){
//            RedisModule_Log(NULL, "warning", "Excpetion raised but not catched, exception='%s'", err);
//        }
//    }

    RedisModule_Assert(false); // this one never returns
    return NULL;
}

static JVM_ThreadPool* JVM_ThreadPoolCreate(size_t numOfThreads){
    JVM_ThreadPool* ret = JVM_ALLOC(sizeof(*ret));
    pthread_cond_init(&ret->cond, NULL);
    pthread_mutex_init(&ret->lock, NULL);
    ret->jobs = JVM_listCreate();
    for(size_t i = 0 ; i < numOfThreads ; ++i){
        pthread_t messagesThread;
        pthread_create(&messagesThread, NULL, JVM_ThreadPoolWorker, ret);
        pthread_detach(messagesThread);
    }
    return ret;
}

static void JVM_ThreadPoolAddJob(void* poolCtx, void (*callback)(void*), void* arg){
    JVM_ThreadPool* pool = poolCtx;
    JVM_ThreadPoolJob* job = JVM_ALLOC(sizeof(*job));
    job->callback = callback;
    job->arg = arg;
    pthread_mutex_lock(&pool->lock);
    JVM_listAddNodeTail(pool->jobs, job);
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
}

static void JVM_ARSetError(JNIEnv *env, jobject objectOrClass, jstring error){
    if(!error){
        (*env)->ThrowNew(env, exceptionCls, "Can not set NULL error on async record");
        return;
    }

    Record* asyncRecord = (Record*)(*env)->GetLongField(env, objectOrClass, futureRecordPtrFieldId);
    if(!asyncRecord){
        (*env)->ThrowNew(env, exceptionCls, "NULL async record was given");
        return;
    }

    const char* errorCStr = (*env)->GetStringUTFChars(env, error, NULL);

    Record* errorRecord = RedisGears_ErrorRecordCreate(JVM_STRDUP(errorCStr), strlen(errorCStr));

    (*env)->ReleaseStringUTFChars(env, error, errorCStr);

    RedisGears_AsyncRecordContinue(asyncRecord, errorRecord);

    (*env)->SetLongField(env, objectOrClass, futureRecordPtrFieldId, 0);
    (*env)->SetIntField(env, objectOrClass, futureRecordTypeFieldId, 0);
}

static void JVM_ARSetResult(JNIEnv *env, jobject objectOrClass, jobject res){
    Record* asyncRecord = (Record*)(*env)->GetLongField(env, objectOrClass, futureRecordPtrFieldId);
    if(!asyncRecord){
        (*env)->ThrowNew(env, exceptionCls, "NULL async record was given");
        return;
    }

    Record* resRecord = NULL;

    ASYNC_RECORD_TYPE asyncRecordType = (*env)->GetIntField(env, objectOrClass, futureRecordTypeFieldId);

    if(!res && asyncRecordType != ASYNC_RECORD_TYPE_FILTER){
        (*env)->ThrowNew(env, exceptionCls, "Can not set NULL object on async record result");
        return;
    }

    switch(asyncRecordType){
    case ASYNC_RECORD_TYPE_DEFAULT:
        res = JVM_TurnToGlobal(env, res);

        resRecord = RedisGears_RecordCreate(JVMRecordType);
        ((JVMRecord*)resRecord)->obj = res;
        break;
    case ASYNC_RECORD_TYPE_FILTER:
        if((*env)->IsInstanceOf(env, res, gearsBooleanCls)){
            jboolean result = (*env)->CallBooleanMethod(env, res, gearsGetBooleanValueMethodId);
            char* err;
            if((err = JVM_GetException(env))){
                RedisModule_Log(NULL, "warning", "Exception raised when calling booleanValue function, err='%s'", err);
                JVM_FREE(err);
            }else if(result){
                resRecord = RedisGears_GetDummyRecord(); // everithing other then NULL will be true;
            }
        }
        break;
    case ASYNC_RECORD_TYPE_FOREACH:
        resRecord = RedisGears_GetDummyRecord(); // continue with the old record
        break;
    case ASYNC_RECORD_TYPE_FLATMAP:
        break;
    default:
        RedisModule_Assert(false);
    }

    RedisGears_AsyncRecordContinue(asyncRecord, resRecord);

    (*env)->SetLongField(env, objectOrClass, futureRecordPtrFieldId, 0);
    (*env)->SetIntField(env, objectOrClass, futureRecordTypeFieldId, 0);
}

static void JVM_ARFree(JNIEnv *env, jobject objectOrClass){
    Record* asyncRecord = (Record*)(*env)->GetLongField(env, objectOrClass, futureRecordPtrFieldId);
    if(!asyncRecord){
        (*env)->ThrowNew(env, exceptionCls, "NULL async record was given");
        return;
    }

    RedisGears_FreeRecord(asyncRecord);
}

static void JVM_ARCreate(JNIEnv *env, jobject objectOrClass){
    JVM_ThreadLocalData* tld = JVM_GetThreadLocalData(NULL);

    if(!tld->eCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can only create async record inside execution step");
        return;
    }

    if(tld->createFuture){
        (*env)->ThrowNew(env, exceptionCls, "Can not create async record twice on the same step");
        return;
    }

    char* err = NULL;
    Record* asyncRecord = RedisGears_AsyncRecordCreate(tld->eCtx, &err);

    if(!asyncRecord){
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return;
    }

    (*env)->SetLongField(env, objectOrClass, futureRecordPtrFieldId, (jlong)asyncRecord);
    (*env)->SetIntField(env, objectOrClass, futureRecordTypeFieldId, tld->asyncRecorType);

    tld->createFuture = JVM_TurnToGlobal(env, objectOrClass);
}

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader, jstring desc){
    if(!strReader){
        (*env)->ThrowNew(env, exceptionCls, "Null reader given");
        return;
    }
    const char* reader = (*env)->GetStringUTFChars(env, strReader, NULL);
    char* err = NULL;
    FlatExecutionPlan* fep = RedisGears_CreateCtx((char*)reader, &err);
    (*env)->ReleaseStringUTFChars(env, strReader, reader);
    if(!fep){
        if(!err){
            err = JVM_STRDUP("Failed create Gears Builder");
        }
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return;
    }
    RGM_SetFlatExecutionOnStartCallback(fep, JVM_OnStart, NULL);
    RGM_SetFlatExecutionOnUnpausedCallback(fep, JVM_OnUnpaused, NULL);
    RedisGears_SetExecutionThreadPool(fep, jvmExecutionPool);

    if(desc){
        const char* descStr = (*env)->GetStringUTFChars(env, desc, NULL);
        RedisGears_SetDesc(fep, descStr);
        (*env)->ReleaseStringUTFChars(env, desc, descStr);
    }


    JVM_ThreadLocalData* tld = JVM_GetThreadLocalData(NULL);

    JVMFlatExecutionSession* fepSession = JVM_FepSessionCreate(tld->env, JVM_SessionDup(tld->currSession), &err);
    if(!fepSession){
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        RedisGears_FreeFlatExecution(fep);
        return;
    }

    RedisGears_SetFlatExecutionPrivateData(fep, JVM_SESSION_TYPE_NAME, fepSession);

    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);

    (*env)->SetLongField(env, objectOrClass, ptrFieldId, (jlong)fep);
}

static void JVM_GBDestroy(JNIEnv *env, jobject objectOrClass){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    RedisGears_FreeFlatExecution(fep);
}

static jobject JVM_GBFilter(JNIEnv *env, jobject objectOrClass, jobject filter){
    if(!filter){
        (*env)->ThrowNew(env, exceptionCls, "Null filter function given");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    filter = JVM_TurnToGlobal(env, filter);
    RGM_Filter(fep, JVM_Filter, filter);
    return objectOrClass;
}

static jobject JVM_GBCollect(JNIEnv *env, jobject objectOrClass){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    RGM_Collect(fep);
    return objectOrClass;
}

static jobject JVM_GBForeach(JNIEnv *env, jobject objectOrClass, jobject foreach){
    if(!foreach){
        (*env)->ThrowNew(env, exceptionCls, "Null foreach function given");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    foreach = JVM_TurnToGlobal(env, foreach);
    RGM_ForEach(fep, JVM_Foreach, foreach);
    return objectOrClass;
}

static jobject JVM_GBAccumulate(JNIEnv *env, jobject objectOrClass, jobject accumulator){
    if(!accumulator){
        (*env)->ThrowNew(env, exceptionCls, "Null accumulator given");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_Accumulate(fep, JVM_Accumulate, accumulator);
    return objectOrClass;
}

static jobject JVM_GBLocalAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator){
    if(!extractor){
        (*env)->ThrowNew(env, exceptionCls, "Null extractor given");
        return NULL;
    }
    if(!accumulator){
        (*env)->ThrowNew(env, exceptionCls, "Null accumulator given");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_LocalAccumulateBy(fep, JVM_Extractor, extractor, JVM_AccumulateByKey, accumulator);
    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);
    return objectOrClass;
}

static jobject JVM_GBRepartition(JNIEnv *env, jobject objectOrClass, jobject extractor){
    if(!extractor){
        (*env)->ThrowNew(env, exceptionCls, "Null extractor given");
        return NULL;
    }
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    RGM_Repartition(fep, JVM_Extractor, extractor);
    return objectOrClass;
}

static jobject JVM_GBAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator){
    if(!extractor){
        (*env)->ThrowNew(env, exceptionCls, "Null extractor given");
        return NULL;
    }
    if(!accumulator){
        (*env)->ThrowNew(env, exceptionCls, "Null accumulator given");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_AccumulateBy(fep, JVM_Extractor, extractor, JVM_AccumulateByKey, accumulator);
    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);
    return objectOrClass;
}

static jobject JVM_GBFlatMap(JNIEnv *env, jobject objectOrClass, jobject mapper){
    if(!mapper){
        (*env)->ThrowNew(env, exceptionCls, "Null mapper given");
        return NULL;
    }
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    mapper = JVM_TurnToGlobal(env, mapper);
    RGM_FlatMap(fep, JVM_FlatMapper, mapper);
    return objectOrClass;
}

static jobject JVM_GBMap(JNIEnv *env, jobject objectOrClass, jobject mapper){
    if(!mapper){
        (*env)->ThrowNew(env, exceptionCls, "Null mapper given");
        return NULL;
    }
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    mapper = JVM_TurnToGlobal(env, mapper);
    RGM_Map(fep, JVM_Mapper, mapper);
    return objectOrClass;
}

static void JVM_OnExecutionDoneCallback(ExecutionPlan* ctx, void* privateData){
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    RedisGears_ReturnResultsAndErrors(ctx, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ctx);
    RedisModule_FreeThreadSafeContext(rctx);
}

void* JVM_CreateRunStreamReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if(!(*env)->IsSameObject(env, readerCls, gearsStreamReaderCls)){
        (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
        return NULL;
    }

    jobject pattern = (*env)->GetObjectField(env, reader, streamReaderPatternField);
    if(!pattern){
        (*env)->ThrowNew(env, exceptionCls, "Stream reader pattern argument can not be NULL");
        return NULL;
    }

    jobject startId = (*env)->GetObjectField(env, reader, streamReaderStartIdField);
    if(!startId){
        (*env)->ThrowNew(env, exceptionCls, "Stream reader startId argument can not be NULL");
        return NULL;
    }

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, NULL);
    const char* startIdStr = (*env)->GetStringUTFChars(env, startId, NULL);

    StreamReaderCtx* readerCtx = RedisGears_StreamReaderCtxCreate(patternStr, startIdStr);

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);
    (*env)->ReleaseStringUTFChars(env, startId, startIdStr);

    return readerCtx;
}

#define LRU_BITS 24
typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRU_BITS; /* LRU time (relative to global lru_clock) or
                            * LFU data (least significant 8 bits frequency
                            * and most significant 16 bits access time). */
    int refcount;
    void *ptr;
} robj;

static void JVM_ScanKeyCallback(RedisModuleKey *key, RedisModuleString *field, RedisModuleString *value, void *privdata){
    Gears_BufferWriter* bw = privdata;
    size_t fieldCStrLen;
    const char* fieldCStr = RedisModule_StringPtrLen(field, &fieldCStrLen);
    size_t valCStrLen;
    const char* valCStr = RedisModule_StringPtrLen(value, &valCStrLen);
    RedisGears_BWWriteBuffer(bw, fieldCStr, fieldCStrLen);
    RedisGears_BWWriteBuffer(bw, valCStr, valCStrLen);
}

static jobject JVM_GetSerializedVal(RedisModuleCtx* rctx, JNIEnv *env, RedisModuleKey* keyPtr, RedisModuleString* key, Gears_Buffer* buff){
    Gears_BufferWriter bw;
    RedisGears_BufferWriterInit(&bw, buff);
    if(keyPtr == NULL){
        RedisGears_BWWriteLong(&bw, -1);
    }else{
        int keyType = RedisModule_KeyType(keyPtr);
        RedisGears_BWWriteLong(&bw, keyType);
        if(keyType == REDISMODULE_KEYTYPE_HASH){
            if(!RedisGears_IsCrdt()){
                RedisModuleScanCursor* hashCursor = RedisModule_ScanCursorCreate();
                while(RedisModule_ScanKey(keyPtr, hashCursor, JVM_ScanKeyCallback, &bw));
                RedisModule_ScanCursorDestroy(hashCursor);
            }else{
                // fall back to RM_Call
                RedisModuleCallReply *reply = RedisModule_Call(rctx, "HGETALL", "s", key);
                RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
                size_t len = RedisModule_CallReplyLength(reply);
                RedisModule_Assert(len % 2 == 0);
                for(int i = 0 ; i < len ; i+=2){
                    RedisModuleCallReply *keyReply = RedisModule_CallReplyArrayElement(reply, i);
                    RedisModuleCallReply *valReply = RedisModule_CallReplyArrayElement(reply, i + 1);
                    size_t keyStrLen;
                    const char* keyStr = RedisModule_CallReplyStringPtr(keyReply, &keyStrLen);
                    size_t valStrLen;
                    const char* valStr = RedisModule_CallReplyStringPtr(valReply, &valStrLen);
                    RedisGears_BWWriteBuffer(&bw, keyStr, keyStrLen);
                    RedisGears_BWWriteBuffer(&bw, valStr, valStrLen);
                }
                RedisModule_FreeCallReply(reply);
            }
        }
        if(keyType == REDISMODULE_KEYTYPE_STRING){
            size_t len;
            const char* val;
            RedisModuleCallReply *r = NULL;
            if(!RedisGears_IsCrdt()){
                val = RedisModule_StringDMA(keyPtr, &len, REDISMODULE_READ);
            }else{
                // fall back to RM_Call
                RedisModuleCallReply *r = RedisModule_Call(rctx, "GET", "s", key);
                val = (char*)RedisModule_CallReplyStringPtr(r, &len);
            }
            RedisGears_BWWriteBuffer(&bw, val, len);
            if(r){
                RedisModule_FreeCallReply(r);
            }
        }
    }
    size_t len;
    const char* data = RedisGears_BufferGet(buff, &len);
    return (*env)->NewDirectByteBuffer(env, (void*)data, len);
}

static Gears_Buffer* recordBuff = NULL;

static Record* JVM_KeyReaderReadRecord(RedisModuleCtx* rctx, RedisModuleString* key, RedisModuleKey* keyPtr, bool readValue, const char* event){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env  = jvm_tld->env;

    JVM_PushFrame(env);

    const char* keyCStr = RedisModule_StringPtrLen(key, NULL);
    jstring jkey = (*env)->NewStringUTF(env, keyCStr);
    jstring jevent = (*env)->NewStringUTF(env, event);


    RedisGears_BufferClear(recordBuff);
    jobject serializedValue = NULL;
    if(readValue){
        RedisModuleKey* tmpPtr = keyPtr;
        if(!tmpPtr){
            // we do not want key missed to jump here accidently
            int oldAvoidEvents = RedisGears_KeysReaderSetAvoidEvents(1);
            tmpPtr = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
            RedisGears_KeysReaderSetAvoidEvents(oldAvoidEvents);
        }
        serializedValue = JVM_GetSerializedVal(rctx, env, tmpPtr, key, recordBuff);
        if(!keyPtr){
            RedisModule_CloseKey(tmpPtr);
        }
    }
    jobject obj = (*env)->NewObject(env, gearsKeyReaderRecordCls, gearsKeyReaderRecordCtrMethodId, jkey, jevent, readValue, serializedValue);

    char* err;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured when reading key, error='%s'", err);
        JVM_FREE(err);
        JVM_PopFrame(env);
        return NULL;
    }

    obj = JVM_TurnToGlobal(env, obj);

    JVM_PopFrame(env);

    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;
    return &r->baseRecord;
}

static void* JVM_CreateRunKeyReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if(!(*env)->IsSameObject(env, readerCls, gearsKeyReaderCls)){
        (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
        return NULL;
    }

    jobject pattern = (*env)->GetObjectField(env, reader, keysReaderPatternField);
    if(!pattern){
        (*env)->ThrowNew(env, exceptionCls, "Keys reader pattern argument can not be NULL");
        return NULL;
    }

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, NULL);

    jboolean readValues = (*env)->GetBooleanField(env, reader, keysReaderReadValuesField);
    jboolean noScan = (*env)->GetBooleanField(env, reader, keysReaderNoscanField);

    KeysReaderCtx* readerCtx = RedisGears_KeysReaderCtxCreate(patternStr, readValues, NULL, noScan);

    RGM_KeysReaderSetReadRecordCallback(readerCtx, JVM_KeyReaderReadRecord);

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);

    return readerCtx;
}

static void* JVM_CreateRunReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    if(strcmp(RedisGears_GetReader(fep), "KeysReader") == 0){
        return JVM_CreateRunKeyReaderArgs(env, fep, reader);
    }else if(strcmp(RedisGears_GetReader(fep), "StreamReader") == 0){
        return JVM_CreateRunStreamReaderArgs(env, fep, reader);
    }else if(strcmp(RedisGears_GetReader(fep), "JavaReader") == 0){
        return JVM_TurnToGlobal(env, reader);
    }
    (*env)->ThrowNew(env, exceptionCls, "Given reader does not exists or does not support run");
    return NULL;
}

static int JVM_RegisterStrKeyTypeToInt(const char* keyType){
    if(strcmp(keyType, "string") == 0){
        return REDISMODULE_KEYTYPE_STRING;
    }
    if(strcmp(keyType, "list") == 0){
        return REDISMODULE_KEYTYPE_LIST;
    }
    if(strcmp(keyType, "hash") == 0){
        return REDISMODULE_KEYTYPE_HASH;
    }
    if(strcmp(keyType, "set") == 0){
        return REDISMODULE_KEYTYPE_SET;
    }
    if(strcmp(keyType, "zset") == 0){
        return REDISMODULE_KEYTYPE_ZSET;
    }
    if(strcmp(keyType, "module") == 0){
        return REDISMODULE_KEYTYPE_MODULE;
    }
    return -1;
}

void* JVM_CreateRegisterCommandReaderOverrideArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){

    jobject command = (*env)->GetObjectField(env, reader, commandOverriderCommandField);

    if(!command){
        (*env)->ThrowNew(env, exceptionCls, "command overrider command fields must be set");
        return NULL;
    }

    jobject prefix = (*env)->GetObjectField(env, reader, commandOverriderPrefixField);

    const char* commandStr = (*env)->GetStringUTFChars(env, command, NULL);
    const char* prefixStr = NULL;
    if(prefix){
        prefixStr = (*env)->GetStringUTFChars(env, prefix, NULL);
    }

    CommandReaderTriggerArgs* triggerArgs = RedisGears_CommandReaderTriggerArgsCreateHook(commandStr, prefixStr, false);

    (*env)->ReleaseStringUTFChars(env, command, commandStr);

    if(prefixStr){
        (*env)->ReleaseStringUTFChars(env, prefix, prefixStr);
    }

    return triggerArgs;
}

void* JVM_CreateRegisterCommandReaderTriggerArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){

    jobject trigger = (*env)->GetObjectField(env, reader, commandReaderTriggerField);

    if(!trigger){
        (*env)->ThrowNew(env, exceptionCls, "command reader trigger must be set");
        return NULL;
    }

    const char* triggerStr = (*env)->GetStringUTFChars(env, trigger, NULL);

    CommandReaderTriggerArgs* triggerArgs = RedisGears_CommandReaderTriggerArgsCreate(triggerStr, false);

    (*env)->ReleaseStringUTFChars(env, trigger, triggerStr);

    return triggerArgs;
}

void* JVM_CreateRegisterCommandReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if((*env)->IsSameObject(env, readerCls, gearsCommandReaderCls)){
        return JVM_CreateRegisterCommandReaderTriggerArgs(env, fep, reader);
    }

    if((*env)->IsSameObject(env, readerCls, gearsCommandOverriderCls)){
        return JVM_CreateRegisterCommandReaderOverrideArgs(env, fep, reader);
    }

    (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
    return NULL;
}

void* JVM_CreateRegisterStreamReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if(!(*env)->IsSameObject(env, readerCls, gearsStreamReaderCls)){
        (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
        return NULL;
    }

    jobject pattern = (*env)->GetObjectField(env, reader, streamReaderPatternField);

    if(!pattern){
        (*env)->ThrowNew(env, exceptionCls, "stream reader pattern argument can not be NULL");
        return NULL;
    }

    jint batchSize = (*env)->GetIntField(env, reader, streamReaderBatchSizeField);
    jint duration = (*env)->GetIntField(env, reader, streamReaderDurationField);
    jint retryInterval = (*env)->GetIntField(env, reader, streamReaderRetryIntervalField);
    jboolean trimStream = (*env)->GetBooleanField(env, reader, streamReaderTrimStreamField);

    jobject jfailurePolicy = (*env)->GetObjectField(env, reader, streamReaderFailurePolicyField);

    OnFailedPolicy failurePolicy = OnFailedPolicyContinue;
    if((*env)->IsSameObject(env, jfailurePolicy, gearsStreamReaderFailedPolicyContinueCls)){
        failurePolicy = OnFailedPolicyContinue;
    }else if((*env)->IsSameObject(env, jfailurePolicy, gearsStreamReaderFailedPolicyAbortCls)){
        failurePolicy = OnFailedPolicyAbort;
    }else if((*env)->IsSameObject(env, jfailurePolicy, gearsStreamReaderFailedPolicyRetryCls)){
        failurePolicy = OnFailedPolicyRetry;
    }else{
        RedisModule_Assert(false);
    }

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, NULL);


    StreamReaderTriggerArgs* triggerArgsCtx =
            RedisGears_StreamReaderTriggerArgsCreate(patternStr,
                                                     batchSize,
                                                     duration,
                                                     failurePolicy,
                                                     retryInterval,
                                                     trimStream);

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);

    return triggerArgsCtx;
}

void* JVM_CreateRegisterKeysReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if(!(*env)->IsSameObject(env, readerCls, gearsKeyReaderCls)){
        (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
        return NULL;
    }

    jobject pattern = (*env)->GetObjectField(env, reader, keysReaderPatternField);

    if(!pattern){
        (*env)->ThrowNew(env, exceptionCls, "Keys reader pattern argument can not be NULL");
        return NULL;
    }

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, NULL);

    jboolean readValues = (*env)->GetBooleanField(env, reader, keysReaderReadValuesField);

    jobject jkeyTypes = (*env)->GetObjectField(env, reader, keysReaderKeyTypesField);

    int* keyTypes = NULL;
    if(jkeyTypes){
        jsize jkeyTypesLen = (*env)->GetArrayLength(env, jkeyTypes);
        if(jkeyTypesLen > 0){
            keyTypes = array_new(int, jkeyTypesLen);
            for(size_t i = 0 ; i < jkeyTypesLen ; ++i){
                jobject jkey = (*env)->GetObjectArrayElement(env, jkeyTypes, i);
                const char* jkeyStr = (*env)->GetStringUTFChars(env, jkey, NULL);
                int jkeyInt = JVM_RegisterStrKeyTypeToInt(jkeyStr);
                (*env)->ReleaseStringUTFChars(env, jkey, jkeyStr);
                if(jkeyInt == -1){
                    array_free(keyTypes);
                    (*env)->ThrowNew(env, exceptionCls, "No such key type exists");
                    return NULL;
                }
                keyTypes = array_append(keyTypes, jkeyInt);
            }
        }
    }

    jobject jeventTypes = (*env)->GetObjectField(env, reader, keysReaderEventTypesField);

    char** eventTypes = NULL;
    if(jeventTypes){
        jsize jeventTypesLen = (*env)->GetArrayLength(env, jeventTypes);
        if(jeventTypesLen > 0){
            eventTypes = array_new(char*, jeventTypesLen);
            for(size_t i = 0 ; i < jeventTypesLen ; ++i){
                jobject jevent = (*env)->GetObjectArrayElement(env, jeventTypes, i);
                const char* jeventStr = (*env)->GetStringUTFChars(env, jevent, NULL);
                eventTypes = array_append(eventTypes, JVM_STRDUP(jeventStr));
                (*env)->ReleaseStringUTFChars(env, jevent, jeventStr);
            }
        }
    }

    jobject jcommands = (*env)->GetObjectField(env, reader, keysReaderCommandsField);
    char** commands = NULL;
    if(jcommands){
        jsize jcommandsLen = (*env)->GetArrayLength(env, jcommands);
        if(jcommandsLen > 0){
            commands = array_new(char*, jcommandsLen);
            for(size_t i = 0 ; i < jcommandsLen ; ++i){
                jobject jcommand = (*env)->GetObjectArrayElement(env, jcommands, i);
                const char* jcommandStr = (*env)->GetStringUTFChars(env, jcommand, NULL);
                commands = array_append(commands, JVM_STRDUP(jcommandStr));
                (*env)->ReleaseStringUTFChars(env, jcommand, jcommandStr);
            }
        }
    }

    KeysReaderTriggerArgs* triggerArgsCtx = RedisGears_KeysReaderTriggerArgsCreate(patternStr, eventTypes, keyTypes, readValues);

    if(commands){
        RedisGears_KeysReaderTriggerArgsSetHookCommands(triggerArgsCtx, commands);
    }

    RGM_KeysReaderTriggerArgsSetReadRecordCallback(triggerArgsCtx, JVM_KeyReaderReadRecord);

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);

    return triggerArgsCtx;
}

void* JVM_CreateRegisterReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    if(strcmp(RedisGears_GetReader(fep), "KeysReader") == 0){
        return JVM_CreateRegisterKeysReaderArgs(env, fep, reader);
    }else if(strcmp(RedisGears_GetReader(fep), "StreamReader") == 0){
        return JVM_CreateRegisterStreamReaderArgs(env, fep, reader);
    }else if(strcmp(RedisGears_GetReader(fep), "CommandReader") == 0){
        return JVM_CreateRegisterCommandReaderArgs(env, fep, reader);
    }
    (*env)->ThrowNew(env, exceptionCls, "Given reader does not exists or does not support register");
    return NULL;
}

static void JVM_GBRun(JNIEnv *env, jobject objectOrClass, jobject reader){
    if(!reader){
        (*env)->ThrowNew(env, exceptionCls, "Null reader give to run function");
        return;
    }
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(NULL);
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    char* err = NULL;
    void* krCtx = NULL;
    // ShardsIDReader needs no arguments
    if(strcmp(RedisGears_GetReader(fep), "ShardIDReader") != 0){
        krCtx = JVM_CreateRunReaderArgs(env, fep, reader);
        if(!krCtx){
            return;
        }
    }
    ExecutionPlan* ep = RedisGears_Run(fep, ExecutionModeAsync, krCtx, NULL, NULL, NULL, &err);
    if(!ep){
        if(!err){
            err = JVM_STRDUP("Error occured when tried to create execution");
        }
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);

        if(strcmp(RedisGears_GetReader(fep), "StreamReader") == 0){
            RedisGears_StreamReaderCtxFree(krCtx);
        }else if(strcmp(RedisGears_GetReader(fep), "KeysReader") == 0){
            RedisGears_KeysReaderCtxFree(krCtx);
        }else if(strcmp(RedisGears_GetReader(fep), "ShardIDReader") == 0){
            // nothing to free on ShardIDReader
        }else if(strcmp(RedisGears_GetReader(fep), "JavaReader") == 0){
            (*env)->DeleteGlobalRef(env, krCtx);
        }else{
            RedisModule_Log(NULL, "warning", "unknown reader when try to free reader args on jvm");
            RedisModule_Assert(false);
        }

        return;
    }
    if(jvm_ltd->allowBlock){
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(jvm_ltd->rctx, NULL, NULL, NULL, 0);
        RedisGears_AddOnDoneCallback(ep, JVM_OnExecutionDoneCallback, bc);
        jvm_ltd->isBlocked = true;
    }
}

static jobject JVM_GBExecuteParseReply(JNIEnv *env, RedisModuleCallReply *reply){
    char* err = NULL;
    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
        jobject ret = (*env)->NewObjectArray(env, RedisModule_CallReplyLength(reply), gearsObjectCls, NULL);
        for(size_t i = 0 ; i < RedisModule_CallReplyLength(reply) ; ++i){
            RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply, i);
            jobject val = JVM_GBExecuteParseReply(env, subReply);
            (*env)->SetObjectArrayElement(env, ret, i, val);
            if((err = JVM_GetException(env))){
                ret = (*env)->NewStringUTF(env, err);
                break;
            }
        }
        return ret;
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING ||
            RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        size_t len;
        const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
        char temp[len + 1];
        memcpy(temp, replyStr, len);
        temp[len] = '\0';
        jobject ret = (*env)->NewStringUTF(env, temp);
        return ret;
    }

    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER){
        long long val = RedisModule_CallReplyInteger(reply);
        jobject ret = (*env)->CallStaticObjectMethod(env, gearsLongCls, gearsLongValueOfMethodId, val);
        if((err = JVM_GetException(env))){
            ret = (*env)->NewStringUTF(env, err);
        }
        return ret;
    }
    return NULL;
}

static jstring JVM_GBConfigGet(JNIEnv *env, jobject objectOrClass, jstring key){
    if(!key){
        (*env)->ThrowNew(env, exceptionCls, "Got a NULL key on configGet function");
        return NULL;
    }

    const char* keyStr = (*env)->GetStringUTFChars(env, key, NULL);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(ctx);
    const char* valCStr = RedisGears_GetConfig(keyStr);
    if(!valCStr){
        RedisGears_LockHanlderRelease(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        (*env)->ReleaseStringUTFChars(env, key, keyStr);
        return NULL;
    }
    jstring val = (*env)->NewStringUTF(env, valCStr);
    RedisGears_LockHanlderRelease(ctx);
    RedisModule_FreeThreadSafeContext(ctx);

    (*env)->ReleaseStringUTFChars(env, key, keyStr);

    return val;
}

static jstring JVM_GBHashtag(JNIEnv *env, jobject objectOrClass){
    return (*env)->NewStringUTF(env, RedisGears_GetMyHashTag());
}

static void JVM_GBLog(JNIEnv *env, jobject objectOrClass, jstring msg, jobject logLevel){
    if(!msg){
        (*env)->ThrowNew(env, exceptionCls, "Got a NULL msg on log function");
        return;
    }
    const char* msgStr = (*env)->GetStringUTFChars(env, msg, NULL);
    char* logLevelStr = NULL;
    if((*env)->IsSameObject(env, logLevel, gearsLogLevelNotice)){
        logLevelStr = "notice";
    }else if((*env)->IsSameObject(env, logLevel, gearsLogLevelDebug)){
        logLevelStr = "debug";
    }else if((*env)->IsSameObject(env, logLevel, gearsLogLevelVerbose)){
        logLevelStr = "verbose";
    }else if((*env)->IsSameObject(env, logLevel, gearsLogLevelWarning)){
        logLevelStr = "warning";
    }else{
        RedisModule_Assert(false);
    }
    RedisModule_Log(NULL, logLevelStr, "JAVA_GEARS: %s", msgStr);

    (*env)->ReleaseStringUTFChars(env, msg, msgStr);
}

static void JVM_GBAcquireRedisGil(JNIEnv *env, jobject objectOrClass){
    RedisGears_LockHanlderAcquire(staticCtx);
}

static void JVM_GBReleaseRedisGil(JNIEnv *env, jobject objectOrClass){
    RedisGears_LockHanlderRelease(staticCtx);
}

static jboolean JVM_GBSetAvoidNotifications(JNIEnv *env, jobject objectOrClass, jboolean val){
    if(RedisGears_KeysReaderSetAvoidEvents(val ? 1 : 0)){
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

static jfloat JVM_GBGetMemoryRatio(JNIEnv *env, jobject objectOrClass){
    if(!RMAPI_FUNC_SUPPORTED(RedisModule_GetUsedMemoryRatio)){
        (*env)->ThrowNew(env, exceptionCls, "getMemoryRatio is not implemented on this redis version");
        return 0;
    }
    RedisGears_LockHanlderRegister();
    RedisGears_LockHanlderAcquire(staticCtx);
    float res = RedisModule_GetUsedMemoryRatio();
    RedisGears_LockHanlderRelease(staticCtx);
    return res;
}

static void JVM_GBOverriderReply(JNIEnv *env, jobject objectOrClass, jobject reply){
    if(!reply){
        (*env)->ThrowNew(env, exceptionCls, "Can not override with NULL values");
        return;
    }

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    if(!jvm_tld->eCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no call next without execution ctx");
        return;
    }

    CommandCtx* cmdCtx = RedisGears_CommandCtxGet(jvm_tld->eCtx);
    if(!cmdCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no get command ctx");
        return;
    }

    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = JVM_TurnToGlobal(jvm_tld->env, reply);

    char* err = NULL;
    if(RedisGears_CommandCtxOverrideReply(cmdCtx, &r->baseRecord, &err) != REDISMODULE_OK){
        RedisGears_FreeRecord(&r->baseRecord);
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return;
    }
}

static jobject JVM_GBGetCommand(JNIEnv *env, jobject objectOrClass){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    if(!jvm_tld->eCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no call next without execution ctx");
        return NULL;
    }

    CommandCtx* cmdCtx = RedisGears_CommandCtxGet(jvm_tld->eCtx);
    if(!cmdCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no get command ctx");
        return NULL;
    }

    // we must take the lock, it is not safe to access the command args without the lock because redis might
    // change them under our noise
    RedisGears_LockHanlderAcquire(staticCtx);

    size_t len;
    RedisModuleString** command = RedisGears_CommandCtxGetCommand(cmdCtx, &len);

    jobjectArray res = (*env)->NewObjectArray(env, len, gearsByteArrayCls, NULL);

    for(size_t i = 0 ; i < len ; ++i){
        size_t argLen;
        const char* arg = RedisModule_StringPtrLen(command[i], &argLen);
        jbyteArray jarg = (*env)->NewByteArray(env, argLen);

        (*env)->SetByteArrayRegion(env, jarg, 0, argLen, arg);

        (*env)->SetObjectArrayElement(env, res, i, jarg);
    }

    RedisGears_LockHanlderRelease(staticCtx);

    return res;
}

static jobject JVM_GBCallNext(JNIEnv *env, jobject objectOrClass, jobjectArray args){
    if(!args){
        (*env)->ThrowNew(env, exceptionCls, "Got a NULL command");
        return NULL;
    }
    size_t len = (*env)->GetArrayLength(env, args);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    if(!jvm_tld->eCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no call next without execution ctx");
        return NULL;
    }
    CommandReaderTriggerCtx* crtCtx = RedisGears_GetCommandReaderTriggerCtx(jvm_tld->eCtx);
    if(!crtCtx){
        (*env)->ThrowNew(env, exceptionCls, "Can no call next out of the command reader ctx");
        return NULL;
    }

    RedisModuleString** argsRedisStr = array_new(RedisModuleString*, len);
    for(size_t i = 0 ; i < len ; ++i){
        jstring arg = (*env)->GetObjectArrayElement(env, args, i);
        if(!arg){
            array_free_ex(argsRedisStr, RedisModule_FreeString(NULL, *(RedisModuleString**)ptr));
            (*env)->ThrowNew(env, exceptionCls, "Got a null argument on command");
            return NULL;
        }
        const char* argStr = (*env)->GetStringUTFChars(env, arg, NULL);
        RedisModuleString* argRedisStr = RedisModule_CreateString(NULL, argStr, strlen(argStr));
        (*env)->ReleaseStringUTFChars(env, arg, argStr);
        argsRedisStr = array_append(argsRedisStr, argRedisStr);
    }

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(ctx);

    RedisModuleCallReply* reply = RedisGears_CommandReaderTriggerCtxNext(crtCtx, argsRedisStr, array_len(argsRedisStr));

    RedisGears_LockHanlderRelease(ctx);

    array_free_ex(argsRedisStr, RedisModule_FreeString(NULL, *(RedisModuleString**)ptr));

    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        char* err;
        if(reply){
            size_t len;
            const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
            err = JVM_ALLOC(len + 1);
            memcpy(err, replyStr, len);
            err[len] = '\0';
            RedisModule_FreeCallReply(reply);
        }else{
            err = JVM_STRDUP("Got a NULL reply from redis");
        }
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return NULL;
    }

    jobject res = JVM_GBExecuteParseReply(env, reply);

    RedisModule_FreeCallReply(reply);

    RedisModule_FreeThreadSafeContext(ctx);

    return res;
}

static jobject JVM_GBExecute(JNIEnv *env, jobject objectOrClass, jobjectArray command){
    if(!command){
        (*env)->ThrowNew(env, exceptionCls, "Got a NULL command");
        return NULL;
    }
    size_t len = (*env)->GetArrayLength(env, command);
    if(len == 0){
        (*env)->ThrowNew(env, exceptionCls, "No command given to execute");
        return NULL;
    }
    jstring c = (*env)->GetObjectArrayElement(env, command, 0);
    if(!c){
        (*env)->ThrowNew(env, exceptionCls, "Null command given to execute");
        return NULL;
    }
    const char* cStr = (*env)->GetStringUTFChars(env, c, NULL);

    RedisModuleString** args = array_new(RedisModuleString*, len);
    for(size_t i = 1 ; i < len ; ++i){
        jstring arg = (*env)->GetObjectArrayElement(env, command, i);
        if(!arg){
            array_free_ex(args, RedisModule_FreeString(NULL, *(RedisModuleString**)ptr));
            (*env)->ReleaseStringUTFChars(env, c, cStr);
            (*env)->ThrowNew(env, exceptionCls, "Got a null argument on command");
            return NULL;
        }
        const char* argStr = (*env)->GetStringUTFChars(env, arg, NULL);
        RedisModuleString* argRedisStr = RedisModule_CreateString(NULL, argStr, strlen(argStr));
        (*env)->ReleaseStringUTFChars(env, arg, argStr);
        args = array_append(args, argRedisStr);
    }

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisGears_LockHanlderAcquire(ctx);

    RedisModuleCallReply *reply = RedisModule_Call(ctx, cStr, "!v", args, array_len(args));

    RedisGears_LockHanlderRelease(ctx);

    array_free_ex(args, RedisModule_FreeString(NULL, *(RedisModuleString**)ptr));

    if(!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR){
        char* err;
        if(reply){
            size_t len;
            const char* replyStr = RedisModule_CallReplyStringPtr(reply, &len);
            err = JVM_ALLOC(len + 1);
            memcpy(err, replyStr, len);
            err[len] = '\0';
            RedisModule_FreeCallReply(reply);
        }else{
            err = JVM_STRDUP("Got a NULL reply from redis");
        }
        (*env)->ReleaseStringUTFChars(env, c, cStr);
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return NULL;
    }

    jobject res = JVM_GBExecuteParseReply(env, reply);

    RedisModule_FreeCallReply(reply);

    (*env)->ReleaseStringUTFChars(env, c, cStr);

    RedisModule_FreeThreadSafeContext(ctx);

    return res;
}

void JVM_FreeRegisterReaderArgs(FlatExecutionPlan* fep, void* triggerCtx){
    if(strcmp(RedisGears_GetReader(fep), "KeysReader") == 0){
        RedisGears_KeysReaderTriggerArgsFree(triggerCtx);
    }else if(strcmp(RedisGears_GetReader(fep), "StreamReader") == 0){
        RedisGears_StreamReaderTriggerArgsFree(triggerCtx);
    }else if(strcmp(RedisGears_GetReader(fep), "CommandReader") == 0){
        RedisGears_CommandReaderTriggerArgsFree(triggerCtx);
    }
}

static jstring JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader, jobject jmode, jobject onRegistered, jobject onUnregistered){
    if(!reader){
        (*env)->ThrowNew(env, exceptionCls, "Null reader give to register function");
        return NULL;
    }
    if(!jmode){
        (*env)->ThrowNew(env, exceptionCls, "Null execution mode give to register function");
        return NULL;
    }

    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    char* err = NULL;

    ExecutionMode mode = ExecutionModeAsync;
    if((*env)->IsSameObject(env, jmode, gearsExecutionModeAsync)){
        mode = ExecutionModeAsync;
    }else if((*env)->IsSameObject(env, jmode, gearsExecutionModeSync)){
        mode = ExecutionModeSync;
    }else if((*env)->IsSameObject(env, jmode, gearsExecutionModeAsyncLocal)){
        mode = ExecutionModeAsyncLocal;
    }else{
        RedisModule_Assert(false);
    }

    if(onRegistered){
        onRegistered = JVM_TurnToGlobal(env, onRegistered);
        RGM_SetFlatExecutionOnRegisteredCallback(fep, JVM_OnRegistered, onRegistered);
    }

    if(onUnregistered){
        onUnregistered = JVM_TurnToGlobal(env, onUnregistered);
        RGM_SetFlatExecutionOnUnregisteredCallback(fep, JVM_OnUnregistered, onUnregistered);
    }

    void* triggerCtx = JVM_CreateRegisterReaderArgs(env, fep, reader);
    if(!triggerCtx){
        return NULL;
    }
    char* registrationId = NULL;
    int res = RedisGears_Register(fep, mode, triggerCtx, &err, &registrationId);
    if(!res){
        if(!err){
            err = JVM_STRDUP("Failed register execution");
        }
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return NULL;
    }

    jstring regId = (*env)->NewStringUTF(env, registrationId);
    return regId;
}

static int JVM_JVMStats(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 1){
        return RedisModule_WrongArity(ctx);
    }

    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;

    JVM_PushFrame(env);

    jobject obj = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsGetStatsMethodId);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed getting jvm stats, error='%s'", err);
        RedisModule_ReplyWithError(ctx, err);
    }else{
        JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
        if(obj){
            r->obj = JVM_TurnToGlobal(env, obj);
        }else{
            r->obj = NULL;
        }
        JVMRecord_SendReply(&r->baseRecord, ctx);
        RedisGears_FreeRecord(&r->baseRecord);
    }

    JVM_PopFrame(env);
    return REDISMODULE_OK;
}

static int JVM_DumpHeap(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 2){
        return RedisModule_WrongArity(ctx);
    }

    const char* fileName = RedisModule_StringPtrLen(argv[1], NULL);

    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;

    JVM_PushFrame(env);

    jstring workingDirJStr = (*env)->NewStringUTF(env, workingDir);
    jstring fileNameJStr = (*env)->NewStringUTF(env, fileName);

    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsDumpHeapMethodId, workingDirJStr, fileNameJStr);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed create memory dump, error='%s'", err);
        RedisModule_ReplyWithError(ctx, err);
    }else{
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    JVM_PopFrame(env);
    return REDISMODULE_OK;
}

static int JVM_RunGC(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 1){
        return RedisModule_WrongArity(ctx);
    }


    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_ltd->env;

    JVM_PushFrame(env);

    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsRunGCMethodId);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed running jvm gc, error='%s'", err);
        RedisModule_ReplyWithError(ctx, err);
    }else{
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    JVM_PopFrame(env);
    return REDISMODULE_OK;
}

static int JVM_DumpSessions(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 1){
        return RedisModule_WrongArity(ctx);
    }

    pthread_mutex_lock(&JVMSessionsLock);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(JVMSessions, "^", NULL, 0);
    char* key;
    size_t keyLen;
    JVMRunSession* s;
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithCString(ctx, "active");
    RedisModule_ReplyWithArray(ctx, RedisModule_DictSize(JVMSessions));
    while((key = RedisModule_DictNextC(iter, &keyLen, (void**)&s))){
        RedisModule_ReplyWithArray(ctx, 10);
        RedisModule_ReplyWithCString(ctx, "id");
        RedisModule_ReplyWithCString(ctx, s->uuidStr);
        RedisModule_ReplyWithCString(ctx, "mainClassName");
        RedisModule_ReplyWithCString(ctx, s->mainClassName);
        RedisModule_ReplyWithCString(ctx, "version");
        RedisModule_ReplyWithLongLong(ctx, s->version);
        RedisModule_ReplyWithCString(ctx, "jar");
        RedisModule_ReplyWithCString(ctx, s->jarFilePath);
        RedisModule_ReplyWithCString(ctx, "refCount");
        RedisModule_ReplyWithLongLong(ctx, s->refCount);
    }

    RedisModule_ReplyWithCString(ctx, "ts");
    RedisModule_ReplyWithArray(ctx, JVM_listLength(JVMTSSessions));
    for(JVM_listNode* curr = JVM_listFirst(JVMTSSessions) ; curr != NULL ; curr = JVM_listNextNode(curr)){
        s = JVM_listNodeValue(curr);
        RedisModule_ReplyWithArray(ctx, 10);
        RedisModule_ReplyWithCString(ctx, "id");
        RedisModule_ReplyWithCString(ctx, s->uuidStr);
        RedisModule_ReplyWithCString(ctx, "mainClassName");
        RedisModule_ReplyWithCString(ctx, s->mainClassName);
        RedisModule_ReplyWithCString(ctx, "version");
        RedisModule_ReplyWithLongLong(ctx, s->version);
        RedisModule_ReplyWithCString(ctx, "jar");
        RedisModule_ReplyWithCString(ctx, s->jarFilePath);
        RedisModule_ReplyWithCString(ctx, "refCount");
        RedisModule_ReplyWithLongLong(ctx, s->refCount);
    }

    pthread_mutex_unlock(&JVMSessionsLock);

    return REDISMODULE_OK;
}

static int JVM_Run(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 3){
        return RedisModule_WrongArity(ctx);
    }
    char* err = NULL;
    size_t clsNameLen;
    const char* clsName = RedisModule_StringPtrLen(argv[1], &clsNameLen);
    size_t bytesLen;
    const char* bytes = RedisModule_StringPtrLen(argv[2], &bytesLen);

    JVMRunSession* s = JVM_SessionCreate(NULL, clsName, bytes, bytesLen, &err);
    if(!s){
        if(!err){
            err = JVM_STRDUP("Failed creating session");
        }
        RedisModule_ReplyWithError(ctx, err);
        JVM_FREE(err);
        return REDISMODULE_OK;
    }

    // creating new class loader
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s, NULL);
    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(&jectx);

    jvm_ltd->rctx = ctx;
    jvm_ltd->allowBlock = true;
    JNIEnv *env = jvm_ltd->env;

    JVM_PushFrame(env);

    jstring clsNameJString = (*env)->NewStringUTF(env, clsName);

    jclass cls = (*env)->CallObjectMethod(env, jvm_ltd->currSession->sessionClsLoader, javaLoadClassNewMid, clsNameJString);

    if((err = JVM_GetException(env))){
        goto error;
    }

    jfieldID versionField = (*env)->GetStaticFieldID(env, cls, "VERSION", "I");

    if((err = JVM_GetException(env)) || !versionField){
        RedisModule_Log(NULL, "debug", "No field id found of class %s, err='%s'", clsName, err? err : "NULL");
        versionField = 0;
        if(err){
            JVM_FREE(err);
        }
    }

    if(versionField){
        s->version = (*env)->GetStaticIntField(env, cls, versionField);
    }

    jmethodID mid = (*env)->GetStaticMethodID(env, cls, "main", "([Ljava/lang/String;)V");

    if(!(err = JVM_GetException(env))){
        jobject javaArgs = (*env)->NewObjectArray(env, argc - 3, gearsStringCls, NULL);

        for(size_t i = 3 ; i < argc ; ++i){
            const char* argCStr = RedisModule_StringPtrLen(argv[i], NULL);
            jstring javaStr = (*env)->NewStringUTF(env, argCStr);
            (*env)->SetObjectArrayElement(env, javaArgs, i - 3, javaStr);
        }

        (*env)->CallStaticVoidMethod(env, cls, mid, javaArgs);
    }else{
        JVM_FREE(err);
        err = NULL;
        mid = (*env)->GetStaticMethodID(env, cls, "main", "()V");

        if((err = JVM_GetException(env))){
            goto error;
        }

        (*env)->CallStaticVoidMethod(env, cls, mid);
    }

    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!jvm_ltd->isBlocked){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    // clean the thread ctx class loader just in case
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsCleanCtxClassLoaderMethodId);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed cleaning thread ctx class loader, error='%s'", err);
    }

    JVM_PopFrame(env);

    JVM_SessionFree(jvm_ltd->currSession);

    jvm_ltd->rctx = NULL;

    JVM_ThreadLocalDataRestor(jvm_ltd, &jectx);

    return REDISMODULE_OK;

error:
    // clean the thread ctx class loader just in case
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsCleanCtxClassLoaderMethodId);

    char* internalErr = NULL;
    if((internalErr = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed cleaning thread ctx class loader, error='%s'", internalErr);
    }

    RedisModule_ReplyWithError(ctx, err);
    JVM_FREE(err);
    JVM_PopFrame(env);

    JVM_SessionFree(jvm_ltd->currSession);

    jvm_ltd->rctx = NULL;
    JVM_ThreadLocalDataRestor(jvm_ltd, &jectx);

    return REDISMODULE_OK;
}

static jobject JVM_ToJavaRecordMapperInternal(JNIEnv *env, ExecutionCtx* rctx, Record *data, void* arg, bool* isError){
    if(!data){
        return NULL;
    }

    char* err = NULL;

    jobject obj = NULL;
    if(RedisGears_RecordGetType(data) == RedisGears_GetHashSetRecordType()){
        obj = (*env)->NewObject(env, hashRecordCls, hashRecordCtor);
        Arr(char*) keys = RedisGears_HashSetRecordGetAllKeys(data);
        for(size_t i = 0 ; i < array_len(keys) ; ++i){
            jstring javaKeyStr = (*env)->NewStringUTF(env, keys[i]);
            Record* val = RedisGears_HashSetRecordGet(data, keys[i]);
            jobject jvmVal = JVM_ToJavaRecordMapperInternal(env, rctx, val, arg, isError);
            if(*isError){
                return NULL;
            }
            (*env)->CallObjectMethod(env, obj, hashRecordSet, javaKeyStr, jvmVal);
            if((err = JVM_GetException(env))){
                RedisGears_SetError(rctx, err);
                *isError = true;
                return NULL;
            }
        }
        array_free(keys);
    }else if(RedisGears_RecordGetType(data) == RedisGears_GetStringRecordType()){
        size_t len;
        char* str = RedisGears_StringRecordGet(data, &len);
        obj = (*env)->NewByteArray(env, len);
        (*env)->SetByteArrayRegion(env, obj, 0, len, str);
    }else if(RedisGears_RecordGetType(data) == RedisGears_GetKeyRecordType()){
        obj = (*env)->NewObject(env, hashRecordCls, hashRecordCtor);
        size_t keyLen;
        const char* key = RedisGears_KeyRecordGetKey(data, &keyLen);
        Record* val = RedisGears_KeyRecordGetVal(data);
        jstring javaKeyStr = (*env)->NewStringUTF(env, key);
        jobject jvmVal = JVM_ToJavaRecordMapperInternal(env, rctx, val, arg, isError);
        if(*isError){
            return NULL;
        }
        (*env)->CallObjectMethod(env, obj, hashRecordSet, javaKeyStr, jvmVal);
        if((err = JVM_GetException(env))){
            RedisGears_SetError(rctx, err);
            *isError = true;
            return NULL;
        }
    }else if(RedisGears_RecordGetType(data) == JVMRecordType){
        JVMRecord* jvmVal = (JVMRecord*)data;
        obj = (*env)->NewLocalRef(env, jvmVal->obj);
    }else if(RedisGears_RecordGetType(data) == RedisGears_GetListRecordType()){
        obj = (*env)->NewObjectArray(env, RedisGears_ListRecordLen(data), gearsObjectCls, NULL);
        for(size_t i = 0 ; i < RedisGears_ListRecordLen(data) ; ++i){
            Record* temp = RedisGears_ListRecordGet(data, i);
            jobject jvmTemp = JVM_ToJavaRecordMapperInternal(env, rctx, temp, arg, isError);
            if(*isError){
                return NULL;
            }
            (*env)->SetObjectArrayElement(env, obj, i, jvmTemp);
        }
    }else if(RedisGears_RecordGetType(data) == RedisGears_GetErrorRecordType()){
        size_t len;
        char* str = RedisGears_StringRecordGet(data, &len);
        RedisGears_SetError(rctx, JVM_STRDUP(str));
    }else{
        RedisModule_Assert(false);
    }
    return obj;
}

static Record* JVM_ToJavaRecordMapper(ExecutionCtx* ectx, Record *data, void* arg){
    if(RedisGears_RecordGetType(data) == JVMRecordType){
        return data;
    }

    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);

    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);

    JVM_PushFrame(jvm_tld->env);

    bool isError = false;
    jobject obj = JVM_ToJavaRecordMapperInternal(jvm_tld->env, ectx, data, arg, &isError);

    if(isError){
        JVM_PopFrame(jvm_tld->env);
        JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
        return NULL;
    }

    if(obj){
        obj = JVM_TurnToGlobal(jvm_tld->env, obj);
    }

    JVM_PopFrame(jvm_tld->env);

    RedisGears_FreeRecord(data);

    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return &r->baseRecord;
}

static Record* JVM_AccumulateByKey(ExecutionCtx* ectx, char* key, Record *accumulate, Record *data, void* arg){
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    jvm_tld->asyncRecorType = ASYNC_RECORD_TYPE_DEFAULT;


    JVMRecord* r = (JVMRecord*)data;
    JVMRecord* a = (JVMRecord*)accumulate;
    jobject accumulatorBy = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    jstring jkey = (*env)->NewStringUTF(env, key);
    jobject res = (*env)->CallObjectMethod(env, accumulatorBy, gearsAccumulateByMethodId, jkey, a ? a->obj : NULL, r->obj);
    char* err = NULL;
    if((err = JVM_GetException(env))){
        goto error;
    }

    if(jvm_tld->createFuture && (*env)->IsSameObject(env, res, jvm_tld->createFuture)){
        // an async record was created during the step run
        JVM_PopFrame(jvm_tld->env);

        JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

        return RedisGears_GetDummyRecord();
    }

    if(!res){
        err = JVM_STRDUP("Got null accumulator on accumulateby");
        goto error;
    }

    res = JVM_TurnToGlobal(env, res);
    if(!a){
        a = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    }else{
        (*env)->DeleteGlobalRef(env, a->obj);
    }
    a->obj = res;
    RedisGears_FreeRecord(data);

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return &a->baseRecord;

error:
    RedisGears_FreeRecord(accumulate);
    RedisGears_FreeRecord(data);
    JVM_PopFrame(env);
    RedisGears_SetError(ectx, err);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return NULL;
}

static Record* JVM_Accumulate(ExecutionCtx* ectx, Record *accumulate, Record *data, void* arg){
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    jvm_tld->asyncRecorType = ASYNC_RECORD_TYPE_DEFAULT;

    JVMRecord* r = (JVMRecord*)data;
    JVMRecord* a = (JVMRecord*)accumulate;
    jobject accumulator = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    jobject res = (*env)->CallObjectMethod(env, accumulator, gearsAccumulatorMethodId, a? a->obj : NULL, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        goto error;
    }

    if(jvm_tld->createFuture && (*env)->IsSameObject(env, res, jvm_tld->createFuture)){
        // an async record was created during the step run
        JVM_PopFrame(jvm_tld->env);

        JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

        return RedisGears_GetDummyRecord();
    }

    if(!res){
        err = JVM_STRDUP("Got null accumulator on accumulate step");
        goto error;
    }

    res = JVM_TurnToGlobal(env, res);
    if(!a){
        a = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    }else{
        (*env)->DeleteGlobalRef(env, a->obj);
    }
    a->obj = res;
    RedisGears_FreeRecord(data);

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return &a->baseRecord;

error:
    RedisGears_FreeRecord(accumulate);
    RedisGears_FreeRecord(data);
    RedisGears_SetError(ectx, err);
    JVM_PopFrame(env);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return NULL;
}

static int JVM_Filter(ExecutionCtx* ectx, Record *data, void* arg){
    int result;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    jvm_tld->asyncRecorType = ASYNC_RECORD_TYPE_FILTER;

    JVMRecord* r = (JVMRecord*)data;
    jobject filter = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    jboolean res = (*env)->CallBooleanMethod(env, filter, gearsFilterMethodId, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisGears_SetError(ectx, err);
    }

    if(jvm_tld->createFuture){
        result = RedisGears_StepHold;
    }else{
        result = res ? RedisGears_StepSuccess : RedisGears_StepFailed;
    }

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

    return result;
}

static int JVM_Foreach(ExecutionCtx* ectx, Record *data, void* arg){
    int result = RedisGears_StepSuccess;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    jvm_tld->asyncRecorType = ASYNC_RECORD_TYPE_FOREACH;

    JVMRecord* r = (JVMRecord*)data;
    jobject foreach = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    (*env)->CallVoidMethod(env, foreach, gearsForeachMethodId, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisGears_SetError(ectx, err);
        goto done;
    }

    if(jvm_tld->createFuture){
        result = RedisGears_StepHold;
    }

done:
    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

    return result;
}

static char* JVM_Extractor(ExecutionCtx* ectx, Record *data, void* arg, size_t* len){
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);

    JVMRecord* r = (JVMRecord*)data;
    jobject extractor = (arg);
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    jobject res = (*env)->CallObjectMethod(env, extractor, gearsExtractorMethodId, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!res){
        err = JVM_STRDUP("Got null string on extractor");
        goto error;
    }

    const char* resStr = (*env)->GetStringUTFChars(env, res, NULL);
    char* extractedData = JVM_STRDUP(resStr);

    (*env)->ReleaseStringUTFChars(env, res, resStr);

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return extractedData;

error:
    JVM_PopFrame(env);
    RedisGears_SetError(ectx, err);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return NULL;
}

static Record* JVM_FlatMapper(ExecutionCtx* ectx, Record *data, void* arg){
    char* err = NULL;
    Record* listRecord = NULL;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);

    JVM_PushFrame(jvm_tld->env);

    JVMRecord* r = (JVMRecord*)data;
    jobject mapper = (arg);
    JNIEnv *env = jvm_tld->env;

    jobject res = (*env)->CallObjectMethod(env, mapper, gearsFlatMapMethodId, r->obj);
    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!res){
        err = JVM_STRDUP("Got null record on flat mapper");
        goto error;
    }

    jobject iterator = NULL;
    RedisModule_Assert((*env)->IsInstanceOf(env, res, iterableCls));

    iterator = (*env)->CallObjectMethod(env, res, iteratorMethodId);
    if((err = JVM_GetException(env))){
        goto error;
    }

    listRecord = RedisGears_ListRecordCreate(20);
    while((*env)->CallBooleanMethod(env, iterator, iteratorHasNextMethodId)){

        if((err = JVM_GetException(env))){
            goto error;
        }

        jobject obj = (*env)->CallObjectMethod(env, iterator, iteratorNextMethodId);

        if((err = JVM_GetException(env))){
            goto error;
        }

        obj = JVM_TurnToGlobal(env, obj);
        JVMRecord* innerRecord = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
        innerRecord->obj = obj;
        RedisGears_ListRecordAdd(listRecord, (Record*)innerRecord);
    }

    if((err = JVM_GetException(env))){
        goto error;
    }

    RedisGears_FreeRecord(data);

    JVM_PopFrame(jvm_tld->env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return listRecord;

error:
    RedisGears_SetError(ectx, err);
    RedisGears_FreeRecord(data);
    if(listRecord){
        RedisGears_FreeRecord(listRecord);
    }
    JVM_PopFrame(jvm_tld->env);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return NULL;
}

static Record* JVM_Mapper(ExecutionCtx* ectx, Record *data, void* arg){
    char* err = NULL;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    jvm_tld->asyncRecorType = ASYNC_RECORD_TYPE_DEFAULT;

    JVM_PushFrame(jvm_tld->env);

    JVMRecord* r = (JVMRecord*)data;
    jobject mapper = (arg);
    JNIEnv *env = jvm_tld->env;

    jobject res = (*env)->CallObjectMethod(env, mapper, gearsMapMethodId, r->obj);

    if((err = JVM_GetException(env))){
        goto error;
    }

    if(jvm_tld->createFuture && (*env)->IsSameObject(env, res, jvm_tld->createFuture)){
        // an async record was created during the step run
        RedisGears_FreeRecord(data);

        JVM_PopFrame(jvm_tld->env);

        JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

        return RedisGears_GetDummyRecord();
    }

    if(!res){
        err = JVM_STRDUP("Got null record on mapper");
        goto error;
    }

    res = JVM_TurnToGlobal(env, res);
    (*env)->DeleteGlobalRef(env, r->obj);
    r->obj = res;

    JVM_PopFrame(jvm_tld->env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return &r->baseRecord;

error:
    RedisGears_SetError(ectx, err);
    RedisGears_FreeRecord(data);
    JVM_PopFrame(jvm_tld->env);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    return NULL;
}

static int JVMRecord_SendReply(Record* base, RedisModuleCtx* rctx){
    JVMRecord* r = (JVMRecord*)base;
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    char* err = NULL;
    if(!r->obj){
        RedisModule_ReplyWithNull(rctx);
        JVM_PopFrame(env);
        return REDISMODULE_OK;
    }

    if((*env)->IsInstanceOf(env, r->obj, gearsLongCls)){
        jlong res = (*env)->CallLongMethod(env, r->obj, gearsLongValMethodId);
        if((err = JVM_GetException(env))){
            goto error;
        }
        RedisModule_ReplyWithLongLong(rctx, res);
        JVM_PopFrame(env);
        return REDISMODULE_OK;
    }

    jobject iterator = NULL;
    if((*env)->IsInstanceOf(env, r->obj, iterableCls)){
        jobject iterator = (*env)->CallObjectMethod(env, r->obj, iteratorMethodId);
        if((err = JVM_GetException(env))){
            goto error;
        }

        size_t len = 0;
        RedisModule_ReplyWithArray(rctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        JVMRecord* innerRecord = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
        innerRecord->obj = NULL;

        while((*env)->CallBooleanMethod(env, iterator, iteratorHasNextMethodId)){
            ++len;
            if((err = JVM_GetException(env))){
                RedisModule_ReplyWithError(rctx, err);
                JVM_FREE(err);
                break;
            }

            jobject obj = (*env)->CallObjectMethod(env, iterator, iteratorNextMethodId);

            if((err = JVM_GetException(env))){
                RedisModule_ReplyWithError(rctx, err);
                JVM_FREE(err);
                break;
            }

            innerRecord->obj = obj;
            JVMRecord_SendReply(&innerRecord->baseRecord, rctx);
            innerRecord->obj = NULL;
        }

        if((err = JVM_GetException(env))){
            RedisModule_ReplyWithError(rctx, err);
            JVM_FREE(err);
            ++len;
        }

        RedisModule_ReplySetArrayLength(rctx, len);

        RedisGears_FreeRecord(&innerRecord->baseRecord);

        JVM_PopFrame(env);
        return REDISMODULE_OK;
    }

    jobject res = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, recordToStr, r->obj);
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Excpetion raised but not catched, exception='%s'", err);
        RedisModule_ReplyWithError(rctx, err);
    }else{
        const char* resStr = (*env)->GetStringUTFChars(env, res, NULL);
        size_t len = strlen(resStr);
        if(len > 1){
            if(resStr[0] == '-'){
                RedisModule_ReplyWithError(rctx, resStr + 1);
            } else if(resStr[0] == '+'){
                RedisModule_ReplyWithSimpleString(rctx, resStr + 1);
            } else {
                RedisModule_ReplyWithStringBuffer(rctx, resStr, len);
            }
        }else{
            RedisModule_ReplyWithStringBuffer(rctx, resStr, len);
        }
        (*jvm_tld->env)->ReleaseStringUTFChars(jvm_tld->env, res, resStr);
        (*jvm_tld->env)->DeleteLocalRef(jvm_tld->env, res);
    }
    JVM_PopFrame(env);
    return REDISMODULE_OK;

error:
    RedisModule_Log(NULL, "warning", "Excpetion raised but not catched, exception='%s'", err);
    RedisModule_ReplyWithCString(rctx, err);
    JVM_FREE(err);
    JVM_PopFrame(env);
    return REDISMODULE_OK;
}

static void JVMRecord_Free(Record* base){
    JVMRecord* r = (JVMRecord*)base;
    if(!r->obj){
        return;
    }
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;
    (*env)->DeleteGlobalRef(env, r->obj);
}

static void* JVM_ObjectDup(void* arg){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    return (*jvm_tld->env)->NewGlobalRef(jvm_tld->env, (jobject)arg);
}

static void JVM_ObjectFree(void* arg){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    (*jvm_tld->env)->DeleteGlobalRef(jvm_tld->env, (jobject)arg);
}

static char* JVM_ObjectToString(void* arg){
    return JVM_STRDUP("java object");
}

static int JVM_ObjectSerializeInternal(jobject outputStream, void* arg, Gears_BufferWriter* bw, char** err, bool reset){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);

    jobject obj = arg;
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderSerializeObjectMethodId, obj, outputStream, reset);

    if((*err = JVM_GetException(env))){
        return REDISMODULE_ERR;
    }

    size_t len = (*env)->GetArrayLength(env, bytes);
    jbyte* buf = JVM_ALLOC(len * sizeof(jbyte));
    (*env)->GetByteArrayRegion(env, bytes, 0, len, buf);

    RedisGears_BWWriteBuffer(bw, buf, len);

    JVM_FREE(buf);

    (*env)->DeleteLocalRef(env, bytes);

    return REDISMODULE_OK;
}

static void* JVM_ObjectDeserializeInternal(jobject inputStream, Gears_BufferReader* br, char** err, bool reset){
    size_t len;
    const char* buf = RedisGears_BRReadBuffer(br, &len);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->NewByteArray(env, len);

    (*env)->SetByteArrayRegion(env, bytes, 0, len, buf);

    jobject obj = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderDeserializeObjectMethodId, bytes, inputStream, reset);

    (*env)->DeleteLocalRef(env, bytes);

    if((*err = JVM_GetException(env))){
        if(obj){
            (*env)->DeleteLocalRef(env, obj);
        }
        return NULL;
    }

    return JVM_TurnToGlobal(env, obj);
}

static int JVM_ObjectSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){

    JVMFlatExecutionSession* session = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);

    return JVM_ObjectSerializeInternal(session->flatExecutionOutputStream, arg, bw, err, false);
}

static void* JVM_ObjectDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > JOBJECT_TYPE_VERSION){
        *err = JVM_STRDUP("Missmatch version, update to newest JVM module");
        return NULL;
    }

    JVMFlatExecutionSession* currSession = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);

    return JVM_ObjectDeserializeInternal(currSession->flatExecutionInputStream, br, err, false);
}

static int JVMRecord_Serialize(ExecutionCtx* ectx, Gears_BufferWriter* bw, Record* base){
    JVMRecord* r = (JVMRecord*)base;
    JVMExecutionSession* es = RedisGears_GetPrivateData(ectx);
    RedisModule_Assert(es);
    char* err = NULL;
    int res = JVM_ObjectSerializeInternal(es->executionOutputStream, r->obj, bw, &err, true);
    if(err){
        RedisGears_SetError(ectx, err);
    }
    return res;
}

static Record* JVMRecord_Deserialize(ExecutionCtx* ectx, Gears_BufferReader* br){
    char* err;
    JVMExecutionSession* es = RedisGears_GetPrivateData(ectx);
    RedisModule_Assert(es);
    jobject obj = JVM_ObjectDeserializeInternal(es->executionInputStream, br, &err, true);

    // record deserialization can not failed
    if(!obj){
        RedisModule_Log(NULL, "warning", "Failed deserializing jvm object, error='%s'", err);
        RedisModule_Assert(false);
    }

    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;
    return &r->baseRecord;
}

static void JVM_OnUnregistered(FlatExecutionPlan* fep, void* arg){
    char* err = NULL;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, NULL);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);

    JVM_PushFrame(jvm_tld->env);

    jobject onUnregister = arg;
    JNIEnv *env = jvm_tld->env;

    (*env)->CallVoidMethod(env, onUnregister, gearsOnUnregisteredMethodId);
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured while running OnUnregister callback: %s", err);
        JVM_FREE(err);
    }

    // clean the thread ctx class loader just in case
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsCleanCtxClassLoaderMethodId);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed cleaning thread ctx class loader, error='%s'", err);
    }

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
}

static void JVM_OnRegistered(FlatExecutionPlan* fep, void* arg){
    char* err = NULL;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, NULL);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);

    JVM_PushFrame(jvm_tld->env);

    jobject onRegister = arg;
    JNIEnv *env = jvm_tld->env;

    const char* fepId = RedisGears_FepGetId(fep);
    jstring idJStr = (*env)->NewStringUTF(env, fepId);

    (*env)->CallVoidMethod(env, onRegister, gearsOnRegisteredMethodId, idJStr);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured while running OnRegister callback: %s", err);
        JVM_FREE(err);
    }

    // clean the thread ctx class loader just in case
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsCleanCtxClassLoaderMethodId);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Failed cleaning thread ctx class loader, error='%s'", err);
    }

    JVM_PopFrame(env);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
}

static void JVM_OnExecutionDone(ExecutionPlan* ctx, void* privateData){
    JVMExecutionSession* executionSession = privateData;
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;

    (*env)->DeleteGlobalRef(env, executionSession->executionInputStream);
    (*env)->DeleteGlobalRef(env, executionSession->executionOutputStream);

    JVM_FREE(executionSession);
}

static void JVM_OnStart(ExecutionCtx* ectx, void* arg){
    // create Object Reader and Object writer for the execution
    // set it on Execution PD

    char* err = NULL;

    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    if(RedisGears_ExecutionPlanIsLocal(ep)){
        // execution plan is local to the shard, there is no way it will ever
        // serialize or deserialize record. No need to create records input and
        // output stream
        return;
    }

    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    JNIEnv *env = jvm_tld->env;

    jobject inputStream = (*env)->CallStaticObjectMethod(env, gearsObjectInputStreamCls, gearsObjectInputStreamGetMethodId, s->session->sessionClsLoader);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Fatal error, failed creating inputStream for execution. error='%s'", err);
        RedisModule_Assert(false);
    }

    inputStream = JVM_TurnToGlobal(env, inputStream);

    jobject outputStream = (*env)->CallStaticObjectMethod(env, gearsObjectOutputStreamCls, gearsObjectOutputStreamGetMethodId);

    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Fatal error, failed creating outputStream for execution. error='%s'", err);
        RedisModule_Assert(false);
    }

    outputStream = JVM_TurnToGlobal(env, outputStream);

    JVMExecutionSession* executionSession = JVM_ALLOC(sizeof(*executionSession));

    executionSession->executionInputStream = inputStream;
    executionSession->executionOutputStream = outputStream;

    RedisGears_SetPrivateData(ectx, executionSession);

    RedisGears_AddOnDoneCallback(ep, JVM_OnExecutionDone, executionSession);

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
}

static void JVM_OnUnpaused(ExecutionCtx* ectx, void* arg){
    JVMFlatExecutionSession* session = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(session->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    JNIEnv *env = jvm_tld->env;
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsBuilderOnUnpausedMethodId, session->session->sessionClsLoader);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured while running OnRegister callback: %s", err);
        JVM_FREE(err);
    }

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
}

typedef struct JVMReaderCtx{
    jobject reader;
    jobject iterator;
}JVMReaderCtx;

static Record* JVM_ReaderNext(ExecutionCtx* ectx, void* ctx){
    char* err = NULL;
    JVMReaderCtx* readerCtx = ctx;
    JVMFlatExecutionSession* s = RedisGears_GetFlatExecutionPrivateData(ectx);
    JVM_ExecutionCtx jectx = JVM_ExecutionCtxInit(s->session, ectx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(&jectx);
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    if(!readerCtx->iterator){
        readerCtx->iterator = (*env)->CallObjectMethod(env, readerCtx->reader, iteratorMethodId);
        if((err = JVM_GetException(env))){
            goto error;
        }
        readerCtx->iterator = JVM_TurnToGlobal(env, readerCtx->iterator);
    }

    jobject obj = NULL;

    bool hasNext = (*env)->CallBooleanMethod(env, readerCtx->iterator, iteratorHasNextMethodId);

    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!hasNext){
        JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
        JVM_PopFrame(env);
        return NULL;
    }

    obj = (*env)->CallObjectMethod(env, readerCtx->iterator, iteratorNextMethodId);

    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!obj){
        err = JVM_STRDUP("Got NULL object on reader");
        goto error;
    }

    obj = JVM_TurnToGlobal(env, obj);
    JVMRecord* record = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    record->obj = obj;

    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);

    JVM_PopFrame(env);
    return &record->baseRecord;

error:
    RedisGears_SetError(ectx, err);
    JVM_ThreadLocalDataRestor(jvm_tld, &jectx);
    JVM_PopFrame(env);
    return NULL;
}

static void JVM_ReaderFree(void* ctx){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;
    JVMReaderCtx* readerCtx = ctx;
    if(readerCtx->reader){
        (*env)->DeleteGlobalRef(env, readerCtx->reader);
    }
    if(readerCtx->iterator){
        (*env)->DeleteGlobalRef(env, readerCtx->iterator);
    }

    JVM_FREE(readerCtx);

}

static int JVM_ReaderSerialize(ExecutionCtx* ectx, void* ctx, Gears_BufferWriter* bw){
    JVMReaderCtx* readerCtx = ctx;
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    char* err = NULL;
    int res = JVM_ObjectSerialize(RedisGears_GetFep(ep), readerCtx->reader, bw, &err);
    if(err){
        RedisGears_SetError(ectx, err);
    }
    return res;
}

static int JVM_ReaderDeserialize(ExecutionCtx* ectx, void* ctx, Gears_BufferReader* br){
    JVMReaderCtx* readerCtx = ctx;
    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ectx);
    char* err = NULL;
    readerCtx->reader = JVM_ObjectDeserialize(RedisGears_GetFep(ep), br, JOBJECT_TYPE_VERSION, &err);
    if(!readerCtx->reader){
        if(err){
            err = JVM_STRDUP("Failed deserialize reader");
        }
        RedisGears_SetError(ectx, err);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}



static Reader* JVM_CreateReader(void* arg){
    JVMReaderCtx* readerCtx = JVM_ALLOC(sizeof(*readerCtx));
    readerCtx->iterator = NULL;
    if(arg){
        readerCtx->reader = arg;
    }
    Reader* reader = JVM_ALLOC(sizeof(*reader));
    *reader = (Reader){
        .ctx = readerCtx,
        .next = JVM_ReaderNext,
        .free = JVM_ReaderFree,
//        .reset = JVM_ReaderReset,
        .serialize = JVM_ReaderSerialize,
        .deserialize = JVM_ReaderDeserialize,
    };
    return reader;
}

RedisGears_ReaderCallbacks JavaReader = {
        .create = JVM_CreateReader,
};

static void JVM_OnLoadedEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data){
    if(subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
            subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
            subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START){
        // we will create a new empty session dictionary, when we start loading we want a fresh
        // start. Otherwise old sessions might mixed with new loaded session.
        pthread_mutex_lock(&JVMSessionsLock);
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(JVMSessions, "^", NULL, 0);

        char *key;
        size_t keyLen;
        JVMRunSession* session;
        while((key = RedisModule_DictNextC(iter,&keyLen,(void**)&session))) {
            JVM_listAddNodeHead(JVMTSSessions, session);
            session->tsNode = JVM_listFirst(JVMTSSessions);
        }

        RedisModule_DictIteratorStop(iter);

        RedisModule_FreeDict(ctx, JVMSessions);

        JVMSessions = RedisModule_CreateDict(ctx);

        pthread_mutex_unlock(&JVMSessionsLock);
    }
}

int RedisGears_OnLoad(RedisModuleCtx *ctx) {
    if(!RedisGears_InitAsGearPlugin(ctx, REDISGEARSJVM_PLUGIN_NAME, REDISGEARSJVM_PLUGIN_VERSION)){
        RedisModule_Log(ctx, "warning", "Failed initialize RedisGears API");
        return REDISMODULE_ERR;
    }

    if(RMAPI_FUNC_SUPPORTED(RedisModule_GetDetachedThreadSafeContext)){
        staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);
    }else{
        staticCtx = RedisModule_GetThreadSafeContext(NULL);
    }

    RedisGears_RegisterLoadingEvent(JVM_OnLoadedEvent);

    recordBuff = RedisGears_BufferCreate(100);
    int err = pthread_key_create(&threadLocalData, NULL);
    if(err){
        return REDISMODULE_ERR;
    }

    // this will initialize the jvm
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    if(!jvm_tld){
        RedisModule_Log(ctx, "warning", "Failed initializing jvm.");
        return REDISMODULE_ERR;
    }

    pthread_mutex_init(&JVMSessionsLock, NULL);
    JVMSessions = RedisModule_CreateDict(ctx);
    JVMTSSessions = JVM_listCreate();

    JVMRecordType = RedisGears_RecordTypeCreate("JVMRecord",
                                                sizeof(JVMRecord),
                                                JVMRecord_SendReply,
                                                JVMRecord_Serialize,
                                                JVMRecord_Deserialize,
                                                JVMRecord_Free);

    ArgType* jvmObjectType = RedisGears_CreateType("JVMObjectType",
                                                   JOBJECT_TYPE_VERSION,
                                                   JVM_ObjectFree,
                                                   JVM_ObjectDup,
                                                   JVM_ObjectSerialize,
                                                   JVM_ObjectDeserialize,
                                                   JVM_ObjectToString);

    jvmSessionType = RedisGears_CreateType(JVM_SESSION_TYPE_NAME,
                                           JSESSION_TYPE_VERSION,
                                           JVM_FepSessionFree,
                                           JVM_FepSessionDup,
                                           JVM_FepSessionSerialize,
                                           JVM_FepSessionDeserialize,
                                           JVM_FepSessionToString);

    RedisGears_RegisterFlatExecutionPrivateDataType(jvmSessionType);

    RGM_KeysReaderRegisterReadRecordCallback(JVM_KeyReaderReadRecord);

    RGM_RegisterFlatExecutionOnRegisteredCallback(JVM_OnRegistered, jvmObjectType);
    RGM_RegisterFlatExecutionOnUnregisteredCallback(JVM_OnUnregistered, jvmObjectType);
    RGM_RegisterExecutionOnUnpausedCallback(JVM_OnUnpaused, jvmObjectType);
    RGM_RegisterExecutionOnStartCallback(JVM_OnStart, jvmObjectType);

    RGM_RegisterMap(JVM_ToJavaRecordMapper, NULL);
    RGM_RegisterMap(JVM_Mapper, jvmObjectType);
    RGM_RegisterMap(JVM_FlatMapper, jvmObjectType);
    RGM_RegisterGroupByExtractor(JVM_Extractor, jvmObjectType);
    RGM_RegisterAccumulatorByKey(JVM_AccumulateByKey, jvmObjectType);
    RGM_RegisterForEach(JVM_Foreach, jvmObjectType);
    RGM_RegisterFilter(JVM_Filter, jvmObjectType);
    RGM_RegisterAccumulator(JVM_Accumulate, jvmObjectType);

    RGM_RegisterReader(JavaReader);

#define NUM_OF_THREADS 3
#define JVM_THREAD_POOL_NAME "JVMPool"
    JVM_ThreadPool* pool = JVM_ThreadPoolCreate(NUM_OF_THREADS);
    jvmExecutionPool = RedisGears_ExecutionThreadPoolDefine("JVMPool", pool, JVM_ThreadPoolAddJob);

    JVM_GetShardUniqueId();
    JVM_GetWorkingDir();
    JVM_asprintf(&jarsDir, "%s/%s-jars", workingDir, shardUniqueId);
    int ret = RedisGears_ExecuteCommand(NULL, "notice", "mkdir -p %s", jarsDir);
    if(ret != 0){
        RedisModule_Log(ctx, "warning", "Failed creating jars direcotry os %s", jarsDir);
        return REDISMODULE_ERR;
    }
    ret = RedisGears_ExecuteCommand(NULL, "notice", "rm -rf %s/*", jarsDir);

    if(ret != 0){
        RedisModule_Log(ctx, "warning", "Failed cleaning jars direcotry os %s", jarsDir);
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.jexecute", JVM_Run, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.jexecute");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.jdumpsessions", JVM_DumpSessions, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.jdumpsessions");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.jrungc", JVM_RunGC, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.jrungc");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.jdumpheap", JVM_DumpHeap, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.jdumpheap");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rg.jstats", JVM_JVMStats, "readonly", 0, 0, 0) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "could not register command rg.jstats");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
