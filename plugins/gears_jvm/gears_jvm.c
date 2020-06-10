#include <jni.h>       /* where everything is defined */
#include <stdbool.h>
#include "redismodule.h"
#include "redisgears.h"
#include "version.h"
#include "utils/adlist.h"

#include <pthread.h>

#define JOBJECT_TYPE_VERSION 1
#define JSESSION_TYPE_VERSION 1

#define JVM_SESSION_TYPE_NAME "JVMSessionType"

typedef struct JVM_ThreadLocalData JVM_ThreadLocalData;
typedef struct JVMRunSession JVMRunSession;

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader);
static void JVM_GBDestroy(JNIEnv *env, jobject objectOrClass);
static jobject JVM_GBMap(JNIEnv *env, jobject objectOrClass, jobject mapper);
static void JVM_GBRun(JNIEnv *env, jobject objectOrClass, jobject reader);
static jobject JVM_GBExecute(JNIEnv *env, jobject objectOrClass, jobjectArray command);
static void JVM_GBLog(JNIEnv *env, jobject objectOrClass, jstring msg, jobject logLevel);
static jstring JVM_GBHashtag(JNIEnv *env, jobject objectOrClass);
static jstring JVM_GBConfigGet(JNIEnv *env, jobject objectOrClass, jstring key);
static void JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader, jobject jmode, jobject onRegistered);
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
static JVM_ThreadLocalData* JVM_GetThreadLocalData(JVMRunSession* s);
static JVMRunSession* JVM_SessionCreate(const char* id, const char* jarBytes, size_t len, char** err);
static void JVM_ThreadPoolWorkerHelper(JNIEnv *env, jobject objectOrClass, jlong ctx);

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

typedef struct JVMExecutionSession{
    jobject executionInputStream;
    jobject executionOutputStream;
}JVMExecutionSession;

typedef struct JVMRunSession{
    size_t refCounf;
    char uuid[ID_LEN];
    char uuidStr[STR_ID_LEN];
    char* jarFilePath;
    jobject sessionClsLoader;
    jobject sessionObjectInputStream;
    jobject sessionObjectOutputStream;
}JVMRunSession;

typedef struct JVM_ThreadLocalData{
    JNIEnv *env;
    RedisModuleCtx* rctx;
    bool isBlocked;
    bool allowBlock;
    JVMRunSession* currSession;
}JVM_ThreadLocalData;

RedisModuleDict* JVMSessions = NULL;

pthread_key_t threadLocalData;

JavaVM *jvm = NULL;       /* denotes a Java VM */

jfieldID ptrFieldId = NULL;

jclass gearsObjectCls = NULL;
jclass gearsStringCls = NULL;
jclass gearsClassCls = NULL;
jmethodID gearsClassGetNameMethodId = NULL;

jclass gearsLongCls = NULL;
jmethodID gearsLongValueOfMethodId = NULL;

jclass gearsBuilderCls = NULL;
jmethodID gearsBuilderSerializeObjectMethodId = NULL;
jmethodID gearsBuilderDeserializeObjectMethodId = NULL;
jmethodID gearsBuilderOnUnpausedMethodId = NULL;
jmethodID gearsJNICallHelperMethodId = NULL;

jclass gearsObjectInputStreamCls = NULL;
jmethodID gearsObjectInputStreamGetMethodId = NULL;
jclass gearsObjectOutputStreamCls = NULL;
jmethodID gearsObjectOutputStreamGetMethodId = NULL;

jclass gearsClassLoaderCls = NULL;
jmethodID gearsClassLoaderNewMid = NULL;

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
jmethodID gearsOnRegisteredMethodId = NULL;

jclass baseRecordCls = NULL;
jmethodID baseRecordToStr = NULL;

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
jfieldID commandReaderTriggerField = NULL;

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

JNINativeMethod nativeMethod[] = {
        {
            .name = "init",
            .signature = "(Ljava/lang/String;)V",
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
            .signature = "(Lgears/readers/BaseReader;Lgears/ExecutionMode;Lgears/operations/OnRegisteredOperation;)V",
            .fnPtr = JVM_GBRegister,
        },
        {
            .name = "executeArray",
            .signature = "([Ljava/lang/String;)Ljava/lang/Object;",
            .fnPtr = JVM_GBExecute,
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
    };

static void JVM_SessionFree(void* arg){
    JVMRunSession* s = arg;
    if(--s->refCounf == 0){
        RedisModule_DictDelC(JVMSessions, s->uuid, ID_LEN, NULL);
        int ret = RedisGears_ExecuteCommand(NULL, "verbose", "rm -rf %s", s->jarFilePath);
        if(ret != 0){
            RedisModule_Log(NULL, "warning", "Failed deleting session jar %s", s->jarFilePath);
        }

        JVM_FREE(s->jarFilePath);
        JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(s);
        JNIEnv *env = jvm_ltd->env;

        if(s->sessionClsLoader){
            (*env)->DeleteGlobalRef(env, s->sessionClsLoader);
        }

        if(s->sessionObjectInputStream){
            (*env)->DeleteGlobalRef(env, s->sessionObjectInputStream);
        }

        if(s->sessionObjectOutputStream){
            (*env)->DeleteGlobalRef(env, s->sessionObjectOutputStream);
        }

        JVM_FREE(s);
    }
}

static void* JVM_SessionDup(void* arg){
    JVMRunSession* s = arg;
    ++s->refCounf;
    return s;
}

static int JVM_SessionSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    JVMRunSession* s = arg;

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

static void* JVM_SessionDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > JSESSION_TYPE_VERSION){
        *err = JVM_STRDUP("Missmatch jvm session version, update to newest JVM module");
        return NULL;
    }
    size_t idLen;
    const char* id = RedisGears_BRReadBuffer(br, &idLen);
    RedisModule_Assert(idLen == ID_LEN);

    size_t dataLen;
    const char* data = RedisGears_BRReadBuffer(br, &dataLen);

    JVMRunSession* s = RedisModule_DictGetC(JVMSessions, (char*)id, idLen, NULL);
    if(!s){
        s = JVM_SessionCreate(id, data, dataLen, err);
    }else{
        s = JVM_SessionDup(s);
    }
    return s;
}

static char* JVM_SessionToString(void* arg){
    return JVM_STRDUP("java session");
}

static JVMRunSession* JVM_SessionCreate(const char* id, const char* jarBytes, size_t len, char** err){
    int ret = RedisGears_ExecuteCommand(NULL, "verbose", "mkdir -p %s/%s-jars", workingDir, shardUniqueId);
    if(ret != 0){
        *err = JVM_STRDUP("Failed create jar directory");
        return NULL;
    }

    JVMRunSession* s = JVM_ALLOC(sizeof(*s));
    RedisGears_GetShardUUID((char*)id, s->uuid, s->uuidStr, &sessionsId);

    s->refCounf = 1;

    s->sessionClsLoader = NULL;
    s->sessionObjectInputStream = NULL;
    s->sessionObjectOutputStream = NULL;

    JVM_asprintf(&s->jarFilePath, "%s/%s-jars/%s.jar", workingDir, shardUniqueId, s->uuidStr);

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
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(s);
    JNIEnv *env = jvm_ltd->env;

    jstring jarPath = (*env)->NewStringUTF(env, s->jarFilePath);

    jobject clsLoader = (*env)->CallStaticObjectMethod(env, gearsClassLoaderCls, gearsClassLoaderNewMid, jarPath);

    (*env)->DeleteLocalRef(env, jarPath);

    if((*err = JVM_GetException(env))){
        JVM_SessionFree(s);
        return NULL;
    }

    s->sessionClsLoader = JVM_TurnToGlobal(env, clsLoader);

    s->sessionObjectInputStream = (*env)->CallStaticObjectMethod(env, gearsObjectInputStreamCls, gearsObjectInputStreamGetMethodId, s->sessionClsLoader);

    if((*err = JVM_GetException(env))){
        JVM_SessionFree(s);
        return NULL;
    }

    s->sessionObjectInputStream = JVM_TurnToGlobal(env, s->sessionObjectInputStream);

    s->sessionObjectOutputStream = (*env)->CallStaticObjectMethod(env, gearsObjectOutputStreamCls, gearsObjectOutputStreamGetMethodId);

    if((*err = JVM_GetException(env))){
        JVM_SessionFree(s);
        return NULL;
    }

    s->sessionObjectOutputStream = JVM_TurnToGlobal(env, s->sessionObjectOutputStream);

    RedisModule_DictSetC(JVMSessions, s->uuid, ID_LEN, s);

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

static JavaVMOption* JVM_GetJVMOptions(char** jvmOptionsString){
    JavaVMOption* options = array_new(JavaVMOption, 10);
    *jvmOptionsString = JVM_STRDUP(RedisGears_GetConfig(JVM_OPTIONS_CONFIG));
    if(!*jvmOptionsString){
        return options;
    }
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

    JavaVMOption option;
    option.optionString = "-Xrs";
    options = array_append(options, option);

    return options;
}

static JVM_ThreadLocalData* JVM_GetThreadLocalData(JVMRunSession* s){
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
            JNI_CreateJavaVM(&jvm, (void**)&jvm_tld->env, &vm_args);

            array_free(options);
            JVM_FREE(jvmOptionsString);

            // register native functions
            JVM_TryFindClass(jvm_tld->env, "java/lang/Object", gearsObjectCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/String", gearsStringCls);

            JVM_TryFindClass(jvm_tld->env, "java/lang/Class", gearsClassCls);
            JVM_TryFindMethod(jvm_tld->env, gearsClassCls, "getName", "()Ljava/lang/String;", gearsClassGetNameMethodId);


            JVM_TryFindClass(jvm_tld->env, "java/lang/Long", gearsLongCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsLongCls, "valueOf", "(J)Ljava/lang/Long;", gearsLongValueOfMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsBuilder", gearsBuilderCls);

            jint res = (*jvm_tld->env)->RegisterNatives(jvm_tld->env, gearsBuilderCls, nativeMethod, sizeof(nativeMethod)/sizeof(JNINativeMethod));

            if(res != JNI_OK){
                (*jvm_tld->env)->ExceptionDescribe(jvm_tld->env);
                RedisModule_Log(NULL, "warning", "could not initialize GearsBuilder natives");
                return NULL;
            }

            JVM_TryFindClass(jvm_tld->env, "gears/GearsObjectInputStream", gearsObjectInputStreamCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsObjectInputStreamCls, "getGearsObjectInputStream", "(Ljava/lang/ClassLoader;)Lgears/GearsObjectInputStream;", gearsObjectInputStreamGetMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsObjectOutputStream", gearsObjectOutputStreamCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsObjectOutputStreamCls, "getGearsObjectOutputStream", "()Lgears/GearsObjectOutputStream;", gearsObjectOutputStreamGetMethodId);

            JVM_TryFindField(jvm_tld->env, gearsBuilderCls, "ptr", "J", ptrFieldId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsClassLoader", gearsClassLoaderCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsClassLoaderCls, "getNew", "(Ljava/lang/String;)Ljava/net/URLClassLoader;", gearsClassLoaderNewMid);

            JVM_TryFindClass(jvm_tld->env, "java/lang/ClassLoader", javaClassLoaderCls);
            JVM_TryFindMethod(jvm_tld->env, javaClassLoaderCls, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;", javaLoadClassNewMid);

            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "serializeObject", "(Ljava/lang/Object;Lgears/GearsObjectOutputStream;)[B", gearsBuilderSerializeObjectMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "deserializeObject", "([BLgears/GearsObjectInputStream;)Ljava/lang/Object;", gearsBuilderDeserializeObjectMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "onUnpaused", "(Ljava/lang/ClassLoader;)V", gearsBuilderOnUnpausedMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "jniCallHelper", "(J)V", gearsJNICallHelperMethodId);

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
            JVM_TryFindMethod(jvm_tld->env, gearsOnRegisteredCls, "onRegistered", "()V", gearsOnRegisteredMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/records/BaseRecord", baseRecordCls);

            JVM_TryFindMethod(jvm_tld->env, baseRecordCls, "toString", "()Ljava/lang/String;", baseRecordToStr);

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
    // NULL means do not touch the values
    if(s){
        jvm_tld->currSession = s;
        jvm_tld->rctx = NULL;
        jvm_tld->isBlocked = false;
        jvm_tld->allowBlock = false;
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
    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

    jclass clazz = (*env)->GetObjectClass(env, e);
    jmethodID getMessage = (*env)->GetMethodID(env, clazz,
                                               "getMessage",
                                               "()Ljava/lang/String;");
    jstring message = (jstring)(*env)->CallObjectMethod(env, e, getMessage);
    char* err = NULL;
    if(message){
        const char *mstr = (*env)->GetStringUTFChars(env, message, JNI_FALSE);
        err = JVM_STRDUP(mstr);
        (*env)->ReleaseStringUTFChars(env, message, mstr);
        (*env)->DeleteLocalRef(env, message);
    }else{
        // Call the getName() to get a jstring object back
        jstring strObj = (jstring)(*env)->CallObjectMethod(env, clazz, gearsClassGetNameMethodId);

        if(strObj){

            // Now get the c string from the java jstring object
            const char* str = (*env)->GetStringUTFChars(env, strObj, JNI_FALSE);

            // Print the class name
            err = JVM_STRDUP(str);

            // Release the memory pinned char array
            (*env)->ReleaseStringUTFChars(env, strObj, str);
            (*env)->DeleteLocalRef(env, strObj);
        }

    }
    if(!err){
        err = JVM_STRDUP("Exception occured during execution");
    }
    (*env)->DeleteLocalRef(env, clazz);
    (*env)->DeleteLocalRef(env, e);
    RedisModule_Assert(err);

    e = (*env)->ExceptionOccurred(env);
    if(e){
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        (*env)->DeleteLocalRef(env, e);
    }

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

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader){
    const char* reader = (*env)->GetStringUTFChars(env, strReader, JNI_FALSE);
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

    JVM_ThreadLocalData* tld = JVM_GetThreadLocalData(NULL);
    RedisGears_SetFlatExecutionPrivateData(fep, JVM_SESSION_TYPE_NAME, JVM_SessionDup(tld->currSession));

    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);

    (*env)->SetLongField(env, objectOrClass, ptrFieldId, (jlong)fep);
}

static void JVM_GBDestroy(JNIEnv *env, jobject objectOrClass){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    RedisGears_FreeFlatExecution(fep);
}

static jobject JVM_GBFilter(JNIEnv *env, jobject objectOrClass, jobject filter){
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
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    foreach = JVM_TurnToGlobal(env, foreach);
    RGM_ForEach(fep, JVM_Foreach, foreach);
    return objectOrClass;
}

static jobject JVM_GBAccumulate(JNIEnv *env, jobject objectOrClass, jobject accumulator){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_Accumulate(fep, JVM_Accumulate, accumulator);
    return objectOrClass;
}

static jobject JVM_GBLocalAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_LocalAccumulateBy(fep, JVM_Extractor, extractor, JVM_AccumulateByKey, accumulator);
    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);
    return objectOrClass;
}

static jobject JVM_GBRepartition(JNIEnv *env, jobject objectOrClass, jobject extractor){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    RGM_Repartition(fep, JVM_Extractor, extractor);
    return objectOrClass;
}

static jobject JVM_GBAccumulateby(JNIEnv *env, jobject objectOrClass, jobject extractor, jobject accumulator){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    extractor = JVM_TurnToGlobal(env, extractor);
    accumulator = JVM_TurnToGlobal(env, accumulator);
    RGM_AccumulateBy(fep, JVM_Extractor, extractor, JVM_AccumulateByKey, accumulator);
    RGM_Map(fep, JVM_ToJavaRecordMapper, NULL);
    return objectOrClass;
}

static jobject JVM_GBFlatMap(JNIEnv *env, jobject objectOrClass, jobject mapper){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    mapper = JVM_TurnToGlobal(env, mapper);
    RGM_FlatMap(fep, JVM_FlatMapper, mapper);
    return objectOrClass;
}

static jobject JVM_GBMap(JNIEnv *env, jobject objectOrClass, jobject mapper){
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

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, JNI_FALSE);
    const char* startIdStr = (*env)->GetStringUTFChars(env, startId, JNI_FALSE);

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
    size_t valCStrLen = 7;
    const char* valCStr = "unknown";
    if(((robj*)value)->encoding == 8){
        valCStr = RedisModule_StringPtrLen(value, &valCStrLen);
    }
    RedisGears_BWWriteBuffer(bw, fieldCStr, fieldCStrLen);
    RedisGears_BWWriteBuffer(bw, valCStr, valCStrLen);
}

static jobject JVM_GetSerializedVal(JNIEnv *env, RedisModuleKey* keyPtr, Gears_Buffer* buff){
    Gears_BufferWriter bw;
    RedisGears_BufferWriterInit(&bw, buff);
    if(keyPtr == NULL){
        RedisGears_BWWriteLong(&bw, -1);
    }else{
        int keyType = RedisModule_KeyType(keyPtr);
        RedisGears_BWWriteLong(&bw, keyType);
        if(keyType == REDISMODULE_KEYTYPE_HASH){
            RedisModuleScanCursor* hashCursor = RedisModule_ScanCursorCreate();
            while(RedisModule_ScanKey(keyPtr, hashCursor, JVM_ScanKeyCallback, &bw));
            RedisModule_ScanCursorDestroy(hashCursor);
        }
        if(keyType == REDISMODULE_KEYTYPE_STRING){
            size_t len;
            const char* val = RedisModule_StringDMA(keyPtr, &len, REDISMODULE_READ);
            RedisGears_BWWriteBuffer(&bw, val, len);
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
            tmpPtr = RedisModule_OpenKey(rctx, key, REDISMODULE_READ);
        }
        serializedValue = JVM_GetSerializedVal(env, tmpPtr, recordBuff);
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

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, JNI_FALSE);

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
    }
    (*env)->ThrowNew(env, exceptionCls, "Given reader does not exists");
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

void* JVM_CreateRegisterCommandReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    jclass readerCls = (*env)->GetObjectClass(env, reader);
    if(!(*env)->IsSameObject(env, readerCls, gearsCommandReaderCls)){
        (*env)->ThrowNew(env, exceptionCls, "Reader was changed!!!! Stop hacking!!!!");
        return NULL;
    }

    jobject trigger = (*env)->GetObjectField(env, reader, commandReaderTriggerField);

    if(!trigger){
        (*env)->ThrowNew(env, exceptionCls, "command reader trigger must be set");
        return NULL;
    }

    const char* triggerStr = (*env)->GetStringUTFChars(env, trigger, JNI_FALSE);

    CommandReaderTriggerArgs* triggerArgs = RedisGears_CommandReaderTriggerArgsCreate(triggerStr);

    (*env)->ReleaseStringUTFChars(env, trigger, triggerStr);

    return triggerArgs;
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

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, JNI_FALSE);


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

    const char* patternStr = (*env)->GetStringUTFChars(env, pattern, JNI_FALSE);

    jboolean readValues = (*env)->GetBooleanField(env, reader, keysReaderReadValuesField);

    jobject jkeyTypes = (*env)->GetObjectField(env, reader, keysReaderKeyTypesField);

    int* keyTypes = NULL;
    if(jkeyTypes){
        jsize jkeyTypesLen = (*env)->GetArrayLength(env, jkeyTypes);
        if(jkeyTypesLen > 0){
            keyTypes = array_new(int, jkeyTypesLen);
            for(size_t i = 0 ; i < jkeyTypesLen ; ++i){
                jobject jkey = (*env)->GetObjectArrayElement(env, jkeyTypes, i);
                const char* jkeyStr = (*env)->GetStringUTFChars(env, jkey, JNI_FALSE);
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
                const char* jeventStr = (*env)->GetStringUTFChars(env, jevent, JNI_FALSE);
                eventTypes = array_append(eventTypes, JVM_STRDUP(jeventStr));
                (*env)->ReleaseStringUTFChars(env, jevent, jeventStr);
            }
        }
    }

    KeysReaderTriggerArgs* triggerArgsCtx = RedisGears_KeysReaderTriggerArgsCreate(patternStr, eventTypes, keyTypes, readValues);

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
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData(NULL);
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    char* err = NULL;
    void* krCtx = JVM_CreateRunReaderArgs(env, fep, reader);
    if(!krCtx){
        return;
    }
    ExecutionPlan* ep = RedisGears_Run(fep, ExecutionModeAsync, krCtx, NULL, NULL, NULL, &err);
    if(!ep){
        if(!err){
            err = JVM_STRDUP("Error occured when tried to create execution");
        }
        (*env)->ThrowNew(env, exceptionCls, err);
        JVM_FREE(err);
        return;
    }
    if(jvm_ltd->allowBlock){
        RedisModuleBlockedClient *bc = RedisModule_BlockClient(jvm_ltd->rctx, NULL, NULL, NULL, 0);
        RedisGears_AddOnDoneCallback(ep, JVM_OnExecutionDoneCallback, bc);
        jvm_ltd->isBlocked = true;
    }
}

static jobject JVM_GBExecuteParseReply(JNIEnv *env, RedisModuleCallReply *reply){
    if(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY){
        jobject ret = (*env)->NewObjectArray(env, RedisModule_CallReplyLength(reply), gearsObjectCls, NULL);
        for(size_t i = 0 ; i < RedisModule_CallReplyLength(reply) ; ++i){
            RedisModuleCallReply *subReply = RedisModule_CallReplyArrayElement(reply, i);
            jobject val = JVM_GBExecuteParseReply(env, subReply);
            (*env)->SetObjectArrayElement(env, ret, i, val);
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
        return ret;
    }
    return NULL;
}

static jstring JVM_GBConfigGet(JNIEnv *env, jobject objectOrClass, jstring key){
    if(!key){
        (*env)->ThrowNew(env, exceptionCls, "Got a NULL key on configGet function");
        return NULL;
    }

    const char* keyStr = (*env)->GetStringUTFChars(env, key, JNI_FALSE);

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
    const char* msgStr = (*env)->GetStringUTFChars(env, msg, JNI_FALSE);
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

static jobject JVM_GBExecute(JNIEnv *env, jobject objectOrClass, jobjectArray command){
    size_t len = (*env)->GetArrayLength(env, command);
    if(len == 0){
        (*env)->ThrowNew(env, exceptionCls, "No command given to execute");
        return NULL;
    }
    jstring c = (*env)->GetObjectArrayElement(env, command, 0);
    const char* cStr = (*env)->GetStringUTFChars(env, c, JNI_FALSE);

    RedisModuleString** args = array_new(RedisModuleString*, len);
    for(size_t i = 1 ; i < len ; ++i){
        jstring arg = (*env)->GetObjectArrayElement(env, command, i);
        const char* argStr = (*env)->GetStringUTFChars(env, arg, JNI_FALSE);
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

static void JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader, jobject jmode, jobject onRegistered){
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

    void* triggerCtx = JVM_CreateRegisterReaderArgs(env, fep, reader);
    if(!triggerCtx){
        return;
    }
    RGM_Register(fep, mode, triggerCtx, &err);
}

int JVM_Run(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 3){
        return RedisModule_WrongArity(ctx);
    }
    char* err = NULL;
    size_t clsNameLen;
    const char* clsName = RedisModule_StringPtrLen(argv[1], &clsNameLen);
    size_t bytesLen;
    const char* bytes = RedisModule_StringPtrLen(argv[2], &bytesLen);

    JVMRunSession* s = JVM_SessionCreate(NULL, bytes, bytesLen, &err);
    if(!s){
        if(!err){
            err = JVM_STRDUP("Failed creating session");
        }
        RedisModule_ReplyWithError(ctx, err);
        JVM_FREE(err);
        return REDISMODULE_OK;
    }

    // creating new class loader
    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData(s);

    jvm_ltd->rctx = ctx;
    jvm_ltd->allowBlock = true;
    JNIEnv *env = jvm_ltd->env;

    JVM_PushFrame(env);

    jstring clsNameJString = (*env)->NewStringUTF(env, clsName);

    jclass cls = (*env)->CallObjectMethod(env, jvm_ltd->currSession->sessionClsLoader, javaLoadClassNewMid, clsNameJString);

    if((err = JVM_GetException(env))){
        goto error;
    }

    jmethodID mid = (*env)->GetStaticMethodID(env, cls, "main", "()V");

    if((err = JVM_GetException(env))){
        goto error;
    }

    (*env)->CallStaticVoidMethod(env, cls, mid);

    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!jvm_ltd->isBlocked){
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    JVM_PopFrame(env);

    JVM_SessionFree(jvm_ltd->currSession);

    jvm_ltd->rctx = NULL;
    jvm_ltd->currSession = NULL;

    return REDISMODULE_OK;

error:
    RedisModule_ReplyWithError(ctx, err);
    JVM_FREE(err);
    JVM_PopFrame(env);

    jvm_ltd->rctx = NULL;
    jvm_ltd->currSession = NULL;

    return REDISMODULE_OK;
}

static jobject JVM_ToJavaRecordMapperInternal(ExecutionCtx* rctx, Record *data, void* arg){
    if(!data){
        return NULL;
    }

    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);
    JNIEnv *env = jvm_tld->env;
    jobject obj = NULL;
    if(RedisGears_RecordGetType(data) == RedisGears_GetHashSetRecordType()){
        obj = (*env)->NewObject(env, hashRecordCls, hashRecordCtor);
        Arr(char*) keys = RedisGears_HashSetRecordGetAllKeys(data);
        for(size_t i = 0 ; i < array_len(keys) ; ++i){
            jstring javaKeyStr = (*env)->NewStringUTF(env, keys[i]);
            Record* val = RedisGears_HashSetRecordGet(data, keys[i]);
            jobject jvmVal = JVM_ToJavaRecordMapperInternal(rctx, val, arg);
            (*env)->CallVoidMethod(env, obj, hashRecordSet, javaKeyStr, jvmVal);
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
        jobject jvmVal = JVM_ToJavaRecordMapperInternal(rctx, val, arg);
        (*env)->CallVoidMethod(env, obj, hashRecordSet, javaKeyStr, jvmVal);
    }else if(RedisGears_RecordGetType(data) == JVMRecordType){
        JVMRecord* jvmVal = (JVMRecord*)data;
        obj = jvmVal->obj;
    }else if(RedisGears_RecordGetType(data) == RedisGears_GetListRecordType()){
        obj = (*env)->NewObjectArray(env, RedisGears_ListRecordLen(data), gearsObjectCls, NULL);
        for(size_t i = 0 ; i < RedisGears_ListRecordLen(data) ; ++i){
            Record* temp = RedisGears_ListRecordGet(data, i);
            jobject jvmTemp = JVM_ToJavaRecordMapperInternal(rctx, temp, arg);
            (*env)->SetObjectArrayElement(env, obj, i, jvmTemp);
        }
    }else{
        RedisModule_Assert(false);
    }
    return obj;
}

static Record* JVM_ToJavaRecordMapper(ExecutionCtx* rctx, Record *data, void* arg){
    if(RedisGears_RecordGetType(data) == JVMRecordType){
        return data;
    }

    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

    JVM_PushFrame(jvm_tld->env);

    jobject obj = JVM_ToJavaRecordMapperInternal(rctx, data, arg);
    if(obj){
        obj = JVM_TurnToGlobal(jvm_tld->env, obj);
    }

    JVM_PopFrame(jvm_tld->env);

    RedisGears_FreeRecord(data);

    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;

    return &r->baseRecord;
}

static Record* JVM_AccumulateByKey(ExecutionCtx* rctx, char* key, Record *accumulate, Record *data, void* arg){
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);


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

    return &a->baseRecord;

error:
    RedisGears_FreeRecord(accumulate);
    RedisGears_FreeRecord(data);
    JVM_PopFrame(env);
    RedisGears_SetError(rctx, err);
    return NULL;
}

static Record* JVM_Accumulate(ExecutionCtx* rctx, Record *accumulate, Record *data, void* arg){
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

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

    return &a->baseRecord;

error:
    RedisGears_FreeRecord(accumulate);
    RedisGears_FreeRecord(data);
    RedisGears_SetError(rctx, err);
    JVM_PopFrame(env);
    return NULL;
}

static bool JVM_Filter(ExecutionCtx* rctx, Record *data, void* arg){
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

    JVMRecord* r = (JVMRecord*)data;
    jobject filter = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    jboolean res = (*env)->CallBooleanMethod(env, filter, gearsFilterMethodId, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisGears_SetError(rctx, err);
    }

    JVM_PopFrame(env);

    return res;
}

static void JVM_Foreach(ExecutionCtx* rctx, Record *data, void* arg){
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

    JVMRecord* r = (JVMRecord*)data;
    jobject foreach = arg;
    JNIEnv *env = jvm_tld->env;

    JVM_PushFrame(env);

    (*env)->CallVoidMethod(env, foreach, gearsForeachMethodId, r->obj);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisGears_SetError(rctx, err);
    }

    JVM_PopFrame(env);
}

static char* JVM_Extractor(ExecutionCtx* rctx, Record *data, void* arg, size_t* len){
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

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

    const char* resStr = (*env)->GetStringUTFChars(env, res, JNI_FALSE);;
    char* extractedData = JVM_STRDUP(resStr);

    (*env)->ReleaseStringUTFChars(env, res, resStr);

    JVM_PopFrame(env);

    return extractedData;

error:
    JVM_PopFrame(env);
    RedisGears_SetError(rctx, err);
    return NULL;
}

static Record* JVM_FlatMapper(ExecutionCtx* rctx, Record *data, void* arg){
    char* err = NULL;
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

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

    Record* listRecord = RedisGears_ListRecordCreate(20);
    while((*env)->CallBooleanMethod(env, iterator, iteratorHasNextMethodId)){
        jobject obj = (*env)->CallObjectMethod(env, iterator, iteratorNextMethodId);
        obj = JVM_TurnToGlobal(env, obj);
        JVMRecord* innerRecord = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
        innerRecord->obj = obj;
        RedisGears_ListRecordAdd(listRecord, (Record*)innerRecord);
    }

    RedisGears_FreeRecord(data);

    JVM_PopFrame(jvm_tld->env);
    return listRecord;

error:
    RedisGears_SetError(rctx, err);
    RedisGears_FreeRecord(data);
    JVM_PopFrame(jvm_tld->env);
    return NULL;
}

static Record* JVM_Mapper(ExecutionCtx* rctx, Record *data, void* arg){
    char* err = NULL;
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(rctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

    JVM_PushFrame(jvm_tld->env);

    JVMRecord* r = (JVMRecord*)data;
    jobject mapper = (arg);
    JNIEnv *env = jvm_tld->env;

    jobject res = (*env)->CallObjectMethod(env, mapper, gearsMapMethodId, r->obj);
    if((err = JVM_GetException(env))){
        goto error;
    }

    if(!res){
        err = JVM_STRDUP("Got null record on mapper");
        goto error;
    }

    res = JVM_TurnToGlobal(env, res);
    (*env)->DeleteGlobalRef(env, r->obj);
    r->obj = res;

    JVM_PopFrame(jvm_tld->env);

    return &r->baseRecord;

error:
    RedisGears_SetError(rctx, err);
    RedisGears_FreeRecord(data);
    JVM_PopFrame(jvm_tld->env);
    return NULL;
}

static int JVMRecord_SendReply(Record* base, RedisModuleCtx* rctx){
    JVMRecord* r = (JVMRecord*)base;
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;
    jobject res = (*env)->CallObjectMethod(env, r->obj, baseRecordToStr);
    const char* resStr = (*env)->GetStringUTFChars(env, res, JNI_FALSE);
    RedisModule_ReplyWithCString(rctx, resStr);
    (*jvm_tld->env)->ReleaseStringUTFChars(jvm_tld->env, res, resStr);
    (*jvm_tld->env)->DeleteLocalRef(jvm_tld->env, res);
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

static int JVM_ObjectSerializeInternal(jobject outputStream, void* arg, Gears_BufferWriter* bw, char** err){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);

    jobject obj = arg;
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderSerializeObjectMethodId, obj, outputStream);

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

static void* JVM_ObjectDeserializeInternal(jobject inputStream, Gears_BufferReader* br, char** err){
    size_t len;
    const char* buf = RedisGears_BRReadBuffer(br, &len);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->NewByteArray(env, len);

    (*env)->SetByteArrayRegion(env, bytes, 0, len, buf);

    jobject obj = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderDeserializeObjectMethodId, bytes, inputStream);

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

    JVMRunSession* session = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);

    return JVM_ObjectSerializeInternal(session->sessionObjectOutputStream, arg, bw, err);
}

static void* JVM_ObjectDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > JOBJECT_TYPE_VERSION){
        *err = JVM_STRDUP("Missmatch version, update to newest JVM module");
        return NULL;
    }

    JVMRunSession* currSession = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);

    return JVM_ObjectDeserializeInternal(currSession->sessionObjectInputStream, br, err);
}

static int JVMRecord_Serialize(ExecutionCtx* ectx, Gears_BufferWriter* bw, Record* base, char** err){
    JVMRecord* r = (JVMRecord*)base;
    JVMExecutionSession* es = RedisGears_GetPrivateData(ectx);
    RedisModule_Assert(es);
    return JVM_ObjectSerializeInternal(es->executionOutputStream, r->obj, bw, err);
}

static Record* JVMRecord_Deserialize(ExecutionCtx* ectx, Gears_BufferReader* br){
    char* err;
    JVMExecutionSession* es = RedisGears_GetPrivateData(ectx);
    RedisModule_Assert(es);
    jobject obj = JVM_ObjectDeserializeInternal(es->executionInputStream, br, &err);

    // record deserialization can not failed
    RedisModule_Assert(obj);
    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;
    return &r->baseRecord;
}

static void JVM_OnRegistered(FlatExecutionPlan* fep, void* arg){
    char* err = NULL;
    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);

    JVM_PushFrame(jvm_tld->env);

    jobject onRegister = arg;
    JNIEnv *env = jvm_tld->env;

    (*env)->CallVoidMethod(env, onRegister, gearsOnRegisteredMethodId);
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured while running OnRegister callback: %s", err);
        JVM_FREE(err);
    }

    JVM_PopFrame(env);
}

static void JVM_OnExecutionDone(ExecutionPlan* ctx, void* privateData){
    JVMExecutionSession* executionSession = privateData;
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(NULL);
    JNIEnv *env = jvm_tld->env;

    (*env)->DeleteGlobalRef(env, executionSession->executionInputStream);
    (*env)->DeleteGlobalRef(env, executionSession->executionOutputStream);

    JVM_FREE(executionSession);
}

static void JVM_OnStart(ExecutionCtx* ctx, void* arg){
    // create Object Reader and Object writer for the execution
    // set it on Execution PD

    char* err = NULL;

    ExecutionPlan* ep = RedisGears_GetExecutionFromCtx(ctx);
    if(RedisGears_ExecutionPlanIsLocal(ep)){
        // execution plan is local to the shard, there is no way it will ever
        // serialize or deserialize record. No need to create records input and
        // output stream
        return;
    }

    JVMRunSession* s = RedisGears_GetFlatExecutionPrivateData(ctx);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(s);
    JNIEnv *env = jvm_tld->env;

    jobject inputStream = (*env)->CallStaticObjectMethod(env, gearsObjectInputStreamCls, gearsObjectInputStreamGetMethodId, s->sessionClsLoader);

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

    RedisGears_SetPrivateData(ctx, executionSession);

    RedisGears_AddOnDoneCallback(ep, JVM_OnExecutionDone, executionSession);
}

static void JVM_OnUnpaused(ExecutionCtx* ctx, void* arg){
    JVMRunSession* session = RedisGears_GetFlatExecutionPrivateData(ctx);
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData(session);
    JNIEnv *env = jvm_tld->env;
    (*env)->CallStaticVoidMethod(env, gearsBuilderCls, gearsBuilderOnUnpausedMethodId, session->sessionClsLoader);

    char* err = NULL;
    if((err = JVM_GetException(env))){
        RedisModule_Log(NULL, "warning", "Exception occured while running OnRegister callback: %s", err);
        JVM_FREE(err);
    }
}

int RedisGears_OnLoad(RedisModuleCtx *ctx) {
    if(RedisGears_InitAsGearPlugin(ctx, REDISGEARSJVM_PLUGIN_NAME, REDISGEARSJVM_PLUGIN_VERSION) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Failed initialize RedisGears API");
        return REDISMODULE_ERR;
    }

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

    JVMSessions = RedisModule_CreateDict(ctx);

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
                                           JVM_SessionFree,
                                           JVM_SessionDup,
                                           JVM_SessionSerialize,
                                           JVM_SessionDeserialize,
                                           JVM_SessionToString);

    RedisGears_RegisterFlatExecutionPrivateDataType(jvmSessionType);

    RGM_KeysReaderRegisterReadRecordCallback(JVM_KeyReaderReadRecord);

    RGM_RegisterFlatExecutionOnRegisteredCallback(JVM_OnRegistered, jvmObjectType);
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

    return REDISMODULE_OK;
}
