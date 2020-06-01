#include <jni.h>       /* where everything is defined */
#include <stdbool.h>
#include "redismodule.h"
#include "redisgears.h"
#include "version.h"

#include <pthread.h>

#define JOBJECT_TYPE_VERSION 1

#define JVM_SESSION_TYPE_NAME "JVMSessionType"

typedef struct JVM_ThreadLocalData JVM_ThreadLocalData;
typedef struct JVMRunSession JVMRunSession;

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader);
static void JVM_GBDestroy(JNIEnv *env, jobject objectOrClass);
static jobject JVM_GBMap(JNIEnv *env, jobject objectOrClass, jobject mapper);
static void JVM_GBRun(JNIEnv *env, jobject objectOrClass, jobject reader);
static void JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader);
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
static JVM_ThreadLocalData* JVM_GetThreadLocalData();
static JVMRunSession* JVM_SessionCreate(const char* id, const char* jarBytes, size_t len, char** err);


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

typedef struct JVMRunSession{
    size_t refCounf;
    char uuid[ID_LEN];
    char uuidStr[STR_ID_LEN];
    char* jarFilePath;
    jobject sessionClsLoader;
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

jclass gearsBuilderCls = NULL;
jmethodID gearsBuilderSerializeObjectMethodId = NULL;
jmethodID gearsBuilderDeserializeObjectMethodId = NULL;

jclass gearsClassLoaderCls = NULL;
jmethodID gearsClassLoaderNewMid = NULL;

jclass javaClassLoaderCls = NULL;
jmethodID javaLoadClassNewMid = NULL;

jclass gearsMappCls = NULL;
jmethodID gearsMapMethodId = NULL;

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

jclass stringRecordCls = NULL;
jmethodID stringRecordCtor = NULL;

jclass hashRecordCls = NULL;
jmethodID hashRecordCtor = NULL;
jmethodID hashRecordSet = NULL;

jclass listRecordCls = NULL;
jmethodID listRecordGetMethodId = NULL;
jmethodID listRecordLenMethodId = NULL;

jclass gearsBaseReaderCls = NULL;
jfieldID keysBaseReaderModeField = NULL;
jfieldID keysBaseReaderOnRegisteredField = NULL;

jclass gearsKeyReaderCls = NULL;
jfieldID keysReaderPatternField = NULL;
jfieldID keysReaderNoscanField = NULL;
jfieldID keysReaderReadValuesField = NULL;
jfieldID keysReaderEventTypesField = NULL;
jfieldID keysReaderKeyTypesField = NULL;

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

jclass gearsStreamReaderFailedPolicyCls = NULL;
jclass gearsStreamReaderFailedPolicyContinueCls = NULL;
jclass gearsStreamReaderFailedPolicyAbortCls = NULL;
jclass gearsStreamReaderFailedPolicyRetryCls = NULL;

jclass exceptionCls = NULL;

RecordType* JVMRecordType = NULL;
ArgType* jvmSessionType = NULL;

JNINativeMethod nativeMethod[] = {
        {
            .name = "Init",
            .signature = "(Ljava/lang/String;)V",
            .fnPtr = JVM_GBInit,
        },
        {
            .name = "Destroy",
            .signature = "()V",
            .fnPtr = JVM_GBDestroy,
        },
        {
            .name = "Map",
            .signature = "(Lgears/operations/MapOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBMap,
        },
        {
            .name = "FlatMap",
            .signature = "(Lgears/operations/MapOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBFlatMap,
        },
        {
            .name = "AccumulateBy",
            .signature = "(Lgears/operations/ExtractorOperation;Lgears/operations/AccumulateByOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBAccumulateby,
        },
        {
            .name = "Repartition",
            .signature = "(Lgears/operations/ExtractorOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBRepartition,
        },
        {
            .name = "LocalAccumulateBy",
            .signature = "(Lgears/operations/ExtractorOperation;Lgears/operations/AccumulateByOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBLocalAccumulateby,
        },
        {
            .name = "Accumulate",
            .signature = "(Lgears/operations/AccumulateOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBAccumulate,
        },
        {
            .name = "Foreach",
            .signature = "(Lgears/operations/ForeachOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBForeach,
        },
        {
            .name = "Filter",
            .signature = "(Lgears/operations/FilterOperation;)Lgears/GearsBuilder;",
            .fnPtr = JVM_GBFilter,
        },
        {
            .name = "Collect",
            .signature = "()Lgears/GearsBuilder;",
            .fnPtr = JVM_GBCollect,
        },
        {
            .name = "InnerRun",
            .signature = "(Lgears/readers/BaseReader;)V",
            .fnPtr = JVM_GBRun,
        },
        {
            .name = "InnerRegister",
            .signature = "(Lgears/readers/BaseReader;)V",
            .fnPtr = JVM_GBRegister,
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
        JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData();
        JNIEnv *env = jvm_ltd->env;

        (*env)->DeleteGlobalRef(env, s->sessionClsLoader);

        JVM_FREE(s);
    }
}

static void* JVM_SessionDup(void* arg){
    JVMRunSession* s = arg;
    ++s->refCounf;
    return s;
}

static int JVM_SessionSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    JVMRunSession* s = arg;

    RedisGears_BWWriteBuffer(bw, s->uuid, ID_LEN);

    FILE *f = fopen(s->jarFilePath, "rb");
    if(!f){
        JVM_asprintf(err, "Could not open jar file %s", s->jarFilePath);
        RedisModule_Log(NULL, "warning", *err);
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
        RedisModule_Log(NULL, "warning", *err);
        return REDISMODULE_ERR;
    }
    fclose(f);

    RedisGears_BWWriteBuffer(bw, data, fsize);

    JVM_FREE(data);

    return REDISMODULE_OK;
}

static void* JVM_SessionDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
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
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData();
    JNIEnv *env = jvm_ltd->env;

    jstring jarPath = (*env)->NewStringUTF(env, s->jarFilePath);

    jobject clsLoader = (*env)->CallStaticObjectMethod(env, gearsClassLoaderCls, gearsClassLoaderNewMid, jarPath);

    (*env)->DeleteLocalRef(env, jarPath);

    if((*err = JVM_GetException(env))){
        JVM_SessionFree(s);
        return NULL;
    }

    s->sessionClsLoader = JVM_TurnToGlobal(env, clsLoader);

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
    return options;
}

static JVM_ThreadLocalData* JVM_GetThreadLocalData(){
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
            JVM_TryFindClass(jvm_tld->env, "gears/GearsBuilder", gearsBuilderCls);

            jint res = (*jvm_tld->env)->RegisterNatives(jvm_tld->env, gearsBuilderCls, nativeMethod, sizeof(nativeMethod)/sizeof(JNINativeMethod));

            if(res != JNI_OK){
                (*jvm_tld->env)->ExceptionDescribe(jvm_tld->env);
                RedisModule_Log(NULL, "warning", "could not initialize GearsBuilder natives");
                return NULL;
            }

            JVM_TryFindField(jvm_tld->env, gearsBuilderCls, "ptr", "J", ptrFieldId);

            JVM_TryFindClass(jvm_tld->env, "gears/GearsClassLoader", gearsClassLoaderCls);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsClassLoaderCls, "GetNew", "(Ljava/lang/String;)Ljava/net/URLClassLoader;", gearsClassLoaderNewMid);

            JVM_TryFindClass(jvm_tld->env, "java/lang/ClassLoader", javaClassLoaderCls);
            JVM_TryFindMethod(jvm_tld->env, javaClassLoaderCls, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;", javaLoadClassNewMid);

            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "SerializeObject", "(Ljava/lang/Object;)[B", gearsBuilderSerializeObjectMethodId);
            JVM_TryFindStaticMethod(jvm_tld->env, gearsBuilderCls, "DeserializeObject", "([BLjava/lang/ClassLoader;)Ljava/lang/Object;", gearsBuilderDeserializeObjectMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/MapOperation", gearsMappCls);
            JVM_TryFindMethod(jvm_tld->env, gearsMappCls, "Map", "(Lgears/records/BaseRecord;)Lgears/records/BaseRecord;", gearsMapMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/ExtractorOperation", gearsExtractorCls);
            JVM_TryFindMethod(jvm_tld->env, gearsExtractorCls, "Extract", "(Lgears/records/BaseRecord;)Ljava/lang/String;", gearsExtractorMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/ForeachOperation", gearsForeachCls);
            JVM_TryFindMethod(jvm_tld->env, gearsForeachCls, "Foreach", "(Lgears/records/BaseRecord;)V", gearsForeachMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/FilterOperation", gearsFilterCls);
            JVM_TryFindMethod(jvm_tld->env, gearsFilterCls, "Filter", "(Lgears/records/BaseRecord;)Z", gearsFilterMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/AccumulateOperation", gearsAccumulatorCls);
            JVM_TryFindMethod(jvm_tld->env, gearsAccumulatorCls, "Accumulate", "(Lgears/records/BaseRecord;Lgears/records/BaseRecord;)Lgears/records/BaseRecord;", gearsAccumulatorMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/AccumulateByOperation", gearsAccumulateByCls);
            JVM_TryFindMethod(jvm_tld->env, gearsAccumulateByCls, "Accumulateby", "(Ljava/lang/String;Lgears/records/BaseRecord;Lgears/records/BaseRecord;)Lgears/records/BaseRecord;", gearsAccumulateByMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/operations/OnRegisteredOperation", gearsOnRegisteredCls);
            JVM_TryFindMethod(jvm_tld->env, gearsOnRegisteredCls, "OnRegistered", "()V", gearsOnRegisteredMethodId);

            JVM_TryFindClass(jvm_tld->env, "gears/records/BaseRecord", baseRecordCls);

            JVM_TryFindMethod(jvm_tld->env, baseRecordCls, "toString", "()Ljava/lang/String;", baseRecordToStr);

            JVM_TryFindClass(jvm_tld->env, "gears/records/StringRecord", stringRecordCls);

            JVM_TryFindMethod(jvm_tld->env, stringRecordCls, "<init>", "(Ljava/lang/String;)V", stringRecordCtor);

            JVM_TryFindClass(jvm_tld->env, "gears/records/HashRecord", hashRecordCls);

            JVM_TryFindMethod(jvm_tld->env, hashRecordCls, "<init>", "()V", hashRecordCtor);

            JVM_TryFindMethod(jvm_tld->env, hashRecordCls, "Set", "(Ljava/lang/String;Lgears/records/BaseRecord;)V", hashRecordSet);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/BaseReader", gearsBaseReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsBaseReaderCls, "mode", "Lgears/readers/ExecutionMode;", keysBaseReaderModeField);
            JVM_TryFindField(jvm_tld->env, gearsBaseReaderCls, "onRegistered", "Lgears/operations/OnRegisteredOperation;", keysBaseReaderOnRegisteredField);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/KeysReader", gearsKeyReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "pattern", "Ljava/lang/String;", keysReaderPatternField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "noScan", "Z", keysReaderNoscanField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "readValues", "Z", keysReaderReadValuesField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "eventTypes", "[Ljava/lang/String;", keysReaderEventTypesField);
            JVM_TryFindField(jvm_tld->env, gearsKeyReaderCls, "keyTypes", "[Ljava/lang/String;", keysReaderKeyTypesField);

            JVM_TryFindClass(jvm_tld->env, "gears/readers/StreamReader", gearsStreamReaderCls);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "pattern", "Ljava/lang/String;", streamReaderPatternField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "startId", "Ljava/lang/String;", streamReaderStartIdField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "batchSize", "I", streamReaderBatchSizeField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "duration", "I", streamReaderDurationField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "failurePolicy", "Lgears/readers/StreamReader$FailurePolicy;", streamReaderFailurePolicyField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "failureRertyInterval", "I", streamReaderRetryIntervalField);
            JVM_TryFindField(jvm_tld->env, gearsStreamReaderCls, "trimStream", "Z", streamReaderTrimStreamField);

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

            JVM_TryFindClass(jvm_tld->env, "gears/readers/ExecutionMode", gearsExecutionModeCls);
            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "ASYNC", "Lgears/readers/ExecutionMode;");
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

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "SYNC", "Lgears/readers/ExecutionMode;");
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

            temp = (*jvm_tld->env)->GetStaticFieldID(jvm_tld->env, gearsExecutionModeCls, "ASYNC_LOCAL", "Lgears/readers/ExecutionMode;");
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


            JVM_TryFindClass(jvm_tld->env, "java/lang/Exception", exceptionCls);


            JVM_TryFindClass(jvm_tld->env, "gears/records/ListRecord", listRecordCls);
            JVM_TryFindMethod(jvm_tld->env, listRecordCls, "Get", "(I)Lgears/records/BaseRecord;", listRecordGetMethodId);
            JVM_TryFindMethod(jvm_tld->env, listRecordCls, "Len", "()I", listRecordLenMethodId);


        }else{
            JavaVMAttachArgs args;
            args.version = JNI_VERSION_10; // choose your JNI version
            args.name = NULL; // you might want to give the java thread a name
            args.group = NULL; // you might want to assign the java thread to a ThreadGroup

            (*jvm)->AttachCurrentThread(jvm, (void**)&jvm_tld->env, &args);
        }

        pthread_setspecific(threadLocalData, jvm_tld);
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
        jmethodID mid = (*env)->GetMethodID(env, clazz, "getName", "()Ljava/lang/String;");

        if(mid){

            // Call the getName() to get a jstring object back
            jstring strObj = (jstring)(*env)->CallObjectMethod(env, clazz, mid);

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
    }
    if(!err){
        err = JVM_STRDUP("Exception occured during execution");
    }
    (*env)->DeleteLocalRef(env, clazz);
    (*env)->DeleteLocalRef(env, e);
    RedisModule_Assert(err);
    return err;
}

static jobject JVM_TurnToGlobal(JNIEnv *env, jobject local){
    jobject global = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    return global;
}

static void JVM_GBInit(JNIEnv *env, jobject objectOrClass, jstring strReader){
    const char* reader = (*env)->GetStringUTFChars(env, strReader, JNI_FALSE);
    FlatExecutionPlan* fep = RedisGears_CreateCtx((char*)reader);

    JVM_ThreadLocalData* tld = JVM_GetThreadLocalData();
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

void* JVM_CreateRunKeyReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
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

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);

    return readerCtx;
}

void* JVM_CreateRunReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
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

    (*env)->ReleaseStringUTFChars(env, pattern, patternStr);

    return triggerArgsCtx;
}

void* JVM_CreateRegisterReaderArgs(JNIEnv *env, FlatExecutionPlan* fep, jobject reader){
    if(strcmp(RedisGears_GetReader(fep), "KeysReader") == 0){
        return JVM_CreateRegisterKeysReaderArgs(env, fep, reader);
    }else if(strcmp(RedisGears_GetReader(fep), "StreamReader") == 0){
        return JVM_CreateRegisterStreamReaderArgs(env, fep, reader);
    }
    (*env)->ThrowNew(env, exceptionCls, "Given reader does not exists");
    return NULL;
}

static void JVM_GBRun(JNIEnv *env, jobject objectOrClass, jobject reader){
    JVM_ThreadLocalData* jvm_ltd = JVM_GetThreadLocalData();
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    char* err = NULL;
    void* krCtx = JVM_CreateRunReaderArgs(env, fep, reader);
    if(!krCtx){
        return;
    }
    ExecutionPlan* ep = RGM_Run(fep, ExecutionModeAsync, krCtx, NULL, NULL, &err);
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

static void JVM_GBRegister(JNIEnv *env, jobject objectOrClass, jobject reader){
    FlatExecutionPlan* fep = (FlatExecutionPlan*)(*env)->GetLongField(env, objectOrClass, ptrFieldId);
    char* err = NULL;

    ExecutionMode mode = ExecutionModeAsync;
    jobject jmode = (*env)->GetObjectField(env, reader, keysBaseReaderModeField);
    if((*env)->IsSameObject(env, jmode, gearsExecutionModeAsync)){
        mode = ExecutionModeAsync;
    }else if((*env)->IsSameObject(env, jmode, gearsExecutionModeSync)){
        mode = ExecutionModeSync;
    }else if((*env)->IsSameObject(env, jmode, gearsExecutionModeAsyncLocal)){
        mode = ExecutionModeAsyncLocal;
    }else{
        RedisModule_Assert(false);
    }

    jobject onRegistered = (*env)->GetObjectField(env, reader, keysBaseReaderOnRegisteredField);
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
    JVM_ThreadLocalData* jvm_ltd= JVM_GetThreadLocalData();

    jvm_ltd->rctx = ctx;
    jvm_ltd->isBlocked = false;
    jvm_ltd->allowBlock = true;
    jvm_ltd->currSession = s;
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

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
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
        jstring javaStr = (*env)->NewStringUTF(env, str);
        obj = (*env)->NewObject(env, stringRecordCls, stringRecordCtor, javaStr);
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
    }else{
        RedisModule_Assert(false);
    }
    return obj;
}

static Record* JVM_ToJavaRecordMapper(ExecutionCtx* rctx, Record *data, void* arg){

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();


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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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

    if(!(*env)->IsInstanceOf(env, res, listRecordCls)){
        // normal mapping ..
        res = JVM_TurnToGlobal(env, res);
        (*env)->DeleteGlobalRef(env, r->obj);
        r->obj = res;

        JVM_PopFrame(jvm_tld->env);

        return &r->baseRecord;
    }

    size_t len = (*env)->CallIntMethod(env, res, listRecordLenMethodId);
    Record* listRecord = RedisGears_ListRecordCreate(len);
    for(size_t i = 0 ; i < len ; ++i){
        jobject obj = (*env)->CallObjectMethod(env, res, listRecordGetMethodId, i);
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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
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
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
    JNIEnv *env = jvm_tld->env;
    (*env)->DeleteGlobalRef(env, r->obj);
}

static void* JVM_ObjectDup(void* arg){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
    return (*jvm_tld->env)->NewGlobalRef(jvm_tld->env, (jobject)arg);
}

static void JVM_ObjectFree(void* arg){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
    (*jvm_tld->env)->DeleteGlobalRef(jvm_tld->env, (jobject)arg);
}

static char* JVM_ObjectToString(void* arg){
    return JVM_STRDUP("java object");
}

static int JVM_ObjectSerialize(void* arg, Gears_BufferWriter* bw, char** err){
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

    jobject obj = arg;
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderSerializeObjectMethodId, obj);

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

static void* JVM_ObjectDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    if(version > JOBJECT_TYPE_VERSION){
        *err = JVM_STRDUP("Missmatch version, update to newest JVM module");
        return NULL;
    }

    JVMRunSession* currSession = RedisGears_GetFlatExecutionPrivateDataFromFep(fep);

    size_t len;
    const char* buf = RedisGears_BRReadBuffer(br, &len);

    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
    JNIEnv *env = jvm_tld->env;

    jbyteArray bytes = (*env)->NewByteArray(env, len);

    (*env)->SetByteArrayRegion(env, bytes, 0, len, buf);

    jobject obj = (*env)->CallStaticObjectMethod(env, gearsBuilderCls, gearsBuilderDeserializeObjectMethodId, bytes, currSession->sessionClsLoader);

    (*env)->DeleteLocalRef(env, bytes);

    if((*err = JVM_GetException(env))){
        if(obj){
            (*env)->DeleteLocalRef(env, obj);
        }
        return NULL;
    }

    return JVM_TurnToGlobal(env, obj);
}

static int JVMRecord_Serialize(Gears_BufferWriter* bw, Record* base, char** err){
    JVMRecord* r = (JVMRecord*)base;
    return JVM_ObjectSerialize(r->obj, bw, err);
}

static Record* JVMRecord_Deserialize(FlatExecutionPlan* fep, Gears_BufferReader* br){
    char* err;
    jobject obj = JVM_ObjectDeserialize(fep, br, JOBJECT_TYPE_VERSION, &err);
    RedisModule_Assert(obj);
    JVMRecord* r = (JVMRecord*)RedisGears_RecordCreate(JVMRecordType);
    r->obj = obj;
    return &r->baseRecord;
}

static void JVM_OnRegistered(FlatExecutionPlan* fep, void* arg){
    char* err = NULL;
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();

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

int RedisGears_OnLoad(RedisModuleCtx *ctx) {
    if(RedisGears_InitAsGearPlugin(ctx, REDISGEARSJVM_PLUGIN_NAME, REDISGEARSJVM_PLUGIN_VERSION) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Failed initialize RedisGears API");
        return REDISMODULE_ERR;
    }

    int err = pthread_key_create(&threadLocalData, NULL);
    if(err){
        return REDISMODULE_ERR;
    }

    // this will initialize the jvm
    JVM_ThreadLocalData* jvm_tld = JVM_GetThreadLocalData();
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
                                           JOBJECT_TYPE_VERSION,
                                           JVM_SessionFree,
                                           JVM_SessionDup,
                                           JVM_SessionSerialize,
                                           JVM_SessionDeserialize,
                                           JVM_SessionToString);

    RedisGears_RegisterFlatExecutionPrivateDataType(jvmSessionType);

    RGM_RegisterFlatExecutionOnRegisteredCallback(JVM_OnRegistered, jvmObjectType);

    RGM_RegisterMap(JVM_ToJavaRecordMapper, NULL);
    RGM_RegisterMap(JVM_Mapper, jvmObjectType);
    RGM_RegisterMap(JVM_FlatMapper, jvmObjectType);
    RGM_RegisterGroupByExtractor(JVM_Extractor, jvmObjectType);
    RGM_RegisterAccumulatorByKey(JVM_AccumulateByKey, jvmObjectType);
    RGM_RegisterForEach(JVM_Foreach, jvmObjectType);
    RGM_RegisterFilter(JVM_Filter, jvmObjectType);
    RGM_RegisterAccumulator(JVM_Accumulate, jvmObjectType);

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
        RedisModule_Log(ctx, "warning", "could not register command gvm.run");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
