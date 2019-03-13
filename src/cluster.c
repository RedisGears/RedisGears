#include "cluster.h"
#include "redismodule.h"
#include "utils/dict.h"
#include "utils/adlist.h"
#include <stdlib.h>
#include <assert.h>
#include <event2/event.h>
#include <async.h>
#include <unistd.h>
#include <pthread.h>
#include <redisgears_memory.h>
#include "lock_handler.h"

#include <libevent.h>

#define MAX_SLOT 16384

typedef struct Node{
    char* id;
    char* ip;
    unsigned short port;
    char* password;
    char* unixSocket;
    redisAsyncContext *c;
}Node;

typedef struct Cluster{
    char* myId;
    bool isClusterMode;
    dict* nodes;
    Node* slots[MAX_SLOT];
}Cluster;

dict* RemoteCallbacks;

Cluster* CurrCluster = NULL;
int notify[2];
pthread_t messagesThread;
struct event_base *main_base = NULL;

typedef struct MsgArriveCtx{
    char* sender;
    char* msg;
    size_t msgLen;
    RedisModuleClusterMessageReceiver receiver;
}MsgArriveCtx;

pthread_t messagesArriveThread;
list* msgArriveList;
pthread_mutex_t msgArriveLock;
pthread_cond_t msgArriveCond;

static MsgArriveCtx* MsgArriveCtx_Create(const char* sender, const char* msg,
                                         size_t msgLen, RedisModuleClusterMessageReceiver receiver){
    MsgArriveCtx* ret = RG_ALLOC(sizeof(*ret));
    ret->sender = RG_STRDUP(sender);
    ret->receiver = receiver;
    ret->msgLen = msgLen;
    ret->msg = RG_ALLOC(sizeof(char) * (msgLen + 1));
    memcpy(ret->msg, msg, msgLen);
    ret->msg[msgLen] = '\0';
    return ret;
}

static void MsgArriveCtx_Free(MsgArriveCtx* ctx){
    RG_FREE(ctx->sender);
    RG_FREE(ctx->msg);
    RG_FREE(ctx);
}

typedef enum MsgType{
    SEND_MSG, CLUSTER_REFRESH_MSG, CLUSTER_SET_MSG
}MsgType;

typedef struct SendMsg{
    char idToSend[REDISMODULE_NODE_ID_LEN + 1];
    char* function;
    char* msg;
    size_t msgLen;
}SendMsg;

typedef struct ClusterRefreshMsg{
    RedisModuleBlockedClient* bc;
}ClusterRefreshMsg;

typedef struct ClusterSetMsg{
    RedisModuleBlockedClient* bc;
    RedisModuleString** argv;
    int argc;
}ClusterSetMsg;

typedef struct Msg{
    union{
        SendMsg sendMsg;
        ClusterRefreshMsg clusterRefresh;
        ClusterSetMsg clusterSet;
    };
    MsgType type;
}Msg;

static Node* GetNode(const char* id){
    dictEntry *entry = dictFind(CurrCluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = dictGetVal(entry);
    }
    return n;
}

static void FreeNode(Node* n){
    if(n->c){
        redisAsyncFree(n->c);
    }
    RG_FREE(n->id);
    RG_FREE(n->ip);
    if(n->unixSocket){
        RG_FREE(n->unixSocket);
    }
    RG_FREE(n);
}

static Node* CreateNode(const char* id, const char* ip, unsigned short port, const char* password, const char* unixSocket){
    assert(!GetNode(id));
    Node* n = RG_ALLOC(sizeof(*n));
    *n = (Node){
            .id = RG_STRDUP(id),
            .ip = RG_STRDUP(ip),
            .port = port,
            .password = password ? RG_STRDUP(password) : NULL,
            .unixSocket = unixSocket ? RG_STRDUP(unixSocket) : NULL,
            .c = NULL,
    };
    dictAdd(CurrCluster->nodes, n->id, n);
    return n;
}

static void Cluster_Free(){
    if(CurrCluster->isClusterMode){
        RG_FREE(CurrCluster->myId);
        dictIterator *iter = dictGetIterator(CurrCluster->nodes);
        dictEntry *entry = NULL;
        while((entry = dictNext(iter))){
            Node* n = dictGetVal(entry);
            FreeNode(n);
        }
        dictReleaseIterator(iter);
        dictRelease(CurrCluster->nodes);
    }

    RG_FREE(CurrCluster);
    CurrCluster = NULL;
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status);
static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status);

static void OnResponseArrived(struct redisAsyncContext* c, void* a, void* b){
//    printf("response arrived : %s:%d\r\n", c->c.tcp.host, c->c.tcp.port);
}

static void Cluster_ConnectToShard(Node* n){
    n->c = redisAsyncConnect(n->ip, n->port);
    if (n->c->err) {
        /* Let *c leak for now... */
        printf("Error: %s\n", n->c->errstr);
        //todo: handle this!!!
    }
    if(n->password){
        redisAsyncCommand(n->c, OnResponseArrived, NULL, "AUTH %s", n->password);
    }
    n->c->data = n;
    redisLibeventAttach(n->c, main_base);
    redisAsyncSetConnectCallback(n->c, Cluster_ConnectCallback);
    redisAsyncSetDisconnectCallback(n->c, Cluster_DisconnectCallback);
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status){
    printf("disconnected : %s:%d, status : %d, will try to reconnect.\r\n", c->c.tcp.host, c->c.tcp.port, status);
    Cluster_ConnectToShard((Node*)c->data);
}
static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status){
    printf("connected : %s:%d\r\n", c->c.tcp.host, c->c.tcp.port);
}

static void Cluster_ConnectToShards(){
    dictIterator *iter = dictGetIterator(CurrCluster->nodes);
    dictEntry *entry = NULL;
    while((entry = dictNext(iter))){
        Node* n = dictGetVal(entry);
        if(strcmp(n->id, CurrCluster->myId) == 0){
            continue;
        }
        Cluster_ConnectToShard(n);
    }
    dictReleaseIterator(iter);
}

static void Cluster_Set(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    if(CurrCluster){
        Cluster_Free();
    }

    CurrCluster = RG_ALLOC(sizeof(*CurrCluster));

    size_t myIdLen;
    const char* myId = RedisModule_StringPtrLen(argv[6], &myIdLen);
    CurrCluster->myId = RG_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
    memset(CurrCluster->myId, '0', zerosPadding);
    memcpy(CurrCluster->myId + zerosPadding, myId, myIdLen);
    CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';

    CurrCluster->nodes = dictCreate(&dictTypeHeapStrings, NULL);

    long long numOfRanges;
    assert(RedisModule_StringToLongLong(argv[8], &numOfRanges) == REDISMODULE_OK);

    CurrCluster->isClusterMode = numOfRanges > 1;

    for(size_t i = 9, j = 0 ; j < numOfRanges ; i += 8, ++j){
        size_t shardIdLen;
        const char* shardId = RedisModule_StringPtrLen(argv[i + 1], &shardIdLen);
        char realId[REDISMODULE_NODE_ID_LEN + 1];
        size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
        memset(realId, '0', zerosPadding);
        memcpy(realId + zerosPadding, shardId, myIdLen);
        realId[REDISMODULE_NODE_ID_LEN] = '\0';

        long long minslot;
        assert(RedisModule_StringToLongLong(argv[i + 3], &minslot) == REDISMODULE_OK);
        long long maxslot;
        assert(RedisModule_StringToLongLong(argv[i + 4], &maxslot) == REDISMODULE_OK);

        const char* addr = RedisModule_StringPtrLen(argv[i + 6], NULL);
        char* passEnd = strstr(addr, "@");
        size_t passSize = passEnd - addr;
        char password[passSize + 1];
        memcpy(password, addr, passSize);
        password[passSize] = '\0';

        addr = passEnd + 1;

        char* ipEnd = strstr(addr, ":");
        size_t ipSize = ipEnd - addr;
        char ip[ipSize + 1];
        memcpy(ip, addr, ipSize);
        ip[ipSize] = '\0';

        addr = ipEnd + 1;

        unsigned short port = (unsigned short)atoi(addr);

        Node* n = GetNode(realId);
        if(!n){
            n = CreateNode(realId, ip, port, password, NULL);
        }
        for(int i = minslot ; i <= maxslot ; ++i){
            CurrCluster->slots[i] = n;
        }

        if(j < numOfRanges - 1){
            // we are not at the last range
            const char* unixAdd = RedisModule_StringPtrLen(argv[i + 7], NULL);
            if(strcmp(unixAdd, "UNIXADDR") == 0){
                i += 2;
            }
        }
    }

    Cluster_ConnectToShards();
}

static void Cluster_Refresh(RedisModuleCtx* ctx){
    if(CurrCluster){
        Cluster_Free();
    }

    CurrCluster = RG_ALLOC(sizeof(*CurrCluster));

    if(!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        CurrCluster->isClusterMode = false;
        return;
    }

    CurrCluster->isClusterMode = true;

    CurrCluster->myId = RG_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    memcpy(CurrCluster->myId, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
    CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    CurrCluster->nodes = dictCreate(&dictTypeHeapStrings, NULL);

    RedisModuleCallReply *allSlotsRelpy = RedisModule_Call(ctx, "cluster", "c", "slots");
    assert(RedisModule_CallReplyType(allSlotsRelpy) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(allSlotsRelpy) ; ++i){
        RedisModuleCallReply *slotRangeRelpy = RedisModule_CallReplyArrayElement(allSlotsRelpy, i);

        RedisModuleCallReply *minslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 0);
        assert(RedisModule_CallReplyType(minslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long minslot = RedisModule_CallReplyInteger(minslotRelpy);

        RedisModuleCallReply *maxslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 1);
        assert(RedisModule_CallReplyType(maxslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long maxslot = RedisModule_CallReplyInteger(maxslotRelpy);

        RedisModuleCallReply *nodeDetailsRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 2);
        assert(RedisModule_CallReplyType(nodeDetailsRelpy) == REDISMODULE_REPLY_ARRAY);
        assert(RedisModule_CallReplyLength(nodeDetailsRelpy) == 3);
        RedisModuleCallReply *nodeipReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 0);
        RedisModuleCallReply *nodeportReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 1);
        RedisModuleCallReply *nodeidReply = RedisModule_CallReplyArrayElement(nodeDetailsRelpy, 2);
        size_t idLen;
        size_t ipLen;
        const char* id = RedisModule_CallReplyStringPtr(nodeidReply,&idLen);
        const char* ip = RedisModule_CallReplyStringPtr(nodeipReply,&ipLen);
        long long port = RedisModule_CallReplyInteger(nodeportReply);

        char nodeId[REDISMODULE_NODE_ID_LEN + 1];
        memcpy(nodeId, id, REDISMODULE_NODE_ID_LEN);
        nodeId[REDISMODULE_NODE_ID_LEN] = '\0';

        char nodeIp[ipLen + 1];
        memcpy(nodeIp, ip, ipLen);
        nodeIp[ipLen] = '\0';

        Node* n = GetNode(nodeId);
        if(!n){
            n = CreateNode(nodeId, nodeIp, (unsigned short)port, NULL, NULL);
        }
        for(int i = minslot ; i <= maxslot ; ++i){
            CurrCluster->slots[i] = n;
        }
    }
    RedisModule_FreeCallReply(allSlotsRelpy);

    Cluster_ConnectToShards();
}

static void Cluster_FreeMsg(Msg* msg){
    switch(msg->type){
    case SEND_MSG:
        RG_FREE(msg->sendMsg.function);
        RG_FREE(msg->sendMsg.msg);
        break;
    case CLUSTER_REFRESH_MSG:
    case CLUSTER_SET_MSG:
        break;
    default:
        assert(false);
    }
    RG_FREE(msg);
}

static void Cluster_SendMsgToNode(Node* node, SendMsg* msg){
    size_t sizes[4];
    const char* args[4];
    args[0] = RG_INNER_MSG_COMMAND;
    sizes[0] = strlen(args[0]);
    args[1] = CurrCluster->myId;
    sizes[1] = strlen(args[1]);
    args[2] = msg->function;
    sizes[2] = strlen(args[2]);
    args[3] = msg->msg;
    sizes[3] = msg->msgLen;
    redisAsyncCommandArgv(node->c, OnResponseArrived, NULL, 4, args, sizes);
}

static void Cluster_SendMessage(SendMsg* sendMsg){
    if(sendMsg->idToSend[0] != '\0'){
        Node* n = GetNode(sendMsg->idToSend);
        assert(n);
        Cluster_SendMsgToNode(n, sendMsg);
    }else{
        dictIterator *iter = dictGetIterator(CurrCluster->nodes);
        dictEntry *entry = NULL;
        while((entry = dictNext(iter))){
            Node* n = dictGetVal(entry);
            if(n->c){
                Cluster_SendMsgToNode(n, sendMsg);
            }
        }
        dictReleaseIterator(iter);
    }
}

static void Cluster_MsgArrive(evutil_socket_t s, short what, void *arg){
    Msg* msg;
    RedisModuleCtx* ctx;
    read(s, &msg, sizeof(Msg*));
    switch(msg->type){
    case SEND_MSG:
        Cluster_SendMessage(&msg->sendMsg);
        break;
    case CLUSTER_REFRESH_MSG:
        ctx = RedisModule_GetThreadSafeContext(msg->clusterRefresh.bc);
        LockHandler_Acquire(ctx);
        Cluster_Refresh(ctx);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_UnblockClient(msg->clusterRefresh.bc, NULL);
        LockHandler_Realse(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        break;
    case CLUSTER_SET_MSG:
        ctx = RedisModule_GetThreadSafeContext(msg->clusterSet.bc);
        LockHandler_Acquire(ctx);
        Cluster_Set(ctx, msg->clusterSet.argv, msg->clusterSet.argc);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_UnblockClient(msg->clusterRefresh.bc, NULL);
        LockHandler_Realse(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        break;
    default:
        assert(false);
    }
    Cluster_FreeMsg(msg);
}

static void* Cluster_MessageThreadMain(void *arg){
    event_base_loop(main_base, 0);
    return NULL;
}

static struct timespec timespecAdd(struct timespec *a, struct timespec *b) {
  struct timespec ret;
  ret.tv_sec = a->tv_sec + b->tv_sec;

  long long ns = a->tv_nsec + b->tv_nsec;
  ret.tv_sec += ns / 1000000000;
  ret.tv_nsec = ns % 1000000000;
  return ret;
}

static void* Cluster_MessageArriveThread(void *arg){
    struct timespec ts;
    struct timespec interval;
    interval.tv_sec = 1;
    interval.tv_nsec = 0;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
    pthread_mutex_lock(&msgArriveLock);
    while (true) {
        clock_gettime(CLOCK_REALTIME, &ts);
        struct timespec timeout = timespecAdd(&ts, &interval);
        int rc = pthread_cond_timedwait(&msgArriveCond, &msgArriveLock, &timeout);
        if (rc == EINVAL) {
            perror("Error waiting for condition");
            exit(1);
        }

        while(listLength(msgArriveList) > 0){
            listNode *node = listFirst(msgArriveList);
            MsgArriveCtx* msgCtx = listNodeValue(node);
            listDelNode(msgArriveList, node);
            pthread_mutex_unlock(&msgArriveLock);
            msgCtx->receiver(rctx, msgCtx->sender, 0, msgCtx->msg, msgCtx->msgLen);
            MsgArriveCtx_Free(msgCtx);
            pthread_mutex_lock(&msgArriveLock);
        }
    }
    return NULL;
}

static void Cluster_StartMsgArriveThread(){
    pthread_create(&messagesArriveThread, NULL, Cluster_MessageArriveThread, NULL);
}

static void Cluster_StartClusterThread(){
    pipe(notify);
    main_base = (struct event_base*)event_base_new();
    struct event *readEvent = event_new(main_base,
                                        notify[0],
                                        EV_READ | EV_PERSIST,
                                        Cluster_MsgArrive,
                                        NULL);
    event_base_set(main_base, readEvent);
    event_add(readEvent,0);

    pthread_create(&messagesThread, NULL, Cluster_MessageThreadMain, NULL);
}

void Cluster_RegisterMsgReceiver(char* function, RedisModuleClusterMessageReceiver receiver){
    dictAdd(RemoteCallbacks, function, receiver);
}

void Cluster_SendClusterRefresh(RedisModuleCtx *ctx){
    Msg* msgStruct = RG_ALLOC(sizeof(*msgStruct));
    msgStruct->clusterRefresh.bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 2000000);
    msgStruct->type = CLUSTER_REFRESH_MSG;
    write(notify[1], &msgStruct, sizeof(Msg*));
}

void Cluster_SendClusterSet(RedisModuleCtx *ctx, RedisModuleString** argv, int argc){
    Msg* msgStruct = RG_ALLOC(sizeof(*msgStruct));
    msgStruct->clusterSet.bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 2000000);
    msgStruct->clusterSet.argv = argv;
    msgStruct->clusterSet.argc = argc;
    msgStruct->type = CLUSTER_SET_MSG;
    write(notify[1], &msgStruct, sizeof(Msg*));
}

void Cluster_SendMsg(char* id, char* function, char* msg, size_t len){
    Msg* msgStruct = RG_ALLOC(sizeof(*msgStruct));
    if(id){
        memcpy(msgStruct->sendMsg.idToSend, id, REDISMODULE_NODE_ID_LEN);
        msgStruct->sendMsg.idToSend[REDISMODULE_NODE_ID_LEN] = '\0';
    }else{
        msgStruct->sendMsg.idToSend[0] = '\0';
    }
    msgStruct->sendMsg.function = RG_STRDUP(function);
    msgStruct->sendMsg.msg = RG_ALLOC(len);
    memcpy(msgStruct->sendMsg.msg, msg, len);
    msgStruct->sendMsg.msgLen = len;
    msgStruct->type = SEND_MSG;
    write(notify[1], &msgStruct, sizeof(Msg*));
}

bool Cluster_IsClusterMode(){
    return CurrCluster && CurrCluster->isClusterMode && Cluster_GetSize() > 1;
}

size_t Cluster_GetSize(){
    return dictSize(CurrCluster->nodes);
}

void Cluster_Init(){
    RemoteCallbacks = dictCreate(&dictTypeHeapStrings, NULL);
    msgArriveList = listCreate();
    pthread_cond_init(&msgArriveCond, NULL);
    pthread_mutex_init(&msgArriveLock, NULL);
    Cluster_StartClusterThread();
    Cluster_StartMsgArriveThread();
}

char* Cluster_GetMyId(){
    return CurrCluster->myId;
}

unsigned int keyHashSlot(char *key, int keylen);

char* Cluster_GetNodeIdByKey(char* key){
    unsigned int slot = keyHashSlot(key, strlen(key));
    return CurrCluster->slots[slot]->id;
}

bool Cluster_IsMyId(char* id){
	return memcmp(CurrCluster->myId, id, REDISMODULE_NODE_ID_LEN) == 0;
}

int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define NO_CLUSTER_MODE_REPLY "no cluster mode"
    if(!Cluster_IsClusterMode()){
        RedisModule_ReplyWithStringBuffer(ctx, NO_CLUSTER_MODE_REPLY, strlen(NO_CLUSTER_MODE_REPLY));
        return REDISMODULE_OK;
    }
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithStringBuffer(ctx, "MyId", strlen("MyId"));
    RedisModule_ReplyWithStringBuffer(ctx, CurrCluster->myId, strlen(CurrCluster->myId));
    RedisModule_ReplyWithArray(ctx, dictSize(CurrCluster->nodes));
    dictIterator *iter = dictGetIterator(CurrCluster->nodes);
    dictEntry *entry = NULL;
    while((entry = dictNext(iter))){
        Node* n = dictGetVal(entry);
        RedisModule_ReplyWithArray(ctx, 8);
        RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
        RedisModule_ReplyWithStringBuffer(ctx, n->id, strlen(n->id));
        RedisModule_ReplyWithStringBuffer(ctx, "ip", strlen("ip"));
        RedisModule_ReplyWithStringBuffer(ctx, n->ip, strlen(n->ip));
        RedisModule_ReplyWithStringBuffer(ctx, "port", strlen("port"));
        RedisModule_ReplyWithLongLong(ctx, n->port);
        RedisModule_ReplyWithStringBuffer(ctx, "unixSocket", strlen("unixSocket"));
        if(n->unixSocket){
            RedisModule_ReplyWithStringBuffer(ctx, n->unixSocket, strlen(n->unixSocket));
        }else{
            RedisModule_ReplyWithStringBuffer(ctx, "None", strlen("None"));
        }
    }
    dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 4){
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleString* senderId = argv[1];
    RedisModuleString* functionToCall = argv[2];
    RedisModuleString* msg = argv[3];

    const char* senderIdStr = RedisModule_StringPtrLen(senderId, NULL);
    const char* functionToCallStr = RedisModule_StringPtrLen(functionToCall, NULL);
    size_t msgLen;
    const char* msgStr = RedisModule_StringPtrLen(msg, &msgLen);

    dictEntry *entry = dictFind(RemoteCallbacks, functionToCallStr);
    if(!entry){
        RedisModule_Log(ctx, "warning", "can not find the callback requested : %s", functionToCallStr);
        RedisModule_ReplyWithError(ctx, "can not find the callback requested");
        return REDISMODULE_OK;
    }
    RedisModuleClusterMessageReceiver receiver = dictGetVal(entry);

    MsgArriveCtx* msgArriveCtx = MsgArriveCtx_Create(senderIdStr, msgStr, msgLen, receiver);

    pthread_mutex_lock(&msgArriveLock);
    listAddNodeTail(msgArriveList, msgArriveCtx);
    pthread_mutex_unlock(&msgArriveLock);
    pthread_cond_signal(&msgArriveCond);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* this cluster refresh is a hack for now, we should come up with a better solution!! */
int Cluster_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    Cluster_SendClusterRefresh(ctx);
    return REDISMODULE_OK;
}

int Cluster_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    Cluster_SendClusterSet(ctx, argv, argc);
    return REDISMODULE_OK;
}
