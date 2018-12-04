#include "cluster.h"
#include "redismodule.h"
#include "redistar_memory.h"
#include "utils/dict.h"
#include <assert.h>
#include <event2/event.h>
#include <async.h>
#include <unistd.h>
#include <pthread.h>
#include <libevent.h>

#define MAX_SLOT 16384

typedef struct Node{
    char* id;
    char* ip;
    unsigned short port;
    char* unixSocket;
    redisAsyncContext *c;
}Node;

typedef struct Cluster{
    char* myId;
    bool isClusterMode;
    dict* nodes;
    Node* slots[MAX_SLOT];
}Cluster;

dict* remoteCallbacks;

Cluster* cluster;
int notify[2];
pthread_t messagesThread;
struct event_base *main_base = NULL;

typedef struct Msg{
    char idToSend[REDISMODULE_NODE_ID_LEN + 1];
    char* function;
    char* msg;
    size_t msgLen;
}Msg;

static Node* GetNode(const char* id){
    dictEntry *entry = dictFind(cluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = dictGetVal(entry);
    }
    return n;
}

//static void CallbackFn(struct redisAsyncContext* c, void* v1 , void* v2){
//    printf("reply arrived\r\n");
//}

static void Cluster_SendMsgToNode(Node* node, Msg* msg){
    size_t sizes[4];
    const char* args[4];
    args[0] = RS_INNER_MSG_COMMAND;
    sizes[0] = strlen(args[0]);
    args[1] = cluster->myId;
    sizes[1] = strlen(args[1]);
    args[2] = msg->function;
    sizes[2] = strlen(args[2]);
    args[3] = msg->msg;
    sizes[3] = msg->msgLen;
    redisAsyncCommandArgv(node->c, NULL, NULL, 4, args, sizes);
}

static void Cluster_MsgArrive(evutil_socket_t s, short what, void *arg){
    Msg* msg;
    read(s, &msg, sizeof(Msg*));
    if(msg->idToSend[0] != '\0'){
        Node* n = GetNode(msg->idToSend);
        assert(n);
        Cluster_SendMsgToNode(n, msg);
    }else{
        dictIterator *iter = dictGetIterator(cluster->nodes);
        dictEntry *entry = NULL;
        while((entry = dictNext(iter))){
            Node* n = dictGetVal(entry);
            if(n->c){
                Cluster_SendMsgToNode(n, msg);
            }
        }
        dictReleaseIterator(iter);
    }
    RS_FREE(msg->function);
    RS_FREE(msg->msg);
    RS_FREE(msg);
}

static void* Cluster_MessageThreadMain(void *arg){
    event_base_loop(main_base, 0);
    return NULL;
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status){
    printf("disconnected : %s:%d\r\n", c->c.tcp.host, c->c.tcp.port);
}
static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status){
    printf("connected : %s:%d\r\n", c->c.tcp.host, c->c.tcp.port);
}

static void Cluster_StartMessagesThread(){
    pipe(notify);
    main_base = (struct event_base*)event_base_new();
    struct event *readEvent = event_new(main_base,
                                        notify[0],
                                        EV_READ | EV_PERSIST,
                                        Cluster_MsgArrive,
                                        NULL);
    event_base_set(main_base, readEvent);
    event_add(readEvent,0);

    dictIterator *iter = dictGetIterator(cluster->nodes);
    dictEntry *entry = NULL;
    while((entry = dictNext(iter))){
        Node* n = dictGetVal(entry);
        if(strcmp(n->id, cluster->myId) == 0){
            continue;
        }
        n->c = redisAsyncConnect(n->ip, n->port);
        if (n->c->err) {
            /* Let *c leak for now... */
            printf("Error: %s\n", n->c->errstr);
            //todo: handle this!!!
        }
        redisLibeventAttach(n->c, main_base);
        redisAsyncSetConnectCallback(n->c, Cluster_ConnectCallback);
        redisAsyncSetDisconnectCallback(n->c, Cluster_DisconnectCallback);
    }
    dictReleaseIterator(iter);

    pthread_create(&messagesThread, NULL, Cluster_MessageThreadMain, NULL);

//    char a = '1';

//    write(notify[1], &a, sizeof(char));
}

void Cluster_RegisterMsgReceiver(char* function, RedisModuleClusterMessageReceiver receiver){
    dictAdd(remoteCallbacks, function, receiver);
}

void Cluster_SendMsg(char* id, char* function, char* msg, size_t len){
    Msg* msgStruct = RS_ALLOC(sizeof(*msgStruct));
    if(id){
        memcpy(msgStruct->idToSend, id, REDISMODULE_NODE_ID_LEN);
        msgStruct->idToSend[REDISMODULE_NODE_ID_LEN] = '\0';
    }else{
        msgStruct->idToSend[0] = '\0';
    }
    msgStruct->function = RS_STRDUP(function);
    msgStruct->msg = RS_ALLOC(len);
    memcpy(msgStruct->msg, msg, len);
    msgStruct->msgLen = len;
    write(notify[1], &msgStruct, sizeof(Msg*));
}

static void FreeNode(Node* n){
    RS_FREE(n->id);
    RS_FREE(n->ip);
    if(n->unixSocket){
        RS_FREE(n->unixSocket);
    }
    RS_FREE(n);
}

static Node* CreateNode(const char* id, const char* ip, unsigned short port, const char* unixSocket){
    assert(!GetNode(id));
    Node* n = RS_ALLOC(sizeof(*n));
    *n = (Node){
            .id = RS_STRDUP(id),
            .ip = RS_STRDUP(ip),
            .port = port,
            .unixSocket = unixSocket ? RS_STRDUP(unixSocket) : NULL,
            .c = NULL,
    };
    dictAdd(cluster->nodes, n->id, n);
    return n;
}

static void Cluster_Free(RedisModuleCtx* ctx){
    RS_FREE(cluster->myId);
    dictIterator *iter = dictGetIterator(cluster->nodes);
    dictEntry *entry = NULL;
    while((entry = dictNext(iter))){
        Node* n = dictGetVal(entry);
        FreeNode(n);
    }
    dictReleaseIterator(iter);
    dictRelease(cluster->nodes);

    RS_FREE(cluster);
    cluster = NULL;
}

bool Cluster_IsClusterMode(){
    return cluster && cluster->isClusterMode && Cluster_GetSize() > 1;
}

size_t Cluster_GetSize(){
    return dictSize(cluster->nodes);
}

void Cluster_Init(){
    remoteCallbacks = dictCreate(&dictTypeHeapStrings, NULL);
}

void Cluster_Refresh(){
    RedisModuleCtx* ctx = RedisModule_GetThreadSafeContext(NULL);
    if(cluster){
        Cluster_Free(ctx);
    }

    cluster = RS_ALLOC(sizeof(*cluster));

    if(!(RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_CLUSTER)){
        cluster->isClusterMode = false;
        RedisModule_FreeThreadSafeContext(ctx);
        return;
    }

    cluster->isClusterMode = true;

    cluster->myId = RS_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    memcpy(cluster->myId, RedisModule_GetMyClusterID(), REDISMODULE_NODE_ID_LEN);
    cluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';
    cluster->nodes = dictCreate(&dictTypeHeapStrings, NULL);

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
            n = CreateNode(nodeId, nodeIp, (unsigned short)port, NULL);
        }
        for(int i = minslot ; i <= maxslot ; ++i){
            cluster->slots[i] = n;
        }
    }
    RedisModule_FreeCallReply(allSlotsRelpy);
    Cluster_StartMessagesThread();
    RedisModule_FreeThreadSafeContext(ctx);
}

char* Cluster_GetMyId(){
    return cluster->myId;
}

unsigned int keyHashSlot(char *key, int keylen);

char* Cluster_GetNodeIdByKey(char* key){
    unsigned int slot = keyHashSlot(key, strlen(key));
    return cluster->slots[slot]->id;
}

bool Cluster_IsMyId(char* id){
	return memcmp(cluster->myId, id, REDISMODULE_NODE_ID_LEN) == 0;
}

int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define NO_CLUSTER_MODE_REPLY "no cluste mode"
    if(!Cluster_IsClusterMode()){
        RedisModule_ReplyWithStringBuffer(ctx, NO_CLUSTER_MODE_REPLY, strlen(NO_CLUSTER_MODE_REPLY));
        return REDISMODULE_OK;
    }
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithStringBuffer(ctx, "MyId", strlen("MyId"));
    RedisModule_ReplyWithStringBuffer(ctx, cluster->myId, strlen(cluster->myId));
    RedisModule_ReplyWithArray(ctx, dictSize(cluster->nodes));
    dictIterator *iter = dictGetIterator(cluster->nodes);
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

    dictEntry *entry = dictFind(remoteCallbacks, functionToCallStr);
    if(!entry){
        RedisModule_Log(ctx, "warning", "can not find the callback requested : %s", functionToCallStr);
        RedisModule_ReplyWithError(ctx, "can not find the callback requested");
        return REDISMODULE_OK;
    }
    RedisModuleClusterMessageReceiver receiver = dictGetVal(entry);
    receiver(ctx, senderIdStr, 0, msgStr, msgLen);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}
