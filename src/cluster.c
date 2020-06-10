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
#include "slots_table.h"
#include "config.h"
#include <libevent.h>

typedef enum NodeStatus{
    NodeStatus_Connected, NodeStatus_Disconnected, NodeStatus_Free
}NodeStatus;

typedef struct Node{
    char* id;
    char* ip;
    unsigned short port;
    char* password;
    char* unixSocket;
    redisAsyncContext *c;
    char* runId;
    unsigned long long msgId;
    Gears_list* pendingMessages;
    size_t minSlot;
    size_t maxSlot;
    bool isMe;
    NodeStatus status;
    struct event *reconnectEvent;
}Node;

Gears_dict* nodesMsgIds;

typedef struct Cluster{
    char* myId;
    const char* myHashTag;
    bool isClusterMode;
    Gears_dict* nodes;
    Node* slots[MAX_SLOT];
}Cluster;

Gears_dict* RemoteCallbacks;

Cluster* CurrCluster = NULL;
int notify[2];
pthread_t messagesThread;
struct event_base *main_base = NULL;

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
    Gears_dictEntry *entry = Gears_dictFind(CurrCluster->nodes, id);
    Node* n = NULL;
    if(entry){
        n = Gears_dictGetVal(entry);
    }
    return n;
}

static void FreeNodeInternals(Node* n){
    event_free(n->reconnectEvent);
    RG_FREE(n->id);
    RG_FREE(n->ip);
    if(n->unixSocket){
        RG_FREE(n->unixSocket);
    }
    RG_FREE(n);
}

static void FreeNode(Node* n){
    if(n->isMe){
        FreeNodeInternals(n);
        return;
    }
    n->c->data = NULL;
//    redisAsyncFree(n->c);
    if(n->status == NodeStatus_Disconnected){
        n->status = NodeStatus_Free;
        return;
    }
    FreeNodeInternals(n);
}

typedef struct SentMessages{
    size_t sizes[5];
    char* args[5];
    size_t retries;
}SentMessages;

static void SentMessages_Free(void* ptr){
    SentMessages* msg = ptr;
    RG_FREE(msg->args[2]);
    RG_FREE(msg->args[3]);
    RG_FREE(msg->args[4]);
    RG_FREE(msg);
}

static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status);
static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status);

static void Cluster_ConnectToShard(Node* n){
    redisAsyncContext* c = redisAsyncConnect(n->ip, n->port);
    if (!c) {
        return;
    }
    if (c->err) {
        /* Let *c leak for now... */
        RedisModule_Log(NULL, "warning", "Error: %s\n", n->c->errstr);
        //todo: handle this!!!
    }
    c->data = n;
    n->c = c;
    redisLibeventAttach(c, main_base);
    redisAsyncSetConnectCallback(c, Cluster_ConnectCallback);
    redisAsyncSetDisconnectCallback(c, Cluster_DisconnectCallback);
}

static void Cluster_Reconnect(evutil_socket_t s, short what, void *arg){
    Node* n = arg;
    if(n->status == NodeStatus_Free){
        FreeNodeInternals(n);
        return;
    }
    Cluster_ConnectToShard(n);
}

static Node* CreateNode(const char* id, const char* ip, unsigned short port, const char* password, const char* unixSocket, size_t minSlot, size_t maxSlot){
    RedisModule_Assert(!GetNode(id));
    Node* n = RG_ALLOC(sizeof(*n));
    *n = (Node){
            .id = RG_STRDUP(id),
            .ip = RG_STRDUP(ip),
            .port = port,
            .password = password ? RG_STRDUP(password) : NULL,
            .unixSocket = unixSocket ? RG_STRDUP(unixSocket) : NULL,
            .c = NULL,
            .msgId = 0,
            .pendingMessages = Gears_listCreate(),
            .minSlot = minSlot,
            .maxSlot = maxSlot,
            .isMe = false,
            .status = NodeStatus_Disconnected,
    };
    n->reconnectEvent = event_new(main_base, -1, 0, Cluster_Reconnect, n);
    Gears_listSetFreeMethod(n->pendingMessages, SentMessages_Free);
    Gears_dictAdd(CurrCluster->nodes, n->id, n);
    if(strcmp(id, CurrCluster->myId) == 0){
        CurrCluster->myHashTag = slot_table[minSlot];
        n->isMe = true;
    }
    return n;
}

static void Cluster_Free(){
    if(CurrCluster->isClusterMode){
        RG_FREE(CurrCluster->myId);
        Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            Node* n = Gears_dictGetVal(entry);
            FreeNode(n);
        }
        Gears_dictReleaseIterator(iter);
        Gears_dictRelease(CurrCluster->nodes);
    }

    RG_FREE(CurrCluster);
    CurrCluster = NULL;
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status);
static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status);

static void OnResponseArrived(struct redisAsyncContext* c, void* a, void* b){
    redisReply* reply = (redisReply*)a;
    if(!reply){
        return;
    }
    if(!c->data){
        return;
    }
    Node* n = (Node*)b;
    if(reply->type != REDIS_REPLY_STATUS){
        RedisModule_Log(NULL, "warning", "Received an invalid status reply from shard %s, will disconnect and try to reconnect. This is usually because the Redis server's 'proto-max-bulk-len' configuration setting is too low.", n->id);
        redisAsyncDisconnect(c);
        return;
    }
    Gears_listNode* node = Gears_listFirst(n->pendingMessages);
    Gears_listDelNode(n->pendingMessages, node);
}

static void RG_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b){
    redisReply* reply = (redisReply*)a;
    if(!reply){
        return;
    }
    RedisModule_Assert(reply->type == REDIS_REPLY_STRING);
    if(!c->data){
        return;
    }
    Node* n = (Node*)b;
    if(n->status == NodeStatus_Free){
        FreeNodeInternals(n);
        return;
    }

    if(n->runId){
        if(strcmp(n->runId, reply->str) != 0){
            // here we know that the shard has crashed
            // todo: notify all running executions to abort
            n->msgId = 0;
            Gears_listEmpty(n->pendingMessages);
        }else{
            // shard is alive, tcp disconnected
            Gears_listIter* iter = Gears_listGetIterator(n->pendingMessages, AL_START_HEAD);
            Gears_listNode *node = NULL;
            while((node = Gears_listNext(iter)) != NULL){
                SentMessages* sentMsg = Gears_listNodeValue(node);
                ++sentMsg->retries;
                if(GearsConfig_SendMsgRetries() == 0 || sentMsg->retries < GearsConfig_SendMsgRetries()){
                    redisAsyncCommandArgv(c, OnResponseArrived, n, 5, (const char**)sentMsg->args, sentMsg->sizes);
                }else{
                    RedisModule_Log(NULL, "warning", "Gave up of message because failed to send it for more then %lld time", GearsConfig_SendMsgRetries());
                    Gears_listDelNode(n->pendingMessages, node);
                }
            }
            Gears_listReleaseIterator(iter);
        }
        RG_FREE(n->runId);
    }
    n->runId = RG_STRDUP(reply->str);
    n->status = NodeStatus_Connected;
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status){
    RedisModule_Log(NULL, "warning", "disconnected : %s:%d, status : %d, will try to reconnect.\r\n", c->c.tcp.host, c->c.tcp.port, status);
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    n->status = NodeStatus_Disconnected;
    struct timeval tv = {
            .tv_sec = 1,
    };
    event_add(n->reconnectEvent, &tv);
}

static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status){
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    if(status == -1){
        // connection failed lets try again
        struct timeval tv = {
                .tv_sec = 1,
        };
        event_add(n->reconnectEvent, &tv);
    }else{
        RedisModule_Log(NULL, "notice", "connected : %s:%d, status = %d\r\n", c->c.tcp.host, c->c.tcp.port, status);
        if(n->password){
            redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s", n->password);
        }
        redisAsyncCommand((redisAsyncContext*)c, RG_HelloResponseArrived, n, "RG.HELLO");
    }
}

static void Cluster_ConnectToShards(){
    Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        Node* n = Gears_dictGetVal(entry);
        if(n->isMe){
            continue;
        }
        Cluster_ConnectToShard(n);
    }
    Gears_dictReleaseIterator(iter);
    Gears_dictEmpty(nodesMsgIds, NULL);
}

static char* Cluster_ReadRunId(RedisModuleCtx* ctx){
    RedisModuleCallReply *infoReply = RedisModule_Call(ctx, "info", "c", "server");
    RedisModule_Assert(RedisModule_CallReplyType(infoReply) == REDISMODULE_REPLY_STRING);
    size_t len;
    const char* infoStrReply = RedisModule_CallReplyStringPtr(infoReply, &len);
    const char* runId = strstr(infoStrReply, "run_id:");
    RedisModule_Assert(runId);
    const char* runIdStr = runId + strlen("run_id:");
    RedisModule_Assert(runIdStr);
    const char* endLine = strstr(runIdStr, "\r\n");
    RedisModule_Assert(endLine);
    len = (size_t)(endLine - runIdStr);
    char* runIdFinal = RG_ALLOC(sizeof(char) * (len + 1));
    memcpy(runIdFinal, runIdStr, len);
    runIdFinal[len] = '\0';
    RedisModule_FreeCallReply(infoReply);
    return runIdFinal;
}

static void Cluster_Set(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    if(CurrCluster){
        Cluster_Free();
    }

    if(argc < 10){
        RedisModule_Log(ctx, "warning", "Could not parse cluster set arguments");
        return;
    }

    CurrCluster = RG_ALLOC(sizeof(*CurrCluster));

    size_t myIdLen;
    const char* myId = RedisModule_StringPtrLen(argv[6], &myIdLen);
    CurrCluster->myId = RG_ALLOC(REDISMODULE_NODE_ID_LEN + 1);
    size_t zerosPadding = REDISMODULE_NODE_ID_LEN - myIdLen;
    memset(CurrCluster->myId, '0', zerosPadding);
    memcpy(CurrCluster->myId + zerosPadding, myId, myIdLen);
    CurrCluster->myId[REDISMODULE_NODE_ID_LEN] = '\0';

    CurrCluster->nodes = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    long long numOfRanges;
    RedisModule_Assert(RedisModule_StringToLongLong(argv[8], &numOfRanges) == REDISMODULE_OK);

    CurrCluster->isClusterMode = numOfRanges > 1;

    for(size_t i = 9, j = 0 ; j < numOfRanges ; i += 8, ++j){
        size_t shardIdLen;
        const char* shardId = RedisModule_StringPtrLen(argv[i + 1], &shardIdLen);
        char realId[REDISMODULE_NODE_ID_LEN + 1];
        size_t zerosPadding = REDISMODULE_NODE_ID_LEN - shardIdLen;
        memset(realId, '0', zerosPadding);
        memcpy(realId + zerosPadding, shardId, shardIdLen);
        realId[REDISMODULE_NODE_ID_LEN] = '\0';

        long long minslot;
        RedisModule_Assert(RedisModule_StringToLongLong(argv[i + 3], &minslot) == REDISMODULE_OK);
        long long maxslot;
        RedisModule_Assert(RedisModule_StringToLongLong(argv[i + 4], &maxslot) == REDISMODULE_OK);

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
            n = CreateNode(realId, ip, port, password, NULL, minslot, maxslot);
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
    CurrCluster->nodes = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);

    RedisModuleCallReply *allSlotsRelpy = RedisModule_Call(ctx, "cluster", "c", "slots");
    RedisModule_Assert(RedisModule_CallReplyType(allSlotsRelpy) == REDISMODULE_REPLY_ARRAY);
    for(size_t i = 0 ; i < RedisModule_CallReplyLength(allSlotsRelpy) ; ++i){
        RedisModuleCallReply *slotRangeRelpy = RedisModule_CallReplyArrayElement(allSlotsRelpy, i);

        RedisModuleCallReply *minslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 0);
        RedisModule_Assert(RedisModule_CallReplyType(minslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long minslot = RedisModule_CallReplyInteger(minslotRelpy);

        RedisModuleCallReply *maxslotRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 1);
        RedisModule_Assert(RedisModule_CallReplyType(maxslotRelpy) == REDISMODULE_REPLY_INTEGER);
        long long maxslot = RedisModule_CallReplyInteger(maxslotRelpy);

        RedisModuleCallReply *nodeDetailsRelpy = RedisModule_CallReplyArrayElement(slotRangeRelpy, 2);
        RedisModule_Assert(RedisModule_CallReplyType(nodeDetailsRelpy) == REDISMODULE_REPLY_ARRAY);
        RedisModule_Assert(RedisModule_CallReplyLength(nodeDetailsRelpy) == 3);
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
            n = CreateNode(nodeId, nodeIp, (unsigned short)port, NULL, NULL, minslot, maxslot);
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
        RedisModule_Assert(false);
    }
    RG_FREE(msg);
}

static void Cluster_SendMsgToNode(Node* node, SendMsg* msg){
    SentMessages* sentMsg = RG_ALLOC(sizeof(SentMessages));
    sentMsg->retries = 0;
    sentMsg->args[0] = RG_INNER_MSG_COMMAND;
    sentMsg->sizes[0] = strlen(sentMsg->args[0]);
    sentMsg->args[1] = CurrCluster->myId;
    sentMsg->sizes[1] = strlen(sentMsg->args[1]);
    sentMsg->args[2] = RG_STRDUP(msg->function);
    sentMsg->sizes[2] = strlen(sentMsg->args[2]);
    sentMsg->args[3] = RG_ALLOC(sizeof(char) * msg->msgLen);
    memcpy(sentMsg->args[3], msg->msg, msg->msgLen);
    sentMsg->sizes[3] = msg->msgLen;

    RedisModuleString *msgIdStr = RedisModule_CreateStringFromLongLong(NULL, node->msgId++);
    size_t msgIdStrLen;
    const char* msgIdCStr = RedisModule_StringPtrLen(msgIdStr, &msgIdStrLen);

    sentMsg->args[4] = RG_STRDUP(msgIdCStr);
    sentMsg->sizes[4] = msgIdStrLen;

    RedisModule_FreeString(NULL, msgIdStr);

    if(node->status == NodeStatus_Connected){
        redisAsyncCommandArgv(node->c, OnResponseArrived, node, 5, (const char**)sentMsg->args, sentMsg->sizes);
    }else{
        RedisModule_Log(NULL, "warning", "message was not sent because status is not connected");
    }
    Gears_listAddNodeTail(node->pendingMessages, sentMsg);
}

static void Cluster_SendMessage(SendMsg* sendMsg){
    if(sendMsg->idToSend[0] != '\0'){
        Node* n = GetNode(sendMsg->idToSend);
        if(!n){
            RedisModule_Log(NULL, "warning", "Could not find node to send message to");
            return;
        }
        Cluster_SendMsgToNode(n, sendMsg);
    }else{
        Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            Node* n = Gears_dictGetVal(entry);
            if(!n->isMe){
                Cluster_SendMsgToNode(n, sendMsg);
            }
        }
        Gears_dictReleaseIterator(iter);
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
        LockHandler_Release(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        break;
    case CLUSTER_SET_MSG:
        ctx = RedisModule_GetThreadSafeContext(msg->clusterSet.bc);
        LockHandler_Acquire(ctx);
        Cluster_Set(ctx, msg->clusterSet.argv, msg->clusterSet.argc);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_UnblockClient(msg->clusterRefresh.bc, NULL);
        LockHandler_Release(ctx);
        RedisModule_FreeThreadSafeContext(ctx);
        break;
    default:
        RedisModule_Assert(false);
    }
    Cluster_FreeMsg(msg);
}

static void* Cluster_MessageThreadMain(void *arg){
    event_base_loop(main_base, 0);
    return NULL;
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
    Gears_dictAdd(RemoteCallbacks, function, receiver);
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

void Cluster_SendMsg(const char* id, char* function, char* msg, size_t len){
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
    return Gears_dictSize(CurrCluster->nodes);
}

void Cluster_Init(){
    RemoteCallbacks = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    nodesMsgIds = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    Cluster_StartClusterThread();
}

char* Cluster_GetMyId(){
    return CurrCluster->myId;
}

const char* Cluster_GetMyHashTag(){
    if(!Cluster_IsClusterMode()){
        return slot_table[0];
    }
    return CurrCluster->myHashTag;
}

uint16_t Gears_crc16(const char *buf, int len);

static unsigned int keyHashSlot(const char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return Gears_crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return Gears_crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return Gears_crc16(key+s+1,e-s-1) & 0x3FFF;
}

const char* Cluster_GetNodeIdByKey(const char* key){
    unsigned int slot = keyHashSlot(key, strlen(key));
    return CurrCluster->slots[slot]->id;
}

bool Cluster_IsMyId(const char* id){
	return memcmp(CurrCluster->myId, id, REDISMODULE_NODE_ID_LEN) == 0;
}

int Cluster_RedisGearsHello(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    char* runId = Cluster_ReadRunId(ctx);
    RedisModule_ReplyWithStringBuffer(ctx, runId, strlen(runId));
    RG_FREE(runId);
    return REDISMODULE_OK;
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
    RedisModule_ReplyWithArray(ctx, Gears_dictSize(CurrCluster->nodes));
    Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        Node* n = Gears_dictGetVal(entry);
        RedisModule_ReplyWithArray(ctx, 14);
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
        RedisModule_ReplyWithStringBuffer(ctx, "runid", strlen("runid"));
        if(n->runId){
            RedisModule_ReplyWithStringBuffer(ctx, n->runId, strlen(n->runId));
        }else{
            char* runId = Cluster_ReadRunId(ctx);
            RedisModule_ReplyWithStringBuffer(ctx, runId, strlen(runId));
            RG_FREE(runId);
        }
        RedisModule_ReplyWithStringBuffer(ctx, "minHslot", strlen("minHslot"));
        RedisModule_ReplyWithLongLong(ctx, n->minSlot);
        RedisModule_ReplyWithStringBuffer(ctx, "maxHslot", strlen("maxHslot"));
        RedisModule_ReplyWithLongLong(ctx, n->maxSlot);

    }
    Gears_dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 5){
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleString* senderId = argv[1];
    RedisModuleString* functionToCall = argv[2];
    RedisModuleString* msg = argv[3];
    RedisModuleString* msgIdStr = argv[4];
    long long msgId;
    if(RedisModule_StringToLongLong(msgIdStr, &msgId) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "bad msg id given");
        RedisModule_ReplyWithError(ctx, "bad msg id given");
        return REDISMODULE_OK;
    }

    const char* senderIdStr = RedisModule_StringPtrLen(senderId, NULL);
    Gears_dictEntry* entity = Gears_dictFind(nodesMsgIds, senderIdStr);
    long long currId = -1;
    if(entity){
        currId = Gears_dictGetSignedIntegerVal(entity);
    }else{
        entity = Gears_dictAddRaw(nodesMsgIds, (char*)senderIdStr, NULL);
    }
    if(msgId <= currId){
        RedisModule_Log(ctx, "warning", "duplicate message ignored");
        RedisModule_ReplyWithSimpleString(ctx, "duplicate message ignored");
        return REDISMODULE_OK;
    }
    Gears_dictSetSignedIntegerVal(entity, msgId);
    const char* functionToCallStr = RedisModule_StringPtrLen(functionToCall, NULL);
    size_t msgLen;
    const char* msgStr = RedisModule_StringPtrLen(msg, &msgLen);

    Gears_dictEntry *entry = Gears_dictFind(RemoteCallbacks, functionToCallStr);
    if(!entry){
        RedisModule_Log(ctx, "warning", "can not find the callback requested : %s", functionToCallStr);
        RedisModule_ReplyWithError(ctx, "can not find the callback requested");
        return REDISMODULE_OK;
    }
    RedisModuleClusterMessageReceiver receiver = Gears_dictGetVal(entry);
    receiver(ctx, senderIdStr, 0, msgStr, msgLen);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* this cluster refresh is a hack for now, we should come up with a better solution!! */
int Cluster_RefreshCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    Cluster_SendClusterRefresh(ctx);
    return REDISMODULE_OK;
}

int Cluster_ClusterSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 10){
        RedisModule_ReplyWithError(ctx, "Could not parse cluster set arguments");
        return REDISMODULE_OK;
    }
    Cluster_SendClusterSet(ctx, argv, argc);
    return REDISMODULE_OK;
}
