#include "cluster.h"
#include "redisgears_memory.h"
#include "lock_handler.h"
#include "slots_table.h"
#include "config.h"
#include "utils/dict.h"
#include "utils/adlist.h"

#include <hiredis.h>
#include <hiredis_ssl.h>
#include <stdlib.h>
#include <assert.h>
#include <event2/event.h>
#include <async.h>
#include <unistd.h>
#include <pthread.h>
#include <libevent.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#define CLUSTER_SET_MY_ID_INDEX 6

#define RUN_ID_SIZE 40

// forward declaration
static void RG_HelloResponseArrived(struct redisAsyncContext* c, void* a, void* b);

typedef enum NodeStatus{
    NodeStatus_Connected, NodeStatus_Disconnected, NodeStatus_HelloSent, NodeStatus_Free
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
    struct event *asyncDisconnectEvent;
    struct event *resendHelloMessage;
    bool sendClusterTopologyOnNextConnect;
}Node;

Gears_dict* nodesMsgIds;

typedef struct Cluster{
    char* myId;
    const char* myHashTag;
    bool isClusterMode;
    Gears_dict* nodes;
    Node* slots[MAX_SLOT];
    size_t clusterSetCommandSize;
    char** clusterSetCommand;
    char runId[RUN_ID_SIZE + 1];
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
    bool force;
}ClusterSetMsg;

typedef struct Msg{
    union{
        SendMsg sendMsg;
        ClusterRefreshMsg clusterRefresh;
        ClusterSetMsg clusterSet;
    };
    MsgType type;
}Msg;

char* getConfigValue(const char* confName){
    RedisModuleCallReply *rep = RedisModule_Call(staticCtx, "config", "cc", "get",
            confName);
    RedisModule_Assert(
            RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
    if (RedisModule_CallReplyLength(rep) == 0) {
        RedisModule_FreeCallReply(rep);
        return NULL;
    }
    RedisModule_Assert(RedisModule_CallReplyLength(rep) == 2);
    RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 1);
    RedisModule_Assert(
            RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING);
    size_t len;
    const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);

    char* res = RG_CALLOC(1, len + 1);
    memcpy(res, valueRepCStr, len);

    RedisModule_FreeCallReply(rep);

    return res;
}

static int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass){
    int ret = 1;
    LockHandler_Acquire(staticCtx);
    char* clusterTls = NULL;
    char* tlsPort = NULL;

    clusterTls = getConfigValue("tls-cluster");
    if (!clusterTls || strcmp(clusterTls, "yes")) {
        tlsPort = getConfigValue("tls-port");
        if (!tlsPort || !strcmp(tlsPort, "0")) {
            ret = 0;
            goto done;
        }
    }

    *client_key = getConfigValue("tls-key-file");
    *client_cert = getConfigValue("tls-cert-file");
    *ca_cert = getConfigValue("tls-ca-cert-file");
    *key_pass = getConfigValue("tls-key-file-pass");

    if (!*client_key || !*client_cert || !*ca_cert) {
        ret = 0;
        if (*client_key) {
            RG_FREE(*client_key);
        }
        if (*client_cert) {
            RG_FREE(*client_cert);
        }
        if (*ca_cert) {
            RG_FREE(*client_cert);
        }
        if (*key_pass) {
            RG_FREE(*key_pass);
        }
    }

done:
    if (clusterTls) {
        RG_FREE(clusterTls);
    }
    if (tlsPort) {
        RG_FREE(tlsPort);
    }
    LockHandler_Release(staticCtx);
    return ret;
}

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
    event_free(n->asyncDisconnectEvent);
    event_free(n->resendHelloMessage);
    RG_FREE(n->id);
    RG_FREE(n->ip);
    if(n->unixSocket){
        RG_FREE(n->unixSocket);
    }
    if(n->password){
        RG_FREE(n->password);
    }
    if(n->runId){
        RG_FREE(n->runId);
    }
    if(n->c){
        redisAsyncFree(n->c);
    }
    Gears_listRelease(n->pendingMessages);
    RG_FREE(n);
}

static void FreeNode(Node* n){
    if(n->c){
        n->c->data = NULL;
    }
    n->status = NodeStatus_Free;
    FreeNodeInternals(n);
}

typedef struct SentMessages{
    size_t sizes[6];
    char* args[6];
    size_t retries;
}SentMessages;

static void SentMessages_Free(void* ptr){
    SentMessages* msg = ptr;
    RG_FREE(msg->args[3]);
    RG_FREE(msg->args[4]);
    RG_FREE(msg->args[5]);
    RG_FREE(msg);
}

static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status);
static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status);

static void Cluster_ConnectToShard(Node* n){
    redisAsyncContext* c = redisAsyncConnect(n->ip, n->port);
    if (!c) {
        RedisModule_Log(staticCtx, "warning", "Got NULL async connection");
        return;
    }
    if (c->err) {
        /* Let *c leak for now... */
        RedisModule_Log(staticCtx, "warning", "Error: %s\n", c->errstr);
        return;
    }
    c->data = n;
    n->c = c;
    redisLibeventAttach(c, main_base);
    redisAsyncSetConnectCallback(c, Cluster_ConnectCallback);
    redisAsyncSetDisconnectCallback(c, Cluster_DisconnectCallback);
}

static void Cluster_ResendHelloMessage(evutil_socket_t s, short what, void *arg){
    Node* n = arg;
    if(n->status == NodeStatus_Disconnected){
        // we will resent the hello request when reconnect
        return;
    }
    if(n->sendClusterTopologyOnNextConnect && CurrCluster->clusterSetCommand){
        RedisModule_Log(staticCtx, "notice", "Sending cluster topology to %s (%s:%d) on rg.hello retry", n->id, n->ip, n->port);
        CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = RG_STRDUP(n->id);
        redisAsyncCommandArgv(n->c, NULL, NULL, CurrCluster->clusterSetCommandSize, (const char**)CurrCluster->clusterSetCommand, NULL);
        RG_FREE(CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX]);
        CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = NULL;
        n->sendClusterTopologyOnNextConnect = false;
    }

    RedisModule_Log(staticCtx, "notice", "Resending hello request to %s (%s:%d)", n->id, n->ip, n->port);
    redisAsyncCommand(n->c, RG_HelloResponseArrived, n, "RG.HELLO");
}

static void Cluster_Reconnect(evutil_socket_t s, short what, void *arg){
    Node* n = arg;
    Cluster_ConnectToShard(n);
}

static void Cluster_AsyncDisconnect(evutil_socket_t s, short what, void *arg){
    Node* n = arg;
    redisAsyncContext *c = n->c;
    n->c = NULL;
    redisAsyncDisconnect(n->c);
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
            .sendClusterTopologyOnNextConnect = false,
            .runId = NULL,
    };
    n->reconnectEvent = event_new(main_base, -1, 0, Cluster_Reconnect, n);
    n->asyncDisconnectEvent = event_new(main_base, -1, 0, Cluster_AsyncDisconnect, n);
    n->resendHelloMessage = event_new(main_base, -1, 0, Cluster_ResendHelloMessage, n);
    Gears_listSetFreeMethod(n->pendingMessages, SentMessages_Free);
    Gears_dictAdd(CurrCluster->nodes, n->id, n);
    if(strcmp(id, CurrCluster->myId) == 0){
        CurrCluster->myHashTag = slot_table[minSlot];
        n->isMe = true;
    }
    return n;
}

static void Cluster_Free(){
    if(CurrCluster->myId){
        RG_FREE(CurrCluster->myId);
    }
    if(CurrCluster->nodes){
        Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
        Gears_dictEntry *entry = NULL;
        while((entry = Gears_dictNext(iter))){
            Node* n = Gears_dictGetVal(entry);
            FreeNode(n);
        }
        Gears_dictReleaseIterator(iter);
        Gears_dictRelease(CurrCluster->nodes);
    }

    if(CurrCluster->clusterSetCommand){
        for(int i = 0 ; i < CurrCluster->clusterSetCommandSize ; ++i){
            if(CurrCluster->clusterSetCommand[i]){
                RG_FREE(CurrCluster->clusterSetCommand[i]);
            }
        }
        RG_FREE(CurrCluster->clusterSetCommand);
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
    if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
        n->sendClusterTopologyOnNextConnect = true;
        RedisModule_Log(staticCtx, "warning", "Received ERRCLUSTER reply from shard %s (%s:%d), will send cluster topology to the shard on next connect", n->id, n->ip, n->port);
        redisAsyncDisconnect(c);
        return;
    }
    if(reply->type != REDIS_REPLY_STATUS){
        RedisModule_Log(staticCtx, "warning", "Received an invalid status reply from shard %s (%s:%d), will disconnect and try to reconnect. This is usually because the Redis server's 'proto-max-bulk-len' configuration setting is too low.", n->id, n->ip, n->port);
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
    Node* n = (Node*)b;
    if(!c->data){
        return;
    }

    if(reply->type != REDIS_REPLY_STRING){
        // we did not got a string reply
        // the shard is probably not yet up.
        // we will try again in one second.
        if(reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, CLUSTER_ERROR, strlen(CLUSTER_ERROR)) == 0){
            RedisModule_Log(staticCtx, "warning", "Got uninitialize cluster error on hello response from %s (%s:%d), will resend cluster topology in next try in 1 second.", n->id, n->ip, n->port);
            n->sendClusterTopologyOnNextConnect = true;
        }else{
            RedisModule_Log(staticCtx, "warning", "Got bad hello response from %s (%s:%d), will try again in 1 second", n->id, n->ip, n->port);
        }
        struct timeval tv = {
                .tv_sec = 1,
        };
        event_add(n->resendHelloMessage, &tv);
        return;
    }

    bool resendPendingMessages = true;;

    if(n->runId){
        if(strcmp(n->runId, reply->str) != 0){
            /* here we know that the shard has crashed
             * There is no need to send pending messages
             */
            resendPendingMessages = false;
            n->msgId = 0;
            Gears_listEmpty(n->pendingMessages);
        }
        RG_FREE(n->runId);
    }

    if(resendPendingMessages){
        // we need to send pending messages to the shard
        Gears_listIter* iter = Gears_listGetIterator(n->pendingMessages, AL_START_HEAD);
        Gears_listNode *node = NULL;
        while((node = Gears_listNext(iter)) != NULL){
            SentMessages* sentMsg = Gears_listNodeValue(node);
            ++sentMsg->retries;
            if(GearsConfig_SendMsgRetries() == 0 || sentMsg->retries < GearsConfig_SendMsgRetries()){
                redisAsyncCommandArgv(c, OnResponseArrived, n, 6, (const char**)sentMsg->args, sentMsg->sizes);
            }else{
                RedisModule_Log(staticCtx, "warning", "Gave up of message because failed to send it for more than %lld time", GearsConfig_SendMsgRetries());
                Gears_listDelNode(n->pendingMessages, node);
            }
        }
        Gears_listReleaseIterator(iter);
    }
    n->runId = RG_STRDUP(reply->str);
    n->status = NodeStatus_Connected;
}

static void Cluster_DisconnectCallback(const struct redisAsyncContext* c, int status){
    RedisModule_Log(staticCtx, "warning", "disconnected : %s:%d, status : %d, will try to reconnect.\r\n", c->c.tcp.host, c->c.tcp.port, status);
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    n->status = NodeStatus_Disconnected;
    n->c = NULL;
    struct timeval tv = {
            .tv_sec = 1,
    };
    event_add(n->reconnectEvent, &tv);
}

/* Callback for passing a keyfile password stored as an sds to OpenSSL */
static int RG_TlsPasswordCallback(char *buf, int size, int rwflag, void *u) {
    const char *pass = u;
    size_t pass_len;

    if (!pass) return -1;
    pass_len = strlen(pass);
    if (pass_len > (size_t) size) return -1;
    memcpy(buf, pass, pass_len);

    return (int) pass_len;
}

SSL_CTX* RG_CreateSSLContext(const char *cacert_filename,
                             const char *cert_filename,
                             const char *private_key_filename,
                             const char *private_key_pass,
                             redisSSLContextError *error)
{
    SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_client_method());
    if (!ssl_ctx) {
        if (error) *error = REDIS_SSL_CTX_CREATE_FAILED;
        goto error;
    }

    SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

    /* always set the callback, otherwise if key is encrypted and password
     * was not given, we will be waiting on stdin. */
    SSL_CTX_set_default_passwd_cb(ssl_ctx, RG_TlsPasswordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(ssl_ctx, (void *) private_key_pass);

    if ((cert_filename != NULL && private_key_filename == NULL) ||
            (private_key_filename != NULL && cert_filename == NULL)) {
        if (error) *error = REDIS_SSL_CTX_CERT_KEY_REQUIRED;
        goto error;
    }

    if (cacert_filename) {
        if (!SSL_CTX_load_verify_locations(ssl_ctx, cacert_filename, NULL)) {
            if (error) *error = REDIS_SSL_CTX_CA_CERT_LOAD_FAILED;
            goto error;
        }
    }

    if (cert_filename) {
        if (!SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_filename)) {
            if (error) *error = REDIS_SSL_CTX_CLIENT_CERT_LOAD_FAILED;
            goto error;
        }
        if (!SSL_CTX_use_PrivateKey_file(ssl_ctx, private_key_filename, SSL_FILETYPE_PEM)) {
            if (error) *error = REDIS_SSL_CTX_PRIVATE_KEY_LOAD_FAILED;
            goto error;
        }
    }

    return ssl_ctx;

error:
    if (ssl_ctx) SSL_CTX_free(ssl_ctx);
    return NULL;
}

static void Cluster_ConnectCallback(const struct redisAsyncContext* c, int status){
    if(!c->data){
        return;
    }
    Node* n = (Node*)c->data;
    if(status == -1){
        // connection failed lets try again
        n->c = NULL;
        struct timeval tv = {
                .tv_sec = 1,
        };
        event_add(n->reconnectEvent, &tv);
    }else{
        char* client_cert = NULL;
        char* client_key = NULL;
        char* ca_cert = NULL;
        char* key_file_pass = NULL;
        if(checkTLS(&client_key, &client_cert, &ca_cert, &key_file_pass)){
            redisSSLContextError ssl_error = 0;
            SSL_CTX *ssl_context = RG_CreateSSLContext(ca_cert, client_cert, client_key, key_file_pass, &ssl_error);
            RG_FREE(client_key);
            RG_FREE(client_cert);
            RG_FREE(ca_cert);
            if (key_file_pass) {
                RG_FREE(key_file_pass);
            }
            if(ssl_context == NULL || ssl_error != 0) {
                event_add(n->asyncDisconnectEvent, 0);
                return;
            }
            SSL *ssl = SSL_new(ssl_context);
            if (redisInitiateSSL((redisContext *)(&c->c), ssl) != REDIS_OK) {
                event_add(n->asyncDisconnectEvent, 0);
                return;
            }
        }

        RedisModule_Log(staticCtx, "notice", "connected : %s:%d, status = %d\r\n", c->c.tcp.host, c->c.tcp.port, status);
        if(n->password){
            redisAsyncCommand((redisAsyncContext*)c, NULL, NULL, "AUTH %s", n->password);
        }
        if(n->sendClusterTopologyOnNextConnect && CurrCluster->clusterSetCommand){
            RedisModule_Log(staticCtx, "notice", "Sending cluster topology to %s (%s:%d) after reconnect", n->id, n->ip, n->port);
            CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = RG_STRDUP(n->id);
            redisAsyncCommandArgv((redisAsyncContext*)c, NULL, NULL, CurrCluster->clusterSetCommandSize, (const char**)CurrCluster->clusterSetCommand, NULL);
            RG_FREE(CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX]);
            CurrCluster->clusterSetCommand[CLUSTER_SET_MY_ID_INDEX] = NULL;
            n->sendClusterTopologyOnNextConnect = false;
        }
        redisAsyncCommand((redisAsyncContext*)c, RG_HelloResponseArrived, n, "RG.HELLO");
        n->status = NodeStatus_HelloSent;
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

static const char* Cluster_ReadRunId(){
    return CurrCluster->runId;
}

static void Cluster_Set(RedisModuleCtx* ctx, RedisModuleString** argv, int argc){
    if(CurrCluster){
        Cluster_Free();
    }

    RedisModule_Log(staticCtx, "notice", "Got cluster set command");

    if(argc < 10){
        RedisModule_Log(staticCtx, "warning", "Could not parse cluster set arguments");
        return;
    }

    CurrCluster = RG_CALLOC(1, sizeof(*CurrCluster));

    // generate runID
    RedisModule_GetRandomHexChars(CurrCluster->runId, RUN_ID_SIZE);
    CurrCluster->runId[RUN_ID_SIZE] = '\0';

    CurrCluster->clusterSetCommand = RG_ALLOC(sizeof(char*) * argc);
    CurrCluster->clusterSetCommandSize = argc;

    CurrCluster->clusterSetCommand[0] = RG_STRDUP(RG_CLUSTER_SET_FROM_SHARD_COMMAND);

    for(int i = 1 ; i < argc ; ++i){
        if(i == CLUSTER_SET_MY_ID_INDEX){
            CurrCluster->clusterSetCommand[i] = NULL;
            continue;
        }
        const char* arg = RedisModule_StringPtrLen(argv[i], NULL);
        CurrCluster->clusterSetCommand[i] = RG_STRDUP(arg);
    }

    size_t myIdLen;
    const char* myId = RedisModule_StringPtrLen(argv[CLUSTER_SET_MY_ID_INDEX], &myIdLen);
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

        if (addr[0] == '[') {
            addr += 1; /* skip ipv6 opener `[` */
        }

        /* Find last `:` */
        char* iter = strstr(addr, ":");
        char* ipEnd = NULL;
        while (iter) {
            ipEnd = iter;
            iter++;
            iter = strstr(iter, ":");
        }

        RedisModule_Assert(ipEnd);

        size_t ipSize = ipEnd - addr;

        if (addr[ipSize - 1] == ']') {
            --ipSize; /* Skip ipv6 closer `]` */
        }

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

    RedisModule_Log(staticCtx, "notice", "Got cluster refresh command");

    CurrCluster = RG_CALLOC(1, sizeof(*CurrCluster));

    // generate runID
    RedisModule_GetRandomHexChars(CurrCluster->runId, RUN_ID_SIZE);
    CurrCluster->runId[RUN_ID_SIZE] = '\0';

    CurrCluster->clusterSetCommand = NULL;
    CurrCluster->clusterSetCommandSize = 0;

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
        RedisModule_Assert(RedisModule_CallReplyLength(nodeDetailsRelpy) >= 3);
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
    case CLUSTER_SET_MSG:
        for(size_t i = 0 ; i < msg->clusterSet.argc ; ++i){
            RedisModule_FreeString(0, msg->clusterSet.argv[i]);
        }
        RG_FREE(msg->clusterSet.argv);
        break;
    case CLUSTER_REFRESH_MSG:
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
    sentMsg->args[2] = CurrCluster->runId;
    sentMsg->sizes[2] = strlen(CurrCluster->runId);
    sentMsg->args[3] = RG_STRDUP(msg->function);
    sentMsg->sizes[3] = strlen(sentMsg->args[3]);
    sentMsg->args[4] = RG_ALLOC(sizeof(char) * msg->msgLen);
    memcpy(sentMsg->args[4], msg->msg, msg->msgLen);
    sentMsg->sizes[4] = msg->msgLen;

    RedisModuleString *msgIdStr = RedisModule_CreateStringFromLongLong(NULL, node->msgId++);
    size_t msgIdStrLen;
    const char* msgIdCStr = RedisModule_StringPtrLen(msgIdStr, &msgIdStrLen);

    sentMsg->args[5] = RG_STRDUP(msgIdCStr);
    sentMsg->sizes[5] = msgIdStrLen;

    RedisModule_FreeString(NULL, msgIdStr);

    if(node->status == NodeStatus_Connected){
        redisAsyncCommandArgv(node->c, OnResponseArrived, node, 6, (const char**)sentMsg->args, sentMsg->sizes);
    }else{
        RedisModule_Log(staticCtx, "warning", "message was not sent because status is not connected");
    }
    Gears_listAddNodeTail(node->pendingMessages, sentMsg);
}

static void Cluster_SendMessage(SendMsg* sendMsg){
    if(sendMsg->idToSend[0] != '\0'){
        Node* n = GetNode(sendMsg->idToSend);
        if(!n){
            RedisModule_Log(staticCtx, "warning", "Could not find node to send message to");
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
        if(msg->clusterSet.force || !CurrCluster){
            // we will update the cluster topology only if we are not aware
            // to the cluster topology yet or we are forced to do it
            Cluster_Set(ctx, msg->clusterSet.argv, msg->clusterSet.argc);
        }
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
    msgStruct->clusterRefresh.bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    msgStruct->type = CLUSTER_REFRESH_MSG;
    write(notify[1], &msgStruct, sizeof(Msg*));
}

void Cluster_SendClusterSet(RedisModuleCtx *ctx, RedisModuleString** argv, int argc, bool force){
    Msg* msgStruct = RG_ALLOC(sizeof(*msgStruct));
    msgStruct->clusterSet.bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    msgStruct->clusterSet.argv = argv;
    msgStruct->clusterSet.argc = argc;
    msgStruct->clusterSet.force = force;
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

bool Cluster_IsInitialized(){
    if (CurrCluster){
        return true;
    }
    if(IsEnterprise()){
        // on enterprise we will always get the cluster set command so until we get it
        // we assume cluster is not initialized.
        // when cluster is not initialized we will not allow almost all operations.
        // the only operations we will allows are local registrations that can not be
        // postpond.
        return false;
    }
    // On oss we need to check if Redis in configured to be cluster,
    // if so, we will wait for RG.CLUSTERREFRESH command, otherwise we will
    // assume we are on a single shard.
    bool res = true;
    LockHandler_Acquire(staticCtx);
    if(RedisModule_GetContextFlags(staticCtx) & REDISMODULE_CTX_FLAGS_CLUSTER){
		res = true;
	}
    LockHandler_Release(staticCtx);
    return res;
}

bool Cluster_IsClusterMode(){
    return CurrCluster && CurrCluster->isClusterMode && Cluster_GetSize() > 1;
}

size_t Cluster_GetSize(){
    return Gears_dictSize(CurrCluster->nodes);
}

char myDefaultId[REDISMODULE_NODE_ID_LEN + 1];

void Cluster_Init(){
    memset(myDefaultId, '0', REDISMODULE_NODE_ID_LEN);
    myDefaultId[REDISMODULE_NODE_ID_LEN] = '\0';
    RemoteCallbacks = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    nodesMsgIds = Gears_dictCreate(&Gears_dictTypeHeapStrings, NULL);
    Cluster_StartClusterThread();
}


char* Cluster_GetMyId(){
    return (CurrCluster && CurrCluster->myId) ? CurrCluster->myId : myDefaultId;
}

const char* Cluster_GetMyHashTag(){
    if(RedisModule_ShardingGetSlotRange){
        int first, last;
        RedisModule_ShardingGetSlotRange(&first, &last);
        if (first < 0) {
            /* first < 0 means we are on a single shard database, set first=0 */
            first = 0;
        }
        return slot_table[first];
    }

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
    if(!CurrCluster){
        RedisModule_Log(staticCtx, "warning", "Got hello msg while cluster is NULL");
        return RedisModule_ReplyWithError(ctx, "ERRCLUSTER NULL cluster state on hello msg");
    }
    RedisModule_ReplyWithStringBuffer(ctx, CurrCluster->runId, strlen(CurrCluster->runId));
    return REDISMODULE_OK;
}

int Cluster_GetClusterInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
#define NO_CLUSTER_MODE_REPLY "no cluster mode"
    if(!Cluster_IsClusterMode()){
        RedisModule_ReplyWithStringBuffer(ctx, NO_CLUSTER_MODE_REPLY, strlen(NO_CLUSTER_MODE_REPLY));
        return REDISMODULE_OK;
    }
    RedisModule_ReplyWithArray(ctx, 5);
    RedisModule_ReplyWithStringBuffer(ctx, "MyId", strlen("MyId"));
    RedisModule_ReplyWithStringBuffer(ctx, CurrCluster->myId, strlen(CurrCluster->myId));
    RedisModule_ReplyWithStringBuffer(ctx, "MyRunId", strlen("MyRunId"));
    RedisModule_ReplyWithStringBuffer(ctx, CurrCluster->runId, strlen(CurrCluster->runId));
    RedisModule_ReplyWithArray(ctx, Gears_dictSize(CurrCluster->nodes));
    Gears_dictIterator *iter = Gears_dictGetIterator(CurrCluster->nodes);
    Gears_dictEntry *entry = NULL;
    while((entry = Gears_dictNext(iter))){
        Node* n = Gears_dictGetVal(entry);
        RedisModule_ReplyWithArray(ctx, 16);
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
            if(n->isMe){
                const char* runId = Cluster_ReadRunId(ctx);
                RedisModule_ReplyWithStringBuffer(ctx, runId, strlen(runId));
            }else{
                RedisModule_ReplyWithNull(ctx);
            }
        }
        RedisModule_ReplyWithStringBuffer(ctx, "minHslot", strlen("minHslot"));
        RedisModule_ReplyWithLongLong(ctx, n->minSlot);
        RedisModule_ReplyWithStringBuffer(ctx, "maxHslot", strlen("maxHslot"));
        RedisModule_ReplyWithLongLong(ctx, n->maxSlot);
        RedisModule_ReplyWithStringBuffer(ctx, "pendingMessages", strlen("pendingMessages"));
        RedisModule_ReplyWithLongLong(ctx, Gears_listLength(n->pendingMessages));

    }
    Gears_dictReleaseIterator(iter);
    return REDISMODULE_OK;
}

int Cluster_OnMsgArrive(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != 6){
        return RedisModule_WrongArity(ctx);
    }

    if(!CurrCluster){
        RedisModule_Log(staticCtx, "warning", "Got msg from another shard while cluster is not NULL");
        return RedisModule_ReplyWithError(ctx, "ERRCLUSTER NULL cluster state");
    }

    if(!Cluster_IsInitialized()){
        RedisModule_Log(staticCtx, "warning", "Got msg from another shard while cluster is not initialized");
        return RedisModule_ReplyWithError(ctx, "ERRCLUSTER Uninitialized cluster state");
    }

    RedisModuleString* senderId = argv[1];
    RedisModuleString* senderRunId = argv[2];
    RedisModuleString* functionToCall = argv[3];
    RedisModuleString* msg = argv[4];
    RedisModuleString* msgIdStr = argv[5];

    long long msgId;
    if(RedisModule_StringToLongLong(msgIdStr, &msgId) != REDISMODULE_OK){
        RedisModule_Log(staticCtx, "warning", "bad msg id given");
        RedisModule_ReplyWithError(ctx, "bad msg id given");
        return REDISMODULE_OK;
    }

    size_t senderIdLen;
    const char* senderIdStr = RedisModule_StringPtrLen(senderId, &senderIdLen);
    size_t senderRunIdLen;
    const char* senderRunIdStr = RedisModule_StringPtrLen(senderRunId, &senderRunIdLen);

    char combinedId[senderIdLen + senderRunIdLen + 1]; // +1 is for '\0'
    memcpy(combinedId, senderIdStr, senderIdLen);
    memcpy(combinedId + senderIdLen, senderRunIdStr, senderRunIdLen);
    combinedId[senderIdLen + senderRunIdLen] = '\0';

    Gears_dictEntry* entity = Gears_dictFind(nodesMsgIds, combinedId);
    long long currId = -1;
    if(entity){
        currId = Gears_dictGetSignedIntegerVal(entity);
    }else{
        entity = Gears_dictAddRaw(nodesMsgIds, (char*)combinedId, NULL);
    }
    if(msgId <= currId){
        RedisModule_Log(staticCtx, "warning", "duplicate message ignored, msgId: %lld, currId: %lld", msgId, currId);
        RedisModule_ReplyWithSimpleString(ctx, "duplicate message ignored");
        return REDISMODULE_OK;
    }
    Gears_dictSetSignedIntegerVal(entity, msgId);
    const char* functionToCallStr = RedisModule_StringPtrLen(functionToCall, NULL);
    size_t msgLen;
    const char* msgStr = RedisModule_StringPtrLen(msg, &msgLen);

    Gears_dictEntry *entry = Gears_dictFind(RemoteCallbacks, functionToCallStr);
    if(!entry){
        RedisModule_Log(staticCtx, "warning", "can not find the callback requested : %s", functionToCallStr);
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
    // we must copy argv because if the client will disconnect the redis will free it
    RedisModuleString **argvNew = RG_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_CreateStringFromString(NULL, argv[i]);
    }
    Cluster_SendClusterSet(ctx, argvNew, argc, true);
    return REDISMODULE_OK;
}

int Cluster_ClusterSetFromShard(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc < 10){
        RedisModule_ReplyWithError(ctx, "Could not parse cluster set arguments");
        return REDISMODULE_OK;
    }
    // we must copy argv because if the client will disconnect the redis will free it
    RedisModuleString **argvNew = RG_ALLOC(sizeof(RedisModuleString *) * argc);
    for(size_t i = 0 ; i < argc ; ++i){
        argvNew[i] = RedisModule_CreateStringFromString(NULL, argv[i]);
    }
    Cluster_SendClusterSet(ctx, argvNew, argc, false);
    return REDISMODULE_OK;
}
