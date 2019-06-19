/*
 * consensus.h
 *
 *  Created on: Jun 17, 2019
 *      Author: root
 */

#ifndef CLUSTER_CONSENSUS_H_
#define CLUSTER_CONSENSUS_H_

#include "utils/adlist.h"
#include <stddef.h>
#include <stdbool.h>

typedef void (*Consensus_OnMsgAproved)(void* privateData, const char* msg, size_t len, void* additionalData);

typedef struct Proposer{
    long long proposalId;
    long long biggerProposalId;
    char* val;
    size_t len;
    size_t recruitedNumber;
    size_t acceptedNumber;
}Proposer;

typedef struct Acceptor{
    long long proposalId;
    char* val;
    size_t len;
}Acceptor;

typedef struct Learner{
    long long proposalId;
    long long learnedNumber;
    char* originalVal;
    size_t originalLen;
    bool valueLeared;
}Learner;

typedef enum ConsensusPhase{
    PHASE_ONE, PHASE_TWO, PHASE_DONE
}ConsensusPhase;

typedef struct ConsensusInstance{
    long long consensusId;
    Proposer proposer;
    Acceptor acceptor;
    Learner learner;
    ConsensusPhase phase;
    void* additionalData;
}ConsensusInstance;

typedef struct Consensus{
    char* name;
    long long currConsensusId;
    Gears_list* consensusInstances;
    Consensus_OnMsgAproved approvedCallback;
    void* privateData;
}Consensus;

int Consensus_Init();
Consensus* Consensus_Create(const char* name, Consensus_OnMsgAproved approvedCallback, void* privateData);
void Consensus_Send(Consensus* consensus, const char* msg, size_t len, void* additionalData);

#endif /* CLUSTER_CONSENSUS_H_ */
