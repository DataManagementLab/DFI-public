/**
 * @file PaxosLeader.h
 * @author jskrzypczak
 * @date 2019-12-11
 */

#pragma once

#include "../../flow-api/dfi.h"
#include "Settings.h"
#include "PaxosAcceptor.h"
#include "utils/Thread.h"
#include <chrono>

#define lgle(message) std::cout << Settings::NODE_ID << " (Leader) Line " << __LINE__ << ": " << (message) << std::endl

//#define lgl(message) std::cout << Settings::NODE_ID << " (Leader) Line " << __LINE__ << ": " << (message) << std::endl
#define lgl(message) (void)0 // noop macro

inline TargetID identityShuffle(const dfi::Tuple &tuple)
{
    return (TargetID) (*((int8_t*) tuple.getDataPtr())) ;
}

struct req_info
{
    uint64_t req_id;
    uint64_t slot_id;
    int8_t client_id;
    Command cmd;
    bool replied;
    uint8_t ack_count;
};

class PaxosLeader
{
    class Sender : public utils::Thread
    {
        private:
            PaxosLeader& leader;
        public:
            Sender(PaxosLeader&);
            ~Sender();

            void run() override;
            void verify_log_consistency();
    };
    class Receiver : public utils::Thread
    {
        private:
            PaxosLeader& leader;
        public:
            Receiver(PaxosLeader&);
            ~Receiver();
            void run() override;
    };

    class LocalPaxosAcceptor : public PaxosAcceptor
    {
        PaxosLeader& leader;
        public:
            LocalPaxosAcceptor(PaxosLeader& leader);
            ~LocalPaxosAcceptor();
            void run() override;
    };

private:
    std::vector<req_info> open_reqs;

    DFI_Shuffle_flow_target client_target;
    DFI_Shuffle_flow_source client_source;

    bool running = true;
    bool established = false;
    uint64_t committed = 0;
    uint64_t slot_id = 0;
    uint64_t req_id = 0;
    uint64_t proposal_num = 0;

    uint64_t last_req_id_finished = 0; // for synchronous code preventing pipelining

    DFI_Replicate_flow_source phase1_source;
    DFI_Replicate_flow_source phase2_source;
    DFI_Shuffle_flow_target phase1_target;
    DFI_Shuffle_flow_target phase2_target;

    PaxosLeader::Sender sender;
    PaxosLeader::Receiver receiver;
    PaxosLeader::LocalPaxosAcceptor acceptor;

    inline bool propose_next_command(Command& cmd, int8_t clientid);
    inline void add_phase2_reply(Phase2Response*);

    inline uint8_t quorum_count();

protected:


public:
    PaxosLeader(/* args */);
    ~PaxosLeader();

    void start();
    void join();
};

PaxosLeader::PaxosLeader(/* args */) :
    open_reqs(Settings::COMMAND_COUNT, req_info()), 
    client_target(Settings::FLOWNAME_CLIENT_REQ, {Settings::LEADER_ID}),
    client_source(Settings::FLOWNAME_CLIENT_RESP, Settings::LEADER_ID),
    phase1_source(Settings::FLOWNAME_P1REQ, Settings::LEADER_ID),
    phase2_source(Settings::FLOWNAME_P2REQ, Settings::LEADER_ID),
    phase1_target(Settings::FLOWNAME_P1RESP, {Settings::LEADER_ID}),
    phase2_target(Settings::FLOWNAME_P2RESP, {Settings::LEADER_ID}),
    sender(*this), receiver(*this), acceptor(*this)
{
}

PaxosLeader::~PaxosLeader()
{
}

void PaxosLeader::start()
{
    sender.start();
    receiver.start();
}

void PaxosLeader::join()
{
    sender.join();
}

PaxosLeader::Sender::Sender(PaxosLeader& x) : leader(x) {}
PaxosLeader::Sender::~Sender() {}

PaxosLeader::Receiver::Receiver(PaxosLeader& x) : leader(x) {}
PaxosLeader::Receiver::~Receiver() {}

PaxosLeader::LocalPaxosAcceptor::LocalPaxosAcceptor(PaxosLeader& x) : leader(x) {}
PaxosLeader::LocalPaxosAcceptor::~LocalPaxosAcceptor() {}

void PaxosLeader::Sender::run()
{
    lgl("Starting leader");

    // Initially wait until we have received a message from every client
    // to ensure that all clients in the benchmark start at the same time.
    // Clients block until receiveing the first response.
    leader.slot_id = 1;
    size_t tuplesConsumed;
    vector<dfi::Tuple> tuples;
    for (size_t i = 1; i <= Settings::CLIENT_COUNT; i++)
    {
        dfi::Tuple t;
        leader.client_target.consume(t, tuplesConsumed);
        tuples.push_back(t);
    }
    for (auto const& t: tuples)
    {
        ClientRequest* request = (ClientRequest*) t.getDataPtr();
        if (leader.propose_next_command(request->cmd, request->clientid))
        {
            leader.slot_id++;
        }
        else
        {
            lgle("Error: command couldn't be proposed for some reason");
        }
    }

    lgae("All clients started! Proceed with normal operation...");
    dfi::Tuple tuple;
    while(leader.client_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        if (tuplesConsumed != 1)
        {
            lgle("Expected ony one consumed tuple; got " + to_string(tuplesConsumed));
        }
        ClientRequest* request = (ClientRequest*) tuple.getDataPtr();

        bool success = leader.propose_next_command(request->cmd, request->clientid);
        if (!success)
        {
            lgle("Error: command couldn't be proposed for some reason");
        }
        else
        {
            leader.slot_id++;
        }

        // wait until the previous request is finished if mode is 'synchronous'
        // i.e. submitt a new request only if the previous one is done
        while (Settings::MODE == Mode::SYNCHRONOUS && leader.req_id != leader.last_req_id_finished);
    }
    leader.phase2_source.close();
    leader.running = false;

    lgae("NORMAL OPERATION COMPLETED");
    verify_log_consistency();
}


// Verify the correctness of the implementation by checking the consistency of the command logs
// on all replicas after the benchmark is done. This is done in a separate message flow. The
// leader simply collects all log entries of the replicas and compares with its own log. 
void PaxosLeader::Sender::verify_log_consistency() {
    size_t tuplesConsumed;
    dfi::Tuple tuple;

    lgle("Staring log verificaiton phase");
    uint64_t checks_done = 0;
    uint64_t issues_found = 0;
    DFI_Shuffle_flow_target verify_target(Settings::FLOWNAME_VERIFICATION, 1);

    while (verify_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        if (tuplesConsumed != 1)
        {
            lgle("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
        }
        VerificationMsg* request = (VerificationMsg*) tuple.getDataPtr();

        Command leaderCmd = leader.acceptor.get_command(request->slotid);
        Command followerCmd = request->cmd;
        if(!commands_equal(leaderCmd, followerCmd))
        {
            lgle("Command mismatch! \n Leader cmd:   " + to_string(leaderCmd.key) + " " + to_string(leaderCmd.operation) + " " + to_string(leaderCmd.value) +
                                    "\n Follower cmd: " + to_string(followerCmd.key) + " " + to_string(followerCmd.operation) + " " + to_string(followerCmd.value));
            issues_found++;
        }
        checks_done++;
    }
    lgae("Log verification complete! Number of issues found: " + to_string(issues_found) +
        ". Remote commands checked: " + to_string(checks_done));

    lgle("LEADER IS DONE");
}


void PaxosLeader::Receiver::run()
{
    size_t tuplesConsumed;
    dfi::Tuple tuple;
    while (leader.phase2_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        if (tuplesConsumed != 1) 
        {
            lgle("Expected ony one consumed tuple; got " + to_string(tuplesConsumed));
        }
        Phase2Response* response = (Phase2Response*) tuple.getDataPtr();
        leader.add_phase2_reply(response);
    }
    leader.client_source.close();
}

void PaxosLeader::LocalPaxosAcceptor::run()
{
}

bool PaxosLeader::propose_next_command(Command& command, int8_t clientid)
{
    if (!established)
    {
        // Leader is newly elected and must therefore execute the first phase
        proposal_num++;
        req_id++;

        lgl("Sending phase1 request with proposal number " + to_string(proposal_num));
        Phase1Request request({(uint64_t) req_id, proposal_num, UNUSED, UNUSED, EMPTY_CMD});

        // first ask local acceptor
        Phase1Response localResponse;
        acceptor.phase1(&request, localResponse);
        if (localResponse.ret == Return::FAILED)
        {
            // shouldn't happen
            lgle("Local acceptor denied phase1");
            proposal_num = localResponse.accepted_proposal;
            return false; // retry with higher round
        } 
        // ask remaining remote acceptors
        if (Settings::REPLICA_COUNT > 1) 
        {
            dfi::Tuple reqTuple(&request);
            phase1_source.push(reqTuple);

            dfi::Tuple responseTuple;
            size_t replies_seen = 1;
            size_t consumed = 0;
            while (phase1_target.consume(responseTuple, consumed) != DFI_FLOW_FINISHED) {
                if (consumed > 1) {
                    lgle("Expected one consumed tuple");
                }
                Phase1Response *response = (Phase1Response*) responseTuple.getDataPtr();
                
                if (response->ret == Return::FAILED)
                {
                    proposal_num = response->accepted_proposal;
                    return false; 
                }
                replies_seen++;
                if (replies_seen >= quorum_count()) {
                    break;
                }
            }
        }

        // a majority has agreed that we are the leader.. we can skip first phase now
        phase1_source.close(); // we don't need that flow anymore and is indication for acceptors to listen to phase2
        established = true;
    }

    // phase2 
    req_id++;
    Phase2Request request2({(uint64_t) req_id, proposal_num, committed, slot_id, command});
    open_reqs[req_id % open_reqs.size()] = {req_id, slot_id, clientid, command, false, (uint8_t) 0};

    Phase2Response p2resp;
    acceptor.phase2(&request2, p2resp);
    add_phase2_reply(&p2resp);
    if (Settings::REPLICA_COUNT > 1)
    {
        dfi::Tuple reqTuple2(&request2);
        phase2_source.push(reqTuple2);
    }

    return true;
}

void PaxosLeader::add_phase2_reply(Phase2Response* response)
{
    if (response->ret != Return::SUCCESS)
    {
        // acceptor denied our request as it has seen higher proposal number

        lgle("Message was denied due to higher existing proposal number?!");
        size_t idx = response->reqid % open_reqs.size();
        open_reqs[idx].req_id = 0; // make this request invalid for now, which means client never gets replies

        //proposal_num = max(proposal_num, response->highest_seen_proposal);
        //established = false;
        return;
    }

    uint64_t reqid = response->reqid;
    size_t idx = reqid % open_reqs.size();
    if (open_reqs[idx].replied == true) 
    {
        lgl("Outdated message");
        return;
    }
    if (open_reqs[idx].req_id > req_id) {
        lgle("Warning: message was so long delayed that newer request already put into same array slot");
        return;
    }

    open_reqs[idx].ack_count++;
    if (open_reqs[idx].ack_count >= quorum_count())
    {
        // got quorum of acks.. the operation is leadnerd
        // Apply the command to the RSM and return the responds to the client
        Command cmd = open_reqs[idx].cmd;
        int8_t success_code;
        uint64_t result = UNUSED;
        if (cmd.operation == Operation::READ)
        {
            success_code = acceptor.store.lookup(cmd, result);
        }
        else
        {
            success_code = acceptor.store.insert(cmd);
        }

        ClientResponse response{open_reqs[idx].client_id, success_code, result};
        dfi::Tuple retTuple(&response);
        client_source.push(retTuple, identityShuffle);
        committed = open_reqs[idx].slot_id; 
        last_req_id_finished = req_id;
        open_reqs[idx].replied = true;
        if (committed % 100000 == 0) {
            lgle("Processed commands up until slot " + to_string(committed));
        }
    }
}

inline uint8_t PaxosLeader::quorum_count() {
    return 1 + (Settings::REPLICA_COUNT / 2);
}