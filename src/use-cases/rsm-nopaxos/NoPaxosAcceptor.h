/**
 * @file NoPaxosAcceptor.h
 * @author jskrzypczak
 * @date 2019-12-11
 */

#pragma once

#include "../../flow-api/dfi.h"
#include "Settings.h"
#include "Log.h"
#include "utils/Thread.h"
#include "KV_SM.h"

#define lgae(message) std::cout << Settings::NODE_ID << " (Accept) Line " << __LINE__ << ": " << (message) << std::endl

//#define lga(message) std::cout << Settings::NODE_ID << " (Accept) Line " << __LINE__ << ": " << (message) << std::endl
#define lga(message) (void)0 // noop macro

inline TargetID identityShuffle(const dfi::Tuple &tuple)
{
    return (TargetID) (*((int8_t*) tuple.getDataPtr())) ;
}

class NoPaxosAcceptor
{
    class GapAgreementWorker : public utils::Thread
    {
        private:
            NoPaxosAcceptor& acceptor;
        public:
            GapAgreementWorker(NoPaxosAcceptor& x);
            ~GapAgreementWorker();
            void run() override;
    };
    class MainWorker : public utils::Thread
    {
        private:
            NoPaxosAcceptor& acceptor;
        public:
            MainWorker(NoPaxosAcceptor& x);
            ~MainWorker();

            void process_leader_gap(uint64_t);
            Command process_follower_gap(uint64_t);
            void verify_log_consistency();
            void run() override;
    };

    private:
        NoPaxosAcceptor::GapAgreementWorker gapworker;
        NoPaxosAcceptor::MainWorker mainworker;

        KV_Store store;
        Log log;
        bool isLeader;
        int8_t replica_id;

        DFI_Replicate_flow_target request_flow;
        DFI_Shuffle_flow_source response_flow;

        // gap agreement
        DFI_Shuffle_flow_source gap_leader_source;
        DFI_Shuffle_flow_source gap_follower_source;
        DFI_Shuffle_flow_target gap_leader_target;
        DFI_Shuffle_flow_target gap_follower_target;

        uint64_t viewid = 1; // should actually be {leader, session} but we do not change leader in demo
        uint64_t session_msg_num = 0;
    public:
        NoPaxosAcceptor(bool isLeader);
        ~NoPaxosAcceptor();

        void start();
        void join();
};

NoPaxosAcceptor::NoPaxosAcceptor(bool leader) :
    gapworker(*this), mainworker(*this),
    store(),
    log(Settings::COMMAND_COUNT), isLeader(leader), replica_id(Settings::NODE_ID),
    request_flow(Settings::FLOWNAME_OUM, Settings::NODE_ID),
    response_flow(Settings::FLOWNAME_RESPONSE),
    gap_leader_source(Settings::FLOWNAME_GAP_LEADER),
    gap_follower_source(Settings::FLOWNAME_GAP_FOLLOWER),
    gap_leader_target(Settings::FLOWNAME_GAP_LEADER, replica_id),
    gap_follower_target(Settings::FLOWNAME_GAP_FOLLOWER, replica_id)
{
}
NoPaxosAcceptor::~NoPaxosAcceptor() {}

void NoPaxosAcceptor::start()
{
    gapworker.start();
    mainworker.start();
}

void NoPaxosAcceptor::join()
{
    mainworker.join();
}

NoPaxosAcceptor::GapAgreementWorker::GapAgreementWorker(NoPaxosAcceptor& x) : acceptor(x) {}
NoPaxosAcceptor::GapAgreementWorker::~GapAgreementWorker() {}

NoPaxosAcceptor::MainWorker::MainWorker(NoPaxosAcceptor& x) : acceptor(x) {}
NoPaxosAcceptor::MainWorker::~MainWorker() {}


void NoPaxosAcceptor::MainWorker::run()
{
    lgae("Acceptor started");

    lgae("Waiting for initial message from all clients...");
    // wait for a first message from all clients to ensure that all start the benchmark at roughly
    // the same time. Client block until receiving the first response.
    size_t tuplesConsumed;
    dfi::Tuple tuple;
    int8_t leader_reply = acceptor.isLeader;
    for (size_t i = 1; i <= Settings::CLIENT_COUNT; i++)
    {
        acceptor.request_flow.consume(tuple, tuplesConsumed);
    }
    // send dummy replies to all clients
    for (size_t i = 1; i <= Settings::CLIENT_COUNT; i++)
    {
        ClientResponse response({(int8_t) i, (uint64_t) 0, (uint64_t) 0, (uint64_t) 0,
            (int8_t) leader_reply, 0, 0});
        dfi::Tuple respTuple(&response);
        acceptor.response_flow.push(respTuple, identityShuffle);
    }

    lgae("All clients started! Proceed with normal operation...");
    int ret;
    while ((ret = acceptor.request_flow.consume(tuple, tuplesConsumed)) != DFI_FLOW_FINISHED)
    {
        ClientRequest* request = (ClientRequest*) tuple.getDataPtr();
        acceptor.session_msg_num++;

        uint64_t current_logslot = acceptor.log.get_next_slot_index();
        if (ret == DFI_MESSAGE_LOST)
        {
            lga("MESSAGE LOST - processing gap agreement; slot " + to_string(current_logslot));
            if (acceptor.isLeader)
            {
                // notify followers to write NOOP in this slot
                acceptor.log.append_command(NOOP_CMD);
                lga("Leader setting NOOP at slot " + to_string(current_logslot));
                process_leader_gap(current_logslot);
            }
            else
            {
                // retrieve cmd from leader
                Command cmd = process_follower_gap(current_logslot);
                lga("Retrieved leader command (Slot " + to_string(current_logslot) + ") to: " + to_string(cmd.key) + " " + to_string(cmd.operation) + " " + to_string(cmd.value));
                acceptor.log.append_command(cmd);
            }
        }
        else
        {
            if (tuplesConsumed != 1)
            {
                lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
            }

            int8_t leader_reply = acceptor.isLeader;
            int8_t success_code = Return::SUCCESS;
            uint64_t result = UNUSED;

            acceptor.log.append_command(request->cmd);

            // As per protocol, only the leader applies its commands immediately to the state machine.
            // The followers are not allowed to do so immediately, as they may recieve a GAP notification
            // from the leader.
            if (acceptor.isLeader)
            {
                if (request->cmd.operation == Operation::READ)
                {
                    success_code = acceptor.store.lookup(request->cmd, result);
                }
                else
                {
                    success_code = acceptor.store.insert(request->cmd);
                }
            }

            int8_t clientid = request->clientid;
            uint64_t reqid = request->reqid;

            ClientResponse response({clientid, acceptor.viewid, current_logslot, reqid, leader_reply, success_code, result});
            dfi::Tuple respTuple(&response);
            acceptor.response_flow.push(respTuple, identityShuffle);
        }
    }

    acceptor.gap_leader_source.close();
    acceptor.gap_follower_source.close();
    acceptor.response_flow.close();

    lgae("NORMAL OPERATION COMPLETED");
    verify_log_consistency();
}

void NoPaxosAcceptor::MainWorker::process_leader_gap(uint64_t log_slot)
{
    // notify followers that they have to commit a NOOP at this slot
    for (uint8_t i = 1; i <= Settings::REPLICA_COUNT; i++)
    {
        if (i == Settings::LEADER_ID) continue;

        GapLeaderMsg request({(int8_t) i, log_slot});
        dfi::Tuple reqTuple(&request);
        acceptor.gap_leader_source.push(reqTuple, identityShuffle);
    }

    // wait for quorum
    size_t acks = 1; // including ourselve
    dfi::Tuple tuple;
    size_t tuplesConsumed;
    while (acks < 1 + (Settings::REPLICA_COUNT / 2))
    {
        if (acceptor.gap_leader_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
        {
            if (tuplesConsumed != 1)
            {
                lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
            }
            GapLeaderMsg* response = (GapLeaderMsg*) tuple.getDataPtr();

            if (response->log_slot_num < log_slot)
            {
                // An outdated response sneaked in
                //lgae("Outdated response!");
            }
            else
            {
                acks++;
            }
        }
        else
        {
            lgae("ERROR: Leader gap agreement flow ended prematurely");
            return;
        }
    }
}

Command NoPaxosAcceptor::MainWorker::process_follower_gap(uint64_t log_slot)
{
    // request command from leader
    GapFollowerMsg request({(int8_t) Settings::LEADER_ID, acceptor.replica_id, log_slot, NOOP_CMD}); // cmd is unused
    dfi::Tuple reqTuple(&request);
    acceptor.gap_follower_source.push(reqTuple, identityShuffle);

    // wait for leader response
    dfi::Tuple tuple;
    size_t tuplesConsumed;
    if (acceptor.gap_follower_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        if (tuplesConsumed != 1)
        {
            lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
        }
        GapFollowerMsg* response = (GapFollowerMsg*) tuple.getDataPtr();

        return response->cmd;
    }
    else
    {
        lgae("ERROR: Follower gap agreement flow ended prematurely");
        return NOOP_CMD;
    }
}

// Verify the correctness of the implementation by checking the consistency of the command logs
// on all replicas after the benchmark is done. This is done in a separate message flow. The
// leader simply collects all log entries of the replicas and compares with its own log. 
void NoPaxosAcceptor::MainWorker::verify_log_consistency()
{
    size_t tuplesConsumed;
    dfi::Tuple tuple;

    if (acceptor.isLeader)
    {
        lgae("VERYFYING THE CONSISTENCY OF LOG ENTRIES OF ALL REPLICAS");

        uint64_t issues_found = 0;
        uint64_t checks_done = 0;
        DFI_Shuffle_flow_target verify_target(Settings::FLOWNAME_VERIFICATION, acceptor.replica_id);
        while (verify_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
        {
            if (tuplesConsumed != 1)
            {
                lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
            }
            VerificationMsg* request = (VerificationMsg*) tuple.getDataPtr();

            Command leaderCmd = acceptor.log.get_command(request->log_slot_num);
            Command followerCmd = request->cmd;
            if(!commands_equal(leaderCmd, followerCmd))
            {
                lgae("Command mismatch (Slot " + to_string(request->log_slot_num) + ") ! \n Leader cmd:   " + to_string(leaderCmd.key) + " " + to_string(leaderCmd.operation) + " " + to_string(leaderCmd.value) +
                                       "\n Follower cmd: " + to_string(followerCmd.key) + " " + to_string(followerCmd.operation) + " " + to_string(followerCmd.value));
                issues_found++;
            }
            checks_done++;
        }
        lgae("Log verification complete! Number of issues found: " + to_string(issues_found) +
            ". Remote commands checked: " + to_string(checks_done));
    }
    else 
    {
        DFI_Shuffle_flow_source verify_source(Settings::FLOWNAME_VERIFICATION);
        for (uint64_t slot = 1; slot < acceptor.log.get_next_slot_index(); slot++)
        {
            VerificationMsg request({slot, acceptor.log.get_command(slot)});
            dfi::Tuple reqTuple(&request);
            verify_source.push(reqTuple);
        }
        verify_source.close();
    }
    lgae("TERMINATING ACCEPTOR");
}


void NoPaxosAcceptor::GapAgreementWorker::run()
{
    dfi::Tuple tuple;
    size_t tuplesConsumed;
    if (acceptor.isLeader)
    {
        // follower got a GAP and notifies leader... leader sends its own slot back
        while (acceptor.gap_follower_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
        {
            if (tuplesConsumed != 1)
            {
                lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
            }
            GapFollowerMsg* request = (GapFollowerMsg*) tuple.getDataPtr();

            uint64_t slot = request->log_slot_num;
            int8_t target = request->sender;

            // wait unitl this slot is filled...
            while (acceptor.log.get_next_slot_index() <= slot)
            {
                lga("Gap: Leader must wait as slot is not filled yet");
            }
            Command cmd = acceptor.log.get_command(slot);

            lga("Leader was requested command (Slot " + to_string(slot) + "). It is: " + to_string(cmd.key) + " " + to_string(cmd.operation) + " " + to_string(cmd.value));
            GapFollowerMsg response({target, acceptor.replica_id, slot, cmd});
            dfi::Tuple respTuple(&response);
            acceptor.gap_follower_source.push(respTuple, identityShuffle);
        }
    }
    else
    {
        // leader got a GAP and notfies followers... set own log to NOOP and respond
        while (acceptor.gap_leader_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
        {
            if (tuplesConsumed != 1)
            {
                lgae("Expected only one consumed tuple; got: " + to_string(tuplesConsumed));
            }
            GapLeaderMsg* request = (GapLeaderMsg*) tuple.getDataPtr();

            // set this log slot to NOOP
            uint64_t slot = request->log_slot_num;
            while (acceptor.log.get_next_slot_index() <= slot)
            {
                lga("Gap: Follower must wait as slot is not filled yet");
            }
            lga("Leader requested that follower sets NOOP at slot " + to_string(slot));
            acceptor.log.set_command(slot, NOOP_CMD);

            int8_t target = Settings::LEADER_ID;
            GapLeaderMsg response({target, slot});
            dfi::Tuple respTuple(&response);
            acceptor.gap_leader_source.push(respTuple, identityShuffle);
        }
    }
}