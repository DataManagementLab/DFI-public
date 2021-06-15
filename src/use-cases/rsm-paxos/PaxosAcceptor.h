/**
 * @file PaxosAcceptor.h
 * @author jskrzypczak
 * @date 2019-12-11
 */

#pragma once

#include "../../flow-api/dfi.h"
#include "Log.h"
#include "Settings.h"
#include "utils/Thread.h"
#include "KV_SM.h"

#define lgae(message) std::cout << Settings::NODE_ID << " (Accept) Line " << __LINE__ << ": " << (message) << std::endl
//#define lga(message) std::cout << Settings::NODE_ID << " (Accept) Line " << __LINE__ << ": " << (message) << std::endl

#define lga(message) (void)0 // noop macro

class PaxosAcceptor : public utils::Thread
{
    protected:
        uint64_t highest_seen_proposal = 0;
        uint64_t accepted_proposal = 0;

        Log log;

    public:
        PaxosAcceptor();
        ~PaxosAcceptor();

        KV_Store store;

        inline void phase1(Phase1Request*, Phase1Response&);
        inline void phase2(Phase2Request*, Phase2Response&);
        inline Command get_command(uint64_t slotid);
};

class RemotePaxosAcceptor : public PaxosAcceptor
{
    private:
        DFI_Replicate_flow_target phase1_target;
        DFI_Replicate_flow_target phase2_target;
        DFI_Shuffle_flow_source phase1_src;
        DFI_Shuffle_flow_source phase2_src;
    
    public:
        RemotePaxosAcceptor();
        ~RemotePaxosAcceptor();
        void run() override;
};

PaxosAcceptor::PaxosAcceptor() : log(10000000)
{
}
PaxosAcceptor::~PaxosAcceptor() {}

RemotePaxosAcceptor::RemotePaxosAcceptor(/* args */) :
    phase1_target(Settings::FLOWNAME_P1REQ, Settings::NODE_ID),
    phase2_target(Settings::FLOWNAME_P2REQ, Settings::NODE_ID),
    phase1_src(Settings::FLOWNAME_P1RESP),
    phase2_src(Settings::FLOWNAME_P2RESP)
{
}
RemotePaxosAcceptor::~RemotePaxosAcceptor() {}

void RemotePaxosAcceptor::run()
{
    dfi::Tuple tuple;
    size_t tuplesConsumed;
    while(phase1_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        Phase1Request *req = (Phase1Request*) tuple.getDataPtr();

        lga("got a phase 1 request");
        Phase1Response p1response;
        phase1(req, p1response);
        dfi::Tuple respTuple(&p1response);
        phase1_src.push(respTuple);
    }
    lgae("Phase 1 closed by leader!");
    phase1_src.close();

    while(phase2_target.consume(tuple, tuplesConsumed) != DFI_FLOW_FINISHED)
    {
        Phase2Request *req = (Phase2Request*) tuple.getDataPtr();
        if (tuplesConsumed != 1)
        {
            lgae("Expected only one tuple, got: " + to_string(tuplesConsumed));
        }

        lga("got a phase 2 request");
        Phase2Response p2response;
        phase2(req, p2response);
        dfi::Tuple respTuple(&p2response);
        phase2_src.push(respTuple);
    }

    phase2_src.close();
    lgae("Phase 2 closed by leader!");

    lgae("Starting log verification phase!");
    DFI_Shuffle_flow_source verify_source(Settings::FLOWNAME_VERIFICATION);
    for (size_t i = 0; i < Settings::COMMAND_COUNT; i++)
    {
        VerificationMsg request({(uint64_t) i, log.get_slot(i)});
        dfi::Tuple reqTuple(&request);
        verify_source.push(reqTuple);  
    }
    verify_source.close();
    lgae("ACCEPTOR IS DONE");

}

inline void PaxosAcceptor::phase1(Phase1Request *req, Phase1Response& response)
{
    if (req->proposal_number > highest_seen_proposal) 
    {
        highest_seen_proposal = req->proposal_number;
        response = {req->reqid, (int8_t) Return::SUCCESS, accepted_proposal, EMPTY_CMD};
    }
    else
    {
        response = {req->reqid, (int8_t) Return::FAILED, highest_seen_proposal, EMPTY_CMD};
    }
}

inline void PaxosAcceptor::phase2(Phase2Request *req, Phase2Response& response)
{  
    // assumes that requests have continuous range of req_ids and all proposed in same proposal number
    uint64_t reqid = req->reqid;
    if (req->proposal_number < highest_seen_proposal)
    {
        response = {reqid, (int8_t) Return::FAILED, highest_seen_proposal};
        return;
    }

    highest_seen_proposal = accepted_proposal = req->proposal_number;
    log.set_committed(req->committed);
    log.set_slot(req->slotid, req->command);

    response = {reqid, (int8_t) Return::SUCCESS, UNUSED};
}

inline Command PaxosAcceptor::get_command(uint64_t slotid)
{
    return log.get_slot(slotid);
}