#include <unistd.h>
#include <stdio.h>
#include <getopt.h>
#include "../../utils/Config.h"
#include "../../flow-api/dfi.h"
#include "Settings.h"
#include "utils/ThreadScheduler.h"
#include "../../dfi/registry/RegistryServer.h"
#include "PaxosClient.h"
#include "PaxosLeader.h"
#include "PaxosAcceptor.h"


void usage(char* argv[]) {
    std::cout << "Usage: " << argv[0] << " benchfile_output leader_mode(sync=0/async=1) total_cmd_count cmd_per_sec nodeid replica_count client_count [replicas] [clients]" << endl;
    cout << "Nodeid is starting from 1. Replica with ID 1 is leader and should be started last" << endl;
}

void addCommandColumns(std::vector<std::pair<std::string, TypeId>> &columns)
{
    for (size_t i = 0; i < CMD_SIZE / 8; i++)
    {
        columns.push_back({"cmd"+to_string(i), TypeId::BIGUINT});
    }
}

int main(int argc, char *argv[])
{
    // -------------- Configuration based on CL parameters
	if (argc < 9) { 
		usage(argv);
		return -1;
	}

    Settings::LEADER_ID = 1; // the leader is on the same node as this replica

    dfi::Config dfiConf(argv[0]);
    rdma::Config rdmaConf(argv[0]);

    Settings::BENCH_FILE = argv[1];
    Settings::MODE = static_cast<Mode>(atoi(argv[2]));
    Settings::COMMAND_COUNT = atoi(argv[3]);
    Settings::COMMANDS_PER_SECOND = atoi(argv[4]);
    Settings::NODE_ID = atoi(argv[5]);
    Settings::REPLICA_COUNT = atoi(argv[6]);
    Settings::CLIENT_COUNT = atoi(argv[7]);
    size_t argv_ip_list_start = 8; 

    string registry_ip = string(argv[Settings::LEADER_ID + argv_ip_list_start - 1]); // registry server and sequencer on leader
    string replica_ip = string(argv[Settings::NODE_ID + argv_ip_list_start - 1]);
    bool isLeader = (Settings::NODE_ID == Settings::LEADER_ID);
    bool isClient = (Settings::NODE_ID > Settings::REPLICA_COUNT);

    rdma::Config::SEQUENCER_PORT = 5550;
    dfi::Config::DFI_REGISTRY_PORT = 5350;
    dfi::Config::DFI_NODE_PORT = 5450;
    rdma::Config::RDMA_INTERFACE = string("ib") + replica_ip.back();
    rdma::Config::RDMA_NUMAREGION = replica_ip.back() - '0';
    dfi::Config::DFI_REGISTRY_SERVER = registry_ip;
    rdma::Config::SEQUENCER_IP = registry_ip;
    dfi::Config::DFI_FULL_SEGMENT_SIZE = sizeof(Phase2Request) + sizeof(dfi::DFI_SEGMENT_FOOTER_t);
    dfi::Config::DFI_SEGMENTS_PER_RING = 1000;

    vector<string> replicas;
    for (size_t i = 0; i < Settings::REPLICA_COUNT + Settings::CLIENT_COUNT; i++)
    {
        replicas.push_back(string(argv[argv_ip_list_start+i]) + ":" + to_string(dfi::Config::DFI_NODE_PORT + i + 1));
        cout << "Add DFI node " << string(argv[argv_ip_list_start+i]) + ":" + to_string(dfi::Config::DFI_NODE_PORT + i + 1) << endl;
    }
    dfi::Config::DFI_NODES = replicas;

    cout << "Configuration for node " << Settings::NODE_ID << " IP: " << rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) << endl
        << "DFI_REGISTRY_SERVER: " << dfi::Config::DFI_REGISTRY_SERVER << endl
        << "DFI_REGISTRY_PORT: " << dfi::Config::DFI_REGISTRY_PORT << endl
        << "DFI_SEQUENCER_IP: " << rdma::Config::SEQUENCER_IP << endl
        << "DFI_SEQUENCER_PORT: " << rdma::Config::SEQUENCER_PORT << endl
        << "RDMA_NUMAREGION: " << rdma::Config::RDMA_NUMAREGION << endl
        << "DFI_NODE_PORT: " << dfi::Config::DFI_NODE_PORT << endl
        << "DFI_SEGMENTS_PER_RING: " << dfi::Config::DFI_SEGMENTS_PER_RING << endl
        << "DFI_FULL_SEGMENT_SIZE: " << dfi::Config::DFI_FULL_SEGMENT_SIZE << endl
        << "DFI_SOURCE_SEGMENT_COUNT: " << dfi::Config::DFI_SOURCE_SEGMENT_COUNT << endl
        << "RDMA_MEMSIZE: " << rdma::Config::RDMA_MEMSIZE << endl;

    // -------------- Setup and start everything

    dfi::RegistryServer *registryServer = nullptr;
    if (isLeader)
    {
        cout << "Registry server IP set to " << replica_ip << std::endl;
        std::cout << "Starting Registry Server on port: " << dfi::Config::DFI_REGISTRY_PORT << std::endl;
        registryServer = new dfi::RegistryServer();
        std::cout << "Registry server started" << std::endl;
    }

    std::cout << "Starting Node Server " << replica_ip << " on port: " << dfi::Config::DFI_NODE_PORT + Settings::NODE_ID << std::endl;
    NodeServer nodeServer(1024ul*1024*1024*2, dfi::Config::DFI_NODE_PORT + Settings::NODE_ID);
    std::cout << "Node Server startet" << std::endl;

    if (isLeader)
    {
        size_t replica_count = Settings::REPLICA_COUNT == 1? 1 : Settings::REPLICA_COUNT - 1; // -1 because leader has local acceptor not using flow
        vector<source_t> leader_sources = {{1, 1}};
        vector<target_t> targets_of_leader;
        for (size_t i = 0; i < replica_count; i++)
        {
            targets_of_leader.push_back({i+2, i+2});
        }
        vector<target_t> targets_of_replicas = {{1, 1}};

        vector<target_t> client_targets;
        for (size_t i = 0; i < Settings::CLIENT_COUNT; i++)
        {
            client_targets.push_back({i+1, Settings::REPLICA_COUNT + i + 1});
        }

        TypeId type_clientid = TypeId::TINYINT;
        TypeId type_reqid = TypeId::BIGUINT;
        TypeId type_response_type = TypeId::TINYINT;    // accept or deny
        TypeId type_return_val = TypeId::BIGUINT; 
        TypeId type_proposal_num = TypeId::BIGUINT;
        TypeId type_slotid = TypeId::BIGUINT;

        // requests send between leader and clients
        std::cout << "Initializing " << Settings::FLOWNAME_CLIENT_REQ << std::endl;
        std::vector<std::pair<std::string, TypeId>> req_columns;   
        req_columns.push_back({"clientid",type_clientid});
        addCommandColumns(req_columns);
        DFI_Schema req_schema(req_columns);

        vector<target_t> t;
        t.push_back({1, 1});
        DFI_Shuffle_flow_init(Settings::FLOWNAME_CLIENT_REQ, Settings::CLIENT_COUNT, t, req_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        std::cout << "Initializing " << Settings::FLOWNAME_CLIENT_RESP << std::endl;
        DFI_Schema resp_schema({
            {"clientid",type_clientid},
            {"ret_code",type_response_type},
            {"ret_val", type_return_val}
         });
        DFI_Shuffle_flow_init(Settings::FLOWNAME_CLIENT_RESP, 1, client_targets, resp_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        // requests send by leader to replicas (acceptors)
        std::cout << "Initializing " << Settings::FLOWNAME_P1REQ << std::endl;
        std::vector<std::pair<std::string, TypeId>> p1req_columns;   
        p1req_columns.push_back({"reqid", type_reqid});
        p1req_columns.push_back({"proposal_num", type_proposal_num});
        p1req_columns.push_back({"committed", type_slotid});
        p1req_columns.push_back({"slotid", type_slotid});
        addCommandColumns(p1req_columns);
        DFI_Schema p1req_schema(p1req_columns);

        DFI_Replicate_flow_init(Settings::FLOWNAME_P1REQ, leader_sources, targets_of_leader,
            p1req_schema, DFI_Replicate_flow_optimization::Latency);

        std::cout << "Initializing " << Settings::FLOWNAME_P2REQ << std::endl;
        std::vector<std::pair<std::string, TypeId>> p2req_columns;   
        p2req_columns.push_back({"reqid", type_reqid});
        p2req_columns.push_back({"proposal_num", type_proposal_num});
        p2req_columns.push_back({"committed", type_slotid});
        p2req_columns.push_back({"slotid", type_slotid});
        addCommandColumns(p2req_columns);
        DFI_Schema p2req_schema(p2req_columns);

        DFI_Replicate_flow_init(Settings::FLOWNAME_P2REQ, leader_sources, targets_of_leader,
            p2req_schema, DFI_Replicate_flow_optimization::Latency);
        
        // responsens from replicas (acceptor) to leader
        std::cout << "Initializing " << Settings::FLOWNAME_P1RESP << std::endl;
        std::vector<std::pair<std::string, TypeId>> p1resp_columns;   
        p1resp_columns.push_back({"reqid", type_reqid});
        p1resp_columns.push_back({"response_type", type_response_type});
        p1resp_columns.push_back({"highest_proposal_num", type_proposal_num});
        addCommandColumns(p1resp_columns);
        DFI_Schema p1resp_schema(p1resp_columns);

        DFI_Shuffle_flow_init(Settings::FLOWNAME_P1RESP, replica_count, targets_of_replicas,
            p1resp_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        std::cout << "Initializing " << Settings::FLOWNAME_P2RESP << std::endl;
        DFI_Schema schema_p2resp({
            {"reqid", type_reqid},
            {"response_type", type_response_type},
            {"highest_seen_proposal_num", type_proposal_num}
        });
        DFI_Shuffle_flow_init(Settings::FLOWNAME_P2RESP, replica_count, targets_of_replicas,
            schema_p2resp, 0, DFI_Shuffle_flow_optimization::Latency);

        // flow for log consistency verification after benchmark is done
        std::cout << "Initializing verification flow: " << Settings::FLOWNAME_VERIFICATION << std::endl;
        std::vector<std::pair<std::string, TypeId>> verification_columns;   
        verification_columns.push_back({"log_slot", type_slotid});
        addCommandColumns(verification_columns);
        DFI_Schema verification_schema(verification_columns);

        vector<target_t> leader;
        leader.push_back({1,1});
        DFI_Shuffle_flow_init(Settings::FLOWNAME_VERIFICATION, Settings::REPLICA_COUNT-1, leader,
            verification_schema, 0, DFI_Shuffle_flow_optimization::Latency);
    }


    if (isLeader)
    {
        std::cout << "Starting as leader NODEID:" << Settings::NODE_ID << std::endl;
        utils::ThreadScheduler::start(2);

        PaxosLeader leader;
        leader.start();
        leader.join(); // wait until leader done

        utils::ThreadScheduler::stop();
    }
    else if (isClient)
    {
        std::cout << "Starting as client NODEID:" << Settings::NODE_ID << std::endl;
        PaxosClient client;
        client.start();
    }
    else
    {
        std::cout << "Starting as acceptor " << std::endl;
        utils::ThreadScheduler::start(1);
        RemotePaxosAcceptor acceptor;
        acceptor.start();
        acceptor.join();
        utils::ThreadScheduler::stop();
    }
    
    std::cout << "Consensus example done" << std::endl;
    std::cout << "Shutting down" << std::endl;
    if (registryServer != nullptr)
        delete registryServer;

    return 0;
}
