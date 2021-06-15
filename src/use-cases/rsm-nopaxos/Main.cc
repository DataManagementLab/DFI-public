#include <unistd.h>
#include <stdio.h>
#include <getopt.h>
#include "../../utils/Config.h"
#include "../../flow-api/dfi.h"
#include "utils/ThreadScheduler.h"
#include "../../dfi/registry/RegistryServer.h"

#include "Settings.h"
#include "NoPaxosClient.h"
#include "NoPaxosAcceptor.h"

void usage(char* argv[]) {
    std::cout << "Usage: " << argv[0] << " benchfile_output total_cmd_count cmd_per_sec nodeid replica_count client_count [replicas] [clients]" << endl;
    std::cout << "Nodeid is starting from 1. Replica with ID 1 is leader and should be started last" << endl;
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
	if (argc < 8) {
		usage(argv);
		return -1;
	}


    dfi::Config dfiConf(argv[0]);
    rdma::Config rdmaConf(argv[0]);

    Settings::BENCH_FILE = argv[1];
    std::cout << "BENCH FILE "<< Settings::BENCH_FILE;
    Settings::COMMAND_COUNT = atoi(argv[2]);
    Settings::COMMANDS_PER_SECOND = atoi(argv[3]);
    Settings::NODE_ID = atoi(argv[4]);
    Settings::REPLICA_COUNT = atoi(argv[5]);
    Settings::CLIENT_COUNT = atoi(argv[6]);
    size_t argv_ip_list_start = 7; 

    Settings::LEADER_ID = 1; // the leader is on the same node as this replica
    Settings::REGISTRY_ID = 1;
    Settings::SEQUENCER_ID = 1;
    string registry_ip = string(argv[Settings::REGISTRY_ID + argv_ip_list_start - 1]);
    string sequencer_ip = string(argv[Settings::SEQUENCER_ID + argv_ip_list_start - 1]);
    string replica_ip = string(argv[Settings::NODE_ID + argv_ip_list_start - 1]);
    bool isLeader = (Settings::NODE_ID == Settings::LEADER_ID);
    bool isRegistry = (Settings::NODE_ID == Settings::REGISTRY_ID);
    bool isClient = (Settings::NODE_ID > Settings::REPLICA_COUNT);

    rdma::Config::SEQUENCER_PORT = 5550;
    dfi::Config::DFI_REGISTRY_PORT = 5350;
    dfi::Config::DFI_NODE_PORT = 5450;
    rdma::Config::RDMA_INTERFACE = string("ib") + replica_ip.back();
    rdma::Config::RDMA_NUMAREGION = replica_ip.back() - '0';
    dfi::Config::DFI_REGISTRY_SERVER = registry_ip;
    rdma::Config::SEQUENCER_IP = sequencer_ip;
    dfi::Config::DFI_FULL_SEGMENT_SIZE = sizeof(GapFollowerMsg) + sizeof(dfi::DFI_SEGMENT_FOOTER_t);
    dfi::Config::DFI_SEGMENTS_PER_RING = 4096;

    vector<string> replicas;
    size_t port_shift = 1;
    for (size_t i = 0; i < Settings::REPLICA_COUNT + Settings::CLIENT_COUNT; i++)
    {
        replicas.push_back(string(argv[argv_ip_list_start+i]) + ":" + to_string(dfi::Config::DFI_NODE_PORT + port_shift++));
        std::cout << "Added node: " + replicas.back() << std::endl;
    }
    for (size_t i = 0; i < Settings::CLIENT_COUNT; i++)
    {
        size_t idx = i + Settings::REPLICA_COUNT;
        replicas.push_back(string(argv[argv_ip_list_start+idx]) + ":" + to_string(dfi::Config::DFI_NODE_PORT + port_shift++));
        std::cout << "Added node: " + replicas.back() << std::endl;
    }
    dfi::Config::DFI_NODES = replicas;

    std::cout << "Configuration for node " << Settings::NODE_ID << " IP: " << rdma::Config::getIP(rdma::Config::RDMA_INTERFACE) << endl
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
    if (isRegistry)
    {
        std::cout << "Registry server IP set to " << replica_ip << std::endl;
        std::cout << "Starting Registry Server on port: " << dfi::Config::DFI_REGISTRY_PORT << std::endl;
        registryServer = new dfi::RegistryServer();
        std::cout << "Registry server started" << std::endl;
    }

    std::cout << "Starting Node Server " << replica_ip << " on port: " << dfi::Config::DFI_NODE_PORT + Settings::NODE_ID << std::endl;
    NodeServer nodeServer(1024ul*1024*1024*2, dfi::Config::DFI_NODE_PORT + Settings::NODE_ID);
    std::cout << "Node Server startet" << std::endl;

    if (isRegistry)
    {
        // setting up flows
        TypeId type_clientid = TypeId::TINYINT;
        TypeId type_replicaid = TypeId::TINYINT;
        TypeId type_viewid = TypeId::BIGUINT;
        TypeId type_logslot = TypeId::BIGUINT;
        TypeId type_requestid = TypeId::BIGUINT;
        TypeId type_return = TypeId::TINYINT;

        // Ordered unreliable multicast flow from clients to replicas
        std::cout << "Initializing Ordered Unreliable Multicast Flow: " << Settings::FLOWNAME_OUM << std::endl;
        std::vector<std::pair<std::string, TypeId>> oum_columns;    
        oum_columns.push_back({"clientid", type_clientid});
        addCommandColumns(oum_columns);
        oum_columns.push_back({"reqid", type_requestid});
        DFI_Schema oum_schema(oum_columns);

        vector<source_t> oum_sources;
        for (size_t i = 1; i <= Settings::CLIENT_COUNT; i++)
        {
            oum_sources.push_back({i, Settings::REPLICA_COUNT + Settings::CLIENT_COUNT + i});
        }
        vector<target_t> oum_targets;
        for (size_t i = 1; i <= Settings::REPLICA_COUNT; i++)
        {
            oum_targets.push_back({i, i});
        }
        DFI_Replicate_flow_init(Settings::FLOWNAME_OUM, oum_sources, oum_targets,
            oum_schema, DFI_Replicate_flow_optimization::LatencyOrderedMulticast);

        // response flow from acceptors to client
        std::cout << "Initializing client response flow: " << Settings::FLOWNAME_RESPONSE << std::endl;
        DFI_Schema response_schema({
            {"clientid", type_clientid},
            {"viewid", type_viewid},
            {"log_slot", type_logslot},
            {"reqid", type_requestid},
            {"leader_reply", type_return},
            {"success_code", type_return},
            {"result", TypeId::BIGUINT}
        });
        vector<target_t> clients;
        for (size_t i = 1; i <= Settings::CLIENT_COUNT; i++)
        {
            clients.push_back({i, Settings::REPLICA_COUNT + i});
        }
        DFI_Shuffle_flow_init(Settings::FLOWNAME_RESPONSE,
            Settings::REPLICA_COUNT, clients, response_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        // flow for Gap agreement protocol
        std::cout << "Initializing gap agreement flow: " << Settings::FLOWNAME_GAP_FOLLOWER << std::endl;

        std::vector<std::pair<std::string, TypeId>> gap_columns;    
        gap_columns.push_back({"sender", type_replicaid});
        gap_columns.push_back({"receiver", type_replicaid});
        gap_columns.push_back({"log_slot", type_logslot});
        addCommandColumns(gap_columns);
        DFI_Schema gap_schema(gap_columns);
        DFI_Shuffle_flow_init(Settings::FLOWNAME_GAP_FOLLOWER, Settings::REPLICA_COUNT, oum_targets,
            gap_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        std::cout << "Initializing gap agreement flow: " << Settings::FLOWNAME_GAP_LEADER << std::endl;
        DFI_Schema gap2_schema({
            {"replicaid", type_replicaid},
            {"log_slot", type_logslot}
        });
        DFI_Shuffle_flow_init(Settings::FLOWNAME_GAP_LEADER, Settings::REPLICA_COUNT, oum_targets,
            gap2_schema, 0, DFI_Shuffle_flow_optimization::Latency);

        // flow for log consistency verification after benchmark is done
        std::cout << "Initializing verification flow: " << Settings::FLOWNAME_VERIFICATION << std::endl;

        std::vector<std::pair<std::string, TypeId>> verification_columns;    
        verification_columns.push_back({"log_slot", type_logslot});
        verification_columns.push_back({"log_slot", type_logslot});
        addCommandColumns(verification_columns);
        DFI_Schema verification_schema(verification_columns);

        vector<target_t> leader;
        leader.push_back({1,1});
        DFI_Shuffle_flow_init(Settings::FLOWNAME_VERIFICATION, Settings::REPLICA_COUNT-1, leader,
            verification_schema, 0, DFI_Shuffle_flow_optimization::Latency);
    }

    if (isClient)
    {
        std::cout << "Starting as client NodeID:" << Settings::NODE_ID << std::endl;
        utils::ThreadScheduler::start(2);

        NoPaxosClient client;
        client.start();
        std::cout << "CLIENT Started" << std::endl;
        
        client.join();

        utils::ThreadScheduler::stop();
    }
    else
    {
        std::cout << "Starting as acceptor NodeID:" << Settings::NODE_ID <<  std::endl;
        utils::ThreadScheduler::start(2);

        NoPaxosAcceptor acceptor(isLeader);
        acceptor.start();
        std::cout << "ACCEPTOR Started" << std::endl;
        acceptor.join();

        utils::ThreadScheduler::stop();
    }

    std::cout << "NOPaxos example done" << std::endl;
    std::cout << "Shutting down" << std::endl;
    if (registryServer != nullptr)
        delete registryServer;

    return 0;
}
