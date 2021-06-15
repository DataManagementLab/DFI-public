#pragma once



#include "Settings.h"
#include <numeric>      // std::iota
#include "../../utils/Config.h"
#include "../../utils/Filehelper.h"
#include "../../flow-api/dfi.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/NodeServer.h"
#include "Relation.h"
// #include "NetworkPartitioner.h"
#include "NetworkReplicator.h"
#include "LocalPartitioner.h"
#include "LocalDFIPartitioner.h"
// #include "LocalReplicator.h"
#include <iomanip>
#include <sys/time.h>
#include "utils/ThreadScheduler.h"

#include "utils/timer.hpp"

#include "BuildProber.h"

class DistributedReplicateJoin
{
public:
    DistributedReplicateJoin();
    void initialize_dfi();
    void generate_test_data();
    void run();

private:

    Relation left_relation;
    Relation right_relation;

    //DFI STUFF
    std::unique_ptr<dfi::NodeServer> dfi_node_server = nullptr;
    std::unique_ptr<dfi::RegistryServer> dfi_registry_server = nullptr;

    void cout_results(long long network_part, long long local_part, long long buildprobe, long long full_join);
    double flow_init_us = 0;
};



DistributedReplicateJoin::DistributedReplicateJoin() : left_relation(Relation::Type::LEFT), right_relation(Relation::Type::RIGHT)
{
    
}


void DistributedReplicateJoin::initialize_dfi() {

    // Modify dfi::Config::DFI_NODES since there exist a internal node server for each multicast target
    #ifdef USE_MULTICAST
    auto temp_nodes = dfi::Config::DFI_NODES;
    dfi::Config::DFI_NODES.clear();
    for (size_t i = 0; i < temp_nodes.size(); i++)
    {
        for (size_t j = 0; j < Settings::PART_THREADS_COUNT; j++) 
        {
            dfi::Config::DFI_NODES.push_back(Network::getAddressOfConnection(temp_nodes[i]) + ":" + to_string(Config::DFI_NODE_PORT + j));
        }
    }
    #endif
    rdma::Config::SEQUENCER_IP = dfi::Config::DFI_REGISTRY_SERVER;
    
    dfi::Config::DFI_FULL_SEGMENT_SIZE = (1024 * 128 * 1) + sizeof(DFI_SEGMENT_FOOTER_t);
    dfi::Config::DFI_SOURCE_SEGMENT_COUNT = 8;
    
    if (Settings::NODE_ID == 0) { //Master
        dfi_registry_server = std::make_unique<dfi::RegistryServer>();
    }

    #ifndef USE_MULTICAST
    const size_t left_relation_size = sizeof(Tuple_t) * Settings::LEFT_REL_SIZE;
    const size_t left_segments_per_ring = 1 + 2*(left_relation_size / dfi::Config::DFI_FULL_SEGMENT_SIZE / Settings::PART_THREADS_COUNT);

	rdma::Config::RDMA_MEMSIZE = dfi::Config::DFI_FULL_SEGMENT_SIZE * (left_segments_per_ring) * Settings::PART_THREADS_COUNT * Settings::NODES_TOTAL * 1.2;

    std::cout << "NodeServer allocating: "  << std::fixed << std::setprecision(3) << ((double)rdma::Config::RDMA_MEMSIZE)/1000000000 << "GB" << '\n';
    std::cout << "left rel size: " << Settings::LEFT_REL_SIZE << " right rel size: " << Settings::RIGHT_REL_SIZE << '\n';
    std::cout << "dfi_segment_width=" << dfi::Config::DFI_FULL_SEGMENT_SIZE << std::endl;
    
    dfi_node_server = std::make_unique<dfi::NodeServer>();  // not needed for multicast
    #else
    const size_t left_segments_per_ring = 2048;
    #endif

    if (Settings::NODE_ID == 0) { //Master
        #ifdef USE_COMPRESSION
        DFI_Schema schema({{"data", dfi::TypeId::BIGINT}});
        #else
        DFI_Schema schema({{"key", dfi::TypeId::BIGINT}, {"value", dfi::TypeId::BIGINT}});
        #endif

        #ifdef USE_MULTICAST
        DFI_Replicate_flow_optimization optimization = DFI_Replicate_flow_optimization::BandwidthMulticast;
        #else
        DFI_Replicate_flow_optimization optimization = DFI_Replicate_flow_optimization::Bandwidth;
        #endif

        for (size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {

            std::vector<dfi::source_t> sources;
            std::vector<dfi::target_t> targets;
            for (size_t j = 0; j < Settings::NODES_TOTAL; j++) {
                #ifdef USE_MULTICAST
                sources.push_back({j+1, (Settings::PART_THREADS_COUNT*j) + i+1}); // node-ids are specific for each source in multicast optimization - must correspond to DFI_NODES
                targets.push_back({j+1, 0});
                #else
                sources.push_back({j+1, 0}); // node-id 0, because source doesn't attach to any NodeServer in normal bw optimization
                targets.push_back({j+1, j+1});
                #endif
            }

            std::string flow_name = Settings::LEFT_REL_FLOW_NAME + std::to_string(i);
            std::cout << "Initializing DFI_Replicate_flow for " << flow_name << '\n';
            
            flow_init_us += time_it([&]() {
                DFI_Replicate_flow_init(flow_name, sources, targets, schema, optimization, left_segments_per_ring); 
            });
        }
    }
}


void DistributedReplicateJoin::run()
{

    NetworkReplicator left_network_replicator(left_relation);
    LocalDFIPartitioner left_local_partitioner(Relation::Type::LEFT);      // calls dfi_consume
    LocalPartitioner right_local_partitioner(right_relation);

    BuildProber build_prober(left_local_partitioner.get_end_partitions(), right_local_partitioner.get_end_partitions());

    std::cout << "Press any key to run...\n";
    cin.get();

    std::cout << "Replicating left relation\n";
    uint64_t time_left_repli = time_it([&]() {
        left_network_replicator.start_threads();    // calls dfi_push
        left_local_partitioner.start_threads();
        left_network_replicator.join_threads();
        left_local_partitioner.join_threads();
    });


    std::cout << "Local partitioning left and right relation" << '\n';
    uint64_t local_partitioning = time_it([&]() {
        right_local_partitioner.start_threads();

        right_local_partitioner.join_threads();
    });


    std::cout << "Start to build and probe" << '\n';
    uint64_t build_probe = time_it([&]() {
        build_prober.run();
        build_prober.join();
    });


    std::cout << "Distributed join finished! Joined tuple count: " << build_prober.joined_tuples_cnt << '\n';

    std::cout << "Replicating left relation took " << time_left_repli << "µs\n";
    std::cout << "Local partitioning took " << local_partitioning << "µs\n";
    std::cout << "Build & probe took: " << build_probe << "µs\n";



    cout_results(time_left_repli, local_partitioning, build_probe, time_left_repli+local_partitioning+build_probe);
}



void DistributedReplicateJoin::generate_test_data()
{
    std::string left_rel = "radix_replicate_LEFT_REL_" + to_string(Settings::LEFT_REL_FULL_SIZE) + "_" + to_string(Settings::NODES_TOTAL) + "_" + to_string(Settings::RANDOM_ORDER) + "_" + to_string(Settings::NODE_ID);
    std::string right_rel = "radix_replicate_RIGHT_REL_"+ to_string(Settings::RIGHT_REL_FULL_SIZE) + "_" + to_string(Settings::NODES_TOTAL) + "_" + to_string(Settings::RANDOM_ORDER) + "_" + to_string(Settings::NODE_ID);
    
    if (Filehelper::fileExists(left_rel))
    {
        std::cout << "Loading left relations from partial relation file" << '\n';
        left_relation.load_from_file(left_rel.c_str());
    }
    else
    {
        std::cout << "Generating and dumping left relation" << '\n';
        left_relation.generate_range_partioned_data();
        left_relation.dump_to_file(left_rel.c_str());
    }

    if (Filehelper::fileExists(right_rel))
    {
        std::cout << "Loading right relation from partial relation file" << '\n';
        right_relation.load_from_file(right_rel.c_str());
    }
    else 
    {
        std::cout << "Generating and dumping right relation" << '\n';
        right_relation.generate_range_partioned_data();
        right_relation.dump_to_file(right_rel.c_str());
        
    }
}


void DistributedReplicateJoin::cout_results(long long network_part, long long local_part, long long buildprobe, long long full_join)
{
    std::cout << "non_temp_writes=1" << std::endl;
    double network = (((double)(Settings::LEFT_REL_SIZE)*sizeof(CompressedTuple_t))/((double)(network_part)/1000000))/1000000;
	string random = (Settings::RANDOM_ORDER ? "true" : "false");
    std::cout << "node=" << Settings::NODE_ID << std::endl;
    std::cout << "nodes=" <<Settings::NODES_TOTAL << std::endl;
    std::cout << "full_left_rel=" << Settings::LEFT_REL_FULL_SIZE << std::endl;
    std::cout << "full_right_rel=" << Settings::RIGHT_REL_FULL_SIZE << std::endl;
    std::cout << "local_parts=" << Settings::LOCAL_PARTITION_COUNT << std::endl;
    std::cout << "part_threads=" <<Settings::TOTAL_THREADS_COUNT << std::endl;
    std::cout << "bandwidth=" << network << std::endl;
    std::cout << "time_network=" << network_part/1000 << std::endl;
    std::cout << "time_local_part=" << local_part/1000 << std::endl;
    std::cout << "time_build_probe=" << buildprobe/1000 << std::endl;
    std::cout << "total_time=" << full_join/1000 << std::endl;
    std::cout << "random_order=" << random << std::endl;

}
