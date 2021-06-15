#pragma once



#include "Settings.h"
#include <numeric>      // std::iota
#include "../../utils/Config.h"
#include "../../utils/StringHelper.h"
#include "../../utils/Filehelper.h"
#include "../../flow-api/dfi.h"
#include "../../dfi/registry/RegistryServer.h"
#include "../../dfi/memory/NodeServer.h"
#include "Relation.h"
#include "NetworkPartitioner.h"
#include "LocalPartitioner.h"
#include <iomanip>
#include <sys/time.h>
#include "utils/ThreadScheduler.h"

#include "BuildProber.h"

class DistributedRadixJoin
{
public:
    DistributedRadixJoin();
    void initialize_dfi();
    void generate_test_data();
    void run();

private:

    Relation left_relation;
    Relation right_relation;

    //DFI STUFF
    std::unique_ptr<dfi::NodeServer> dfi_node_server = nullptr;
    std::unique_ptr<dfi::RegistryServer> dfi_registry_server = nullptr;

    void write_results_to_file(long long network_part_left, long long network_part_right, long long local_part, long long buildprobe, long long full_join);
    double flow_init_us = 0;
};



DistributedRadixJoin::DistributedRadixJoin() : left_relation(Relation::Type::LEFT), right_relation(Relation::Type::RIGHT)
{
    
}


void DistributedRadixJoin::run()
{
    NetworkPartitioner left_network_partitioner(left_relation);
    NetworkPartitioner right_network_partitioner(right_relation);


    LocalPartitioner left_local_partitioner(Relation::Type::LEFT);
    LocalPartitioner right_local_partitioner(Relation::Type::RIGHT);
    BuildProber build_prober(left_local_partitioner.get_end_partitions(), right_local_partitioner.get_end_partitions());

	struct timeval tv_start;
	struct timeval tv_end;
	// struct timeval tv_end_part;
	// struct timeval tv_end_dispatch;
	struct timeval tv_start_full;
	struct timeval tv_end_full;


    std::cout << "Press any key to run..." << '\n';
    cin.get();

	gettimeofday(&tv_start_full, NULL);


    long long part_left = 0;
    long long part_right = 0;
    long long local_part = 0;
    long long build_probe = 0;

    gettimeofday(&tv_start, NULL);
    std::cout << "Partitioning and shuffling left relation" << '\n';
    left_network_partitioner.partition();

    left_network_partitioner.join();
    gettimeofday(&tv_end, NULL);
    part_left = get_time_diff(tv_start, tv_end);

    gettimeofday(&tv_start, NULL);

//         std::cout << "Partition & shuffle left relation took: " << left_part << " us" << endl;
//         std::cout << "Partition & shuffle right relation took: " << right_part << " us" << endl;
//     utils::ThreadScheduler::stop();
//     sleep(2);

// exit(1);

    std::cout << "Partitioning and shuffling right relation" << '\n';
    right_network_partitioner.partition();
    right_network_partitioner.join();
    gettimeofday(&tv_end, NULL);
    part_right = get_time_diff(tv_start, tv_end);

    gettimeofday(&tv_start, NULL);
    std::cout << "Local partitioning left relation" << '\n';
    left_local_partitioner.partition();

    std::cout << "Local partitioning right relation" << '\n';
    right_local_partitioner.partition();
    left_local_partitioner.join();
    right_local_partitioner.join();
    gettimeofday(&tv_end, NULL);
    local_part = get_time_diff(tv_start, tv_end);

    gettimeofday(&tv_start, NULL);
    std::cout << "Start to build and probe" << '\n';
    build_prober.run();
    build_prober.join();
    gettimeofday(&tv_end, NULL);
    build_probe = get_time_diff(tv_start, tv_end);

    std::cout << "Partition & shuffle left took: " << part_left << " us" << '\n';
    std::cout << "Partition & shuffle right took: " << part_right << " us" << '\n';

    std::cout << "Local partition took: " << local_part << " us" << '\n';
    std::cout << "Build & probe took: " << build_probe << " us" << '\n';
    // }

	gettimeofday(&tv_end_full, NULL);
	long long total_us = get_time_diff(tv_start_full, tv_end_full);
    std::cout << "Total time: " << total_us << " us" << '\n';
    std::cout << "Distributed join finished! Joined tuple count: " << build_prober.get_joined_tuples_cnt() << '\n';
    
    write_results_to_file(part_left, part_right, local_part, build_probe, total_us);

    
    sleep(1);
}

void DistributedRadixJoin::initialize_dfi()
{

    // dfi::Config::DFI_REGISTRY_SERVER = "172.18.94.10"; //DM cluster
    rdma::Config::SEQUENCER_IP = dfi::Config::DFI_REGISTRY_SERVER;
    // dfi::Config::DFI_NODES = {
    //                             "172.18.94.10:" + to_string(Config::DFI_NODE_PORT), //dm-node01
    //                             "172.18.94.20:" + to_string(Config::DFI_NODE_PORT)  //dm-node02     
    //                          }; 
    dfi::Config::DFI_FULL_SEGMENT_SIZE = (1024 * 32) + sizeof(DFI_SEGMENT_FOOTER_t);
    // dfi::Config::DFI_FULL_SEGMENT_SIZE = (1024 * 128 * 1) + sizeof(DFI_SEGMENT_FOOTER_t);
    dfi::Config::DFI_SOURCE_SEGMENT_COUNT = 8;
    uint16_t port = std::stoi(StringHelper::split(dfi::Config::DFI_NODES[Settings::NODE_ID], ":")[1]);

    size_t sources_count = Settings::THREADS_COUNT * Settings::NODES_TOTAL; //One source for each thread on each node
    size_t network_parts_per_node = Settings::get_partitions(Settings::NODE_ID, Settings::NETWORK_PARTITION_COUNT).size();
    size_t left_segments_per_ring = 2 + ((sizeof(tuple_type) * (double)Settings::LEFT_REL_SIZE) / (dfi::Config::DFI_FULL_SEGMENT_SIZE-sizeof(DFI_SEGMENT_FOOTER_t)) / sources_count / network_parts_per_node);
    size_t right_segments_per_ring = 2 + ((sizeof(tuple_type) * (double)Settings::RIGHT_REL_SIZE) / (dfi::Config::DFI_FULL_SEGMENT_SIZE-sizeof(DFI_SEGMENT_FOOTER_t)) / sources_count / network_parts_per_node);

	rdma::Config::RDMA_MEMSIZE = network_parts_per_node * dfi::Config::DFI_FULL_SEGMENT_SIZE * (left_segments_per_ring + right_segments_per_ring) * sources_count * 1.2;
	// dfi::Config::RDMA_MEMSIZE = network_parts_per_node * dfi::Config::DFI_FULL_SEGMENT_SIZE * dfi::Config::DFI_SEGMENTS_PER_RING * sources_count * 1.2;
    std::cout << "dfi_segment_width=" << dfi::Config::DFI_FULL_SEGMENT_SIZE << std::endl;
    std::cout << "NodeServer allocating: "  << std::fixed << std::setprecision(3) << ((double)rdma::Config::RDMA_MEMSIZE)/1000000000 << "GB" << '\n';
    std::cout << "Buffer in NodeServer: " << network_parts_per_node << '\n';
    std::cout << "Buffer writers (rings) to each buffer: " << sources_count << '\n';
    std::cout << "Segments per ring (left): " << left_segments_per_ring << " (right): " << right_segments_per_ring << " left rel size: " << Settings::LEFT_REL_SIZE << " right rel size: " << Settings::RIGHT_REL_SIZE << '\n';
    
    
    if (Settings::NODE_ID == 0) //Master
    {
        dfi_registry_server = std::make_unique<dfi::RegistryServer>();
    }

    dfi_node_server = std::make_unique<dfi::NodeServer>(rdma::Config::RDMA_MEMSIZE, port);

    if (Settings::NODE_ID == 0) //Master
    {
    	struct timeval tv_start;
    	struct timeval tv_end;
#ifdef COMPRESSED_TUPLES
        DFI_Schema schema({{"value", dfi::TypeId::BIGUINT}}); //CompressedTuple_t
#else
        DFI_Schema schema({{"key", dfi::TypeId::BIGUINT}, {"value", dfi::TypeId::BIGUINT}});
#endif
        size_t shuffle_key_index = 0;

        std::vector<target_t> targets;
        for (size_t i = 0; i < Settings::NETWORK_PARTITION_COUNT; i++)
        {
            targets.push_back({i, Settings::get_assigment(i, Settings::NETWORK_PARTITION_COUNT)});
        }

	    gettimeofday(&tv_start, NULL);

        std::cout << "Initializing " << Settings::LEFT_REL_FLOW_NAME << '\n';
        DFI_Shuffle_flow_init(Settings::LEFT_REL_FLOW_NAME, sources_count, targets, schema, shuffle_key_index, DFI_Shuffle_flow_optimization::Bandwidth, left_segments_per_ring);
        std::cout << "Initializing " << Settings::RIGHT_REL_FLOW_NAME << '\n';
        DFI_Shuffle_flow_init(Settings::RIGHT_REL_FLOW_NAME, sources_count, targets, schema, shuffle_key_index, DFI_Shuffle_flow_optimization::Bandwidth, right_segments_per_ring);
	    gettimeofday(&tv_end, NULL);
	    flow_init_us = get_time_diff(tv_start, tv_end);
    }
    
    // for(size_t i = 0; i < Settings::NODES_TOTAL; i++)
    // {
    //     for(auto &part : DFI_Partition_node_assignment_range().get_partitions(i, Settings::NETWORK_PARTITION_COUNT))
    //     {
    //         std::cout << "Nodeid: " << i << "Partition:" << part << " --> nodeid: " << DFI_Partition_node_assignment_range().get_assigment(part, Settings::NETWORK_PARTITION_COUNT)-1 <<  '\n';
    //     }
    // }
    

    

    // std::cout << "Press any key to continue..." << '\n';
    // cin.get();
}

void DistributedRadixJoin::generate_test_data()
{
    std::string left_rel = "radix_LEFT_REL_" + to_string(Settings::LEFT_REL_FULL_SIZE) + "_" + to_string(Settings::NODES_TOTAL) + "_" + to_string(Settings::RANDOM_ORDER) + "_" + to_string(Settings::NODE_ID);
    std::string right_rel = "radix_RIGHT_REL_"+ to_string(Settings::RIGHT_REL_FULL_SIZE) + "_" + to_string(Settings::NODES_TOTAL) + "_" + to_string(Settings::RANDOM_ORDER) + "_" + to_string(Settings::NODE_ID);
    
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


void DistributedRadixJoin::write_results_to_file(long long network_part_left, long long network_part_right, long long local_part, long long buildprobe, long long full_join)
{

    std::cout << "non_temp_writes=1" << std::endl;
    double BW_left = (((double)(Settings::LEFT_REL_SIZE)*sizeof(tuple_type))/((double)(network_part_left)/1000000))/1000000;
    double BW_right = (((double)(Settings::RIGHT_REL_SIZE)*sizeof(tuple_type))/((double)(network_part_right)/1000000))/1000000;
	string random = (Settings::RANDOM_ORDER ? "true" : "false");
    std::cout << "node=" << Settings::NODE_ID << std::endl;
    std::cout << "nodes=" <<Settings::NODES_TOTAL << std::endl;
    std::cout << "full_left_rel=" << Settings::LEFT_REL_FULL_SIZE << std::endl;
    std::cout << "full_right_rel=" << Settings::RIGHT_REL_FULL_SIZE << std::endl;
    std::cout << "network_parts=" << Settings::NETWORK_PARTITION_COUNT << std::endl;
    std::cout << "local_parts=" << Settings::LOCAL_PARTITION_COUNT << std::endl;
    std::cout << "part_threads=" <<Settings::THREADS_COUNT << std::endl;
    std::cout << "bandwidth_left=" << BW_left << std::endl;
    std::cout << "bandwidth_right=" << BW_right << std::endl;
    std::cout << "time_network_part_left=" << network_part_left/1000 << std::endl;
    std::cout << "time_network_part_right=" << network_part_right/1000 << std::endl;
    std::cout << "time_local_part=" << local_part/1000 << std::endl;
    std::cout << "time_build_probe=" << buildprobe/1000 << std::endl;
    std::cout << "total_time=" << full_join/1000 << std::endl;
    std::cout << "random_order=" << random << std::endl;
}