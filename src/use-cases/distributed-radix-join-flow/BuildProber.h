#pragma once

#include "Relation.h"
#include "Settings.h"
#include "Partition.h"
#include "BuildProberThread.h"

class BuildProber
{
public:
    BuildProber(PartitionHolder **left_partitions, PartitionHolder **right_partitions);
    ~BuildProber();
    size_t get_joined_tuples_cnt() { return joined_tuples_cnt; };
    void run();
    void join();
private:
    std::vector<BuildProberThread*> builder_threads;
    size_t processed_tuples_cnt = 0;
    PartitionHolder **left_partitions; 
    PartitionHolder **right_partitions; 
    size_t partition_count;
    size_t joined_tuples_cnt = 0;
    size_t consumed_tuples_cnt = 0;
};


BuildProber::BuildProber(PartitionHolder **left_partitions, PartitionHolder **right_partitions) : left_partitions(left_partitions), right_partitions(right_partitions)
{
    auto network_parts = Settings::get_partitions(Settings::NODE_ID, Settings::NETWORK_PARTITION_COUNT).size();

    partition_count = Settings::LOCAL_PARTITION_COUNT * Settings::get_partitions(Settings::NODE_ID, Settings::NETWORK_PARTITION_COUNT).size();

    std::cout << "Creating " << partition_count << " BuildProberThreads" << '\n';

    builder_threads.reserve(partition_count);
    for(size_t i = 0; i < network_parts; i++) {
        for(size_t j = 0; j < Settings::LOCAL_PARTITION_COUNT; j++)
        {
            BuildProberThread *thread = new BuildProberThread(*left_partitions[i]->partitions[j], *right_partitions[i]->partitions[j]);
            builder_threads.push_back(thread);
        }
    }


    // if (count != partition_count)
    // {
    //     std::cout << "Threads created: " << count << " did not match expected " << partition_count << '\n';
    //     exit(1);
    // }
    
    
}

BuildProber::~BuildProber()
{
    for(auto &thread : builder_threads) {
        delete thread;
    }
}

void BuildProber::run()
{
    for(auto &thread : builder_threads) {
        thread->start();
    }
}

void BuildProber::join()
{
    for(auto &thread : builder_threads) {
        thread->join();
        joined_tuples_cnt += thread->get_joined_tuples_cnt();
        consumed_tuples_cnt += thread->get_processed_left_tuples() + thread->get_processed_right_tuples();
    }
    std::cout << "sum_consume_count=" << consumed_tuples_cnt << std::endl;
    std::cout << "sum_join_count=" << joined_tuples_cnt << std::endl;
}