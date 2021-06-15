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
    void run();
    void join();
private:
    std::vector<BuildProberThread*> builder_threads;
    size_t processed_tuples_cnt = 0;
    PartitionHolder **left_partitions; 
    PartitionHolder **right_partitions; 
public:
    size_t joined_tuples_cnt = 0;
    size_t non_matched_tuples = 0;
};


BuildProber::BuildProber(PartitionHolder **left_partitions, PartitionHolder **right_partitions) : left_partitions(left_partitions), right_partitions(right_partitions)
{

    std::cout << "Creating " << Settings::LOCAL_PARTITION_COUNT << " BuildProberThreads" << '\n';
    builder_threads.reserve(Settings::LOCAL_PARTITION_COUNT);
    for(size_t i = 0; i < Settings::LOCAL_PARTITION_COUNT; i++)  {
        BuildProberThread *thread = new BuildProberThread(i, left_partitions, right_partitions);
        builder_threads.push_back(thread);
    }    
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
        joined_tuples_cnt += thread->joined_tuples_cnt;
        non_matched_tuples += thread->non_matched_tuples;
    }

    std::cout << "sum_join_count=" << joined_tuples_cnt << std::endl;
}