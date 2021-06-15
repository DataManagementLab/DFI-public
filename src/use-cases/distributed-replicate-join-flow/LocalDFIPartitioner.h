#pragma once

#include "Partition.h"
#include "Settings.h"
#include "LocalDFIPartitionerThread.h"

class LocalDFIPartitioner
{
public:
    LocalDFIPartitioner(Relation::Type relation_type);
    ~LocalDFIPartitioner();
    void start_threads();
    void join_threads();
    PartitionHolder **get_end_partitions() {
        return end_partitions.data();
    };

private:
    std::vector<LocalDFIPartitionerThread*> partitioner_threads;
    std::vector<PartitionHolder*> end_partitions;
};


LocalDFIPartitioner::LocalDFIPartitioner(Relation::Type relation_type)
{
    size_t end_partition_size = 1.5 * (Settings::LEFT_REL_FULL_SIZE / Settings::LOCAL_PARTITION_COUNT / Settings::PART_THREADS_COUNT);

    partitioner_threads.reserve(Settings::PART_THREADS_COUNT);
    end_partitions.reserve(Settings::PART_THREADS_COUNT);

    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
        PartitionHolder *partition_holder = new PartitionHolder{end_partition_size};
        end_partitions.push_back(partition_holder);

        std::string flow_name = Settings::LEFT_REL_FLOW_NAME+std::to_string(i);
        partitioner_threads.push_back(new LocalDFIPartitionerThread(flow_name, Settings::NODE_ID+1, relation_type, partition_holder));
    }

}

LocalDFIPartitioner::~LocalDFIPartitioner() {
    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
        delete partitioner_threads[i];
        delete end_partitions[i];
    }
}

void LocalDFIPartitioner::start_threads() {
    for (auto &thread : partitioner_threads) {
        thread->start();
    }
}

void LocalDFIPartitioner::join_threads() {
    size_t processed_tuples = 0;

    long long avg_exec_time = 0;
    long long longest_exec_time = 0;

    for (auto &thread : partitioner_threads) {
        thread->join();

        processed_tuples += thread->get_processed_tuples();
        // std::cout << "Local partitioner for network partition " << node_partitions[i] << " processed " << thread->get_processed_tuples() << " tuples." << '\n';
        // std::cout << "(" << thread->execution_part_time << " " << thread->execution_time  << " " << thread->get_processed_tuples()/1000  << ") ";
        avg_exec_time += thread->execution_part_time + thread->execution_time;
        if (thread->execution_part_time > longest_exec_time)
            longest_exec_time = thread->execution_part_time;
    }


    std::cout << "Local DFI Partitioner processed total " << processed_tuples << " tuples." << '\n';
    std::cout << "Average execution time: " << avg_exec_time/partitioner_threads.size()/1000 << " ms. Longest: " << longest_exec_time/1000 << " ms" << '\n';
}