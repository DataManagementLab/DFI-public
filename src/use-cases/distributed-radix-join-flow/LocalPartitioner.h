#pragma once

#include "Relation.h"
#include "Settings.h"
#include "Partition.h"
#include "LocalPartitionerThread.h"

class LocalPartitioner
{
public:
    LocalPartitioner(Relation::Type relation_type);
    ~LocalPartitioner();
    void partition();
    void join();
    PartitionHolder **get_end_partitions() {
        return end_partitions.data();
    };

private:
    std::vector<LocalPartitionerThread*> partitioner_threads;
    std::vector<PartitionHolder*> end_partitions;
    vector<size_t> node_partitions;
};


LocalPartitioner::LocalPartitioner(Relation::Type relation_type)
{
    size_t end_partition_tuples = 1.5 * ((relation_type == Relation::Type::LEFT) ? Settings::LEFT_REL_FULL_SIZE : Settings::RIGHT_REL_FULL_SIZE) / TOTAL_PARTITION_COUNT;

    node_partitions = Settings::get_partitions(Settings::NODE_ID, Settings::NETWORK_PARTITION_COUNT);
    std::cout << "Creating " << node_partitions.size() << " LocalPartitionerThreads. " << end_partition_tuples << std::endl;
    partitioner_threads.reserve(node_partitions.size());
    end_partitions.reserve(node_partitions.size());
    for(size_t i = 0; i < node_partitions.size(); i++)
    {
        PartitionHolder *partition_holder = new PartitionHolder{end_partition_tuples};
        end_partitions.push_back(partition_holder);

        partitioner_threads.push_back(new LocalPartitionerThread(node_partitions[i], relation_type, partition_holder));
    }
}

LocalPartitioner::~LocalPartitioner()
{
    for(size_t i = 0; i < node_partitions.size(); i++)
    {
        delete partitioner_threads[i];
        delete end_partitions[i];
    }
}

void LocalPartitioner::partition()
{
    for (auto &thread : partitioner_threads) {
        thread->start();
    }
}

void LocalPartitioner::join()
{
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
    std::cout << "Local Partitioner processed total " << processed_tuples << " tuples." << '\n';
    std::cout << "Average execution time: " << avg_exec_time/node_partitions.size()/1000 << " ms. Longest: " << longest_exec_time/1000 << " ms" << '\n';
}