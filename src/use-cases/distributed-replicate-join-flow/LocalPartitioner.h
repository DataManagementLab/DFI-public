#pragma once

#include "Partition.h"
#include "Settings.h"
#include "LocalPartitionerThread.h"

class LocalPartitioner
{
public:
    LocalPartitioner(Relation &relation);
    ~LocalPartitioner();
    void start_threads();
    void join_threads();
    PartitionHolder **get_end_partitions() {
        return end_partitions.data();
    };

private:
    Relation &relation;

    std::vector<LocalPartitionerThread*> partitioner_threads;
    std::vector<PartitionHolder*> end_partitions;
};


LocalPartitioner::LocalPartitioner(Relation &relation): relation(relation)
{
    size_t end_partition_size = 1.5 * (Settings::RIGHT_REL_FULL_SIZE / Settings::LOCAL_PARTITION_COUNT / Settings::TOTAL_THREADS_COUNT);
    std::cout << "end_partition_size: " << end_partition_size << std::endl;
    partitioner_threads.reserve(Settings::TOTAL_THREADS_COUNT);
    end_partitions.reserve(Settings::TOTAL_THREADS_COUNT);

    for(size_t i = 0; i < Settings::TOTAL_THREADS_COUNT; i++) {
        PartitionHolder *partition_holder = new PartitionHolder{end_partition_size};
        end_partitions.push_back(partition_holder);

        size_t from = i * (relation.get_size() / Settings::TOTAL_THREADS_COUNT);
        size_t to = (i + 1) * (relation.get_size() / Settings::TOTAL_THREADS_COUNT) - 1;
		if (i == Settings::TOTAL_THREADS_COUNT-1) {
			to = relation.get_size() - 1;
        }
        std::cout << "Creating LocalPartitionerThread, range: " << from << " -> " << to << '\n';

        partitioner_threads.push_back(new LocalPartitionerThread(relation, from, to, partition_holder));
    }
}

LocalPartitioner::~LocalPartitioner() {
    for(size_t i = 0; i < Settings::TOTAL_THREADS_COUNT; i++) {
        delete partitioner_threads[i];
        delete end_partitions[i];
    }
}

void LocalPartitioner::start_threads() {
    for (auto &thread : partitioner_threads) {
        thread->start();
    }
}

void LocalPartitioner::join_threads() {
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
    std::cout << "Average execution time: " << avg_exec_time/partitioner_threads.size()/1000 << " ms. Longest: " << longest_exec_time/1000 << " ms" << '\n';
}