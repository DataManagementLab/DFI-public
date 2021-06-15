#pragma once

#include "Relation.h"
#include "Settings.h"
#include "NetworkPartitionerThread.h"

class NetworkPartitioner
{
public:
    NetworkPartitioner(Relation &relation);
    ~NetworkPartitioner();
    void partition();
    void join();
private:
    Relation &relation;
    NetworkPartitionerThread **partitioner_threads;
};


NetworkPartitioner::NetworkPartitioner(Relation &relation) : relation(relation)
{
    partitioner_threads = new NetworkPartitionerThread*[Settings::THREADS_COUNT];
    for(size_t i = 0; i < Settings::THREADS_COUNT; i++)
    {
        size_t from = i * (relation.get_size() / Settings::THREADS_COUNT);
        size_t to = (i + 1) * (relation.get_size() / Settings::THREADS_COUNT) - 1;
		if (i == Settings::THREADS_COUNT-1)
        {
			to = relation.get_size() - 1;
        }
        std::cout << "Creating NetworkPartitionerThread, range: " << from << " -> " << to << '\n';
        partitioner_threads[i] = new NetworkPartitionerThread(relation, from, to);
    }
}

NetworkPartitioner::~NetworkPartitioner()
{
    for(size_t i = 0; i < Settings::THREADS_COUNT; i++)
    {
        delete partitioner_threads[i];
    }
    delete[] partitioner_threads;
}

void NetworkPartitioner::partition()
{
    for(size_t i = 0; i < Settings::THREADS_COUNT; i++)
    {
        partitioner_threads[i]->start();
    }
}

void NetworkPartitioner::join()
{
    size_t processed_tuples = 0;
    for(size_t i = 0; i < Settings::THREADS_COUNT; i++)
    {
        partitioner_threads[i]->join();
        processed_tuples += partitioner_threads[i]->get_processed_tuples();
    }
    std::cout << "Partitioner processed " << processed_tuples << " tuples." << '\n';
}