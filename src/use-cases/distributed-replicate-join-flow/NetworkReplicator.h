#pragma once

#include "Relation.h"
#include "Settings.h"
#include "NetworkReplicatorThread.h"

class NetworkReplicator
{
public:
    NetworkReplicator(Relation &relation);
    ~NetworkReplicator();
    void start_threads();
    void join_threads();
private:
    Relation &relation;
    NetworkReplicatorThread **replicator_threads;
};


NetworkReplicator::NetworkReplicator(Relation &relation) : relation(relation)
{
    replicator_threads = new NetworkReplicatorThread*[Settings::PART_THREADS_COUNT];
    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++)
    {
        size_t from = i * (relation.get_size() / Settings::PART_THREADS_COUNT);
        size_t to = (i + 1) * (relation.get_size() / Settings::PART_THREADS_COUNT) - 1;
		if (i == Settings::PART_THREADS_COUNT-1) {
			to = relation.get_size() - 1;
        }
        std::string flow_name = Settings::LEFT_REL_FLOW_NAME+to_string(i);
        // std::cout << "Creating NetworkReplicatorThread, range: " << from << " -> " << to << " flow: " << flow_name << '\n';
        replicator_threads[i] = new NetworkReplicatorThread(relation, flow_name, Settings::NODE_ID+1, from, to); // because cli arg starts with 0
    }
}

NetworkReplicator::~NetworkReplicator()
{
    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
        delete replicator_threads[i];
    }
    delete[] replicator_threads;
}

void NetworkReplicator::start_threads()
{
    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
        replicator_threads[i]->start();
    }
}

void NetworkReplicator::join_threads()
{
    size_t processed_tuples = 0;
    for(size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
        replicator_threads[i]->join();
        processed_tuples += replicator_threads[i]->processed_tuples;
    }
    std::cout << "Replicator processed " << processed_tuples << " tuples." << '\n';
}