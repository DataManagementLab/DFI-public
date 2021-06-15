#pragma once
#include <thread>
#include "Settings.h"
#include "Relation.h"
#include "../../flow-api/dfi.h"
#include "utils/Thread.h"



class NetworkReplicatorThread : public utils::Thread
{
public:
    NetworkReplicatorThread(Relation &relation, std::string flow_name, SourceID source_id, size_t from, size_t to);
    void run() override;

private:
    Relation &relation;
    DFI_Replicate_flow_source flow_source;
    size_t from;
    size_t to;
public:
    size_t processed_tuples = 0;
};

NetworkReplicatorThread::NetworkReplicatorThread(Relation &relation, std::string flow_name, SourceID source_id, size_t from, size_t to) : relation(relation),
    flow_source(flow_name, source_id), from(from), to(to)
{
}


void NetworkReplicatorThread::run()
{
    for (size_t i = from; i <= to; ++i) {
        #ifdef USE_COMPRESSION
            CompressedTuple_t tuple(relation.get_tuple(i)->key, relation.get_tuple(i)->value);
            flow_source.push(&tuple);
        #else
            flow_source.push(relation.get_tuple(i));
        #endif
        // std::cout << "pushing tuple key=" + std::to_string(relation.get_tuple(i)->key) + '\n';
        ++processed_tuples;
    }
    flow_source.close();
}