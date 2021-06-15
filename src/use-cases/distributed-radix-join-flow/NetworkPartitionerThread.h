/**
 * @file NetworkPartitionerThread.h
 * @author Lasse Thostrup <lasse.thostrup@cs.tu-darmstadt.de>
 * @brief Adaption of Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
 * Paper: http://www.vldb.org/pvldb/vol10/p517-barthels.pdf
 * @date 2020-06-09
 * 
 */

#pragma once
#include <thread>
#include "Settings.h"
#include "Relation.h"
#include "Partition.h"
#include "../../flow-api/dfi.h"
#include "utils/Thread.h"



class NetworkPartitionerThread : public utils::Thread
{
public:
    NetworkPartitionerThread(Relation &relation, size_t from, size_t to);
    void run();
    size_t get_processed_tuples() { return processed_tuples; };
    static TargetID distribution_function(const Tuple &tuple);
private:
    Relation &relation;
    DFI_Shuffle_flow_source flow_source;
    size_t from;
    size_t to;
    size_t processed_tuples = 0;
};

NetworkPartitionerThread::NetworkPartitionerThread(Relation &relation, size_t from, size_t to) : relation(relation),
    flow_source((relation.get_type() == Relation::Type::LEFT) ? Settings::LEFT_REL_FLOW_NAME : Settings::RIGHT_REL_FLOW_NAME, dfi::Config::DFI_SOURCE_SEGMENT_COUNT, true),
    from(from), to(to)
{
}

TargetID NetworkPartitionerThread::distribution_function(const Tuple &tuple)
{
  return ((*reinterpret_cast<TargetID*>(tuple.getDataPtr()))&(Settings::NETWORK_PARTITION_COUNT-1));
}

//NOTE: Partitioning code taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void NetworkPartitionerThread::run()
{
    #ifdef COMPRESSED_TUPLES
	const uint32_t partitionBits = Settings::NETWORK_PARTITION_BITS;
    #endif

    cacheline_t cacheline_buffer[Settings::NETWORK_PARTITION_COUNT] __attribute__((aligned(Settings::CACHELINE_SIZE)));

    for(size_t i = from; i <= to; ++i)
    {
		// Compute partition
        uint32_t partition_id = HASH_BIT_MODULO(relation.get_tuple(i)->key, Settings::NETWORK_PARTITION_COUNT - 1, 0);
        cacheline_t *cacheline = cacheline_buffer + partition_id;
        #ifdef COMPRESSED_TUPLES
        CompressedTuple_t tuple{relation.get_tuple(i)->value + ((relation.get_tuple(i)->key >> partitionBits) << (partitionBits + Settings::PAYLOAD_BITS))};
        #else
        Tuple_t tuple{relation.get_tuple(i)->key, relation.get_tuple(i)->value};
        #endif
        bool is_full = cacheline->add_tuple(tuple);

        if (is_full) {
            flow_source.pushCacheLineNonTemp(cacheline, partition_id);
            cacheline->clear();
        }

        ++processed_tuples;
    }

	// Flush remaining elements to memory buffers
    for (uint32_t i = 0; i < Settings::NETWORK_PARTITION_COUNT; ++i) {
        cacheline_t *cacheline = cacheline_buffer + i;
		if (cacheline->size() > 0) {
            flow_source.pushBulk(cacheline, i, cacheline->size());
        }
    }

    flow_source.close();    
}