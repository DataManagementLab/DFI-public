/**
 * @file LocalPartitionerThread.h
 * @author Lasse Thostrup <lasse.thostrup@cs.tu-darmstadt.de>
 * @brief Adaption of Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
 * Paper: http://www.vldb.org/pvldb/vol10/p517-barthels.pdf
 * @date 2020-06-09
 * 
 */

#pragma once
#include "Settings.h"
#include "Relation.h"
#include "../../flow-api/dfi.h"
#include "utils/Thread.h"
#include <immintrin.h>
#include <sys/time.h>


class LocalPartitionerThread : public utils::Thread
{


public:
    LocalPartitionerThread(size_t network_partition, Relation::Type relation_type, PartitionHolder *local_partitions);
    void run();
    size_t get_processed_tuples() { return processed_tuples; };
    long long execution_time = 0;
    long long execution_part_time = 0;
private:
    DFI_Shuffle_flow_target flow_target;
    size_t processed_tuples = 0;
    PartitionHolder *local_partitions;
    size_t network_partition;
};

LocalPartitionerThread::LocalPartitionerThread(size_t network_partition, Relation::Type relation_type, PartitionHolder *local_partitions) :
    flow_target((relation_type == Relation::Type::LEFT) ? Settings::LEFT_REL_FLOW_NAME : Settings::RIGHT_REL_FLOW_NAME, network_partition), local_partitions(local_partitions)
    , network_partition(network_partition)
{
}

//NOTE: Partitioning code partly taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void LocalPartitionerThread::run()
{
	struct timeval tv_start;
	struct timeval tv_end;

    #ifdef COMPRESSED_TUPLES
	const uint64_t MASK = (Settings::LOCAL_PARTITION_COUNT - 1) << (Settings::NETWORK_PARTITION_BITS + Settings::PAYLOAD_BITS);
    #else
	const uint64_t MASK = (Settings::LOCAL_PARTITION_COUNT - 1) << (Settings::NETWORK_PARTITION_BITS);
    #endif
    cacheline_t cacheline_buffer[Settings::LOCAL_PARTITION_COUNT] __attribute__((aligned(Settings::CACHELINE_SIZE)));


    dfi::Tuple dfi_tuple;
    size_t tuple_count = 0;

    gettimeofday(&tv_start, NULL);

    while(flow_target.consume(dfi_tuple, tuple_count) != DFI_FLOW_FINISHED)
    {
        // std::cout << "Got " << tuple_count << " tuples." << '\n';
        tuple_type* tuple = (tuple_type*) dfi_tuple.getDataPtr();
        for(size_t i = 0; i < tuple_count; ++i)
        {
            #ifdef COMPRESSED_TUPLES
            uint64_t partition_id = HASH_BIT_MODULO(tuple[i].value, MASK, (Settings::NETWORK_PARTITION_BITS + Settings::PAYLOAD_BITS));
            #else
            uint64_t partition_id = HASH_BIT_MODULO(tuple[i].key, MASK, Settings::NETWORK_PARTITION_BITS);
            #endif

            // std::cout << "key: " << key << " partition: " << partition << " offset: " << outputOffsets[partition] << '\n';
            
            cacheline_t *cacheline = cacheline_buffer + partition_id;
            bool is_full = cacheline->add_tuple(tuple[i]);

            if (is_full) {
                local_partitions->partitions[partition_id]->add_cacheline(cacheline);
                cacheline->clear();
            }
            ++processed_tuples;
        }
    }

    gettimeofday(&tv_end, NULL);
    execution_part_time = get_time_diff(tv_start, tv_end);
    gettimeofday(&tv_start, NULL);


    for (uint32_t i = 0; i < Settings::LOCAL_PARTITION_COUNT; ++i) {
        cacheline_t *cacheline = cacheline_buffer + i;
		if (cacheline->size() > 0) {
            local_partitions->partitions[i]->add_cacheline(cacheline, cacheline->size());
        }
    }

    gettimeofday(&tv_end, NULL);
    execution_time = get_time_diff(tv_start, tv_end);
}
