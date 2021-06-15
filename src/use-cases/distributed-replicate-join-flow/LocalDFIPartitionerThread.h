/**
 * @file LocalDFIPartitionerThread.h
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


class LocalDFIPartitionerThread : public utils::Thread
{

public:
    LocalDFIPartitionerThread(std::string flow_name, TargetID target_id, Relation::Type relation_type, PartitionHolder *local_partitions);
    void run();
    size_t get_processed_tuples() { return processed_tuples; };
    long long execution_time = 0;
    long long execution_part_time = 0;
private:
    DFI_Replicate_flow_target flow_target;
    size_t processed_tuples = 0;
    
    PartitionHolder *local_partitions;
};

LocalDFIPartitionerThread::LocalDFIPartitionerThread(std::string flow_name, TargetID target_id, Relation::Type, PartitionHolder *local_partitions) :
    flow_target(flow_name, target_id), local_partitions(local_partitions)
{
}

//NOTE: Partitioning code partly taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void LocalDFIPartitionerThread::run() {
	struct timeval tv_start;
	struct timeval tv_end;

    gettimeofday(&tv_start, NULL);

    constexpr auto MASK = Settings::LOCAL_PARTITION_COUNT - 1;
    cacheline_t cacheline_buffer[Settings::LOCAL_PARTITION_COUNT] __attribute__((aligned(Settings::CACHELINE_SIZE)));

    dfi::Tuple dfi_tuple;
    size_t tuple_count = 0;
    while(flow_target.consume(dfi_tuple, tuple_count) != DFI_FLOW_FINISHED) {
        #ifdef USE_COMPRESSION
            CompressedTuple_t* tuple = (CompressedTuple_t*) dfi_tuple.getDataPtr();
        #else
            Tuple_t* tuple = (Tuple_t*) dfi_tuple.getDataPtr();
        #endif

        for(size_t i = 0; i < tuple_count; ++i) {
            uint32_t partition_id = tuple[i].key & MASK;

            cacheline_t *cacheline = cacheline_buffer + partition_id;
            bool is_full = cacheline->add_tuple(tuple[i]);

            if (is_full) {
                local_partitions->partitions[partition_id]->add_cacheline(cacheline);
                cacheline->clear();
            }

            ++processed_tuples;
        }
    }


    for (uint32_t i = 0; i < Settings::LOCAL_PARTITION_COUNT; ++i) {
        cacheline_t *cacheline = cacheline_buffer + i;
		if (cacheline->size() > 0) {
            local_partitions->partitions[i]->add_cacheline(cacheline, cacheline->size());
        }
    }


    gettimeofday(&tv_end, NULL);
    execution_part_time = get_time_diff(tv_start, tv_end);
}