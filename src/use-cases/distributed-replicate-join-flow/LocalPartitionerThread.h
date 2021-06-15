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
    LocalPartitionerThread(Relation &relation, size_t from, size_t to, PartitionHolder *local_partitions);
    void run();
    size_t get_processed_tuples() { return processed_tuples; };
    long long execution_time = 0;
    long long execution_part_time = 0;
private:
    Relation &relation;
    size_t processed_tuples = 0;
    PartitionHolder *local_partitions;
    size_t from;
    size_t to;
};

LocalPartitionerThread::LocalPartitionerThread(Relation &relation, size_t from, size_t to, PartitionHolder *local_partitions) :
    relation(relation), local_partitions(local_partitions), from(from), to(to)
{
}

//NOTE: Partitioning code partly taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void LocalPartitionerThread::run() {
	struct timeval tv_start;
	struct timeval tv_end;

    gettimeofday(&tv_start, NULL);

    constexpr auto MASK = Settings::LOCAL_PARTITION_COUNT - 1;
    cacheline_t cacheline_buffer[Settings::LOCAL_PARTITION_COUNT] __attribute__((aligned(Settings::CACHELINE_SIZE)));

    for (size_t i = from; i <= to; ++i) {
        #ifdef USE_COMPRESSION
        CompressedTuple_t tuple{relation.get_tuple(i)->key, relation.get_tuple(i)->value};
        #else
        Tuple_t tuple = *relation.get_tuple(i);
        #endif

        uint32_t partition_id = tuple.key & MASK;
        
        cacheline_t *cacheline = cacheline_buffer + partition_id;
        bool is_full = cacheline->add_tuple(tuple);

        if (is_full) {
            local_partitions->partitions[partition_id]->add_cacheline(cacheline);
            cacheline->clear();
        }

        ++processed_tuples;
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