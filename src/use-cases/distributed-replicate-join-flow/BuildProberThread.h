/**
 * @file BuildProberThread.h
 * @author Lasse Thostrup <lasse.thostrup@cs.tu-darmstadt.de>
 * @brief Adaption of Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
 * Paper: http://www.vldb.org/pvldb/vol10/p517-barthels.pdf
 * @date 2020-06-09
 * 
 */

#pragma once
#include "Settings.h"
#include "../../flow-api/dfi.h"
#include <thread>
#include "utils/Thread.h"
#include "Relation.h"

class BuildProberThread : public utils::Thread
{
	// using Hashtable_t = HashTable<uint64_t, uint64_t, HashStd, HashMapEqualTo<uint64_t>>; //KeyType, Hashmap-Size, Hashing-function

public:
    BuildProberThread(size_t partition_id, PartitionHolder **left_partitions, PartitionHolder **right_partitions);
    void run(); 
private:
    PartitionHolder **left_partitions; 
    PartitionHolder **right_partitions;
	size_t partition_id;
public:
    size_t joined_tuples_cnt = 0;
    size_t non_matched_tuples = 0;
};

BuildProberThread::BuildProberThread(size_t partition_id, PartitionHolder **left_partitions, PartitionHolder **right_partitions) :
	 left_partitions(left_partitions), right_partitions(right_partitions), partition_id(partition_id)
{

}


//NOTE: Build Probe code taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void BuildProberThread::run()
{
	#ifdef USE_COMPRESSION
    const size_t est_merged_size = 1.1 * sizeof(CompressedTuple_t) * (Settings::LEFT_REL_FULL_SIZE / Settings::LOCAL_PARTITION_COUNT);
	#else
    const size_t est_merged_size = 1.1 * sizeof(Tuple_t) * (Settings::LEFT_REL_FULL_SIZE / Settings::LOCAL_PARTITION_COUNT);
	#endif


	uint64_t N = est_merged_size;
	NEXT_POW_2(N);
	uint64_t const MASK = (N-1);

	struct ht_entry {
		uint64_t t;
		uint64_t key;
	};

	ht_entry *hashTableNext = (ht_entry*) calloc(est_merged_size, sizeof(ht_entry));
	uint64_t *hashTableBucket = (uint64_t*) calloc(N, sizeof(uint64_t));	// stores indices


	// build hash table
	uint64_t t = 0;
	for (size_t i = 0; i < Settings::PART_THREADS_COUNT; i++) {
		Partition *left = left_partitions[i]->partitions[partition_id];

		for (size_t j = 0; j < left->size; j++) {
			uint64_t idx = (left->tuples[j].key >> Settings::LOCAL_PARTITION_BITS) & MASK; //HASH_BIT_MODULO(left_partition.get_tuple(t)->value, MASK, shiftBits);
			hashTableNext[t] = {hashTableBucket[idx], left->tuples[j].key};
			hashTableBucket[idx]  = ++t;
		}
	}

	// Probe hash table
	for (size_t i = 0; i < Settings::TOTAL_THREADS_COUNT; i++) {
		Partition *right = right_partitions[i]->partitions[partition_id];

		for (size_t j = 0; j < right->size; j++) {
			uint64_t idx = (right->tuples[j].key >> Settings::LOCAL_PARTITION_BITS) & MASK;

			uint64_t hit;
			for(hit = hashTableBucket[idx]; hit > 0; hit = hashTableNext[hit-1].t) {
				if (right->tuples[j].key == hashTableNext[hit-1].key) {
					++joined_tuples_cnt;
				}
			}
			// if (hashTableBucket[idx] == 0) {
			// 	++non_matched_tuples;
			// }
		}

	}

	free(hashTableNext);
	free(hashTableBucket);
}