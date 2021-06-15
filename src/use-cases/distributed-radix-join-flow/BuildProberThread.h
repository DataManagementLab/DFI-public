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

public:
    BuildProberThread(Partition &left_partition, Partition &right_partition);
    size_t get_joined_tuples_cnt() { return joined_tuples_cnt; };
    size_t get_processed_left_tuples() { return processed_left_tuples; };
    size_t get_processed_right_tuples() { return processed_right_tuples; };
    void run(); 
private:
    Partition &left_partition;
    Partition &right_partition;
    size_t joined_tuples_cnt = 0;
    size_t processed_left_tuples = 0;
    size_t processed_right_tuples = 0;
};

BuildProberThread::BuildProberThread(Partition &left_partition, Partition &right_partition) : left_partition(left_partition), right_partition(right_partition)
{

}


//NOTE: Build Probe code taken from Claude Barthels' MPI join from https://www.systems.ethz.ch/node/334
void BuildProberThread::run()
{
	// size_t non_matched_tuples = 0;
    #ifdef COMPRESSED_TUPLES
	uint32_t const keyShift = Settings::NETWORK_PARTITION_BITS + Settings::PAYLOAD_BITS;
    #else
    uint32_t const keyShift = Settings::NETWORK_PARTITION_BITS;
    #endif
	uint32_t const shiftBits =  keyShift + Settings::LOCAL_PARTITION_BITS;


	uint64_t N = left_partition.size;
	NEXT_POW_2(N);
	uint64_t const MASK = (N-1) << (shiftBits);

	uint64_t *hashTableNext = (uint64_t*) calloc(left_partition.size, sizeof(uint64_t));
	uint64_t *hashTableBucket = (uint64_t*) calloc(N, sizeof(uint64_t));

	// Build hash table
	for (uint64_t t=0; t < left_partition.size;) {
		processed_left_tuples++;
        #ifdef COMPRESSED_TUPLES
        uint64_t idx = HASH_BIT_MODULO(left_partition.tuples[t].value, MASK, shiftBits);
        #else
        uint64_t idx = HASH_BIT_MODULO(left_partition.tuples[t].key, MASK, shiftBits);
        #endif
		hashTableNext[t] = hashTableBucket[idx];
		hashTableBucket[idx]  = ++t;
	}


	// Probe hash table
	for (uint64_t t=0; t<right_partition.size; ++t) {
		processed_right_tuples++;
        #ifdef COMPRESSED_TUPLES
        uint64_t idx = HASH_BIT_MODULO(right_partition.tuples[t].value, MASK, shiftBits);
        #else
        uint64_t idx = HASH_BIT_MODULO(right_partition.tuples[t].key, MASK, shiftBits);
        #endif
		for(uint64_t hit = hashTableBucket[idx]; hit > 0; hit = hashTableNext[hit-1]){
            #ifdef COMPRESSED_TUPLES
			if((right_partition.tuples[t].value >> keyShift) == (left_partition.tuples[hit-1].value >> keyShift)){
            #else
			if((right_partition.tuples[t].key) == (left_partition.tuples[hit-1].key)){
            #endif
                ++joined_tuples_cnt;
			}
		}
		// if (hashTableBucket[idx] == 0) {
		// 	++non_matched_tuples;
		// }
	}


	free(hashTableNext);
	free(hashTableBucket);

	// if (collision > 0)
	// 	std::cout << "collision: " << collision << std::endl;
	// if (collision_right > 0)
	// 	std::cout << "collision_right: " << collision_right << std::endl;
}