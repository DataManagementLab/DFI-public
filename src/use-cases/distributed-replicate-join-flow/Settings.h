#pragma once
#include "../../utils/Config.h"
#include <numeric>

#define RAND_RANGE(N) (((double) rand() / ((double) RAND_MAX + 1)) * (N))


#define USE_COMPRESSION

//#define USE_MULTICAST

struct Tuple_t
{
    uint64_t key;
    uint64_t value;
    Tuple_t() {};
    Tuple_t(uint64_t key, uint64_t value) : key(key), value(value) {};
};

union CompressedTuple_t {
    uint64_t data;

    struct {
        uint32_t key;
        uint32_t value;
    };
    
    CompressedTuple_t() {};
    // CompressedTuple_t(uint64_t key, uint64_t value) : key(key), value(value) {};
    CompressedTuple_t(uint64_t key, uint64_t value) : data(key<<32 | value) {};
};

struct Settings
{
    static const string LEFT_REL_FLOW_NAME;
    static const string RIGHT_REL_FLOW_NAME;

    static uint64_t LEFT_REL_FULL_SIZE;
    static uint64_t RIGHT_REL_FULL_SIZE;

    static uint64_t LEFT_REL_SIZE;
    static uint64_t RIGHT_REL_SIZE;

    static uint32_t NODE_ID;
    static uint32_t NODES_TOTAL;

    static uint32_t PART_THREADS_COUNT;
    static uint32_t TOTAL_THREADS_COUNT;

    static uint32_t TUPLE_SEND_COUNT;

    static bool RANDOM_ORDER;

    static const uint64_t LOCAL_PARTITION_BITS = 9; //LOCAL PARTITIONS PER NETWORK PARTITION. Total partition count = PARTITION_COUNT * LOCAL_PARTITION_COUNT
    
    static const uint64_t LOCAL_PARTITION_COUNT = (1 << LOCAL_PARTITION_BITS); //LOCAL PARTITIONS PER NETWORK PARTITION. Total partition count = PARTITION_COUNT * LOCAL_PARTITION_COUNT

    static const uint32_t CACHELINE_SIZE = 64;

	static const uint32_t PAYLOAD_BITS = 27;
};


#define TOTAL_PARTITION_COUNT (1ul << Settings::LOCAL_PARTITION_BITS)

// #define TUPLES_PER_CACHELINE (Settings::CACHELINE_SIZE / sizeof(Tuple_t))
#define COMPRESSED_TUPLES_PER_CACHELINE (Settings::CACHELINE_SIZE / sizeof(CompressedTuple_t))

#define TUPLES_PER_CACHELINE (Settings::CACHELINE_SIZE / sizeof(Tuple_t))


/* RADIX STUFF */
#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & (MASK)) >> NBITS) //Shifting the network bits, causes the network partitioning to start by filling one partition at a time, meaning remote writes will be spread out

#define NEXT_POW_2(V) \
    do                \
    {                 \
        V--;          \
        V |= V >> 1;  \
        V |= V >> 2;  \
        V |= V >> 4;  \
        V |= V >> 8;  \
        V |= V >> 16; \
        V++;          \
    } while (0)


static inline long long get_time_diff(timeval& tv_start, timeval& tv_end) {
    long long start_us = (tv_start.tv_sec % 86400) * 1000000
            + tv_start.tv_usec;
    long long end_us = (tv_end.tv_sec % 86400) * 1000000 + tv_end.tv_usec;
    return (end_us - start_us);
};