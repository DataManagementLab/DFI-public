#pragma once
#include "../../utils/Config.h"
#include <numeric>

#define COMPRESSED_TUPLES
#define RAND_RANGE(N) (((double) rand() / ((double) RAND_MAX + 1)) * (N))

#ifdef COMPRESSED_TUPLES
struct CompressedTuple_t
{
    uint64_t value;
    // CompressedTuple_t() {};
    // CompressedTuple_t(uint64_t value) : value(value) {};
};
#endif
struct Tuple_t
{
    uint64_t key;
    uint64_t value;
    Tuple_t() {};
    Tuple_t(uint64_t key, uint64_t value) : key(key), value(value) {};
};


#ifdef COMPRESSED_TUPLES
using tuple_type = CompressedTuple_t;
#else
using tuple_type = Tuple_t;
#endif

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

    static uint32_t THREADS_COUNT;

    static bool RANDOM_ORDER;

    static const uint64_t NETWORK_PARTITION_BITS = 8;

    static const uint64_t LOCAL_PARTITION_BITS = 8;

    static const uint64_t NETWORK_PARTITION_COUNT = (1 << NETWORK_PARTITION_BITS);
    static const uint64_t LOCAL_PARTITION_COUNT = (1 << LOCAL_PARTITION_BITS); //LOCAL PARTITIONS PER NETWORK PARTITION. Total partition count = PARTITION_COUNT * LOCAL_PARTITION_COUNT

    static const uint32_t CACHELINE_SIZE = 64;

	static const uint32_t PAYLOAD_BITS = 27;

    static std::vector<size_t> get_partitions(NodeID node, size_t partition_count)
    { 
        std::vector<size_t> partitions;
        if (node == dfi::Config::DFI_NODES.size() - 1)
            partitions.resize(partition_count - node * (partition_count/dfi::Config::DFI_NODES.size()));
        else
            partitions.resize(partition_count/dfi::Config::DFI_NODES.size());
        
        std::iota(partitions.begin(), partitions.end(), node * (partition_count/dfi::Config::DFI_NODES.size()));
        return partitions;
    };

    static NodeID get_assigment(size_t partition, size_t partition_count) 
    {
        auto a = (partition / (partition_count/dfi::Config::DFI_NODES.size())) + 1;
        if (a > dfi::Config::DFI_NODES.size())
            return dfi::Config::DFI_NODES.size();
        return a; 
    };
};


// #define COMPRESSED_TUPLES

#define TOTAL_PARTITION_COUNT ((1ul << Settings::NETWORK_PARTITION_BITS) << Settings::LOCAL_PARTITION_BITS)

#define PARTITION_COUNT_PER_NODE (TOTAL_PARTITION_COUNT / Settings::NODES_TOTAL)

#define TUPLES_PER_CACHELINE (Settings::CACHELINE_SIZE / sizeof(tuple_type))
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