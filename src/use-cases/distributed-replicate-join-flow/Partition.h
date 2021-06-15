#pragma once

#include <cstdint>
#include <array>
#include <immintrin.h>

#include "Settings.h"

#ifdef USE_COMPRESSION
#define Tuple_t CompressedTuple_t
#endif

#define TUPLES_PER_CACHELINE (Settings::CACHELINE_SIZE / sizeof(Tuple_t))


union cacheline_t {
    cacheline_t() {
        data.size = 0;
    };

    struct {
        Tuple_t tuples[TUPLES_PER_CACHELINE];
    } tuples;

    struct {
        Tuple_t tuples[TUPLES_PER_CACHELINE - 1];
        uint32_t size;
    } data;

    inline bool add_tuple(Tuple_t &tuple) {     // return true if full after adding
        uint32_t size_before = data.size++;
        tuples.tuples[size_before] = tuple;
        return (size_before == TUPLES_PER_CACHELINE - 1);
    }

    inline void clear() {
        data.size = 0;
    }

    inline uint32_t size() {
        return data.size;
    }
};



struct Partition {
    Partition(size_t max_tuples): max_tuples(max_tuples) {
        tuples = reinterpret_cast<Tuple_t*>(aligned_alloc(64, max_tuples*sizeof(Tuple_t)));
        std::fill_n(tuples, max_tuples, Tuple_t{1,1});
    }

    inline void add_cacheline(const cacheline_t *cacheline) {
        __m512i *dest = (__m512i *) &tuples[size];
        __m512i src = *((__m512i *) cacheline);

        _mm512_stream_si512(dest, src);
        size += TUPLES_PER_CACHELINE;

    #ifdef DEBUG
        if (size > max_tuples) {
            std::cout << "size: " << size << " max tuples: " << max_tuples << std::endl;
            throw std::runtime_error("partition overflow!");
        }
    #endif
    }

    inline void add_cacheline(const cacheline_t *cacheline, const size_t num_tuples) {
        __m512i *dest = (__m512i *) &tuples[size];
        __m512i src = *((__m512i *) cacheline);

        _mm512_stream_si512(dest, src);
        size += num_tuples;

    #ifdef DEBUG
        if (size > max_tuples) {   
            std::cout << "add partion cacheline: size: " << size << " max tuples: " << max_tuples << std::endl;
            throw std::runtime_error("partition overflow!");
        }
    #endif
    }

    const size_t max_tuples;
    size_t size = 0;
    Tuple_t *tuples;
};


struct PartitionHolder {
    PartitionHolder(size_t partition_size) {
        for (auto &partition : partitions) {
            partition = new Partition{partition_size};
        }
    }

    ~PartitionHolder() {
        for (auto &partition : partitions) {
            delete partition;
        }
    }

    std::array<Partition*, Settings::LOCAL_PARTITION_COUNT> partitions;
};

#ifdef Tuple_t
#undef Tuple_t
#endif