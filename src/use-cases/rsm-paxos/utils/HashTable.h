
#ifndef SRC_DB_QE_HASHTABLE_H_
#define SRC_DB_QE_HASHTABLE_H_

#pragma once
#include <cstdlib>
#include <iterator>
#include <utility>
#include <iostream>
#include <cmath>

// The probing methods
// Linear probing
// #define JUMP_(key, num_probes)    ( 1 )
// Quadratic probing
#define JUMP_(key, num_probes) (num_probes)

// Defines where the array lifes
//#define HEAP_

// Is the hashtable filled by multiple threads
// NOTE statistics are not reliable in multithreaded case

#define MULTITHREAD_INSERT_

// int bits_to_compress_ht(uint64_t num_elements_ht)
// {
//     auto max_number = num_elements_ht; // c++11
//     int num_bits_needed = (((floor(log2(max_number)))) + 1);
//     return num_bits_needed;
// };

template <typename T>
struct HashMapEqualTo
{
    inline constexpr bool operator()(const T &lhs, const T &rhs) const
    {
        return lhs == rhs;
    }
};

//#define HASH_PREPRO_
#ifdef HASH_PREPRO_
#ifndef HASH
#define HASH(KEY) (((923372036854775807) * KEY) >> 37)
#endif
#endif

struct HashMult
{
    // constrcut with bits_to_compress(expected_num_tuples)
    HashMult(uint64_t ht_size) : _ht_size(ht_size), _shift(64 - _ht_size) { std::cout << "shift" << _shift << '\n'; };

    inline uint64_t operator()(const uint64_t key) const
    {

        return (_a * key) >> (_shift);
    }

    inline void hash_info()
    {
        std::cout << "Using Multiply Shift" << '\n';
    }

    uint64_t _ht_size = 0;
    uint64_t _a = 923372036854775807;
    uint64_t _shift = 0;
};

struct HashStd
{
    HashStd() {};
    HashStd(const uint64_t mask) : _mask(mask)
    {
        // std::cout << "mask" << _mask << '\n';
    };

    inline uint64_t operator()(uint64_t key) const
    {
        return hash_fn(key) & _mask;
        // return hash_fn(key);
    }

    inline void hash_info()
    {
        std::cout << "Using std::hash<uint64_t>" << '\n';
        std::cout << "with mask" << _mask << '\n';
    }
    std::hash<uint64_t> hash_fn;
    const uint64_t _mask = 0;
};

template <typename KeyT, typename ValueT, typename HashT = std::hash<KeyT>, typename EqT = HashMapEqualTo<KeyT>>
class HashTable
{
  public:
    HashTable()
    {
        std::cout << "General HT" << '\n';
    };
};

template <typename ValueT, typename HashT>
class HashTable<uint64_t, ValueT, HashT, HashMapEqualTo<uint64_t>>
{

  public:
    //HashTable() : _hasher(bits_to_compress(expected_num_tuples)), _ht_size(expected_num_tuples)
    // STD Hash

    struct bucket_t
    {
        uint64_t first;
        ValueT second;
    };
    HashTable(){};
    HashTable(HashT hasher, size_t expected_num_tuples) : _hasher(hasher), _ht_size(expected_num_tuples)
    {
        // std::cout << "HashTable: expected_num_tuples " << expected_num_tuples << '\n';
        // std::cout << "sizeof(bucket_t) " << sizeof(bucket_t) << '\n';
        _hash_array = (bucket_t *)malloc(expected_num_tuples * sizeof(bucket_t));
        memset(_hash_array, _empty_key, expected_num_tuples * sizeof(bucket_t));
        // _hasher.hash_info();
        this->expected_num_tuples = expected_num_tuples;
    };

    HashTable(HashT hasher, size_t expected_num_tuples, bucket_t *rdma_buffer_ptr) : _hasher(hasher), _ht_size(expected_num_tuples)
    {
        // std::cout << "RDMA Constructor" << '\n';
        // std::cout << "sizeof(bucket_t) " << sizeof(bucket_t) << '\n';
        _hash_array = rdma_buffer_ptr;
        memset(_hash_array, _empty_key, expected_num_tuples * sizeof(bucket_t));
        // _hasher.hash_info();
        this->expected_num_tuples = expected_num_tuples;
    };

    ~HashTable()
    {

        // std::cout << "Desctructing HT" << '\n';
        // std::cout << "Num Buckets filled " << get_num_buckets_filled() << '\n';
        // std::cout << "Num Buckets " << get_num_buckets() << '\n';
        // std::cout << "load factor " << get_load_factor() << '\n';
        // std::cout << "longest chain" << get_longest_chain() << '\n';
        // std::cout << "collisions" << get_num_collisions() << '\n';

        free(_hash_array);
    }

    void clear()
    {
        _longest_probe_chain = 0;
        collisions = 0;
        _num_filled = 0;
        memset(_hash_array, _empty_key, expected_num_tuples * sizeof(bucket_t));
    }

    inline bool insert(uint64_t key, ValueT value)
    {
        auto bucket = find_bucket(key);
        // auto empty_key_ = _empty_key;
        // while (_hash_array[bucket].first != empty_key_)
        // {
        //     bucket = find_bucket(key);
        //     empty_key_ = _empty_key;
        // }
        _hash_array[bucket].first = key;
        _hash_array[bucket].second = value;
        ++_num_filled;
        return true;
    }

    inline bool find(uint64_t key, ValueT &ret_value)
    {
        return find_filled_bucket(key, ret_value);
    }

    inline ValueT find(uint64_t key)
    {
        ValueT ret_value;
        find_filled_bucket(key, ret_value);
        return ret_value;
    }

    void set_empty_key(uint64_t empty_key)
    {
        _empty_key = empty_key;
    }

    inline bool find_filled_bucket(uint64_t key, ValueT &ret_value) const
    {

        auto bucket_num = _hasher(key);
        int offset = 0;
        // Assumption: HT is never updated while reading
        // first build in creating pipeline and than used read only for reading
        // therefore use _longest chain
        for (; offset <= _max_probe_length; ++offset)
        {
            bucket_num = (bucket_num + JUMP_(key, offset)) & _mask;
            if (_hash_array[bucket_num].first == key)
            {
                ret_value = _hash_array[bucket_num].second;
                return true;
            }
        }
        return false;
    }

    uint64_t find_bucket(uint64_t key)
    {
        auto buck_num = _hasher(key);
        // return buck_num; //Hack - because keys are unique, buck_num will always (for given workload) be unqiue and within the hash map size range
        
        __builtin_prefetch(&_hash_array[buck_num], 1, 3);
        auto local_probe_chain = 0;
        int offset = 0;

        for (; offset <= _max_probe_length; ++offset)
        {
            buck_num = (buck_num + JUMP_(key, offset)) & _mask;

            // DebugCode(std::cout << "Bucket " << buck_num << " for key " << key << '\n';)

                // if (_eq(_hash_array[buck_num].first, key))
                // {
                //     return buck_num;
                // }
            if (_eq(_hash_array[buck_num].first, _empty_key))
            {
                _longest_probe_chain = ((local_probe_chain > _longest_probe_chain) ? local_probe_chain : _longest_probe_chain);
                return buck_num;
            }
            ++collisions;
            ++local_probe_chain;
        }

        std::cerr << "Longest Probe Chain reached, with key" << key << std::endl;
        _hasher.hash_info();
        std::cout << "Num Buckets filled " << get_num_buckets_filled() << '\n';
        std::cout << "Num Buckets " << get_num_buckets() << '\n';
        std::cout << "load factor " << get_load_factor() << '\n';
        std::cout << "longest chain " << get_longest_chain() << '\n';
        std::cout << "collisions " << get_num_collisions() << '\n';
        exit(EXIT_FAILURE);
    }

    uint64_t get_longest_chain()
    {
        return _longest_probe_chain;
    }

    uint64_t get_num_buckets_filled()
    {
        return _num_filled;
    }

    uint64_t get_num_collisions()
    {
        return collisions;
    }

    uint64_t get_num_buckets()
    {
        return _num_buckets;
    }

    double get_load_factor()
    {
        return (double)_num_filled / _num_buckets;
    }

    size_t get_mask()
    {
        return _mask;
    }

    bool print_hash_table(ofstream &logging_files)
    {
        logging_files << "PRINTING HT ##############" << std::endl;
        for (int i = 0; i < _num_buckets; i++)
        {
            if (_hash_array[i].first != _empty_key)
            {
                logging_files << _hash_array[i].first << " :" << _hash_array[i].second << std::endl;
            }
        }
        return true;
    }

    bucket_t *get_bucket_array()
    {
        return _hash_array;
    }

    void set_bucket_array(bucket_t *buckets)
    {
        _hash_array = buckets;
    }

    uint64_t get_replica()
    {
        return _replica;
    }

  private:
    uint64_t _replica = 0;
    HashT _hasher;
    bucket_t *_hash_array;
    uint64_t _ht_size = 0;
    uint64_t _empty_key = 0xFFFFFFFFFFFFFFFF;
    size_t _num_buckets = _ht_size;
    size_t _num_filled{0};
    size_t collisions{0};
    int _max_probe_length = 20;
    int _longest_probe_chain = 0;           // Our longest bucket-brigade is this long. ONLY when we have zero elements is this ever negative (-1).
    int expected_num_tuples = 0;
    size_t _mask = _ht_size - 1; // _num_buckets minus one used to replace modulo
    HashMapEqualTo<uint64_t> _eq;
};

#endif