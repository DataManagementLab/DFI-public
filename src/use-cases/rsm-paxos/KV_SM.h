/**
 * @file Log.h
 * @author jskrzypczak
 * Simple state machine of a key-value store.
 * @date 2020-06-15
 */

#pragma once

#include <cstring>
#include "Settings.h"


using hashtable = HashTable<uint64_t, uint64_t, HashStd, HashMapEqualTo<uint64_t>>; //KeyType, Hashmap-Size, Hashing-function


class KV_Store
{
    private:
        hashtable ht;

    public:
        KV_Store();
        ~KV_Store();
        bool insert(Command&);
        bool lookup(Command&, uint64_t&);
};

KV_Store::KV_Store() : ht(HashStd(HT_SIZE-1), HT_SIZE)
{

}

KV_Store::~KV_Store()
{

}

inline bool KV_Store::insert(Command& cmd)
{
    bool success = ht.insert(cmd.key, cmd.value);
    return success;
}

inline bool KV_Store::lookup(Command& cmd, uint64_t& value)
{
    bool success = ht.find(cmd.key, value);
    return success;
}
