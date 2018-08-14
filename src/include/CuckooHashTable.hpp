#ifndef _CUCKOOHASHTABLE_HPP_
#define _CUCKOOHASHTABLE_HPP_

#include <stdexcept>
#include "HashTable.hpp"
#include "libcuckoo/cuckoohash_map.hh"

template<typename K>
class CuckooHashTable : public HashTable<K>
{
    using E = K;
    public:
        size_t getSize() override { return map.size();} 
        int Insert(const E key, const uint64_t hashv, const uint64_t value) override
        {
        }

        uint64_t SearchKey(const E key, const uint64_t hashv,
                std::vector<uint64_t> &result) const override
        {
            map.find(key);
        }

        void Erase() override {map.clear();}

    private:
        cuckoohash_map<K, uint64_t> map;
};


CuckooHashTable<uint64_t> vv;

#endif
