#ifndef _STDHASHTABLE_HPP_
#define _STDHASHTABLE_HPP_

#include <unordered_map>
#include <map>
#include <algorithm>
#include <vector>
#include <string.h>
#include "Timer.hpp"

#include "HashTable.hpp"


/* A simple hashtable based on std::unordered_multimap */

extern Util::Timer hashTableTimer;
template<typename E>
class SimpleHashTable : public HashTable<E>
{
    public:
        SimpleHashTable(size_t s){};
        ~SimpleHashTable() = default;

        size_t getSize()const {return table_.size();}
        size_t getSize(){return table_.size();}
        int Insert(const E key, const uint64_t hashv, const uint64_t v)
        {
            table_.insert({key,v});
            return 0;
        }
        uint64_t SearchKey(const E key, const uint64_t hashv, std::vector<uint64_t> &result) const
        {
            auto pair = table_.equal_range(key);
            std::transform(pair.first, pair.second, std::back_inserter(result),
                    [](auto it){return it.second;});
            return std::distance(pair.first, pair.second);
        }
        void Erase(){
            table_.clear();
        }
    private:
        std::multimap<E,uint64_t> table_;

};


#endif
