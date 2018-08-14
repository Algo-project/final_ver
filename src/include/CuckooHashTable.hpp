#ifndef _CUCKOOHASHTABLE_HPP_
#define _CUCKOOHASHTABLE_HPP_

#include <stdexcept>
#include "HashTable.hpp"


struct CuckooConfig
{
    const int SLOTS_PER_BUCKET = 4;
};

template<typename K>
class CuckooBucket 
{
    private:
        using key_type = K;
        using value_type = std::pair<K, uint64_t>;
        using reference = value_type &;
        using const_reference = const value_type &;
        using pointer = value_type*;
        using const_pointer = const value_type *;

    public:
        reference at(const int ind);
        value_type insert(const_reference kvp);
        
        

    private:


};



#endif
