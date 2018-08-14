#ifndef _HASHTABLE_HPP_
#define _HASHTABLE_HPP_

#include <stdlib.h>
#include <stdint.h>
#include <vector>

template<typename E>
class HashTable
{
    public:
        /* You must implement following 4 methods! */
        virtual size_t getSize() = 0;
        virtual int Insert(const E key, const uint64_t hashv, const uint64_t value) = 0;
        virtual uint64_t SearchKey(const E key, const uint64_t hashv, 
                                    std::vector<uint64_t> &result_buffer) const = 0;
        virtual void Erase() = 0;
        virtual ~HashTable(){};

    public:
        /* Some method is declared without implementation */
        [[deprecated]]
            virtual int TriggerBuild(){return 0;};     // this is implemented in CHT
};


#endif
