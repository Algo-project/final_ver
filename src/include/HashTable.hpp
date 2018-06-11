#ifndef _HASHTABLE_HPP_
#define _HASHTABLE_HPP_

#include "ConsiseHashTable.hpp"
#include "ArrayHashTable.hpp"
#include "StdHashTable.hpp"

#define _AHT 0
#define _CHT 1
#define _STD 2
#define USING _CHT

template <typename E>
#if USING == _CHT
using HashTable = CHT<E>;
#elif USING == _AHT
using HashTable = ArrayHashTable<E>;
#else 
using HashTable = SimpleHashTable<E>;
#endif

    


#endif
