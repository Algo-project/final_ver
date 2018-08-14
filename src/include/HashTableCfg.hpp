#ifndef _HASHTABLECFG_HPP_
#define _HASHTABLECFG_HPP_

#include "HashTable.hpp"
#include "ConsiseHashTable.hpp"
#include "ArrayHashTable.hpp"
#include "StdHashTable.hpp"

#define _AHT 0
#define _CHT 1
#define _STD 2
#define USING _STD

template <typename E>
#if USING == _CHT
using HashTable_ = CHT<E>;
#elif USING == _AHT
using HashTable_ = ArrayHashTable<E>;
#else 
using HashTable_ = SimpleHashTable<E>;
#endif

    


#endif
