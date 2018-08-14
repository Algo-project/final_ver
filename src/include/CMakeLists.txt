SET(THREAD_SOURCE Threadpool.hpp Threadpool.cpp)
SET(LIBCUCKOO_FILE libcuckoo/cuckoohash_config.hh
    libcuckoo/cuckoohash_map.hh  libcuckoo/cuckoohash_util.hh  
    libcuckoo/libcuckoo_bucket_container.hh)
SET(DATABASE_SOURCE 
            parser.cc parser.h 
            Database.h Database.cpp
            Relation.h Relation.cpp
            Operators.h Operators.cpp         
            HashTable.hpp ConsiseHashTable.hpp ArrayHashTable.hpp StdHashTable.hpp HashTableCfg.hpp
            Joiner.h Joiner.cpp
            Timer.hpp
            CuckooHashTable.hpp ${LIBCUCKOO_FILE}
    )
ADD_LIBRARY(threadpool STATIC ${THREAD_SOURCE})
ADD_LIBRARY(database STATIC ${DATABASE_SOURCE})

