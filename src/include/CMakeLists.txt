SET(THREAD_SOURCE Threadpool.hpp Threadpool.cpp)
SET(DATABASE_SOURCE 
            parser.cc parser.h 
            Database.h Database.cpp
            Relation.h Relation.cpp
            Operators.h Operators.cpp         
            HashTable.hpp ConsiseHashTable.hpp
            Joiner.h Joiner.cpp
            Timer.hpp
    )
ADD_LIBRARY(threadpool STATIC ${THREAD_SOURCE})
ADD_LIBRARY(database STATIC ${DATABASE_SOURCE})

