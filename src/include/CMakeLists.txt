SET(THREAD_SOURCE Threadpool.hpp Threadpool.cpp)
SET(DATABASE_SOURCE parser.cc parser.h)
ADD_LIBRARY(threadpool STATIC ${THREAD_SOURCE})
ADD_LIBRARY(database STATIC ${DATABASE_SOURCE})

