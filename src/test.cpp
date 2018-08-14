#include <iostream>
#include <stdio.h>
#include <random>
#include <string>

#include "include/libcuckoo/cuckoohash_map.hh"
#ifndef OUTPUT
#include "include/ConsiseHashTable.hpp"
#include "include/Threadpool.hpp"
#include "include/CuckooHashTable.hpp"
#else
#include "ConsiseHashTable.hpp"
#include "Threadpool.hpp"
#include "CuckooHashTable.hpp"
#endif

#define MAXN 1000000000
size_t consume_some_cpu(int tid,size_t N)
{
    printf("thread %d start to consume some cpu...\n",tid);
    size_t sum = 0;
    for(size_t i=0;i<N;i++,sum++);
    return sum;
}

void sleep_job(int tid,int sec)
{
    printf("thread %d start to sleep for %d seconds\n",tid,sec);
    std::this_thread::sleep_for(std::chrono::seconds(sec));
    printf("thread %d wakes up\n",tid);
}

class A
{
    public:
        int i;
        void hello(){std::cout<<i<<std::endl;}
        void hello2(int b){std::cout<<b<<std::endl;}
};

void doA(A &a, int m)
{
    a.hello2(m);
}


uint64_t hashFunc(uint64_t key)
{
    return key%37;
}
int main(int argc, char * argv[])
{
    using Map = cuckoohash_map<uint64_t, uint64_t> ;
    Map mp;
    mp.insert(1,2);
    mp.insert(2,2);
    mp.insert(1,3);
    auto v = mp.lock_table().equal_range(2);
    std::cout<<mp.lock_table().count(1)<<std::endl;
    for(auto it = v.first; it!=v.second; it++)
    {
        std::cout<<it->first<<std::endl;
    }
}
