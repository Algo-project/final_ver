#include <iostream>
#include <stdio.h>
#include <random>
#include <string>

#ifndef OUTPUT
#include "include/ConsiseHashTable.hpp"
#include "include/Threadpool.hpp"
#else
#include "ConsiseHashTable.hpp"
#include "Threadpool.hpp"
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
    CHT<uint64_t> hashtable(128);
    for(int i=0;i<120;i++)
    {
        int ii= i%40;
        hashtable.Insert(ii,hashFunc(ii),i);
    }
    hashtable.TriggerBuild();
    std::vector<uint64_t> ans;
    for(int i=0;i<40;i++)
    {
        hashtable.SearchKey(i,hashFunc(i),ans);
        std::cout<<"Searching key "<<i<<" : ";
        for(auto ent : ans)
        {
            std::cout<<(int64_t)ent<<" ";
        }
        std::cout<<std::endl;
        ans.clear();
    }

}
