#include <iostream>
#include <stdio.h>
#include <random>
#include <string>

#ifndef OUTPUT
#include "include/Threadpool.hpp"
#else
#include "Threadpool.hpp"
#endif

#define MAXN 100
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
};

void doA(A &a)
{
    a.hello();
}

int main(int argc, char * argv[])
{
    if (argc!=2)
    {
        std::cerr<<"Usage: ./test <nthreads>"<<std::endl;
        return -1;
    }
    int nn = atoi(argv[1]);
    Aposta::FixedThreadPool pool(nn);

    /* the example for upload-and-forget tasks */

    std::random_device r;
    std::default_random_engine e(r());
    std::uniform_int_distribution<int> uni(1,2);

    for(int i=0;i<nn;i++)
        pool.enqueue(sleep_job,i,uni(e));

    /* the example for FixedThreadPool::barrier */
    /* comment the next line of code to see what happens */
    pool.barrier();
    printf("main function across the barrier!\n");


    /* the example for getting the result of the tasks */
    std::vector<std::future<size_t>> results;
    for(int i=0;i<nn;i++)
    {
        results.emplace_back(
            pool.enqueue(consume_some_cpu,i,MAXN/nn)
        );
    }

    for(auto && result : results)
        std::cout<< result.get() << " ";
    std::cout<<std::endl;
    pool.barrier();

    /* the example when it comes to object's methods */
    A a;
    a.i = 4;
    pool.enqueue(doA,a);
    pool.enqueue([&]{
                a.hello();
            });
    return 0;

}
