#include <iostream>
#include <string>

#ifndef OUTPUT
#include "include/Threadpool.h"
#else
#include "Threadpool.h"
#endif

#define MAXN 10000000000
size_t consume_some_cpu(size_t N)
{
	size_t sum = 0;
	for(size_t i=0;i<N;i++,sum++);
	return sum;
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
	std::vector<std::future<size_t>> results;

	for(int i=0;i<nn;i++)
	{
		results.emplace_back(
			pool.enqueue(consume_some_cpu,MAXN/nn)
		);
	}

	for(auto && result : results)
		std::cout<< result.get() << " ";
	std::cout<<std::endl;
	return 0;

}
