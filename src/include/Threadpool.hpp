#pragma once

#include <thread>
#include <iostream>
#include <atomic>
#include <queue>
#include <vector>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <functional>
#include <future>
#include <stdexcept>

using _Func_type = std::function<void()>;

namespace Aposta
{


class FixedThreadPool
{
	public:
		FixedThreadPool(size_t size);
		~FixedThreadPool();

		template<class F, class ...Arg>
		auto enqueue(F&& func, Arg&&... args)
			-> std::future<typename std::result_of<F(Arg...)>::type>;
		
	private:
		std::vector<std::thread> workers;
		std::queue<_Func_type> tasks;

		std::mutex task_mutex;
		std::condition_variable cond;
		//std::atomic<bool> stop;
		bool stop;
};

inline 
FixedThreadPool::FixedThreadPool(size_t size):stop(false)
{
	for(size_t i=0;i<size;i++)
	{
		workers.emplace_back(
					[this]
					{
						while(true)
						{
							_Func_type task;	//task to be executed

							/* the thread waits until 'cond' is released by other
							 * threads, OR the pool is stopped, OR the pool has 
							 * some unexecuted task
							 */
							{
								std::unique_lock<std::mutex> lock(this->task_mutex);
								this->cond.wait(lock,
										[this]{return this->stop || !this->tasks.empty();}
										);
								if(this->stop && this->tasks.empty())
									return;

								/* get the task */
								task = std::move(this->tasks.front());
								this->tasks.pop();
							}

							task();				//execute the task
						}
					}
				);
	}
}



template<class F, class ...Args>
auto FixedThreadPool::enqueue(F&& func, Args&&... args)
	-> std::future<typename std::result_of<F(Args...)>::type>
{
	using _Ret_type = typename std::result_of<F(Args...)>::type;

	/* make a shared_ptr points to the packaged_task */
	auto task = std::make_shared< std::packaged_task<_Ret_type()> >(
			std::bind(std::forward<F>(func), std::forward<Args>(args)...)
		);

	/* return the future object to query the execution info */
	std::future<_Ret_type> res = task->get_future();
	{
		std::unique_lock<std::mutex> lock(task_mutex);
		if(stop)
			throw std::runtime_error("enqueue() called on a STOPPED threadpool");

		/* Call std::queue::emlpace
		 * Pass the arguments for the constructors
		 * Type of 'tasks' is '_Func_type', which need a function 
		 * to construct
		 *
		 * Thus, the lambda expression is that function to build 
		 * the object of task
		 * The emplaced task object is to execute the operation 
		 * passed by shared_ptr 'task'
		 */
		tasks.emplace([task](){(*task)();});
	}

	/* notify the workers */
	cond.notify_one();
	return res;
}

inline 
FixedThreadPool::~FixedThreadPool()
{
	{
		/* get the lock and stop the execution */
		std::unique_lock<std::mutex> lock(this->task_mutex);
		this->stop = true;
	}

	/* activate all workers */
	this->cond.notify_all();

	/* wait for close */
	for(auto & worker : this->workers)
		worker.join();
}




}
