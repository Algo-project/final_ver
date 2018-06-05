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
        void barrier();
        
    private:
        std::vector<std::thread> workers;
        std::queue<_Func_type> tasks;

        /* the mutex for controlling the task queue */
        std::mutex task_mutex;
        std::condition_variable cond_notempty_;


        size_t threads_;
        std::atomic<size_t> idle_;

        std::mutex barrier_mutex;
        std::condition_variable cond_empty_;

        bool stop;
};

inline 
FixedThreadPool::FixedThreadPool(size_t size)
    : threads_(size), idle_(size), stop(false)
{
    for(size_t i=0;i<size;i++)
    {
        workers.emplace_back(
                    [this]
                    {
                        while(true)
                        {
                            _Func_type task;    //task to be executed

                            /* the thread waits until 'cond_notempty_' is released by other
                             * threads, OR the pool is stopped, OR the pool has 
                             * some unexecuted task
                             */
                            {
                                std::unique_lock<std::mutex> lock(this->task_mutex);

                                /* folling sentense equals to:
                                 * while( !this->stop && this->tasks.empty() )
                                 *      this->cond_notempty_.wait(lock)
                                 */
                                this->cond_notempty_.wait(lock,
                                        [this]{return this->stop || !this->tasks.empty();}
                                        );
                                if(this->stop && this->tasks.empty())
                                    return;

                                /* get the task */
                                --idle_;
                                task = std::move(this->tasks.front());
                                this->tasks.pop();
                            }

                            task();             //execute the task
                            ++idle_;

                            {
                                std::unique_lock<std::mutex> lock(this->barrier_mutex);
                                this->cond_empty_.notify_all();
                            }
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
    cond_notempty_.notify_one();
    return res;
}


inline void
FixedThreadPool::barrier()
{
    {
        std::unique_lock<std::mutex> lock(this->barrier_mutex);
        this->cond_empty_.wait(lock,
                [this]{return this->idle_.load() == this->threads_;}
                );
    }
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
    this->cond_notempty_.notify_all();

    /* wait for close */
    for(auto & worker : this->workers)
        worker.join();
}




}
