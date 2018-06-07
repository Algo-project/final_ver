#include "Threadpool.hpp"

Aposta::FixedThreadPool *Aposta::FixedThreadPool::GlobalPool
    = new Aposta::FixedThreadPool(std::thread::hardware_concurrency());
