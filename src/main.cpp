#include <iostream>
#include <string>
#include <thread>
#ifndef OUTPUT
    #include "include/Threadpool.hpp"
    #include "include/Database.h"
    #include "include/Relation.h"
#else
    #include "Threadpool.hpp"
    #include "Database.h"
    #include "Relation.h"
#endif

using ThreadPool = Aposta::FixedThreadPool;
int main(int argc,char *argv[])
{
    ThreadPool pool(std::thread::hardware_concurrency());
    std::string line;
    Aposta::Database db;
    while(std::getline(std::cin,line))
    {
        if(line == "Done") break;
        db.AddRelation(line.c_str());
        pool.enqueue([&db]{
                    db.relations.back()->InitCatalog();
                });
    }
    pool.barrier();
    return 0;
}
