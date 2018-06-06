#include <iostream>
#include <ctime>
#include <string>
#include <omp.h>
#include <thread>
#ifndef OUTPUT 
    #include "include/Threadpool.hpp"
    #include "include/Database.h"
    #include "include/Relation.h"
    #include "include/Operators.h"
    #include "include/Timer.hpp"
    #include "include/Joiner.h"
#else
    #include "Threadpool.hpp"
    #include "Database.h"
    #include "Relation.h"
    #include "Operators.h"
    #include "Timer.hpp"
    #include "Joiner.h"
#endif

Util::Timer checkSumTimer;
Util::Timer filterTimer;
using ThreadPool = Aposta::FixedThreadPool;
int main(int argc,char *argv[])
{
    ThreadPool *pool = Aposta::FixedThreadPool::GlobalPool;
    std::string line;
    Aposta::Database db;
    while(std::getline(std::cin,line))
    {
        if(line == "Done") break;
        db.AddRelation(line.c_str());
        pool->enqueue([&db]{
                    db.relations.back()->InitCatalog();
                });
    }
    pool->barrier();
    std::cerr<<"Crossed the barrier!"<<std::endl;


/*
    Util::Timer timer;
    timer.Start();
    
    for(int i=0;i<9;i++)
    {
        std::cout<<"-------------------Relation-----------------"<<std::endl;
        std::vector<uint64_t*> a;
        FilterInfo f{{static_cast<unsigned int>(i),0},500000,FilterInfo::Comparison::Greater};
        Operator *scan = new Scan(*db.relations[i], i);
        Operator *filt = new Filter(std::move(scan),f,db.relations[i]->num_cols());
        CheckSum *check = new CheckSum(std::move(filt));
        check->Open();
        check->Next(&a);
        for(size_t j=0;j<a.size();j++)
            std::cout<<((uint64_t)a[j])<<std::endl;
        check->Close();
        delete scan;
        delete check;
    }
    
    pool->barrier();
    timer.Pause();

    std::cout<<"total time is "<<timer.GetTime()<<std::endl;
    std::cout<<"Filter: "<<filterTimer.GetTime()<<" "<<filterTimer.GetStartCount()<<std::endl;
    std::cout<<"CheckSum: "<<checkSumTimer.GetTime()<<" "<<checkSumTimer.GetStartCount()<<std::endl;
    std::cout<<"finished!"<<std::endl;
    */
    QueryInfo q,q2;
    while(std::getline(std::cin,line))
    {
        if(line=="F")
            continue;
        q.parseQuery(line);
        q2.rewriteQuery(q);
        Joiner j(&db);
        std::cout<<j.join(std::move(q2))<<std::endl;
    }
    return 0;
}
