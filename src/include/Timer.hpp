#ifndef _TIMER_HH_
#define _TIMER_HH_

#include <ctime>
#include <stdexcept>

/**
 * Defines a re-enterable timer
 */
namespace Util
{
class Timer
{
    public:
        Timer():state_(0),elapsed_(0.0),startCnt_(0){};
        void Start()
        {
            if(state_!=0)
            {
                throw std::runtime_error("Double Start!");
                return;
            }
            state_ = 1;
            clock_ = std::clock();
        }
        void Pause()
        {
            if(state_!=1) return;
            state_ = 0;
            startCnt_++;
            elapsed_ += (std::clock() - clock_)/(double)CLOCKS_PER_SEC;
        };
        size_t GetStartCount(){return this->startCnt_;}
        double GetTime(){return elapsed_;}
    private:
        std::clock_t clock_;
        int state_;
        double elapsed_;
        size_t startCnt_;
        static const int STARTED = 1;
        static const int PAUSED = 0;
};

}
#endif
