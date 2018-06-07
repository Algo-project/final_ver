#ifndef _OPERATOR_HH_
#define _OPERATOR_HH_

#include <vector>
#include <stdlib.h>
#include <map>
#include <set>
#include <mutex>
#include <thread>

#include "Relation.h"
#include "Threadpool.hpp"
#include "HashTable.hpp"
#include "parser.h"
#include "Timer.hpp"
#include "ConsiseHashTable.hpp"

//#define USE_SYNCHRONIZATION

/*#ifdef USE_SYNCHRONIZATION
#define SYNCHRONIZED(MUTEX) {\
    std::unique_lock<std::mutex> lock(MUTEX);

#define SYN_END(MUTEX) }
#else
#define SYNCHRONIZED(MUTEX) {
#define SYN_END(MUTEX) }
#endif
*/


extern Util::Timer hashJoinTimer;
extern Util::Timer checkSumTimer;
extern Util::Timer filterTimer;
extern Util::Timer partitionTimer;
using Id_t = unsigned;
using ReColPair = std::pair<Id_t,Id_t>;
using ReColList = std::vector<ReColPair>;
using ReColSet = std::set<ReColPair>;
using ReColMap = std::map<ReColPair, unsigned>;

class Operator;
class Scan;
class Projection;
class Filter;
class CheckSum;
class HashPartitioner;
class HashJoin;
class SelfJoin;

/**
 * class Operator
 * Basic operation interface on columns of data
 */

using namespace Aposta;
class Operator
{
    public:
        static constexpr size_t ROWS_AT_A_TIME = 1024u;
    public:
        Operator() = default;
        Operator(ReColList &bind) : bindings(bind){}
        virtual ~Operator() = default;

        /* Standard Interface */
        virtual int Open() = 0;
        virtual int Next(std::vector<uint64_t*> *v) = 0;
        virtual int Close() = 0;

        virtual size_t GetColumnCount() const = 0;
        virtual size_t GetMaxRows() const = 0;    /* the max rows on the next return of Next() */
        ReColList &GetBindings() { return this->bindings;}
        
        virtual Operator *ProjectionPass(ReColMap pbindings) = 0;

    protected:
        ReColList bindings;
};

/**
 * class Scan
 * Scans the given columns of a relation 
 * The Basic wrapper of raw relation array
 * Provided Stream-like access, also cache-friendly
 */
class Scan : public Operator
{
    protected:
        size_t RowsToReturn() const; /* reutrn the count returned by Next */

    public:
        Scan(const Relation &r, unsigned binding);
        [[deprecated]]  /* following constructor maybe wrong */
        Scan(const Relation &r, unsigned binding,
                const uint64_t *sel_cols, uint64_t col_num);
        virtual ~Scan(){delete[] this->selected_cols_;};

        /* Standard Interface */
        virtual int Open() override;
        virtual int Next(std::vector<uint64_t*> *v) override;
        virtual int Close() override;
        
        virtual size_t GetColumnCount() const override {return num_cols_;}
        virtual size_t GetMaxRows() const override {return Operator::ROWS_AT_A_TIME;}

        virtual Operator *ProjectionPass(ReColMap pbindings) override;

        
    protected:
       const Relation &relation_;
       uint64_t **selected_cols_;
       const uint64_t num_cols_;
       uint64_t cur_;   /* current position indicator */

};

/**
 * class Projection
 * Projection operation in relation algebra
 * Use an operator as input, and select serveral
 * columns from the input to be the output
 */
class Projection : public Operator
{
    public:
        Projection(Operator *child, const std::vector<Id_t> &v);
        Projection(Operator *child, std::vector<Id_t> &&v);
        ~Projection() = default;

        /* Standard Interface */
        virtual int Open() override;
        virtual int Next(std::vector<uint64_t*> *v) override;
        virtual int Close() override;

        size_t GetColumnCount() const override {return this->selected_cols_.size();}
        size_t GetMaxRows() const override {return child_->GetMaxRows();}
        virtual Operator *ProjectionPass(ReColMap pbindings) override;

    protected:
        Operator *child_;

    private:
        std::vector<Id_t> selected_cols_;
};


/**
 * class Filter
 * Give an instruction, filter out some rows from input
 */
class Filter : public Operator
{
    private:
        size_t _doFilter(size_t wr_offset,size_t startoff);
    public:
        Filter(Operator *child, const FilterInfo &f, unsigned num_cols);
        virtual ~Filter() = default;

        /* Standard Interface */
        virtual int Open() override;
        virtual int Next(std::vector<uint64_t*> *v) override;
        virtual int Close() override;
        
        virtual size_t GetColumnCount() const override {return num_cols_;}
        virtual size_t GetMaxRows() const override {return Operator::ROWS_AT_A_TIME;}
        virtual Operator *ProjectionPass(ReColMap pbindings) override;

    protected:
        Operator *child_;
    private:
        const FilterInfo filter_;

        unsigned offset_;
        unsigned end_;

        uint64_t **dataOut_;
        std::vector<uint64_t *> dataIn_;

        unsigned num_cols_;
        
};



/**
 * Class CheckSum
 * Accumulate the checksum from input operator
 */

class CheckSum: public Operator
{
    private:
        static constexpr int POOL_SIZE = 1;
    public:
        CheckSum(Operator *child);
        ~CheckSum(){}
        
        /* Standard Interface */
        virtual int Open() override;
        virtual int Next(std::vector<uint64_t*> *v) override;
        virtual int Close() override;
        
        virtual size_t GetColumnCount() const override {return child_->GetColumnCount();}
        virtual size_t GetMaxRows() const override {return 1;}

        virtual Operator *ProjectionPass(ReColMap pbindings) override;

    private:
        Operator *child_;
        uint64_t *buffer;
    private:
//        static Aposta::FixedThreadPool pool_;
};



/**
 * Partitioner class
 * consumes all the tuples provided by its input (operator or another partitioner)
 * produces partitions of its input data
 * each partition is stored in sequential memory
 * partitions share the same radix bits (subset of the bits retrieved with bit wise operator)
 * then its partitions can be accessed by a join or other operations
*/

class HashPartitioner {
    public :
        /**
         * Constructor:
         * create partitioner that creates partitions with these parametes
         * 1 << log_parts partitions
         * colNum: input's number of columns
         * pfield: field used for partitioning
         * histogram: information about partition sizes (NULL if not available)
         * first_bit: least significant radix bit
         */
        HashPartitioner (uint32_t log_parts, uint32_t colNum ,uint32_t pfield, size_t* histogram, uint32_t first_bit);

        ~HashPartitioner ();

        /**
         * fetch arrays of partition i
         */
        uint64_t** GetPartition (uint32_t i);
        size_t GetHist (uint32_t i); /* get size of part i */
        void Generate (Operator* input);/* generate from input*/
        /**
         * generate partition from another partitioner
         * don't inspect overlapping bits
         * the idea is that it splits the existing partitions further
         * used to avoid stressing TLB
         */
        void Generate (HashPartitioner& h_first);
        void Verify ();
    private :
        uint32_t pfield;    /* the field num used for patition */
        uint32_t log_parts; /* equals to log(parts) */
        uint32_t columns;   /* total column number */

        uint64_t*** partitioned_data;
        size_t* histogram;  /* histogram[i] == part i's size */

        uint64_t** cache;   /* used for AVX2 */
        size_t*  offset;    /* used for AVX2 */
        size_t*  cache_offset;  /* used for AVX2 */

        size_t* cur_size;   

        uint32_t first_bit; /* first bit to do partition */
};




class HashJoin : public Operator
{
    private:
    public:
        HashJoin(Operator *left, unsigned cleft, unsigned jleft,
                Operator *right, unsigned cright, unsigned jright,
                int htSize);
        ~HashJoin();
        
        void Configure (uint32_t log_parts1, uint32_t log_parts2, uint32_t first_bit, 
                size_t* histR1, size_t* histR2, 
                size_t* histS1, size_t* histS2);

        /* Standard Interface */
        virtual int Open() override;
        virtual int Next(std::vector<uint64_t*> *v) override;
        virtual int Close() override;
        
        virtual size_t GetColumnCount() const override {return cleft+cright;}
        virtual size_t GetMaxRows() const override {return ROWS_AT_A_TIME;}

        virtual Operator *ProjectionPass(ReColMap pbindings) override;
    private:
        /* Hash Partitioners */
        /* Use 'two pass' partition */
        HashPartitioner *hR1;
        HashPartitioner *hS1;
        HashPartitioner *hR2;
        HashPartitioner *hS2;
        bool partitioner_setup; /* partitioner' state */

        /* hash table used to store one partition each time */
        HashTable<int> *ht;
        size_t htSize;

        /* two operators as input */
        Operator *left;
        Operator *right;

        /* parts = 2^(log_parts1 + log_parts2) */
        uint32_t log_parts1;
        uint32_t log_parts2;
        uint32_t first_bit;

        /* partition size */
        size_t *histogramR1;
        size_t *histogramR2;
        size_t *histogramS1;
        size_t *histogramS2;

        /* cXXXX: column count of XXXX input */
        /* jXXXX: which column in XXXX is to join*/
        unsigned cleft;
        unsigned jleft;
        unsigned cright;
        unsigned jright;

        uint64_t **it;

        /* Used for re-enterablity */
        bool init;
        bool rebuild;
        int64_t last_part;
        int64_t last_probe;
        uint32_t global_cnt;

        std::vector<uint64_t> dataIn_;  /* idx buffer */
        size_t offset_; /* idx offset */
        size_t end_;    /* idx limit */
};


/*
SelfJoin closes a cycle of joins
essentially evaluates equality among two columns of its input
*/
class SelfJoin : public Operator {
    private:
    uint32_t col1;
    uint32_t col2;

    uint32_t colNum;
    uint32_t pred;

    uint64_t** dataOut;

    Operator* child;
    
    std::vector<uint64_t*> dataIn;

    uint32_t offset;
    size_t   end;
    size_t   cnt;



    public :
    /*
    c1 : column number for operand1
    c2 : column number for operand2
    cols: number of columns
    */
    SelfJoin (Operator* child, uint32_t c1, uint32_t c2, uint32_t cols);

    void Print ();

    int Open () override;

    int Next (std::vector<uint64_t*>* output) override;

    int Close () override;

    Operator *ProjectionPass(ReColMap) override;



    size_t GetColumnCount() const override { return this->colNum; }
    size_t GetMaxRows() const override { return Operator::ROWS_AT_A_TIME; }
};
#endif
