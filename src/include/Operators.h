#ifndef _OPERATOR_HH_
#define _OPERATOR_HH_

#include <vector>
#include <stdlib.h>
#include <map>
#include <set>

using Id_t = unsigned;
using ReColPair = std::pair<Id_t,Id_t>;
using ReColList = std::vector<ReColPair>;
using ReColSet = std::set<ReColPair>;
using ReColMap = std::map<ReColPair, int>;

class Operator;
class Scan;
class Projection;
class Filter;
class HashJoin;
class SelfHashJoin;
class CheckSum;

/**
 * class Operator
 * Basic operation interface on columns of data
 */
class Operator
{
    public:
        static constexpr size_t ROWS_AT_A_TIME = 1024u;
    public:
        Operator() = default;

        /* Standard Interface */
        virtual void Open() = 0;
        virtual uint32_t Next(std::vector<uint64_t*> *v) = 0;
        virtual void Close() = 0;

        virtual size_t GetColumnCount() = 0;
        virtual size_t GetMaxRows() = 0;    /* the max rows on the next return of Next() */
        ReColList &GetBindings() { return this->bindings;}
        
        virtual Operator *ProjectionPass(ReColMap *pbindings);

    protected:
        ReColList bindings;
};

/**
 * class Scan
 * Scans the given columns of a relation 
 */
class Scan : public Operator
{
    public:

    private:

};


#endif
