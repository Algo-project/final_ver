#ifndef _JOINER_HH_
#define _JOINER_HH_

#include "Relation.h"
#include "parser.h"
#include "Database.h"
#include "Operators.h"
#include "Threadpool.hpp"

class SetInfo;
class Joiner;

/* use joiner to join */
typedef std::vector<Id_t> IdList;
typedef std::set<Id_t> IdSet;
typedef std::unordered_map<Id_t, std::unordered_map<Id_t, unsigned>> SelPosMap;
class Joiner
{
    private:
        Operator *_addScan(IdSet &used_relations, 
                SelectInfo &sinfo, QueryInfo &qinfo);

    public:
        static Operator *_addFilter(Id_t sel_binding, 
                Operator *child, QueryInfo &qinfo);
    public:
        Joiner(Database *db);
        ~Joiner();

        std::string join(QueryInfo &&info);
        std::string join1(QueryInfo &&info);

    private:
        uint64_t **catalog_;
    public:
        std::vector<Relation *> *relations_;
};






#endif
