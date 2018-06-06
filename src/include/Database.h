#ifndef _DATABASE_HH_
#define _DATABASE_HH_

#include <vector>
#include "Relation.h"

/**
 * class Database
 * Store all relations
 */
namespace Aposta
{


class Database
{
    public:
        std::vector<Relation*> relations;

    public:
        void AddRelation(const char *filename)
        {
            relations.emplace_back(new Relation(filename));
        }

        Relation *GetRelation(unsigned relationID)
        {
            return relations[relationID];
        }

        size_t Size() const
        {
            return relations.size();
        }

        Database() = default;
        ~Database() { for(auto r:relations) delete r;}
};

}

#endif
