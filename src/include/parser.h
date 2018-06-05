#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

typedef unsigned RelationId;

/**
 * struct SelectInfo
 * used to store a 'SQL select' like '3.2' or '0.1'
 *
 * @Fields  relId: Store the real relation id which the selection
 *                 refers to 
 *          binding: Store the FIRST number of the selection pair
 *          colId:  Store the SECOND number of the selection pair
 *
 * @Methods 
 *
 */
struct SelectInfo {

    RelationId relId;
    unsigned binding;
    unsigned colId;

    SelectInfo(RelationId relId,unsigned b,unsigned colId) : relId(relId), binding(b), colId(colId) {};
    /// The constructor if relation id does not matter
    SelectInfo(unsigned b,unsigned colId) : SelectInfo(-1,b,colId) {};


    inline bool operator==(const SelectInfo& o) const
    {
        return o.relId == relId && o.binding == binding && o.colId == colId;
    }
    inline bool operator<(const SelectInfo& o) const 
    {
        return binding<o.binding||colId<o.colId;
    }

    std::string dumpText();
    std::string dumpSQL(bool addSUM=false);

    static const char delimiter=' ';
    /// The delimiter used in SQL
    //TODO remove this shit
    static const char *delimiterSQL;

};


/**
 * Struct FilterInfo
 * Store the 'Filter Predicates'
 *
 * @Fields:
 *
 * @Methods:
 */
struct FilterInfo {
    enum Comparison : char { Less='<', Greater='>', Equal='=' };
    SelectInfo filterColumn;
    uint64_t constant;
    Comparison comparison;

    /* CONSTRUCTOR */
    FilterInfo(SelectInfo filterColumn,uint64_t constant,Comparison comparison) : filterColumn(filterColumn), constant(constant), comparison(comparison) {};

    std::string dumpText();
    std::string dumpSQL();

    /// The delimiter used in our text format
    static const char delimiter='&';
    /// The delimiter used in SQL
    static const char *delimiterSQL;
};

/* Useful in other positions */
static const std::vector<FilterInfo::Comparison> comparisonTypes { FilterInfo::Comparison::Less, FilterInfo::Comparison::Greater, FilterInfo::Comparison::Equal};


/**
 * Struct PredicateInfo
 * Store the 'Join Predicates'
 *
 * @Fields   left & right: two selections.
 * @Methods
 */
struct PredicateInfo {
    SelectInfo left;
    SelectInfo right;

    PredicateInfo(SelectInfo left, SelectInfo right) : left(left), right(right){};

    /// Dump text format
    std::string dumpText();
    /// Dump SQL
    std::string dumpSQL();

    /// The delimiter used in our text format
    static const char delimiter='&';
    /// The delimiter used in SQL
    static const char *delimiterSQL;
};


/**
 * Class QueryInfo
 * The datastructure to store the whole 'Query'
 *
 * @Fields
 *      relationIds: the vector to store all relation ids used.
 *      predicates: the vector to store all join predicates
 *      filters: the vector to store all filter predicates
 *      selections: the vector to store all 'SUM' selections
 *      results: reserved, maybe no use.
 *
 * @Methods
 *      clear(): reset query info
 */
class QueryInfo {
    public:
        std::vector<RelationId> relationIds;
        std::vector<PredicateInfo> predicates;
        std::vector<FilterInfo> filters;
        std::vector<SelectInfo> selections;
        std::vector<uint64_t> results;

        void clear();

    private:
        /// Parse a single predicate
        void parsePredicate(std::string& rawPredicate);
        /// Resolve bindings of relation ids
        void resolveRelationIds();

    public:
        void rewriteQuery(QueryInfo& source) ;
        /// Parse relation ids <r1> <r2> ...
        void parseRelationIds(std::string& rawRelations);
        /// Parse predicates r1.a=r2.b&r1.b=r3.c...
        void parsePredicates(std::string& rawPredicates);
        /// Parse selections r1.a r1.b r3.c...
        void parseSelections(std::string& rawSelections);
        /// Parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
        void parseQuery(std::string& rawQuery);

        /// Dump text format
        std::string dumpText();
        /// Dump SQL
        std::string dumpSQL();

        /// The empty constructor
        QueryInfo() {}
        /// The constructor that parses a query
        QueryInfo(std::string rawQuery);
};
