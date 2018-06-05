// Relation class declaration
#pragma once

#include <cstdint>
#include <sys/types.h>
#include <vector>

namespace Aposta {

/** Class modelling a relation.
 *
 *  It is initialised with a relation file path.
 */
class Relation {
  public:
    Relation(const char *fileName);
    Relation(uint64_t **data, int columns, int rows);
    ~Relation();

    /* return the row at a given index */
    int RowAt(unsigned, uint64_t *) const;

    // accessors
    uint64_t relation_id() const { return relation_id_; }
    const std::vector<uint64_t *> &cols() const { return this->cols_; }
    uint64_t num_rows() const { return this->num_rows_; }
    uint64_t num_cols() const { return this->num_cols_; }
    //void attribute_stats(std::vector<uint64_t>* attr_vector) const;
    char* get_map_addr()const{return this->map_addr_;}
    

    void InitCatalog();
    uint64_t *GetCatalog(){return this->catalog_;}

  private:
    const uint64_t relation_id_ = 0;

    char *map_addr_;
    off_t file_size_;

    uint64_t num_rows_;
    uint64_t num_cols_;

    std::vector<uint64_t *> cols_;
    /* COUNT: 0
     * MIN: 1
     * MAX: 2
     */
    uint64_t *catalog_;

    static uint64_t next_id_;
};

} 
