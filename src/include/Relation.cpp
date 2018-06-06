// Relation class implementation
#include "Relation.h"

// low-level file manipulation
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <iostream>
#include <unordered_set>

#define SAMPLE 1300

using namespace std;


namespace Aposta {

uint64_t Relation::next_id_ = 0;

Relation::Relation(const char *filename) : relation_id_(Relation::next_id_++) {
    /* Open File */
    int fd = open(filename, O_RDONLY);
    /* get and save file size */
    struct stat sb;
    fstat(fd, &sb);
    this->file_size_ = sb.st_size;

    /* map file into memory */
    this->map_addr_ = static_cast<char *>(
            mmap(nullptr, this->file_size_,
                PROT_READ,   // map read-only
                MAP_PRIVATE, // private modifications (with copy-on-write)
                fd, 0u));
    uint64_t *ptr = reinterpret_cast<uint64_t *>(this->map_addr_);

    /* get number of rows in file */
    this->num_rows_ = *ptr;
    ++ptr;
    /* get number of columns in file */
    this->num_cols_ = *ptr;
    ++ptr;

    /* create column pointers (relation files use column-store) */
    this->cols_.reserve(this->num_cols());
    for (unsigned i = 0; i != this->num_cols(); ++i) {
        this->cols_.push_back(ptr);
        ptr += this->num_rows();
    }

    catalog_ = new uint64_t[this->num_cols()*3];
}

Relation::Relation(uint64_t **data, int columns, int rows)
    : relation_id_(Relation::next_id_++) {
        this->file_size_ = 0;
        this->map_addr_ = NULL;

        this->num_rows_ = rows;
        this->num_cols_ = columns;

        this->cols_.reserve(this->num_cols());
        for (unsigned i = 0; i != this->num_cols(); ++i) {
            this->cols_.push_back(data[i]);
        }
    }

Relation::~Relation() {
    delete[] catalog_;
    // munmap(this->map_addr_, this->file_size_);
}

int Relation::RowAt(unsigned index, uint64_t *buf) const {
    // apply the lambda function (last argument)
    // to each element in column pointers
    // and store the result in buf
    std::transform(this->cols_.begin(), this->cols_.end(), buf,
            [&](uint64_t *col) -> uint64_t { return col[index]; });
    return 0;
}

void Relation::InitCatalog(){


    uint64_t *ptr = reinterpret_cast<uint64_t *>(this->map_addr_);

    //Go to the first column
    std::unordered_set<uint64_t> dist_values(1<<12);
    uint64_t sampling_factor = *ptr / SAMPLE;
    //std::cout<<"sampling_factor:"<<sampling_factor<<std::endl;
    ++ptr;
    ++ptr;
    uint64_t max;
    uint64_t min;
    uint64_t count;

    for(unsigned i=0; i< this->num_cols(); i++){
        max = 0;
        min = -1;
        dist_values.clear();
        count =0;
        for(unsigned j=0; j<this->num_rows(); j++){
            //Insert in the set the values of the column
            //By default the set insert doesnt add duplicates
            if(j%sampling_factor == 0){
                dist_values.insert(*ptr);
            }
            if(*ptr < min)
                min = *ptr;
            if(*ptr > max)
                max = *ptr;
            ++ptr;
        }
        //The number of the distinct values is the size of the set
        this->catalog_[i*3] = dist_values.size();
        this->catalog_[i*3+1] = min;
        this->catalog_[i*3+2] = max;
    }   
}


}

