#include "Operators.h"
#include <algorithm>
#include <stdlib.h>
#include <numeric>
#include <cmath>

#define LOG_BATCH_SIZE 8
#define VECTOR_SIZE Operator::ROWS_AT_A_TIME
/* Class Scan */
size_t Scan::RowsToReturn() const
{
    auto n = this->relation_.num_rows() - this->cur_;
    n = (n>0?n:0);
    return n>ROWS_AT_A_TIME? ROWS_AT_A_TIME:n;
}

Scan::Scan(const Relation &r, unsigned binding)
    : relation_(r), num_cols_(r.num_cols()),cur_(0)
{
    this->selected_cols_ = new uint64_t*[r.num_cols()];
    bindings.clear();
    for(unsigned i=0;i<r.num_cols();++i)
    {
        this->selected_cols_[i] = r.cols()[i];  /* may use std::copy here*/
        bindings.emplace_back(binding,i);
    }
}

Scan::Scan(const Relation &r, unsigned binding,
        const uint64_t *sel_cols, uint64_t col_num)
    : relation_(r), num_cols_(col_num), cur_(0)
{
    this->selected_cols_ = new uint64_t*[col_num];
    bindings.clear();
    for(uint64_t i=0;i<col_num;i++)
        bindings.emplace_back(binding, sel_cols[i]);

    /* init selected_cols */
    std::transform(sel_cols, sel_cols+col_num, this->selected_cols_,
            [&](uint64_t i){return r.cols()[i];});
}

int Scan::Open()
{
    this->cur_ = 0;
    return 0;
}

int Scan::Close()
{
    return 0;
}

int Scan::Next(std::vector<uint64_t *> *output)
{
    int ret = 0;
    //SYNCHRONIZED(this->mutex){
        ret = RowsToReturn();
        if(!ret) goto L1;  /* come to the end */

        output->clear();
        output->reserve(this->num_cols_);

        std::transform(selected_cols_, selected_cols_+num_cols_,
                std::back_inserter(*output),
                [&](uint64_t *col){return col + this->cur_;}
                );

        /* update current position */
        this->cur_ += ret;
L1:;
    //}SYN_END(this->mutex);
    return ret;
}


/* Class Projection */
Projection::Projection(Operator *child, const std::vector<Id_t> &v)
    :child_(child), selected_cols_(v)
{
    bindings.clear();
    for(unsigned i = 0;i<selected_cols_.size();i++)
        bindings.emplace_back((child->GetBindings())[v[i]]);
}

Projection::Projection(Operator *child, std::vector<Id_t> &&v)
    :child_(child), selected_cols_(std::move(v))
{
    bindings.clear();
    for(unsigned i = 0;i<selected_cols_.size();i++)
        bindings.emplace_back((child->GetBindings())[v[i]]);
}

int Projection::Open()
{
    return child_->Open();
}

int Projection::Close()
{
    return child_->Close();
}

int Projection::Next(std::vector<uint64_t *> *output)
{
    int ret;
    std::vector<uint64_t *> buf;
    ret = child_->Next(&buf);
    if(ret == 0) return 0;

    output->clear();
    output->reserve(this->selected_cols_.size());

    std::transform(selected_cols_.begin(), selected_cols_.end(),
            std::back_inserter(*output),
            [&](uint64_t i){return buf[i];});
    return ret;
}


/* Class Filter */

inline static bool __Judge(uint64_t data,FilterInfo::Comparison cmp,
        uint64_t constant)
{
    switch(cmp)
    {
        case FilterInfo::Comparison::Greater:
            return data>constant;
        case FilterInfo::Comparison::Equal:
            return data==constant;
        case FilterInfo::Comparison::Less:
            return data<constant;
        default:
            return false;
    }
}

inline size_t Filter::_doFilter(size_t wr_offset,size_t startoff)
{
    /*TODO: parallel this */
    if(startoff >= this->end_) return wr_offset;
    auto pred = filter_.filterColumn.colId;
    auto val = filter_.constant;
    auto cmp = filter_.comparison;
    auto data = this->dataIn_[pred];
    for(size_t i=startoff;i<this->end_;++i)
    {
        if(__Judge(data[i],cmp,val))
        {
            for(unsigned j=0;j<this->num_cols_;++j)
                dataOut_[j][wr_offset] = dataIn_[j][i];
            ++wr_offset;
            if(wr_offset == ROWS_AT_A_TIME)
            {
                this->offset_ = i+1;
                return wr_offset;
            }
        }
    }
    return wr_offset;
}

Filter::Filter(Operator *child, const FilterInfo &f, unsigned num_cols)
    :child_(child), filter_(f), num_cols_(num_cols),
    Operator(child->GetBindings())
{
    this->offset_ = 0;
    this->end_ = 0;
    this->dataOut_ = NULL;
}

int Filter::Open()
{
    this->child_->Open();
    this->offset_ = 0;
    this->end_ = 0;
    this->dataOut_ = new uint64_t *[this->num_cols_];
    for(unsigned i = 0;i<this->num_cols_;i++)
        dataOut_[i] = (uint64_t *)calloc(ROWS_AT_A_TIME,sizeof(uint64_t));
    return 0;
}

int Filter::Close()
{
    for(unsigned i=0;i<this->num_cols_;i++)
        delete[] dataOut_[i];
    delete[] dataOut_;
    child_->Close();
    return 0;
}

int Filter::Next(std::vector<uint64_t *> *output)
{
    size_t wr_offset = 0;
    //SYNCHRONIZED(this->mutex){
    
        output->clear();
        output->reserve(this->num_cols_);
        for(unsigned i = 0;i<this->num_cols_;i++)
            output->push_back(dataOut_[i]);

        wr_offset = _doFilter(wr_offset,this->offset_);
        while( (wr_offset < ROWS_AT_A_TIME)
                        && (this->end_ = child_->Next(&dataIn_)) )
        {
            filterTimer.Start();
            wr_offset = _doFilter(wr_offset,0);

    //}SYN_END(this->mutex);
            filterTimer.Pause();
        }
    return wr_offset;
}


/* Class CheckSum */


CheckSum::CheckSum(Operator *child)
    : child_(child), buffer(NULL)
{
}

int CheckSum::Open()
{
    child_->Open();
    buffer = (uint64_t*)calloc(this->GetColumnCount(),sizeof(uint64_t));
    return 0;
}

int CheckSum::Close()
{
    child_->Close();
    return 0;
}

int CheckSum::Next(std::vector<uint64_t *> *output)
{
    /* don't use multithread */
    output->clear();
    output->reserve(this->GetColumnCount());
    std::vector<uint64_t *>buf;
    unsigned n;
    while((n = this->child_->Next(&buf)))
    {
        checkSumTimer.Start();
        auto size = buf.size();
        for(unsigned i = 0;i!=size;++i)
            this->buffer[i] = std::accumulate(buf[i],buf[i]+n,
                    this->buffer[i]);
        checkSumTimer.Pause();
    }
    for(unsigned i=0;i<this->GetColumnCount();i++)
        output->push_back((uint64_t*)(buffer[i]));
    return 1;
}


/* Class HashPartitioner */

/* ---------------------------------------------------------------------------*/

#define HASH_PARTITIONER_BUFFER_INIT (1 << 14)


HashPartitioner::HashPartitioner (uint32_t log_parts, uint32_t colNum ,uint32_t pfield, size_t* histogram, uint32_t first_bit) : 
                            pfield(pfield), log_parts(log_parts), columns(colNum), first_bit(first_bit) {
    const uint32_t parts = (1 << log_parts);

    cache = new uint64_t* [columns];

    partitioned_data = new uint64_t** [parts];
    
    if (histogram != NULL)
        this->histogram = new size_t [parts];
    else
        this->histogram = NULL;

    //offset = new uint64_t** [parts];
    offset = new size_t [parts];
    cache_offset = new size_t [parts];

    if (histogram == NULL)
        cur_size = new size_t [parts];

    for (uint32_t i = 0; i < parts; i++) {
        cur_size[i] = HASH_PARTITIONER_BUFFER_INIT;

        //offset[i] = 0;
        if (histogram != NULL)
            (this->histogram)[i] = histogram[i];
        partitioned_data[i] = new uint64_t* [columns];

        for (uint32_t j = 0; j < columns; j++)
            if (this->histogram != NULL){
                partitioned_data[i][j] = (uint64_t*) malloc(((histogram[i] + 7)/8)*8 * sizeof(uint64_t));
               // partitioned_data[i][j] = (uint64_t*) aligned_alloc(256, ((histogram[i] + 7)/8)*8 * sizeof(uint64_t));
            }
            else{
                partitioned_data[i][j] = (uint64_t*) malloc(HASH_PARTITIONER_BUFFER_INIT * sizeof(uint64_t));
                //partitioned_data[i][j] = (uint64_t*) aligned_alloc(256, HASH_PARTITIONER_BUFFER_INIT * sizeof(uint64_t));
            }
    }

    for (uint32_t j = 0; j < columns; j++)
        cache[j] = (uint64_t*) aligned_alloc(256, (1 << LOG_BATCH_SIZE)*parts*sizeof(uint64_t));
}

HashPartitioner::~HashPartitioner () {
    const uint32_t parts = (1 << log_parts);

    for (uint32_t i = 0; i < parts; i++) {
        for (uint32_t j = 0; j < columns; j++)
            free(partitioned_data[i][j]);

        delete[] partitioned_data[i];
    }

    for (uint32_t j = 0; j < columns; j++)
        free(cache[j]);

    delete[] cache;

    delete[] offset;
    delete[] cache_offset;
        
    delete[] partitioned_data;

    if (histogram != NULL)
        delete[] histogram;
    else
        delete[] cur_size;
}

uint64_t** HashPartitioner::GetPartition (uint32_t i) {
    return partitioned_data[i];
}

void HashPartitioner::Generate (Operator* input) {
    const uint32_t parts = (1 << log_parts);
    const uint32_t parts_mask = parts - 1;

    int32_t end;

    /*
    prepare cache
    will be used in threaded version to avoid saturating bandwidth
    */

    for (uint32_t i = 0; i < parts; i++) {
        cache_offset[i] = i << LOG_BATCH_SIZE;
        offset[i] = 0;
        //for (int j = 0; j < columns; j++)
        //    offset[i][j] = partitioned_data[i][j];
    }

    std::vector<uint64_t*> vec;

    while ((end = input->Next(&vec))) {
        /*
        assign each element of the vector to its partition
        */
        for (int i = 0; i < end; i++) {
            uint64_t key = vec[pfield][i];
            /*align to first radix bit, then cut off non radix bits*/
            uint32_t partition = (key >> first_bit) & parts_mask; 

                /*
                threaded version black magic
                */
                /*
                uint32_t coffset = (cache_offset[partition])++;
                cache[coffset] = key;

                if ((coffset & ((1 << LOG_BATCH_SIZE) - 1)) == ((1 << LOG_BATCH_SIZE) - 1)) {
                    for (int k = 0; k < (1 << (LOG_BATCH_SIZE - 3)); k++) {
                        __m256i flush_data = *((__m256i*) &cache[(partition << LOG_BATCH_SIZE) + k*8]);
                        _mm256_stream_si256 ((__m256i*) &partitioned_data[partition][offset[partition] + k*8], flush_data);
                    }

                    offset[partition] += (1 << LOG_BATCH_SIZE);
                    cache_offset[partition] = (partition << LOG_BATCH_SIZE);
                }*/

            /*compute write offset*/
            size_t off = (offset[partition])++;
     
            /*resize if needed (no histograms only)*/
            if (histogram == NULL && off == cur_size[partition]) {
                for (uint32_t j = 0; j < columns; j++) {
                    uint64_t* tmp = partitioned_data[partition][j];
                    partitioned_data[partition][j] = (uint64_t*) realloc(tmp, 2 * cur_size[partition] * sizeof(uint64_t));
                    //partitioned_data[partition][j] = (uint64_t*) aligned_alloc(256, 2 * cur_size[partition] * sizeof(uint64_t));
                    //memcpy (partitioned_data[partition][j], tmp, cur_size[partition] * sizeof(uint64_t));
                }

                cur_size[partition] *= 2;

            }

            /*copy all columns to partition*/
            for (uint32_t j = 0; j < columns; j++) {
                //*(offset[partition][j]) = vec[j][i];
                //(offset[partition][j])++;
                
                partitioned_data[partition][j][off] = vec[j][i];
            }
        }
    }

    /*
    more threaded magic
    */

        /*for (int partition = 0; partition < parts; partition++) {
            for (int k = 0; k < (1 << (LOG_BATCH_SIZE - 3)); k++) {
                if (8*k < cache_offset[partition] - (partition << LOG_BATCH_SIZE)) {
                    __m256i flush_data = *((__m256i*) &cache[(partition << LOG_BATCH_SIZE) + k*8]);
                    _mm256_stream_si256 ((__m256i*) &partitioned_data[partition][offset[partition] + k*8], flush_data);
                }
                offset[partition] += cache_offset[partition] - (partition << LOG_BATCH_SIZE);
            }
        }*/
}

/*same as the other ones but used existing partitions as input*/
void HashPartitioner::Generate (HashPartitioner& h_first) {
    const uint32_t parts = (1 << (log_parts - h_first.log_parts));
    const uint32_t parts_mask = parts - 1;

        //uint32_t fb = first_bit;
        //first_bit += h_first.log_parts;

    uint64_t** vec = new uint64_t* [columns];

    for (unsigned p = 0; p < (unsigned)(1 << h_first.log_parts); p++) {
        for (uint32_t i = 0; i < parts; i++) {
            cache_offset[i] = i << LOG_BATCH_SIZE;
            offset[p * parts + i] = 0;
            //for (int j = 0; j < columns; j++)
            //    offset[p * parts + i][j] = partitioned_data[p * parts + i][j];
        }

        bool repeat;
        size_t start = 0;

        while ((repeat = (start < h_first.offset[p]))) {
            size_t end = (h_first.offset[p] - start < VECTOR_SIZE) ? h_first.offset[p] - start : VECTOR_SIZE;

            for (uint32_t j = 0; j < columns; j++)
                vec[j] = h_first.partitioned_data[p][j] + start; 

            for (size_t i = 0; i < end; i++) {
                int32_t key = vec[pfield][i];
                uint32_t partition = p * parts + ((key >> first_bit) & parts_mask); 
                    
                    /*uint32_t coffset = (cache_offset[partition])++;
                    cache[coffset] = key;

                    if ((coffset & ((1 << LOG_BATCH_SIZE) - 1)) == ((1 << LOG_BATCH_SIZE) - 1)) {
                        uint32_t true_partition = p * parts + partition;
                        
                        for (int k = 0; k < (1 << (LOG_BATCH_SIZE - 3)); k++) {
                            __m256i flush_data = *((__m256i*) &cache[(partition << LOG_BATCH_SIZE) + k*8]);
                            _mm256_stream_si256 ((__m256i*) &partitioned_data[true_partition][offset[true_partition] + k*8], flush_data);
                        }

                        offset[true_partition] += (1 << LOG_BATCH_SIZE);
                        cache_offset[partition] = (partition << LOG_BATCH_SIZE);
                    }*/

                    /*
                    size_t off = (offset[partition])++;
                    partitioned_data[partition][off] = key;
                    */
                size_t off = (offset[partition])++;

                if (histogram == NULL && off == cur_size[partition]) {
                    for (uint32_t j = 0; j < columns; j++) {
                        uint64_t* tmp = partitioned_data[partition][j];
                        partitioned_data[partition][j] = (uint64_t*) realloc(tmp, 2 * cur_size[partition] * sizeof(uint64_t));
                        //partitioned_data[partition][j] = (uint64_t*) aligned_alloc(256, 2 * cur_size[partition] * sizeof(uint64_t));
                        //memcpy (partitioned_data[partition][j], tmp, cur_size[partition] * sizeof(uint64_t));
                    }

                    cur_size[partition] *= 2;

                }

                for (uint32_t j = 0; j < columns; j++) {
                    //*(offset[partition][j]) = vec[j][i];
                    //(offset[partition][j])++;
                    partitioned_data[partition][j][off] = vec[j][i];
                }
            }

            start += VECTOR_SIZE;
        }

            /*for (int partition = 0; partition < parts; partition++) {
                uint32_t true_partition = partition * parts + p;

                for (int k = 0; k < (1 << (LOG_BATCH_SIZE - 3)); k++) {
                    if (8*k < cache_offset[partition] - (partition << LOG_BATCH_SIZE)) {
                        __m256i flush_data = *((__m256i*) &cache[(partition << LOG_BATCH_SIZE) + k*8]);
                        _mm256_stream_si256 ((__m256i*) &partitioned_data[true_partition][offset[true_partition] + k*8], flush_data);
                    }
                    offset[true_partition] += cache_offset[partition] - (partition << LOG_BATCH_SIZE);
                }
            }*/
    }

        //first_bit = fb;
}

size_t HashPartitioner::GetHist (uint32_t i) {
    return offset[i];
}

void HashPartitioner::Verify () {
    const uint32_t parts = (1 << log_parts);
    const uint32_t parts_mask = parts - 1;

    for (uint32_t partition = 0; partition < parts; partition++) {
        for (size_t i = 0; i < offset[partition]; i++) {
            int32_t key = partitioned_data[partition][pfield][i];
            uint32_t p = (key >> first_bit) & parts_mask; 

            if (partition != p) {
                //std::cout << "The partition " << partition << " is wrong" << std::endl;
                return;
            }
        }
    }
        
    //std::cout << "All good" << std::endl;
}





/* Class HashJoin */

HashJoin::HashJoin (Operator *left, uint32_t cleft, uint32_t jleft,
        Operator *right, uint32_t cright, uint32_t jright,
        int htSize)
        : htSize(htSize), left(left), right(right), cleft(cleft), jleft(jleft),
          cright(cright), jright(jright), it(NULL) {
    std::vector<std::pair<unsigned, unsigned> >& leftBindings = left->GetBindings();
    std::vector<std::pair<unsigned, unsigned> >& rightBindings = right->GetBindings();

    bindings.clear();
    for (unsigned i = 0; i < rightBindings.size(); i++)
    	bindings.push_back(rightBindings[i]);


    for (unsigned i = 0; i < leftBindings.size(); i++) {
    	bindings.push_back(leftBindings[i]);
    }
}

HashJoin::~HashJoin () {}

void HashJoin::Configure (uint32_t log_parts1, uint32_t log_parts2, uint32_t first_bit, size_t* histR1, size_t* histR2, size_t* histS1, size_t* histS2) {
    this->log_parts1 = log_parts1;
    
    this->log_parts2 = log_parts2;
    this->first_bit  = first_bit;

    histogramR1 = histR1;
    histogramR2 = histR2;
    histogramS1 = histS1;
    histogramS2 = histS2;
}

/*initialize by creating partitioners*/
int HashJoin::Open () {
    left->Open();
    right->Open();

    it = new uint64_t* [cleft + cright];

    for (unsigned j = 0; j < cleft + cright; j++)
        it[j] = new uint64_t[VECTOR_SIZE];

    ht = new HashTable<int>(htSize);
            
    hR1 = new HashPartitioner (log_parts1, cright, jright, histogramR1, first_bit + log_parts2);
    hR2 = new HashPartitioner (log_parts1 + log_parts2, cright, jright, histogramR2, first_bit);

    hS1 = new HashPartitioner (log_parts1, cleft, jleft, histogramS1, first_bit + log_parts2);
    hS2 = new HashPartitioner (log_parts1 + log_parts2, cleft, jleft, histogramS2, first_bit);
    
    offset_ = 0;
    end_ = 0;

    //dataIn_ = new uint32_t [VECTOR_SIZE];
    dataIn_.clear();


    init = false;
    rebuild = true;
    return 1;
}

int HashJoin::Close () {
    for (unsigned j = 0; j < cleft + cright; j++)
        delete[] it[j];
    delete[] it;

    delete ht;
    delete hS1;
    delete hR1;
    delete hS2;
    delete hR2;

    //std::cout << global_cnt << std::endl;
    right->Close();
    left->Close();

    return 1;
}

/*Next has to be restartable so it keeps state (last_part, last_probe) about where to continue from*/
int HashJoin::Next (std::vector<uint64_t*>* output) {
    if (!init) {
        /*first next materializes input partitions*/
        hR1->Generate (right);
        hR2->Generate (*hR1);

        hS1->Generate (left);
        hS2->Generate (*hS1);
        
        /*initialize the state so that we start from beginning*/
        last_part = 0;
        last_probe = -1;
        global_cnt = 0;

        init = true;
    }

    output->clear();
    output->reserve(cleft + cright);
    for (unsigned j = 0; j < cleft + cright; j++)
        output->push_back(it[j]);

    uint32_t mask = (uint32_t) (htSize - 1);

    size_t cnt = 0;

    /*write outstanding tuples from last hash table lookup to output (since hash tables are not restartable)*/
    for (uint64_t i = offset_; i < end_; i++)  {
        uint64_t** ptr1 = hR2->GetPartition(last_part);
        uint64_t** ptr2 = hS2->GetPartition(last_part);
        uint64_t idx = dataIn_[i];

        for (uint32_t j = 0; j < cright; j++)
            it[j][cnt] = ptr1[j][idx];

        for (uint32_t j = 0; j < cleft; j++)
            it[cright + j][cnt] = ptr2[j][last_probe];

        cnt++;
        if (cnt == VECTOR_SIZE) {
                        /*remember where we left off
                        so continue probing from next element
                        also outstanding matches from this element should be copied to results (offset_)*/
                        offset_ = i + 1;
                        return cnt;
        }
    }
    int sum = 0;


    /*join co-partitions*/
    for (unsigned p = last_part;
         p < (unsigned)(1 << (log_parts1 + log_parts2));
         p++) {
        uint64_t** ptr1 = hR2->GetPartition(p);
        
        /*create hashtable for partition
        because we might re-enter next for this partition
        remember if we have built hashtable before and re-use it*/
        if (rebuild) {
            /*sumR2 += hR2->GetHist(p);
            sumS2 += hS2->GetHist(p);*/

            rebuild = false;
            for (uint64_t i = 0; i < hR2->GetHist(p); i++)
                ht->Insert(ptr1[jright][i], ptr1[jright][i] & mask, i);
        }

 
        uint64_t** ptr2 = hS2->GetPartition(p);

        sum += hS2->GetHist(p);

        /*probe hash table from where we left*/
        for (uint64_t i = last_probe + 1; i < hS2->GetHist(p); i++) {
            dataIn_.clear();
            end_ = ht->SearchKey(ptr2[jleft][i], 0, this->dataIn_);
            if (end_ > 0) {

                /*dataIn_ caches matches in case of multiple hits in hashtable*/

                for (uint64_t k = 0; k < end_; k++) {
                    uint64_t idx = dataIn_[k];

                    for (uint32_t j = 0; j < cright; j++)
                        it[j][cnt] = ptr1[j][idx];

                    for (uint32_t j = 0; j < cleft; j++)
                        it[cright + j][cnt] = ptr2[j][i];


                    cnt++;
                    global_cnt++;

                    if (cnt == VECTOR_SIZE) {
                        /*remember where we left off
                        so continue probing from next element
                        also outstanding matches from this element should be copied to results (offset_)*/

                        last_probe = i;
                        offset_ = k + 1;
                        return cnt;
                    }
                }
            }
        }

        /*erase table for next run*/
        ht->Erase();

        rebuild = true;
        last_probe = -1;
        last_part++;
    }
    offset_ = 0;
    end_ = 0;

    return cnt;
}



/* Class SelfJoin */
SelfJoin::SelfJoin (Operator* child, uint32_t c1, uint32_t c2, uint32_t cols)
        : col1(c1), col2(c2), colNum(cols), child(child), Operator(child->GetBindings()) {}

int SelfJoin::Open () {
    child->Open();

    dataOut = new uint64_t* [colNum];

    for (uint32_t i = 0; i < colNum; i++)
        dataOut[i] = new uint64_t [VECTOR_SIZE];

    offset = 0;
    end = 0; 

    return 1;       
}

int SelfJoin::Close () {
    for (uint32_t i = 0; i < colNum; i++)
        delete[] dataOut[i];

    delete[] dataOut;

    child->Close();

    return 1;
}

int SelfJoin::Next (std::vector<uint64_t*>* output) {
    //std::cout << "TIMES" << std::endl;
    //std::cerr<<"SelfJoin Next Start" <<std::endl;
    uint32_t wr_offset = 0;

    output->clear();
    output->reserve(colNum);

    for (uint32_t j = 0; j < colNum; j++)
        output->push_back(dataOut[j]);

    /*results from previous next moved to output*/
    for (size_t i = offset; i < end; i++)
        if (dataIn[col1][i] == dataIn[col2][i]) {
            for (uint32_t j = 0; j < colNum; j++) {
                dataOut[j][wr_offset] = dataIn[j][i];
            }
            wr_offset++;
        }

    /*next loop till we fill the buffer*/
    while ((end = child->Next(&dataIn))) {
        for (size_t i = 0; i < end; i++)
            if (dataIn[col1][i] == dataIn[col2][i]) {
                for (size_t j = 0; j < dataIn.size(); j++) {
                    dataOut[j][wr_offset] = dataIn[j][i];
                }
                wr_offset++;

                if (wr_offset == VECTOR_SIZE) {
                    offset = i + 1;

                    return VECTOR_SIZE;
                }
            }
    }

    //std::cerr<<"SelfJoin Next" <<std::endl;
    return wr_offset;
}


/* Projection Pass */

Operator *Scan::ProjectionPass(ReColMap *binding)
{
    std::vector<Id_t> proj;
    for(unsigned i = 0;i<this->bindings.size();i++)
        if(binding->count(this->bindings[i]))
            proj.push_back(i);
    return new Projection(this, proj);
}

Operator *Projection::ProjectionPass(ReColMap *binding)
{
    ReColMap p_bindings = *binding;
    /* pass (my projection) AND (binding) to child */
    for(unsigned i=0;i<this->bindings.size();i++)
        p_bindings[bindings[i]] = 0;
    Operator *newChild = this->child_->ProjectionPass(&p_bindings);

    /* apply binding on newChild */
    auto &c_bindings = newChild->GetBindings();
    std::vector<Id_t> proj;
    for(auto &bd : this->bindings)
        for(int j=0;j<c_bindings.size();j++)
            if(bd == c_bindings[j])
            {
                proj.push_back(j);
                break;
            }

    return new Projection(newChild, proj);
}

Operator *Filter::ProjectionPass(ReColMap *binding)
{
    auto p_bindings = *binding;
    auto pushed_bindings = p_bindings;
    auto pred = filter_.filterColumn.colId;
    pushed_bindings[bindings[pred]] = 0;

    Operator* newchild = child_->ProjectionPass(&pushed_bindings);

    std::vector<unsigned> selected;

    uint32_t npred = -1;

    for (unsigned i = 0; i < (newchild->GetBindings()).size(); i++)
        if ((newchild->GetBindings())[i] == bindings[pred]) {
            npred = i;
        }

    Operator* newselect = new Filter(newchild, this->filter_, newchild->GetColumnCount());

    for (unsigned i = 0; i < (newselect->GetBindings()).size(); i++)
        if (p_bindings.find((newselect->GetBindings())[i]) != p_bindings.end())       
            selected.push_back(i);

    return new Projection(newselect, selected);
}

Operator *CheckSum::ProjectionPass(ReColMap *binding)
{
    return new CheckSum(this->child_->ProjectionPass(binding));
}

Operator *HashJoin::ProjectionPass(ReColMap *binding)
{
    auto p_bindings = *binding;
    auto pushed_bindings = p_bindings;
    pushed_bindings[bindings[jright]] = 0;
    pushed_bindings[bindings[cright+jleft]] = 0;

    Operator* newleft = left->ProjectionPass(&pushed_bindings);
    Operator* newright = right->ProjectionPass(&pushed_bindings);

    std::vector<unsigned> selected;

    uint32_t njleft = -1;
    uint32_t njright = -1;

    for (unsigned i = 0; i < (newleft->GetBindings()).size(); i++)
        if ((newleft->GetBindings())[i] == bindings[cright + jleft]) {
            njleft = i;
        }

    for (unsigned i = 0; i < (newright->GetBindings()).size(); i++)
        if ((newright->GetBindings())[i] == bindings[jright]) {
            njright = i;
        }

    Operator* newjoin = new HashJoin (newleft, newleft->GetColumnCount(), njleft, 
            newright, newright->GetColumnCount(), njright,
            htSize);

    ((HashJoin*)(newjoin))->Configure (log_parts1, log_parts2, first_bit, NULL, NULL, NULL, NULL);

    for (unsigned i = 0; i < (newjoin->GetBindings()).size(); i++)
        if (p_bindings.find((newjoin->GetBindings())[i]) != p_bindings.end())       
            selected.push_back(i);

    return new Projection(newjoin, selected);
}

Operator *SelfJoin::ProjectionPass(ReColMap *binding)
{
    auto p_bindings = *binding;
    auto pushed_bindings = p_bindings;
    pushed_bindings[bindings[col1]] = 0;
    pushed_bindings[bindings[col2]] = 0;

    Operator* newchild = child->ProjectionPass(&pushed_bindings);

    std::vector<unsigned> selected;

    uint32_t npred1 = -1;

    for (unsigned i = 0; i < (newchild->GetBindings()).size(); i++)
        if ((newchild->GetBindings())[i] == bindings[col1]) {
            npred1 = i;
        }

    uint32_t npred2 = -1;

    for (unsigned i = 0; i < (newchild->GetBindings()).size(); i++)
        if ((newchild->GetBindings())[i] == bindings[col2]) {
            npred2 = i;
        }

    Operator* newselect = new SelfJoin (newchild, npred1, npred2, newchild->GetColumnCount());

    for (unsigned i = 0; i < (newselect->GetBindings()).size(); i++)
        if (p_bindings.find((newselect->GetBindings())[i]) != p_bindings.end())       
            selected.push_back(i);

    return new Projection(newselect, selected);
}
