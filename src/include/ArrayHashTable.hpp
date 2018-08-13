#ifndef ARRAYHASH_TABLE_H
#define ARRAYHASH_TABLE_H

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "HashTable.hpp"

const unsigned int kAllocInit = 10000;  // Initial allocator capacity

// Array-based hash table
// Initialised with number of buckets
// Example use:
// ArrayHashTable<int> ht(size);
// ht.insert(key,value,key%size);
// value = ht.SearchKey(key,key%size);
template <typename E>
class ArrayHashTable : public HashTable<E>
{
public:
	ArrayHashTable(const size_t s)
	{
		size_ = s;
		buckets_ = (uint64_t*) malloc(size_*sizeof(uint64_t));
		// Initialise bucket head
		memset (buckets_, 0, size_*sizeof(uint64_t));

		allocator_capacity_ = kAllocInit;
		allocator_size_ = 1;
		allocator_ = (HashEntry*) malloc(allocator_capacity_*sizeof(HashEntry));
	};
	
	~ArrayHashTable() 
	{ 
		free (allocator_);
		free (buckets_);
		buckets_ = NULL;		
		size_ = 0; 
	};
	size_t getSize() { return size_; }
	
	void Insert(const E key,const uint64_t hash_value, const uint64_t value) 
	{
        //fprintf(stderr,"Insert {%d, %zu, %zu}\n ",key,value,hash_value);
		uint64_t offset = AllocateEntry();
		allocator_[offset].key = key;
		allocator_[offset].next = buckets_[hash_value];
		allocator_[offset].value = value;

		buckets_[hash_value] = offset;
        //fprintf(stderr,"size : %zu\n", this->allocator_size_);
	};
	
	// Return value associated with key + its hash value else NULL
	uint64_t SearchKey(const uint64_t key,const uint64_t hash_value, std::vector<uint64_t>& result_buffer) const
	{
		//std::cerr<<"Mpika stin SearchKey"<<std::endl;
		uint64_t cnt = 0;
		//uint64_t lol_counter = 0;
		//std::cerr<<"HashValue: "<<hash_value<<"Allocator: "<<allocator_ <<std::endl;
		uint64_t head = buckets_[hash_value];
		//std::cerr<<"SYNEXIZEIS????"<<std::endl;
		uint64_t i = head;
		while (i != 0) {
			if (allocator_[i].key == key) {
				//result_buffer[cnt] = allocator_[i].value;
				//std::cerr<<"poutsa myrizei sta stena tou gyzi"<<std::endl;
				result_buffer.push_back(allocator_[i].value);
				//cnt++;
				cnt++;
				//std::cerr<<cnt<<std::endl;
			}
			//lol_counter++;
			//std::cerr<<key<<" | "<<i<<std::endl;

			//if (cnt >= 1024)
			//	std::cout << "KERATAS" << std::endl;

			//std::cout << "match" << key << std::endl; 

			i = allocator_[i].next;
		}
		//std::cerr<<"vgaineis apeytheias re malaka"<<std::endl;

	
		//std::cout << cnt << std::endl;
		//std::cerr<<"Cnt:"<<cnt<< "RBFS: "<<result_buffer.size()<<"lol_counter:"<<lol_counter<<std::endl;
        //fprintf(stderr,"search key {%d, %zu}, found total %zu items\n",
        //           key, hash_value, cnt); 
		return cnt; 
	}
	bool InsertNoDup(const uint64_t key,const uint64_t hash_value, const uint32_t value) 
	{
		
		uint64_t head = buckets_[hash_value];
		uint64_t i = head;
		while (i != 0 && allocator_[i].key != key) {
			i = allocator_[i].next;
		}
		if (i != 0) return false ; 
		uint64_t offset = AllocateEntry();
		allocator_[offset].key = key;
		allocator_[offset].next = head;
		allocator_[offset].value = value;

		buckets_[hash_value] = offset;
		return true;
	};
	void Erase() 
	{
		memset (buckets_, 0, size_*sizeof(uint64_t));
		allocator_size_ = 1;
	}
private:
	// Storage unit for the table
	struct HashEntry
	{
		HashEntry(const uint64_t k) : key(k),next(NULL) {}
		bool operator==(const HashEntry& he)
		{
			return key == he.key;		
		}
		bool operator!=(const HashEntry& he)
		{
			return key != he.key;		
		}

		uint64_t key;
		uint64_t value;
		uint32_t next;		
	};
	
	uint64_t AllocateEntry () {
		if (allocator_size_ >= allocator_capacity_) {
			allocator_capacity_ *= 2;
			//std::cerr<<"ArrayHashTable inside AllocateEntry"<<std::endl;
			//if(allocator_size_ > 1000000){
			//	std::cerr<<"pano apo myrio"<<std::endl;
			//}
			allocator_ = (HashEntry*) realloc(allocator_, allocator_capacity_*sizeof(HashEntry));
		}
		uint64_t offset = allocator_size_;
		allocator_size_++;
		return offset;
	}

	uint64_t allocator_size_;
	uint64_t allocator_capacity_;
	HashEntry *allocator_;	// Actual array of hash entries

	size_t size_;
	uint64_t *buckets_;	// Heads of hash entry lists (offsets of allocator_ array)
};



#endif
