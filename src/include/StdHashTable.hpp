#ifndef _STDHASHTABLE_HPP_
#define _STDHASHTABLE_HPP_

#include <unordered_map>
#include <map>
#include <algorithm>
#include <vector>
#include <string.h>
#include "Timer.hpp"


/* A simple hashtable based on std::unordered_multimap */

extern Util::Timer hashTableTimer;
template<typename E>
class SimpleHashTable
{
    public:
        SimpleHashTable(size_t s){};
        ~SimpleHashTable() = default;

        size_t GetSize()const {return table_.size();}
        void Insert(const uint64_t key, const uint64_t hashv, const uint64_t v)
        {
            table_.insert({key,v});
        }
        int SearchKey(const uint64_t key, const uint64_t hashv, std::vector<uint64_t> &result) const
        {
            auto pair = table_.equal_range(key);
            std::transform(pair.first, pair.second, std::back_inserter(result),
                    [](auto it){return it.second;});
            return std::distance(pair.first, pair.second);
        }
        void Erase(){
            table_.clear();
        }
    private:
        std::multimap<E,uint64_t> table_;

};



/* HashTable Definition */
/*const unsigned int kAllocInit = 10000;  // Initial allocator capacity
template <typename E>
class HashTable
{
public:
	HashTable(const size_t s)
	{
		size_ = s;
		buckets_ = (uint64_t*) malloc(size_*sizeof(uint64_t));
		// Initialise bucket head
		memset (buckets_, 0, size_*sizeof(uint64_t));

		allocator_capacity_ = kAllocInit;
		allocator_size_ = 1;
		allocator_ = (HashEntry*) malloc(allocator_capacity_*sizeof(HashEntry));
	};
	
	~HashTable() 
	{ 
		free (allocator_);
		free (buckets_);
		buckets_ = NULL;		
		size_ = 0; 
	};
	
	size_t get_size() { return size_; }
	
	void Insert(const uint64_t key,const uint64_t hash_value, const uint64_t value) 
	{
		uint64_t offset = AllocateEntry();
		allocator_[offset].key = key;
		allocator_[offset].next = buckets_[hash_value];
		allocator_[offset].value = value;

		buckets_[hash_value] = offset;
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
			//std::cerr<<"HashTable inside AllocateEntry"<<std::endl;
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
};*/

#endif