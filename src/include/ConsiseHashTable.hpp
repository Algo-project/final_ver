#ifndef _CHT_HPP_
#define _CHT_HPP_

#include <string.h>
#include <cassert>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>

#include "HashTable.hpp"

/**
 * Helper class for building CHT
 */

class Bitmap
{
	/* 0-----31 32----64
	 * prefix	bitset
	 */	
	private:
		uint64_t *p;
		size_t s,total;
		void __set(size_t size){
			total = 0;
			s=size;
			auto arrsize = getArraySize();
			p=new uint64_t[arrsize];
			memset(p,0,arrsize*sizeof(*p));
		}

		size_t getArraySize(){return s/32+1;}

		size_t __getOff(size_t off)const{return off+32;}

	public:
		Bitmap(size_t size)	{__set(size);}

		/* return true if it is set */
		bool At(size_t bkt) const
		{
			auto off = bkt % 32,
			     ind = bkt / 32;
			return p[ind] & (1ull << (__getOff(off)));
		}

		/* return true if 0->1 */
		bool Set(size_t bkt)
		{
			if(At(bkt)) return false;
			auto off = bkt % 32,
			     ind = bkt / 32;
			p[ind] |= (1ull <<(__getOff(off)));
			++total;
			return true;
		}

		void FillPrefix()
		{
			size_t arrsize = this->getArraySize();
			int pre = 0;
			for(size_t i=0;i<arrsize;i++)
			{
				p[i] |= pre;
				pre += __builtin_popcountll(p[i] >> 32);
			}
		}

        [[deprecated]]
		size_t GetTotal(){return total;}

		size_t GetIndex(size_t bkt) const
		{
            if(!At(bkt)) return -1;
			auto off = bkt % 32,
			     ind = bkt / 32;
			auto prefix = p[ind] & 0xffffffff;
			auto aa = p[ind] >> 32;
			auto mask = ~(-1<<off);
			aa &= mask;
			auto append = __builtin_popcountll(aa);
			return prefix + append;
		}

        void Clear()
        {
            this->total = 0;
            memset(p,0,this->getArraySize()*sizeof(p[0]));
        }

		~Bitmap()
		{
			delete[] p;
		}

};

template<typename E>
class CHT : public HashTable<E>
{
    private:
        static const int THRESHOLD = 2;
    public:
        CHT(size_t size):size_(size),bitmap(size*8),tempCount(0)
        {
            array = new _Entry[size];
            tempArray = new _Entry[size];
        }
        ~CHT()
        {
            delete[] this->array;
            delete[] this->tempArray;
        }

        size_t getSize(){return this->size_;}
        

        int Insert(const E key, const uint64_t hashv, const uint64_t value) override
        {
            if(tempCount >= this->size_) //realloc
            {
                tempArray = (_Entry*)realloc((void*)tempArray, 2*this->size_ * sizeof(_Entry));
                array = (_Entry*)realloc(array,2*this->size_ * sizeof(_Entry));
                this->size_ *= 2;
            }

            tempArray[tempCount++] = {key,value,hashv};
            bool flag = false;
            for(int i=0;i<THRESHOLD;i++)
            {
                auto posi = i+hashv;
                if(posi >= size_) break;
                flag = bitmap.Set(posi);
                if(flag) {
                    break;
                }
            }
            if(!flag){ 
                overflow.emplace(key,value);
            }
            return 0;
        }

        int TriggerBuild() override
        {
            this->bitmap.FillPrefix();
            bool isset[size_];// = (bool*)calloc(size_,sizeof(bool));
            memset(isset, 0, size_*sizeof(bool));
            for(int i=0;i<tempCount;i++)
            {
                int hv = tempArray[i].hashv;
                for(int j=hv;j<hv+THRESHOLD && j<size_;j++)
                {
                    ssize_t pos = this->bitmap.GetIndex(j);
                    if(pos == -1) break;
                    if(!isset[pos])
                    {
                        this->array[pos] = tempArray[i];
                        isset[pos] = true;
                        break;
                    }
                }
            }
            //free(isset);
            
            return 0;
        }


        uint64_t SearchKey(const E key, const uint64_t hashv, 
               std::vector<uint64_t> &result) const override
        {
            int beforeSize = result.size();
            for(size_t i=hashv;i<hashv+THRESHOLD;i++)
            {
                if(i>=size_) break;
                ssize_t pos = this->bitmap.GetIndex(i);
                if(pos!=-1)
                {
                    auto ent = array[pos];
                    if(ent.key == key) result.push_back(ent.value);
                }
            }

            auto range = overflow.equal_range(key);
            for(auto it = range.first;it!=range.second;it++)
                result.push_back(it->second);

            return result.size() - beforeSize;
        }

        void Erase() override
        {
            bitmap.Clear();
            overflow.clear();
            tempCount = 0;
        }
    private:
        struct _Entry
        {
            E key;
            uint64_t value;
            uint64_t hashv;
            bool operator==(const _Entry &e) const{return this->key == e.key;}
        };
        struct _Hash
        {
            std::hash<E> _h;
            size_t operator()(const _Entry &ent) const noexcept
            {
                return _h(ent.key);
            }
        };
        uint64_t size_;
        Bitmap bitmap;
        _Entry *array;
        _Entry *tempArray;
        uint64_t tempCount;
        //std::vector<_Entry> array;
        //std::vector<_Entry> tempArray;
        std::unordered_multimap<E,uint64_t> overflow;
        
};


#endif
