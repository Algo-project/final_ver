#ifndef _CHT_HPP_
#define _CHT_HPP_

#include "HashTable.hpp"
#include <cassert>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <unistd.h>
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

		size_t __getOff(size_t off)	{return off+32;}

	public:
		Bitmap(size_t size)	{__set(size);}

		/* return true if it is set */
		bool At(size_t bkt)
		{
			//fprintf(stderr,"testing bkt %ld\n",bkt);
			auto off = bkt % 32,
			     ind = bkt / 32;
			return p[ind] & (1ull << (__getOff(off)));
		}

		/* return true if 0->1 */
		bool Set(size_t bkt)
		{
			if(At(bkt)) return false;
			//fprintf(stderr,"seting bkt %ld\n",bkt);
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

		size_t GetIndex(size_t bkt)
		{
			assert(At(bkt));
			auto off = bkt % 32,
			     ind = bkt / 32;
			auto prefix = p[ind] & 0xffffffff;
			auto aa = p[ind] >> 32;
			auto mask = ~(-1<<bkt);
			aa &= mask;
			auto append = __builtin_popcountll(aa);
			return prefix + append;
		}

        void Clear()
        {
            this->total = 0;
            memset(p,0,this->s);
        }

		~Bitmap()
		{
			delete[] p;
		}

};

template<typename E>
class CHT
{
    private:
        static const int THRESHOLD = 2;
    public:
        CHT(size_t size):size_(size),bitmap(size)
        {
            array = new _Entry[size];
        }
        ~CHT()
        {
            delete[] array;
        }

        size_t getSize(){return this->size_;}

        template<class UnaryOp>
        void Build(std::vector<E> &keyArr, std::vector<uint64_t> &valArr,
                const UnaryOp &transfer)
        {
            for(int i=0;i<keyArr.size();i++)
            {
                auto key = keyArr[i];
                uint64_t val = valArr[i];
                auto hashv = transfer(key);
                for(int j=0;j<THRESHOLD;j++)
                    if(this->bitmap.Set(j+hashv)) break;
            }
            this->bitmap.FillPrefix();

            bool *isset = (bool*)calloc(this->size_, sizeof(bool));
            for(int i=0;i<keyArr.size();i++)
            {
                _Entry _kvp{keyArr[i],valArr[i]};
                auto hashv = transfer(_kvp.key);
                bool isIntoOverflow = true;
                for(int j=0;j<THRESHOLD;j++)
                {
                    uint64_t index = 0;
//                    if(this->bitmap.At(hashv + j))
                        index = this->bitmap.GetIndex(hashv + j);
//                    else break;
                    if(!isset[index]) /* insert into array */
                    {
                        isIntoOverflow = false;
                        array[index] = _kvp;
                        isset[index] = true;
                        break;
                    }
                }
                if(isIntoOverflow)
                    overflow.insert({keyArr[i],valArr[i]});
            }

            delete[] isset;
        }

        int SearchKey(const E key, const uint64_t hashv, 
               std::vector<uint64_t> &result)
        {
            int beforeSize = result.size();
            for(size_t i=hashv;i<hashv+THRESHOLD;i++)
                if(this->bitmap.At(i))
                {
                    auto pos = this->bitmap.GetIndex(i);
                    auto ent = array[pos];
                    if(ent.key == key) result.push_back(ent.value);
                }

            auto range = overflow.equal_range(key);
            for(auto it = range.first;it!=range.second;it++)
                result.push_back(it->second);

            return result.size() - beforeSize;
        }

        void Erase()
        {
            bitmap.Clear();
            overflow.clear();
        }
    private:
        struct _Entry
        {
            uint64_t key;
            uint64_t value;
            bool operator==(const _Entry &e) const{return this->key == e.key;}
        };
        uint64_t size_;
        Bitmap bitmap;
        _Entry *array;
        std::unordered_multimap<E, uint64_t> overflow;
        
};


//template<typename E>
//using HashTable = CHT<E>;
#endif
