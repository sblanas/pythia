/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "../lock.h"
#include "custom_asserts.h"
#include "static_assert.h"
#include "numaallocate.h"

#include <vector>
#include <string>
using std::vector;
using std::string;

class HashTable {
	public:
		friend class PrettyPrinterVisitor;

		HashTable() 
			: tuplesize(0), bucksize(0), nbuckets(0), spills(0)
		{
			for (unsigned int i=0; i<MAX_PART; ++i)
				bucket[i] = 0;
		}

		/**
		 * Initializes the hashtable. Not thread-safe.
		 * @param nbuckets Total number of buckets.
		 * @param bucksize Size of each bucket (in bytes).
		 * @param tuplesize Size of each tuple (in bytes).
		 * @param partitions Vector whose size specifies the number of
		 * contiguous memory regions the hashtable will be partitioned into.
		 * Each element of the vector specifies the NUMA node that the
		 * allocation will happen on, or -1 for allocation local to
		 * the thread that calls init(). As a special case, an empty partition
		 * vector is treated as a vector that only stores -1 (ie. local
		 * allocation of a single memory region).
		 * @param allocsource Debugging info passed to allocator.
		 */
		void init(unsigned int nbuckets, unsigned int bucksize, 
				unsigned int tuplesize, vector<char> partitions, void* allocsource);

		/**
		 * Per-bucket reset, and bucket chain deallocation. Must be called
		 * after \a init (to reset memory after allocation), and prior to \a
		 * destroy (to prevent bucket chains from leaking). Not thread-safe: 
		 * This function bypasses the bucket lock, so caller must ensure that
		 * hashtable is not concurrently accessed and is stable to reset.
		 */
		void bucketclear(int thisthread, int totalthreads);

		/**
		 * Deallocates memory, reversing \a init(). Not thread-safe.
		 */
		void destroy();

		/**
		 * Allocates a tuple at bucket at \a offset. Call is not atomic and
		 * might result in a new memory allocation if page is full.
		 * @param allocsource Debugging info passed to allocator.
		 * @return Location that has \a tuplesize bytes for writing.
		 */
		void* allocate(unsigned int offset, void* allocsource);

		/**
		 * Allocates a tuple at bucket at \a offset atomically.
		 * @param allocsource Debugging info passed to allocator.
		 * @return Location that has \a tuplesize bytes for writing.
		 */
		inline void* atomicAllocate(unsigned int offset, void* allocsource)
		{
			void* ret;
			BucketHeader* bh = getBucketHeader(offset);
			dbgassert(*(unsigned long long*)bh != 0xBCBCBCBCBCBCBCBCuLL);
			bh->lock.lock();
			ret = allocate(offset, allocsource);
			bh->lock.unlock();
			return ret;
		}

		class Iterator {
			friend class HashTable;
			public:
				Iterator() : cur(0), free(0), nxt(0), bucksize(0), tuplesize(0) { }
				Iterator(unsigned int bucksize, unsigned int tuplesize);
				
				inline void* next() 
				{
					void* ret;
#ifdef DEBUG
					assert(bucksize != 0);
					assert(tuplesize != 0);
#endif

					if (cur < free) {
						ret = cur;
						cur = ((char*)cur) + tuplesize;
						return ret;
					} else if (nxt != 0) {
						BucketHeader* bh = (BucketHeader*) nxt;
						ret = (char*)bh + sizeof(BucketHeader);
						cur = (char*)ret + tuplesize;
						free = (char*)ret + bh->used;
						nxt = bh->nextBucket;

						// Return null if last chunk exists but is empty.
						// Caveat: will not work correctly if there are empty
						// chunks in the chain.
						return ret < free ? ret : 0;
					} else {
						return 0;
					}
				}

			private:
				void* cur;
				void* free;
				void* nxt;
				/* const */ unsigned int bucksize;
				/* const */ unsigned int tuplesize;
		};

		Iterator createIterator();

		inline void placeIterator(Iterator& it, unsigned int offset)
		{
			BucketHeader* bh = getBucketHeader(offset);
			dbgassert(*(unsigned long long*)bh != 0xBCBCBCBCBCBCBCBCuLL);
			it.cur = (char*)bh + sizeof(BucketHeader);
			it.free = (char*)it.cur + bh->used;
			it.nxt = bh->nextBucket;
		}

		inline void prefetch(unsigned int offset)
		{
			//__asm__ __volatile__ ("prefetcht0 %0" :: "m" (*(unsigned long long*) bucket[offset & (nbuckets-1)]));
			__builtin_prefetch(getBucketHeader(offset));
		}

		inline unsigned int getNumberOfBuckets()
		{
			return nbuckets;
		}

		/**
		 * Returns histogram with number of tuples per bucket.
		 *
		 * For example, if return vector is [157, 94, 0, 5] this means that 
		 * there are 157 buckets with 0 tuples, 94 buckets with 1 tuple, no
		 * buckets with 2 tuples, and 5 buckets with 4 tuples.
		 *
		 * No effort is taken to ensure that the count is stable. The caller
		 * must guarantee that no threads are adding tuples in the hashtable
		 * while this method is called. Otherwise, the results are undefined.
		 */
		vector<unsigned int> statBuckets();

		/**
		 * Returns the number of times a new bucket had to be allocated.
		 * Because of concurrent accesses, the returned value will always be
		 * less than or equal to the current number of bucket allocations.
		 */
		inline unsigned long statSpills()
		{
			return spills;
		}

		/**
		 * Serializes hash table partition \a part at filename.
		 * @pre Partitions must have no overflow buckets.
		 * @pre Hash table must be stable for the entire operation; ie. no threads touching it.
		 */
		void serialize(const string& filename, unsigned int part);

		/**
		 * Loads (deserializes) hash table partition \a part from filename.
		 * Existing data will be discarded.
		 * @pre Hash table must be stable for the entire operation; ie. no threads touching it.
		 */
		void deserialize(const string& filename, unsigned int part);

		/**
		 * Locks bucket. Call blocks until spinlock is held.
		 */
		inline void lockbucket(unsigned int offset)
		{
			BucketHeader* bh = getBucketHeader(offset);
			dbgassert(*(unsigned long long*)bh != 0xBCBCBCBCBCBCBCBCuLL);
			bh->lock.lock();
		}

		/**
		 * Locks bucket. Call blocks until spinlock is held.
		 */
		inline void unlockbucket(unsigned int offset)
		{
			BucketHeader* bh = getBucketHeader(offset);
			dbgassert(*(unsigned long long*)bh != 0xBCBCBCBCBCBCBCBCuLL);
			bh->lock.unlock();
		}


	private:
		static const unsigned int MAX_PART = 4;
		void* bucket[MAX_PART];

		unsigned int log2partitions;
		unsigned int tuplesize;
		unsigned int bucksize; 
		unsigned int nbuckets; 

		static_assert(sizeof(unsigned long) == sizeof(void*));
		volatile unsigned long spills;

		struct BucketHeader
		{
			Lock lock;
			/** Space (in bytes) used, between 0 and bucksize (inclusive). */
			unsigned short used;
			BucketHeader* nextBucket;
			
			/** Must be called non-recursively (requirement for Lock::clear). */
			inline void clear()
			{
				lock.reset();
				used = 0;

				BucketHeader* next = nextBucket;
				while (next != NULL) 
				{
					BucketHeader* tmp = next;
					next = next->nextBucket;
					numadeallocate(tmp);
				}

				nextBucket = 0;
			}
		};

		inline BucketHeader* getBucketHeader(unsigned int offset)
		{
			unsigned int part = offset & ((1 << log2partitions) - 1);
			unsigned int idx = offset >> log2partitions;

			dbg2assert(0 <= offset && offset<nbuckets);
			dbg2assert(bucket[part] != 0);

			BucketHeader* ret = (BucketHeader*) 
				((char*)bucket[part] + idx * (sizeof(BucketHeader) + bucksize));
			return ret;
		}
};
