/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
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

#ifndef __MYBUFFER__
#define __MYBUFFER__

class Buffer 
{
	public:
		/**
		 * Creates a Buffer to operate on the block starting at \a *data.
		 * Class does not own the block.
		 * @param data Start of block.
		 * @param size Size of block.
		 * @param free Free section of the block. If NULL, it is assumed that
		 * the entire block is full with useful data and no further writes are
		 * allowed.
		 */
		Buffer(void* data, unsigned long long size, void* free);

		/**
		 * Allocates an empty buffer of specified size that is owned by this
		 * instance.
		 * \param size Buffer size in bytes.
		 * \param allocsource Debugging info passed to allocator.
		 */
		Buffer(unsigned long long size, void* allocsource, const char tag[4] = "Buff");
		~Buffer();

		/**
		 * Returns true if \a len bytes can be stored in this page.
		 * Does not guarantee that a subsequent allocation will succeed, as
		 * some other thread might beat us to allocate.
		 * @param len Bytes to store.
		 * @return True if \a len bytes can be stored in this page.
		 */
		inline bool canStore(unsigned long long len);

		/**
		 * Returns a memory location and moves the \a free pointer
		 * forward. NOT THREAD-SAFE!
		 * @return Memory location good for writing up to \a len bytes, else
		 * NULL.
		 */
		inline void* allocate(unsigned long long len);

		/**
		 * Returns a memory location and atomically moves the \a free pointer
		 * forward. The memory location is good for writing up to \a len
		 * bytes. 
		 * @return Memory location good for writing up to \a len bytes, else
		 * NULL.
		 */
		inline void* atomicAllocate(unsigned long long len);

		/**
		 * Returns whether this address is valid for reading \a len bytes.
		 * That is, checks if \a loc points anywhere between \a data and \a
		 * data + \a maxsize - \a len.
		 * @return True if address points into this page and \a len bytes can
		 * be read off from it.
		 */
		inline bool isValidAddress(void* loc, unsigned long long len);

		/**
		 * Clears contents of page.
		 */
		inline void clear() { free = data; }

		// TODO: isOwner(): returns true if owner
		// TODO: copy method: returns non-owner class
		// TODO: transferOwnership(from, to): if two data fields equal, transfers ownership
		
		/**
		 * Returns the capacity of this buffer.
		 */
		inline const unsigned long long capacity() { return maxsize; }

		/**
		 * Returns the used space of this buffer.
		 */
		inline const unsigned long long getUsedSpace();

	protected:
		/** Data segment of page. */
		void* data;

		unsigned long long maxsize;
		
		/**
		 * True if class owns the memory at \a *data and is responsible for its
		 * deallocation, false if not.
		 */
		bool owner;

		/** 
		 * Marks the free section of data segment, therefore data <= free <
		 * data+maxsize.
		 */
		/* volatile */ void* free;
};


class TupleBuffer : public Buffer
{
	public:

		TupleBuffer(void* data, unsigned long long size, void* free, unsigned int tuplesize);

		/**
		 * Creates a buffer of size \a size, holding tuples which are
		 * \a tuplesize bytes each.
		 * \param size Buffer size in bytes.
		 * \param tuplesize Size of tuples in bytes.
		 * \param allocsource Debugging info passed to allocator.
		 */
		TupleBuffer(unsigned long long size, unsigned int tuplesize, void* allocsource, const char tag[4] = "Buff");
		~TupleBuffer() { }

		/**
		 * Returns true if a tuple can be stored in this page.
		 * @return True if a tuple can be stored in this page.
		 */
		inline bool canStoreTuple() 
		{ 
			return canStore(tuplesize); 
		}

		/**
		 * Returns the pointer to the start of the \a pos -th tuple,
		 * or NULL if this tuple doesn't exist in this page.
		 *
		 * Note: 
		 * MemMappedTable::close() assumes that data==getTupleOffset(0).
		 */
		inline void* getTupleOffset(unsigned long long pos);

		/**
		 * Returns whether this address is valid, ie. points anywhere between 
		 * \a data and \a data + \a maxsize - \a tuplesize.
		 * @return True if address points into this page.
		 */
		inline bool isValidTupleAddress(void* loc);

		/**
		 * Returns a memory location and moves the \a free pointer
		 * forward. NOT THREAD-SAFE!
		 * @return Memory location good for writing a tuple, else NULL.
		 */
		inline void* allocateTuple();

		/**
		 * Returns a memory location and atomically moves the \a free pointer
		 * forward. The memory location is good for writing up to \a len
		 * bytes. 
		 * @return Memory location good for writing a tuple, else NULL.
		 */
		inline void* atomicAllocateTuple();

		class Iterator {
			friend class TupleBuffer;

			protected:
				Iterator(TupleBuffer* p) : tupleid(0), page(p) { }

			public:
				/**
				 * This constructor will segfault if used without being `place'd, 
				 * because accesses inside next() are not checked.
				 */
				Iterator() : tupleid(0), page(0) { }

				Iterator& operator= (Iterator& rhs) {
					page = rhs.page;
					tupleid = rhs.tupleid;
					return *this;
				}

				inline
				void place(TupleBuffer* p)
				{
					page = p;
					reset();
				}

				inline
				void* next()
				{
					return page->getTupleOffset(tupleid++);
				}

				inline
				void reset()
				{
					tupleid = 0;
				}

			private:
				int tupleid;
				TupleBuffer* page;
		};

		Iterator createIterator()
		{
			return Iterator(this);
		}

		class SubrangeIterator 
		{
			friend class TupleBuffer;

			protected:
				SubrangeIterator(TupleBuffer* p, int mininclusive, int maxexclusive) 
					: tupleid(mininclusive), mintid(mininclusive), maxtid(maxexclusive), page(p) 
				{ }

				SubrangeIterator(TupleBuffer* p) 
					: tupleid(0), mintid(0), maxtid(-1), page(p) 
				{ }

			public:
				/**
				 * This constructor will segfault if used without being `place'd, 
				 * because accesses inside next() are not checked.
				 */
				SubrangeIterator() 
					: tupleid(0), mintid(0), maxtid(-1), page(0) 
				{ }

				SubrangeIterator& operator= (SubrangeIterator& rhs) {
					page = rhs.page;
					mintid = rhs.mintid;
					maxtid = rhs.maxtid;
					tupleid = rhs.tupleid;
					return *this;
				}

				inline
				void place(TupleBuffer* p, int mininclusive, int maxexclusive) 
				{
					page = p;
					mintid = mininclusive;
					maxtid = maxexclusive;
					reset();
				}

				inline
				void* next()
				{
					if (tupleid == maxtid)
						return 0;
					return page->getTupleOffset(tupleid++);
				}

				inline
				void reset()
				{
					tupleid = mintid;
				}

			private:
				int tupleid;
				int mintid;
				int maxtid;
				TupleBuffer* page;
		};

		SubrangeIterator createSubrangeIterator()
		{
			return SubrangeIterator(this);
		}

		SubrangeIterator createSubrangeIterator(int mininclusive, int maxexclusive) 
		{
			return SubrangeIterator(this, mininclusive, maxexclusive);
		}

		/**
		 * Sorts this array. Hack for getting Sort-Merge to work.
		 */
		template <typename KeyT>
		void sort(unsigned int keyoffset);

		/**
		 * SIMD bitonic sort entry point.
		 * Assumes tuple size is 8 bytes.
		 * Assumes key is first, that is, at offset zero.
		 */
		void bitonicsort();
		
		/**
		 * Finds at what offset \a key could be inserted to maintain sorted
		 * array. Useful for determining boundaries for range partitioning.
		 * Hack for getting Sort-Merge to work.
		 */
		template <typename KeyT>
		unsigned int findsmallest(unsigned int keyoffset, KeyT key);

		/**
		 * Gets the number of tuples this TupleBuffer holds.
		 */
		inline const unsigned long long getNumTuples();

	protected:
		unsigned int tuplesize;

};


/**
 * A \a TupleBuffer with a "next" pointer.
 * @deprecated Only used by Table classes.
 */
class LinkedTupleBuffer : public TupleBuffer {
	public:

		LinkedTupleBuffer(void* data, unsigned long long size, void* free, 
				unsigned int tuplesize)
			: TupleBuffer(data, size, free, tuplesize), next(0)
		{ }

		/**
		 * Creates a bucket of size \a size, holding tuples which are
		 * \a tuplesize bytes each.
		 * \param size Bucket size in bytes.
		 * \param tuplesize Size of tuples in bytes.
		 * \param allocsource Debugging info passed to allocator.
		 */
		LinkedTupleBuffer(unsigned long long size, unsigned int tuplesize, void* allocsource) 
			: TupleBuffer(size, tuplesize, allocsource), next(0) { }

		/** 
		 * Returns a pointer to next bucket.
		 * @return Next bucket, or NULL if it doesn't exist.
		 */
		LinkedTupleBuffer* getNext()
		{
			return next;
		}

		/**
		 * Sets the pointer to the next bucket.
		 * @param bucket Pointer to next bucket.
		 */
		void setNext(LinkedTupleBuffer* const bucket)
		{
			next = bucket;
		}	

	private:
		LinkedTupleBuffer* next;
};

#include "buffer.inl"

#endif
