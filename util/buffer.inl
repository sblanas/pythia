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

#include "atomics.h"

inline bool Buffer::isValidAddress(void* loc, unsigned long long len) 
{
	return (data <= loc) && (loc <= ((char*)data + maxsize - len));
}

inline bool Buffer::canStore(unsigned long long len) 
{
	return isValidAddress(free, len);
}

inline void* Buffer::allocate(unsigned long long len) 
{
	if (len == 0)
		return free;

	if (!canStore(len))
		return 0; //nullptr;

	void* ret = free;
	free = reinterpret_cast<char*>(free) + len;
	return ret;
}

inline void* Buffer::atomicAllocate(unsigned long long len) 
{
	if (len == 0)
		return free;

	void* oldval;
	void* newval;
	void** val = &free;

	newval = *val;
	do {
		if (!canStore(len))
			return 0; //nullptr;

		oldval = newval;
		newval = (char*)oldval + len;
		newval = atomic_compare_and_swap(val, oldval, newval);
	} while (newval != oldval);
	return newval;
}

inline const unsigned long long Buffer::getUsedSpace()
{
	return reinterpret_cast<char*>(free) - reinterpret_cast<char*>(data);
}

inline const unsigned long long TupleBuffer::getNumTuples()
{
	return getUsedSpace()/tuplesize;
}

inline void* TupleBuffer::getTupleOffset(unsigned long long pos) 
{
	char* f = reinterpret_cast<char*>(free);
	char* d = reinterpret_cast<char*>(data);
	char* ret = d + pos*tuplesize;
	return ret < f ? ret : 0;
}

inline void* TupleBuffer::allocateTuple()
{
	return Buffer::allocate(tuplesize);
}

inline void* TupleBuffer::atomicAllocateTuple()
{
	return Buffer::atomicAllocate(tuplesize);
}

inline bool TupleBuffer::isValidTupleAddress(void* loc) 
{
	return Buffer::isValidAddress(loc, tuplesize);
}


/* Helper functions/classes for sorting a TupleBuffer. 
 * In .inl file because of templating.
 */

#include <algorithm>
#include <cassert>

template<int tuplesize, int keyoffset, typename KeyT>
struct ElementWithKey
{
	char foo[keyoffset];
	KeyT key;
	char bar[(tuplesize - sizeof(KeyT) - keyoffset > 1024*1024*1024) ? 0 : (tuplesize - sizeof(KeyT) - keyoffset)];

	bool operator< (const ElementWithKey<tuplesize, keyoffset, KeyT>& el) const
	{
		return key < el.key;
	}
} __attribute__((__packed__));


template <typename KeyT>
void TupleBuffer::sort(unsigned int keyoffset)
{
#ifdef BITONIC_SORT
	// Bitonic sort is never invoked through this function call, because it
	// only works for 8-byte tuples. Throw here to catch early.
	//
	throw UnknownAlgorithmException();
#endif

	const unsigned long long tuples = getNumTuples();

	switch (tuplesize)
	{
		case 128:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<128, 0, KeyT>* a = 
							(ElementWithKey<128, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				case 8:
					{
						ElementWithKey<128, 8, KeyT>* a = 
							(ElementWithKey<128, 8, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 64:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<64, 0, KeyT>* a = 
							(ElementWithKey<64, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				case 8:
					{
						ElementWithKey<64, 8, KeyT>* a = 
							(ElementWithKey<64, 8, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 32:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<32, 0, KeyT>* a = 
							(ElementWithKey<32, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 24:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<24, 0, KeyT>* a = 
							(ElementWithKey<24, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 20:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<20, 0, KeyT>* a = 
							(ElementWithKey<20, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 16:
			switch (keyoffset)
			{
				case 8:
					{
						ElementWithKey<16, 8, KeyT>* a = 
							(ElementWithKey<16, 8, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				case 4:
					{
						ElementWithKey<16, 4, KeyT>* a = 
							(ElementWithKey<16, 4, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				case 0:
					{
						ElementWithKey<16, 0, KeyT>* a = 
							(ElementWithKey<16, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		case 8:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<8, 0, KeyT>* a = 
							(ElementWithKey<8, 0, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				case 4:
					{
						ElementWithKey<8, 4, KeyT>* a = 
							(ElementWithKey<8, 4, KeyT>*) data;
						std::sort(a, a+tuples);
						break;
					}
				default:
					assert(false);
			}
			break;

		default:
			assert(false);
	}
}

template <typename KeyT>
unsigned int TupleBuffer::findsmallest(unsigned int keyoffset, KeyT key)
{
	const unsigned long long tuples = getNumTuples();

	switch (tuplesize)
	{
		case 128:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<128, 0, KeyT>* a = 
							(ElementWithKey<128, 0, KeyT>*) data;
						ElementWithKey<128, 0, KeyT> elkey;
						ElementWithKey<128, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 64:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<64, 0, KeyT>* a = 
							(ElementWithKey<64, 0, KeyT>*) data;
						ElementWithKey<64, 0, KeyT> elkey;
						ElementWithKey<64, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 32:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<32, 0, KeyT>* a = 
							(ElementWithKey<32, 0, KeyT>*) data;
						ElementWithKey<32, 0, KeyT> elkey;
						ElementWithKey<32, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 24:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<24, 0, KeyT>* a = 
							(ElementWithKey<24, 0, KeyT>*) data;
						ElementWithKey<24, 0, KeyT> elkey;
						ElementWithKey<24, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 20:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<20, 0, KeyT>* a = 
							(ElementWithKey<20, 0, KeyT>*) data;
						ElementWithKey<20, 0, KeyT> elkey;
						ElementWithKey<20, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 16:
			switch (keyoffset)
			{
				case 4:
					{
						ElementWithKey<16, 4, KeyT>* a = 
							(ElementWithKey<16, 4, KeyT>*) data;
						ElementWithKey<16, 4, KeyT> elkey;
						ElementWithKey<16, 4, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				case 0:
					{
						ElementWithKey<16, 0, KeyT>* a = 
							(ElementWithKey<16, 0, KeyT>*) data;
						ElementWithKey<16, 0, KeyT> elkey;
						ElementWithKey<16, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		case 8:
			switch (keyoffset)
			{
				case 0:
					{
						ElementWithKey<8, 0, KeyT>* a = 
							(ElementWithKey<8, 0, KeyT>*) data;
						ElementWithKey<8, 0, KeyT> elkey;
						ElementWithKey<8, 0, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				case 4:
					{
						ElementWithKey<8, 4, KeyT>* a = 
							(ElementWithKey<8, 4, KeyT>*) data;
						ElementWithKey<8, 4, KeyT> elkey;
						ElementWithKey<8, 4, KeyT>* res;
						elkey.key = key;
						res = std::lower_bound(a, a+tuples, elkey);
						return res - a;
					}
				default:
					assert(false);
			}
			break;

		default:
			assert(false);
	}

	assert(false);
}
