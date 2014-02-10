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

#ifdef ENABLE_NUMA
#include <numaif.h>
#endif

/* Define to protect mbind() in critical segment. This is a workaround for
 * this RHEL6 bug:
 *   http://lkml.org/lkml/2011/6/27/233
 *   https://bugzilla.redhat.com/show_bug.cgi?id=727700
 *
 * #define MBIND_BUG_WORKAROUND
 */

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cassert>
#include <execinfo.h>

#include "numaasserts.h"
#include "atomics.h"
#include "../lock.h"

#ifdef MBIND_BUG_WORKAROUND
Lock NumaAllocLock;
#endif

size_t TotalBytesAllocated = 0;

#ifdef STATS_ALLOCATE
#include <map>
#include <string>
#include <utility>
#include "../query.h"
#include <iostream>
#include <sstream>
#include <iomanip>
using std::map;
using std::pair;
using std::string;
using std::make_pair;

Lock AllocStatsLock;

struct AllocT
{
	AllocT(void* source, string tag, char numareq, char numaacq)
		: source(source), tag(tag), numareq(numareq), numaacq(numaacq)
	{ }

	AllocT()
		: source(0), numareq(-2), numaacq(-2)
	{ }

	inline bool operator< (const AllocT& rhs) const
	{
		if (source != rhs.source)
			return source < rhs.source;
		if (tag != rhs.tag)
			return tag < rhs.tag;
		if (numareq != rhs.numareq)
			return numareq < rhs.numareq;
		if (numaacq != rhs.numaacq)
			return numaacq < rhs.numaacq;
		return false;
	}

	inline bool operator== (const AllocT& rhs) const
	{
		return (source == rhs.source) && (tag == rhs.tag) 
			&& (numareq == rhs.numareq) && (numaacq == rhs.numaacq);
	}

	void* source;	///< What should be "charged" for this allocation.
	string tag;		///< Human-readable tag, printed via dbgPrintAllocations.
	char numareq;	///< Requested NUMA node.
	char numaacq;	///< NUMA node memory was acquired on.
};

typedef map<AllocT, size_t> AllocStatT;
AllocStatT AllocStats;

void innerupdatestats(void* source, const char tag[4], char numareq, char numaacq, size_t allocsize)
{
	// Keep allocation sizes in global static "allocstats" object
	string tagstr(tag, 4);
	AllocStatT::key_type key(source, tagstr, numareq, numaacq);

	AllocStatsLock.lock();
	AllocStats[key] += allocsize;
	AllocStatsLock.unlock();
}

// from visitors/prettyprint.cpp
string addcommas_str(const string& input);

template <typename T>
string addcommas(const T& input)
{
	std::ostringstream ss;
	ss << input;
	return addcommas_str(ss.str());
}

void dbgPrintAllocations(Query& q)
{
	using namespace std;
	cout << "Depth" << " " << " Tag" << " " << "NumaReq" << " " 
		<< "NumaAcq" << " " << "PeakAlloc(bytes)" << endl;
	for (AllocStatT::const_iterator it = AllocStats.begin();
			it != AllocStats.end();
			++it)
	{
		const AllocT& key = it->first;
		cout << setfill(' ') << setw(5) << q.getOperatorDepth((Operator*)key.source) << " ";
		cout << setfill(' ') << setw(4) << key.tag << " ";

		if (key.numareq == -1) 
		{ 
			cout << "      L ";
		}
		else
		{
			cout << setfill(' ') << setw(7) << (int)key.numareq << " ";
		}

		if (key.numaacq == -1) 
		{ 
			cout << "      L ";
		}
		else
		{
			cout << setfill(' ') << setw(7) << (int)key.numaacq << " ";
		}

		cout << setfill(' ') << setw(16) << addcommas(it->second) << endl;
	}
}
#endif

void updatestats(void* source, const char tag[4], char numareq, char numaacq, size_t allocsize)
{
#ifdef STATS_ALLOCATE
	innerupdatestats(source, tag, numareq, numaacq, allocsize);
#endif
	atomic_increment(&TotalBytesAllocated, allocsize);
}

struct AllocHeader
{
	void* calleraddress;
	char tag[4];
	bool mmapalloc;
	size_t allocsize;

	// Bitonic sort needs 16-byte aligned values. Hacking it to do so.
	//
	char padding[8];
};

void populateHeader(void* dest, const char tag[4], bool mmapalloc, size_t allocsize)
{
	AllocHeader* d = (AllocHeader*) dest;
	void* retaddbuf[3];

	assert(3 == backtrace(retaddbuf, 3));

	d->calleraddress = retaddbuf[2];
	d->tag[0] = tag[0];
	d->tag[1] = tag[1];
	d->tag[2] = tag[2];
	d->tag[3] = tag[3];
	d->mmapalloc = mmapalloc;
	d->allocsize = allocsize;
}

struct LookasideHeader
{
	volatile void* free;
	size_t maxsize;
};

struct Lookaside
{
#ifdef ENABLE_NUMA
	static void* arena[4];
#else
	static void* arena[1];
#endif
};

/**
 * Initial population of lookasides.
 */
void* lookaside_init_alloc(size_t allocsize, int node)
{
	void* memory = NULL;
	assert(allocsize > sizeof(LookasideHeader));
	assert(node != -1);

	memory = mmap(NULL, allocsize, 
			PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 
			-1, 0);
	assert(memory != MAP_FAILED);

#ifdef ENABLE_NUMA
	int retries = 1024;
	unsigned long numanodemask = 1uLL << node;
	unsigned long maxnode = sizeof(numanodemask);
	assert(static_cast<int>(maxnode) > node);
	assert(node >= 0);
	int res = 0;
	do
	{
		res = mbind(memory, allocsize, 
				MPOL_BIND, &numanodemask, maxnode, 
				MPOL_MF_STRICT | MPOL_MF_MOVE); 
	}
   	while ((res != 0) && ((--retries) != 0));
#endif

	LookasideHeader* lh = (LookasideHeader*)memory;
	lh->free = &lh[1];
	lh->maxsize = allocsize - sizeof(LookasideHeader);

	assertaddressonnuma(memory, node);

	return memory;
}

#ifdef ENABLE_NUMA
void* Lookaside::arena[4] = {
	lookaside_init_alloc(1uLL*1024*1024*1024, 0),
	lookaside_init_alloc(1uLL*1024*1024*1024, 1),
	lookaside_init_alloc(1uLL*1024*1024*1024, 2),
	lookaside_init_alloc(1uLL*1024*1024*1024, 3)
};
#else
void* Lookaside::arena[1] = {
	lookaside_init_alloc(1uLL*1024*1024*1024, 0)
};
#endif

/**
 * Function does allocation via mmap().
 */
void* slowallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
#ifndef NULL
#define NULL 0
#endif
	void* memory = NULL;
	allocsize += sizeof(AllocHeader);
//	allocsize = (allocsize/4096 + 1) * 4096;	// round up to 4K

	unsigned long numanodemask = 1uLL << node;
	unsigned long maxnode = sizeof(numanodemask);
	assert(static_cast<int>(maxnode) > node);

#ifdef MBIND_BUG_WORKAROUND
	// Locking as a stopgap fix to a known kernel bug when mbind() is called
	// concurrently: 
	//   http://lkml.org/lkml/2011/6/27/233
	//
	NumaAllocLock.lock();
#endif

	memory = mmap(NULL, allocsize, 
			PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 
//			PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 
			-1, 0);
	assert(memory != MAP_FAILED);

#ifdef ENABLE_NUMA
	int res = 0;
	int retries = 1024;
	do
	{
		if (node == -1)
		{
			res = mbind(memory, allocsize, 
					MPOL_PREFERRED, NULL, 0, 
					MPOL_MF_STRICT | MPOL_MF_MOVE); 
		}
		else
		{
			res = mbind(memory, allocsize, 
					MPOL_BIND, &numanodemask, maxnode, 
					MPOL_MF_STRICT | MPOL_MF_MOVE); 
		}
	}
   	while ((res != 0) && ((--retries) != 0));
	assert(res==0);
#endif

#ifdef MBIND_BUG_WORKAROUND
	NumaAllocLock.unlock();
#endif

	populateHeader(memory, tag, true, allocsize);
	updatestats(source, tag, node, node, allocsize);

	return ((char*)memory) + sizeof(AllocHeader);
}

/**
 * Function looks up for space in the local lookaside, and returns NULL if
 * there's no space.
 */
void* fastallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
	int nodeacq = -2;

	allocsize += sizeof(AllocHeader);

	// Round up to the next 64-byte multiple to maintain alignement. 
	// Then add 64 bytes to elimnate false sharing.
	//
	allocsize = (((allocsize+64) / 64) * 64) + 64;

	if (node == -1)
	{
		nodeacq = localnumanode();
	}
	else
	{
		nodeacq = node;
	}

	assert(nodeacq >= 0);
#ifdef ENABLE_NUMA
	assert(nodeacq < 4);
#else
	assert(nodeacq < 1);
#endif

	LookasideHeader* lh = (LookasideHeader*) Lookaside::arena[nodeacq];
	void* data = &lh[1];
	assert(data <= lh->free);

	void* oldval;
	void* newval;
	void** val = (void**)&(lh->free);

	newval = *val;
	do {
		if (lh->free > ((char*)data + lh->maxsize - allocsize))
		{
			// Not enough space in arena, return nothing.
			//
			return NULL;
		}

		oldval = newval;
		newval = (char*)oldval + allocsize;
		newval = atomic_compare_and_swap(val, oldval, newval);
	} while (newval != oldval);

	populateHeader(newval, tag, false, allocsize);
	updatestats(source, tag, node, nodeacq, allocsize);

	return ((char*)newval) + sizeof(AllocHeader);
}

/** 
 * NUMA-aware allocator main entry point.
 * If node is -1, do local allocation, else allocate on specified node.
 * Tag and Source are user-defined values that get printed via dbgPrintAllocations.
 */
void* numaallocate_onnode(const char tag[4], size_t allocsize, int node, void* source)
{
	void* memory = NULL;

	// If more than 16M, go to slow allocator to avoid pollution of arena.
	//
	if (allocsize <= 16uLL*1024*1024)
	{
		memory = fastallocate_onnode(tag, allocsize, node, source);
	}

	if (memory == NULL)
	{
		memory = slowallocate_onnode(tag, allocsize, node, source);
	}
	assert((((unsigned long long)memory) & 0x7) == 0);

	// Assert that NUMA-ness has been repsected.
	//
	if (node == -1)
	{
		assertaddresslocal(memory);
	}
	else
	{
		assertaddressonnuma(memory, node);
	}

	return memory;
}

void* numaallocate_local(const char tag[4], size_t allocsize, void* source)
{
	return numaallocate_onnode(tag, allocsize, -1, source);
}

void numadeallocate(void* space)
{
	AllocHeader* d = (AllocHeader*) (((char*)space) - sizeof(AllocHeader));

	if (d->mmapalloc)
	{
		// If allocated via mmap(), deallocate.
		//
		int res = munmap(d, d->allocsize);
		assert(res==0);
	}
	else
	{
		// Do nothing. 
		//
		// One day this will mark slots as deleted, and trigger compaction.
		//
		;
	}
}
