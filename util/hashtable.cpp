
/*
 * Copyright 2009, Pythia authors (see AUTHORS file).
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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "hashtable.h"
#include "../exceptions.h"
#include "atomics.h"
#include "../hash.h"

// To be picked up from operators/memsegmentwriter.cpp
void* creatememsegment(const string& fullname, size_t size);

#ifdef DEBUG
#include <cstring>
#endif

void dbgassertinitialized(void* bh)
{
	dbgassert(*(unsigned long long*)bh != 0xBCBCBCBCBCBCBCBCuLL);
}

void HashTable::init(unsigned int nbuckets, unsigned int bucksize, 
		unsigned int tuplesize, vector<char> partitions, void* allocsource)
{
	// If partitions is empty, localy allocate a single memory region.
	//
	if (partitions.empty())
		partitions.push_back(-1);

	assertpowerof2(partitions.size());
	assert(partitions.size() <= MAX_PART); // check we don't overflow array
	log2partitions = getlogarithm(partitions.size());

	this->bucksize = bucksize;
	this->tuplesize = tuplesize;
	this->nbuckets = nbuckets;

	unsigned int noparts = 1<<log2partitions;

	for (unsigned int i = 0; i<noparts; ++i)
	{
		size_t partsize = 
			(sizeof(BucketHeader)+bucksize) * ((nbuckets+noparts-1)/noparts);
		bucket[i] = numaallocate_onnode("HTbS", partsize, partitions.at(i), allocsource);
		assert(bucket[i] != NULL);

#ifdef DEBUG
		// 0xBC is checked for in dbgassertinitialized() and
		// Iterator::placeIterator(), atomicallocate().
		memset(bucket[i], 0xBC, partsize);
#endif
	}
#ifdef DEBUG
	// Fix pointers to be NULL, or they will be followed.
	//
	for (unsigned int i = 0; i < nbuckets; ++i)
	{
		BucketHeader* bh = getBucketHeader(i);
		bh->nextBucket = 0;
	}
#endif
}

void HashTable::serialize(const string& fullname, unsigned int part)
{
	assert(part >= 0);
	assert(part < MAX_PART);

	unsigned int noparts = 1<<log2partitions;
	if (part >= noparts)
	{
		return;
	}

	assert(spills == 0);
	assert(bucket[part] != NULL);

	size_t partsize = 
		(sizeof(BucketHeader)+bucksize) * ((nbuckets+noparts-1)/noparts);

	void* addr = creatememsegment(fullname, partsize);
	memcpy(addr, bucket[part], partsize);

	int res = munmap(addr, partsize);
	assert(res == 0);
}

void* attachmemsegment(const string& filename, size_t size) 
{
	string realfilename(filename);
	int openflags = O_RDONLY;
	int memoryprotection = PROT_READ;
	int mmapflags = MAP_PRIVATE | MAP_NORESERVE | MAP_POPULATE /* | MAP_LOCKED */;

#if defined(__linux__)
	// We have adopted linux as a default, nothing extra to do.
	//
#elif defined(__sun__)
	// Solaris maps segments at /tmp/.SHMD* instead, /dev/shm/ does not exist.
	// Make searches on /dev/shm/* go to /tmp/.SHMD*
	//
	if (filename.substr(0, 9) == "/dev/shm/")
	{
		realfilename = "/tmp/.SHMD" + filename.substr(9, string::npos);
	}
#else
#warning Shared memory mapping file prefix not known for this platform.
#endif

	// If a /dev/shm file, use shm_open, otherwise use open.
	//
	int fd;
#if defined(__linux__)
	if (realfilename.substr(0, 8) == "/dev/shm")
	{
		fd = shm_open(realfilename.substr(8, string::npos).c_str(), openflags, 0);
	}
#elif defined(__sun__)
	if (realfilename.substr(0, 10) == "/tmp/.SHMD")
	{
		realfilename[9] = '/';
		fd = shm_open(realfilename.substr(9, string::npos).c_str(), openflags, 0);
	}
#else
#warning Shared memory mapping file prefix not known for this platform.
	if (0) ;
#endif
	else
	{
		fd = open(realfilename.c_str(), openflags);
	}

	assert(fd != -1);  // assert open did not fail

	// Map memory.
	//
	void* mapaddress = mmap(NULL, size, memoryprotection, mmapflags, fd, 0);

	::close(fd);	// fd is no longer needed
	assert(mapaddress != MAP_FAILED);  // assert mmap did not fail

	return mapaddress;
}

void HashTable::deserialize(const string& fullname, unsigned int part)
{
	assert(part >= 0);
	assert(part < MAX_PART);

	unsigned int noparts = 1<<log2partitions;
	if (part >= noparts)
	{
		return;
	}

	assert(bucket[part] != NULL);

	size_t partsize = 
		(sizeof(BucketHeader)+bucksize) * ((nbuckets+noparts-1)/noparts);

	void* addr = attachmemsegment(fullname, partsize);
	memcpy(bucket[part], addr, partsize);

	int res = munmap(addr, partsize);
	assert(res == 0);
}

void HashTable::bucketclear(int thisthread, int totalthreads)
{
	unsigned long long thread = thisthread;

	unsigned int startoffset = static_cast<unsigned int>
		(((thread+0uLL)*nbuckets) / totalthreads);
	unsigned int endoffset   = static_cast<unsigned int>
		(((thread+1uLL)*nbuckets) / totalthreads);

	for (unsigned int i = startoffset; i < endoffset; ++i)
	{
		BucketHeader* bh = getBucketHeader(i);
		bh->clear();
	}
}

void HashTable::destroy()
{
	unsigned int noparts = 1<<log2partitions;

	for (unsigned int i = 0; i<noparts; ++i)
	{
		numadeallocate(bucket[i]);
		bucket[i] = NULL;
	}
}

void* HashTable::allocate(unsigned int offset, void* allocsource)
{
	BucketHeader* bhlast = NULL;

	for (BucketHeader* bh = getBucketHeader(offset)
			; bh != NULL
			; bh = bh->nextBucket)
	{
		dbgassertinitialized(bh);
		dbg2assert(bh->used <= bucksize);
		if (bh->used + tuplesize <= bucksize) 
		{
			// Fast path: it fits!
			//
			void* freeloc = ((char*)bh) + sizeof(BucketHeader) + bh->used;
			bh->used += tuplesize;
			return freeloc;
		}

		bhlast = bh;
	}

	// Overflow. Allocate new page and chain after current bucket.
	// Allocations because of overflow always allocate NUMA-local memory.
	//
	dbgassert(bhlast != NULL);
	dbgassert(bhlast->nextBucket == NULL);
	dbgassert(tuplesize <= bucksize);

	atomic_increment(&spills);
	
	void* newbuck = NULL;
	newbuck = numaallocate_local("HTbO", sizeof(BucketHeader) + bucksize, allocsource);
	assert(newbuck != NULL);

	BucketHeader* bhnew = (BucketHeader*) newbuck;
	bhnew->clear();
	bhnew->used += tuplesize;

	bhlast->nextBucket = bhnew;

	void* freeloc = ((char*)newbuck) + sizeof(BucketHeader);
	return freeloc;
}

HashTable::Iterator HashTable::createIterator()
{
	return Iterator(bucksize, tuplesize);
}

HashTable::Iterator::Iterator(unsigned int bucksize, unsigned int tuplesize)
	: cur(0), free(0), nxt(0), bucksize(bucksize), tuplesize(tuplesize)
{
}

vector<unsigned int> HashTable::statBuckets()
{
	vector<unsigned int> ret;

	HashTable::Iterator it = createIterator();
	for (unsigned int i=0; i<getNumberOfBuckets(); ++i)
	{
		placeIterator(it, i);
		unsigned int count = 0;

		while (it.next())
		{
			++count;
		}

		if (count >= ret.size())
		{
			ret.resize(count+1, 0);
		}

		++ret.at(count);
	}

	return ret;
}

