/*
 * Copyright 2007, Pythia authors (see AUTHORS file).
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

#include "table.h"
#include "loader.h"
#include <glob.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include "sfmt/SFMT.h"

#include "../../util/numaallocate.h"

void Table::init(Schema *s) 
{
	_schema = s;
	data = 0;
	cur = 0;
}

void Table::close() {
	LinkedTupleBuffer* t = data;
	while (t) {
		t=data->getNext();
		delete data;
		data = t;
	}

	reset();
}

void* PreloadedTextTable::allocateTuple()
{
	unsigned int s = _schema->getTupleSize();

	if (!last->canStore(s)) {
		// create a new bucket
		LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size, s, this);
		// link it as the next bucket of last
		last->setNext(tmp);
		// make last point to the new bucket
		last = tmp;
	}

	void* target = last->allocateTuple();
	dbg2assert (target!=NULL);

	return target;
}


void PreloadedTextTable::append(const char** data, unsigned int count) 
{
	void* target = allocateTuple();
	dbg2assert(count==_schema->columns());
	_schema->parseTuple(target, data);
}

void PreloadedTextTable::append(const vector<string>& input) 
{
	void* target = allocateTuple();
	_schema->parseTuple(target, input);
}

void PreloadedTextTable::append(const void* const src) 
{
	void* target = allocateTuple();
	_schema->copyTuple(target, src);
}

void PreloadedTextTable::nontemporalappend16(const void* const src) {
	unsigned int s = _schema->getTupleSize();
	dbgassert(s==16);

	if (!last->canStore(s)) {
		// create a new bucket
		LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size, s, this);
		// link it as the next bucket of last
		last->setNext(tmp);
		// make last point to the new bucket
		last = tmp;
	}

	void* target = last->allocateTuple();
	dbg2assert(target!=NULL);
#ifdef __x86_64__
	__asm__ __volatile__ (
			"	movntiq %q2, 0(%4)	\n"
			"	movntiq %q3, 8(%4)	\n"
			: "=m" (*(unsigned long long*)target), "=m" (*(unsigned long long*)((char*)target+8))
			: "r" (*(unsigned long long*)((char*)src)), "r" (*((unsigned long long*)(((char*)src)+8))), "r" (target)
			: "memory");
#else
#warning MOVNTI not known for this architecture
	_schema->copyTuple(target, src);
#endif
}

void PreloadedTextTable::concatenate(const PreloadedTextTable& table)
{
#ifdef DEBUG
	assert(_schema->getTupleSize() == table._schema->getTupleSize());
#endif
	if (data == 0) {
		data = table.data;
	}
	last->setNext(table.data);
	last = table.last;
}

void PreloadedTextTable::init(Schema* s, unsigned int size)
{
	Table::init(s);

	this->size=size;
	data = new LinkedTupleBuffer(size, s->getTupleSize(), this);
	last = data;
	cur = data;
}

/**
 * Returns end of chain starting at \a head, NULL if \a head is NULL.
 */
LinkedTupleBuffer* findChainEnd(LinkedTupleBuffer* head)
{
	LinkedTupleBuffer* prev = NULL;
	while (head) 
	{
		prev = head;
		head = head->getNext();
	}
	return prev;
}

Table::LoadErrorT MemMappedTable::load(const string& filepattern, 
		const string& separators, VerbosityT verbose /* ignored */,
		GlobParamT globparam)
{
	LoadErrorT ret;

	// MAP_PRIVATE makes updates invisible to the outside world. 
	//  (Okay since we don't update.)
	// MAP_NORESERVE says not to preserve swap space.
	// MAP_POPULATE populates the page table now, versus on first access. Since
	//  in our experiments we know we're going to need the data, we populate
	//  the page table now to remove this cost from the critical path.
	//  (Populating the page table is a costly operation since page table
	//  locking is involved, barring other threads of the same process from
	//  doing so.)
	// MAP_LOCKED hits default 32K limit and fails.
	//
	ret = doload(filepattern, 
			O_RDONLY, 
			PROT_READ, 
			MAP_PRIVATE | MAP_NORESERVE | MAP_POPULATE /* | MAP_LOCKED */,
			globparam == PermuteFiles ? GLOB_NOSORT : 0
			);
	reset();

	return ret;
}

Table::LoadErrorT MemMappedTable::doload(const string& filepattern,
		int openflags, int memoryprotection, int mmapflags, int globflags)
{
	// Check that schema has been initialized.
	//
	dbgassert(_schema != NULL);
	dbgassert(_schema->getTupleSize() != 0);

	glob_t globout;
	int ret;
	bool nothingloaded = true;
	LinkedTupleBuffer* last = findChainEnd(data); 
	string realfilepattern(filepattern);

#if defined(__linux__)
	// We have adopted linux as a default, nothing extra to do.
	//
#elif defined(__sun__)
	// Solaris maps segments at /tmp/.SHMD* instead, /dev/shm/ does not exist.
	// Make searches on /dev/shm/* go to /tmp/.SHMD*
	//
	if (filepattern.substr(0, 9) == "/dev/shm/")
	{
		realfilepattern = "/tmp/.SHMD" + filepattern.substr(9, string::npos);
	}
#else
#warning Shared memory mapping file prefix not known for this platform.
#endif

	ret = glob(realfilepattern.c_str(), GLOB_MARK | globflags, 0, &globout);
	if (ret != 0)
	{
		perror("MemMappedTable::doload: glob() failed");
		return GLOB_FAILED;
	}

	// If NOSORT has been specified, permute returned array.
	// Otherwise, permutation array is identity function. 
	//
	assert(globout.gl_pathc < 0xFFFFFFull);
	vector<unsigned long long> permutator;
	permutator.resize(globout.gl_pathc, 0);
	init_gen_rand(time(NULL));
	for (unsigned int i=0; i<globout.gl_pathc; ++i)
	{
		if (globflags | GLOB_NOSORT)
		{
			permutator[i]  = gen_rand64() & (~0xFFFFFFull);
		}
		permutator[i] |= i & 0xFFFFFFull;
	}
	std::sort(permutator.begin(), permutator.end());

	// Create a LinkedTupleBuffer for each file, and link it.
	// 1. fd = shm_open (if "/dev/shm/*") or open (otherwise)
	// 2. fstat to get size
	// 3. mmap(fd)
	// 4. close(fd)
	// 5. Create a LinkedTupleBuffer on memory.
	for (size_t i=0; i<globout.gl_pathc; ++i)
	{
		unsigned long long idx = permutator.at(i) & 0xFFFFFFull;
		assert(idx < globout.gl_pathc);
		string filename = globout.gl_pathv[idx];

		// If a directory, skip.
		//
		if (filename[filename.length()-1] == '/')
			continue;

		// If a /dev/shm file, use shm_open, otherwise use open.
		//
		int fd;
#if defined(__linux__)
		if (filename.substr(0, 8) == "/dev/shm")
		{
			fd = shm_open(filename.substr(8, string::npos).c_str(), openflags, 0);
		}
#elif defined(__sun__)
		if (filename.substr(0, 10) == "/tmp/.SHMD")
		{
			filename[9] = '/';
			fd = shm_open(filename.substr(9, string::npos).c_str(), openflags, 0);
		}
#else
#warning Shared memory mapping file prefix not known for this platform.
		if (0) ;
#endif
		else
		{
			fd = open(filename.c_str(), openflags);
		}

		if (fd == -1)
		{
			perror("MemMappedTable::doload: open() failed");
			globfree(&globout);
			return OPEN_FAILED;
		}

		// Do fstat() to compute the size of the mapping. 
		// Skip if empty file, or if not regular file.
		//
		struct stat statbuf;
		int size;

		ret = fstat(fd, &statbuf);
		if (ret == -1)
		{
			perror("MemMappedTable::doload: fstat() failed");
			::close(fd);
			globfree(&globout);
			return FSTAT_FAILED;
		}

		size = statbuf.st_size;
		if (!S_ISREG(statbuf.st_mode) || (size == 0) )
		{
			::close(fd);
			continue;
		}

		nothingloaded = false;

		// Map memory.
		//
		void* mapaddress = mmap(NULL, size, memoryprotection, mmapflags, fd, 0);

		if (mapaddress == MAP_FAILED)
		{
			perror("MemMappedTable::doload: mmap() failed");
			::close(fd);
			globfree(&globout);
			return MMAP_FAILED;
		}

		::close(fd);	// fd is no longer needed

		// Create LinkedTupleBuffer on memory, and add it after "last".
		//
		void* space = numaallocate_local("Mtbl", sizeof(LinkedTupleBuffer), this);
		LinkedTupleBuffer* buf 
			= new (space)	LinkedTupleBuffer(mapaddress, size, NULL, _schema->getTupleSize());

		if (last != NULL)
		{
			last->setNext(buf);
		}
		else
		{	
			data = buf;
		}
		last = buf;

	}

	globfree(&globout);

	// If nothing was loaded, glob() probably returned directories, error out.
	//
	if (nothingloaded)
		return GLOB_FAILED;
	
	return LOAD_OK;
}

void MemMappedTable::close()
{
	LinkedTupleBuffer* head = data;

	// Unmap all segments.
	//
	while (head) 
	{
		int res = munmap(head->getTupleOffset(0), head->capacity());
		dbgassert(res == 0);
		head = head->getNext();
	}

	// Destroy buffers -- can't call Table::close because memory hasn't been
	// allocated via new.
	//
	LinkedTupleBuffer* t = data;
	while (t) {
		t=data->getNext();
		data->~LinkedTupleBuffer();
		numadeallocate(data);
		data = t;
	}

	reset();
}

Table::LoadErrorT PreloadedTextTable::load(const string& filepattern, 
		const string& separators, VerbosityT verbose, 
		GlobParamT globparam /* ignored */)
{
	Loader loader(separators);
	loader.load(filepattern, *this, verbose==VerboseLoad);
	return LOAD_OK;
}
