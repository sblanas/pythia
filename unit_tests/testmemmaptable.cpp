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

#include "common.h"
#include "../operators/loaders/table.h"

#include <errno.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

const int MAXTUPLES = 100;	//< Maximum number of tuples per file.

Schema s;

void populate(TupleBuffer* bf, int tuples, int offset)
{
	for (int i=offset; i<tuples+offset; ++i)
	{
		CtInt col1 = i;
		CtInt col2 = i+1;

		void* tuple = bf->allocateTuple();
		assert(tuple);
		s.writeData(tuple, 0, &col1);
		s.writeData(tuple, 1, &col2);
	}
}

void* createsharedmem(const char* shmname, unsigned int length)
{
	assert(length != 0);

	// Create shared mem segment.
	//
	int fd;
	fd = shm_open(shmname, O_RDWR | O_EXCL | O_CREAT, S_IRUSR | S_IWUSR);
	assert(fd != -1);

	// Map memory.
	// MAP_SHARED makes the updates visible to the outside world.
	// MAP_NORESERVE says not to preserve swap space.
	// MAP_LOCKED hits default 32K limit and fails.
	//
	void* memory;
	memory = mmap(NULL, length, PROT_READ | PROT_WRITE, 
			MAP_SHARED | MAP_NORESERVE /* | MAP_LOCKED */, fd, 0);
	if (memory == MAP_FAILED)
	{
		perror("mmap()");
		fail("mmap failed");
	}

	// At this point we have the region in our virtual address space, but it
	// points to an empty file and thus should not be touched.
	// *(unsigned long long*)memory = 42ull; //< would fail with SIGBUS

	// Truncate empty file to extend it to appropriate size. 
	// Now writes will go through.
	//
	int ret;
	ret = ftruncate(fd, length);
	assert(ret == 0);

	close(fd);
	return memory;
}

int prepareonefile(const char* name, int offset)
{
	int tuples = (lrand48() % (MAXTUPLES-1)) + 1;
	int length = tuples * s.getTupleSize();

	void* mem = createsharedmem(name, length);
	TupleBuffer tb(mem, length, mem, s.getTupleSize());
	populate(&tb, tuples, offset);
	assert(munmap(mem, length) == 0);

	return tuples;
}

int prepare()
{
	int tuples = 0;
	tuples += prepareonefile("/memmaptable.first.1", tuples);
	tuples += prepareonefile("/memmaptable.first.2", tuples);
	tuples += prepareonefile("/memmaptable.second", tuples);
	return tuples;
}

void cleanup()
{
	shm_unlink("/memmaptable.first.1");
	shm_unlink("/memmaptable.first.2");
	shm_unlink("/memmaptable.second");
}

// Test:	
// create 3 files with same schema and known content
// table.load() opnes 2 files.
// table.load() opens 1 file.
// Read through them and verify content ok
//
void testload()
{
	int correctcount = prepare();

	MemMappedTable table;
	table.init(&s);

	if (table.load("/dev/shm/memmaptable.f*", "", Table::SilentLoad, Table::PermuteFiles) != MemMappedTable::LOAD_OK)
	{
		table.close();
		cleanup();
		fail("Load failed");
	}
	if (table.load("/dev/shm/memmaptable.s*", "", Table::SilentLoad, Table::PermuteFiles) != MemMappedTable::LOAD_OK)
	{
		table.close();
		cleanup();
		fail("Load failed");
	}

	TupleBuffer* bf;
	void* tuple;
	while ( (bf = table.readNext()) )
	{
		TupleBuffer::Iterator it = bf->createIterator();
		while ( (tuple = it.next()) )
		{
			CtInt col1 = s.asInt(tuple, 0);
			CtInt col2 = s.asInt(tuple, 1);

			if ((col1 < 0) || (col1 >= correctcount))
			{
				table.close();
				cleanup();
				fail("Value that was never generated appears on col1.");
			}

			if (col2 != col1 + 1)
			{
				table.close();
				cleanup();
				fail("col2 does not equal col1 + 1.");
			}
		}
	}

	table.close();

	cleanup();
}


int main()
{
	s.add(CT_INTEGER);
	s.add(CT_INTEGER);
	cleanup();

	srand48(time(NULL));
	testload();
	return 0;
}
