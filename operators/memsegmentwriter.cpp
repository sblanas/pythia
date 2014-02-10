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

#include "operators.h"
#include "operators_priv.h"

#ifdef ENABLE_NUMA
#define NUMA_VERSION1_COMPATIBILITY //< v1 is widely available 
#include <numa.h>
#else
#include <iostream>
using std::cerr;
using std::endl;
#endif

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

/**
 * Attempts to do a full copy of \a in from tuple \a idx into \a out.
 * If there is no space for a full copy, a partial copy of as much data as
 * possible is performed and \a idx is set to the first tuple that did not fit
 * to be written in \a out.
 * Code relies on the fact that tuples are laid out sequentially in memory.
 * @return True if full copy successful, false if partial copy performed.
 */
bool tryFullCopy(TupleBuffer* out, Operator::Page* in, int* idx, const int tuplesize) 
{
	// Try to get enough space for full copy: count bytes from in[idx] to end.
	//
	size_t reqspace = in->getUsedSpace() - ((*idx) * tuplesize);
	void* target = out->atomicAllocate(reqspace);

	if (target != NULL)
	{
		// Allocation succeeded, copy data and return.
		//
		memcpy(target, in->getTupleOffset(*idx), reqspace);
		return true;
	}

	// Full copy is impossible. Try allocating as much space that is
	// available and do partial copy. 
	//
	size_t availspacehint;

	do
	{
		availspacehint = (out->capacity() - out->getUsedSpace()) / tuplesize * tuplesize;
		dbgassert(reqspace > availspacehint);
		target = out->atomicAllocate(availspacehint);

		// Assert no infinite loop if full.
		//
		dbgassertimplies(availspacehint == 0, target != NULL);

	} while (target == NULL);

	// An allocation of "availspacehint" bytes was successful. 
	// Copy as many tuples that fit.
	//
	memcpy(target, in->getTupleOffset(*idx), availspacehint);
	*idx += availspacehint/tuplesize;
	return false;
}

/**
 * Truncates an existing shared memory segment to specified size. If the
 * requested size is zero, the file is removed.
 * @param fullname The path to use, /dev/shm/ is treated as a shared memory
 * segment regardless if it exists (ie. Linux) or not (ie. Solaris).
 * @param size Size to truncate the memory segment to, in bytes. If zero, the
 * file is removed using unlink(2).
 */
void truncatememsegment(const string& fullname, size_t size)
{
	int fd, res;

	// Remove file completely if truncating to zero.
	//
	if (size == 0)
	{
		res = unlink(fullname.c_str());
		if (res != 0)
			throw CreateSegmentFailure();
		return;
	}
	
	// Create shared mem segment.
	//
	if (fullname.substr(0, 8) == "/dev/shm")
	{
		const char* shmfilename = fullname.substr(8, string::npos).c_str();
		fd = shm_open(shmfilename, O_RDWR, 0);
	}
	else 
	{
		fd = open(fullname.c_str(), O_RDWR, 0);
	}

	if (fd == -1)
		throw CreateSegmentFailure();

	// Truncate file to bring it to appropriate size. 
	//
	res = ftruncate(fd, size);
	if (res != 0)
		throw CreateSegmentFailure();

	// Original file descriptor is not needed anymore. Close it.
	//
	close(fd);
}

/**
 * Creates a new shared memory segment of specified size using the filename
 * provided. 
 * @param fullname The path to use, /dev/shm/ is treated as a shared memory
 * segment regardless if it exists (ie. Linux) or not (ie. Solaris).
 * @param size Size of new memory segment, in bytes.
 */
void* creatememsegment(const string& fullname, size_t size)
{
	int fd, res;
	void* memory;

	// Create shared mem segment.
	//
	if (fullname.substr(0, 8) == "/dev/shm")
	{
		const char* shmfilename = fullname.substr(8, string::npos).c_str();
		fd = shm_open(shmfilename, O_RDWR | O_EXCL | O_CREAT, S_IRUSR | S_IWUSR);
	}
	else 
	{
		fd = open(fullname.c_str(), O_RDWR | O_EXCL | O_CREAT, S_IRUSR | S_IWUSR);
	}

	if (fd == -1)
		throw CreateSegmentFailure();

	// Map memory.
	// MAP_SHARED makes the updates visible to the outside world.
	// MAP_NORESERVE says not to preserve swap space.
	// MAP_LOCKED hits default 32K limit and fails.
	//
	memory = mmap(NULL, size, PROT_READ | PROT_WRITE, 
			MAP_SHARED | MAP_NORESERVE /* | MAP_LOCKED */, fd, 0);
	if (memory == MAP_FAILED)
		throw CreateSegmentFailure();

	// At this point we have the region in our virtual address space, but it
	// still doesn't point to anything and thus should not be touched.
	// *(unsigned long long*)memory = 42ull; //< would fail with SIGBUS
	//
	// Truncate file to bring it to appropriate size. 
	// Now writes will go through.
	//
	res = ftruncate(fd, size);
	if (res != 0)
		throw CreateSegmentFailure();

	// Original file descriptor is not needed anymore. Close it.
	//
	close(fd);

	return memory;
}

/**
 * Enforces specified NUMA policy for memory page at the unfaulted \a address
 * for \a size bytes.
 */
void enforcememorypolicy(const MemSegmentWriter::NumaPolicy policy, 
		const vector<unsigned short>& allnodes, const unsigned int currnode,
		void* address, const size_t size)
{
	if (policy == MemSegmentWriter::POLICY_UNSET)
		return;

#ifdef ENABLE_NUMA
	// Bind allocations to numa node at allnodes[currnode].
	//    OR 
	// Interleave allocations to allnodes. 
	//
	if (policy == MemSegmentWriter::POLICY_INTERLEAVE)
	{
		nodemask_t mask;
		nodemask_zero(&mask);
		for (unsigned int idx=0; idx<allnodes.size(); ++idx)
		{
			unsigned int node = allnodes[idx];
			nodemask_set(&mask, node);
		}
		numa_interleave_memory(address, size, &mask);
	}
	else
	{
		dbgassert((currnode >= 0) && (currnode < allnodes.size()));
		unsigned int node = allnodes[currnode];
		numa_tonode_memory(address, size, node);
	}
#endif
}

/**
 * Increments text counter.
 * @throws QueryExecutionError if counter overflowed and will rewrite old data.
 */
void incrementCounter(string& counter)
{
	bool carry = true;
	for (int pos = counter.length()-1; carry && (pos >= 0); --pos)
	{
		if (counter[pos] == '9')
		{
			counter[pos] = '0';
			carry = true;
		}
		else 
		{
			dbgassert(counter[pos] >= '0' && counter[pos] <= '9');
			counter[pos]++;
			carry = false;
		}
	}

	// Check if counter overflowed, and will rewrite old data.
	//
	if (carry)
		throw QueryExecutionError();
}

void MemSegmentWriter::init(libconfig::Config& root, libconfig::Setting& node)
{
	Operator::init(root, node);

	// Change buffer size to the output size.
	//
	buffsize = node["size"];

	// Parse policy string. Might not specify any numa policy.
	//
	if (node.exists("policy"))
	{
		string policystr = node["policy"];
#ifndef ENABLE_NUMA
		cerr << " ** NUMA POLICY WARNING: Memory policy is ignored, "
			<< "NUMA disabled at compile." << endl;
#endif
		if (policystr == "bind")
			policy = POLICY_BIND;
		else if (policystr == "round-robin")
			policy = POLICY_RR;
		else if (policystr == "interleave")
			policy = POLICY_INTERLEAVE;
	}

	// Fill numa nodes array. Might not be present if policy is not set.
	//
	if (node.exists("numanodes"))
	{
		libconfig::Setting& numafield = node["numanodes"];
		if (numafield.isAggregate())
		{
			for (int i=0; i<numafield.getLength(); ++i)
			{
				int numano = numafield[i];
				numanodes.push_back(numano);
			}
		}
		else
		{
			dbgassert(numafield.isNumber());
			int numano = numafield;
			numanodes.push_back(numano);
		}
		dbgassert(numanodes.size() != 0);
	}

	// Fill path prefixes array. Must be present, regardless of numa policy.
	//
	libconfig::Setting& pathsfield = node["paths"];
	if (pathsfield.isAggregate())
	{
		for (int i=0; i<pathsfield.getLength(); ++i)
		{
			string path = pathsfield[i];
			paths.push_back(path);
		}
	}
	else
	{
		dbgassert(pathsfield.isScalar());
		string path = pathsfield;
		paths.push_back(path);
	}
	dbgassert(paths.size() != 0);

	// Check inputs.
	//
	switch (policy)
	{
		// Bind policy only works on a single node, single path.
		//
		case POLICY_BIND:
			if ( ! (numanodes.size() == 1 && paths.size() == 1) )
				throw InvalidParameter();
			break;

		// Round-robin policy rotates through nodes, so #nodes==#paths.
		//
		case POLICY_RR:
			if ( ! (numanodes.size() == paths.size()) )
				throw InvalidParameter();
			break;

		// Interleave policy assignes OS pages of the output to different nodes.
		// Only one output path makes sense.
		//
		case POLICY_INTERLEAVE:
			if ( ! (numanodes.size() >= 1 && paths.size() == 1) )
				throw InvalidParameter();
			break;

		// No NUMA policy has been requested. Write to the single output path.
		//
		case POLICY_UNSET:
			if ( ! (paths.size() == 1) )
				throw InvalidParameter();
			break;

		// How did we get here?!
		//
		default:
			throw InvalidParameter();
	}

#ifdef ENABLE_NUMA
	numa_set_strict(1);
	numa_set_bind_policy(1);
#endif
}

void MemSegmentWriter::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);
}

Operator::GetNextResultT MemSegmentWriter::getNext(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	TupleBuffer* out = NULL;
	void* segmentstart = NULL;

	unsigned int currentnode = 0;
	const unsigned int tuplesize = nextOp->getOutSchema().getTupleSize();

	Operator::GetNextResultT result;

	segmentstart = creatememsegment(paths[currentnode] + counter, buffsize);
	enforcememorypolicy(policy, numanodes, currentnode, segmentstart, buffsize);
	out = new TupleBuffer(segmentstart, buffsize, segmentstart, tuplesize);

	do
	{
		result = nextOp->getNext(threadid);

		if (result.first == Error)
			return result;

		int tupleidx = 0;

		// Copy data from result.second into out. Allocate new memory segment
		// if data doesn't fit in current output segment.
		//
		while ( tryFullCopy(out, result.second, &tupleidx, tuplesize) == false )
		{
			// Output full. Close \a old and truncate to real size.
			//
			int res = munmap(segmentstart, buffsize);
			dbgassert(res == 0);
			segmentstart = NULL;

			unsigned int realsize = out->getUsedSpace();
			truncatememsegment(paths[currentnode] + counter, realsize);

			delete out;
			out = NULL;

			// Cycle through paths, if at path 0, increment counter.
			//
			currentnode += 1;
			currentnode %= paths.size();

			if (currentnode == 0)
				incrementCounter(counter);

			// Allocate new segment and enforce chosen memory policy.
			//
			segmentstart = creatememsegment(paths[currentnode] + counter, buffsize);
			enforcememorypolicy(policy, numanodes, currentnode, segmentstart, buffsize);
			out = new TupleBuffer(segmentstart, buffsize, segmentstart, tuplesize);

		}

	} while (result.first == Ready);

	// Unmap last segment and truncate to its real size.
	//
	int res = munmap(segmentstart, buffsize);
	dbgassert(res == 0);
	segmentstart = NULL;

	unsigned int realsize = out->getUsedSpace();
	truncatememsegment(paths[currentnode] + counter, realsize);

	delete out;
	out = NULL;

	return std::make_pair(Finished, &EmptyPage);
}

void MemSegmentWriter::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);
}

