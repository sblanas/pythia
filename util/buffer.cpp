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

#include "buffer.h"
#include "custom_asserts.h"

#ifdef DEBUG
#include <cstring>
#endif

#ifndef NULL
#define NULL 0
#endif

#include "numaallocate.h"

Buffer::Buffer(void* data, unsigned long long size, void* free)
	: data(data), maxsize(size), owner(false), free(free)
{ 
	if (free == NULL)
		this->free = reinterpret_cast<char*>(data)+size;
}

Buffer::Buffer(unsigned long long size, void* allocsource, const char tag[4]) 
	: maxsize(size), owner(true)
{
	data = numaallocate_local(tag, size, allocsource);
	dbgassert(data != NULL);
#ifdef DEBUG
	memset(data, 0xBF, size);
#endif
	free = data;
}

Buffer::~Buffer() 
{
	if (owner)
		numadeallocate(data);

	data = 0;
	free = 0;
	maxsize = 0;
	owner = false;
}

TupleBuffer::TupleBuffer(unsigned long long size, unsigned int tuplesize, void* allocsource, const char tag[4])
	: Buffer(size, allocsource, tag), tuplesize(tuplesize)
{ 
	// Sanity check: Fail if page doesn't fit even a single tuple.
	//
	dbgassert(size >= tuplesize);
}

TupleBuffer::TupleBuffer(void* data, unsigned long long size, void* free, unsigned int tuplesize)
	: Buffer(data, size, free), tuplesize(tuplesize)
{ 
	// Sanity check: Fail if page doesn't fit even a single tuple.
	//
	dbgassert(size >= tuplesize);
}

#ifdef BITONIC_SORT
#include "bitonicsort.cpp"
#endif

void
TupleBuffer::bitonicsort()
{
	unsigned long long tuples = getNumTuples();

	assert(tuplesize == 8);
	assert(tuples > 0x1000);
	assert(tuples != 0);
	assert((tuples & (tuples-1)) == 0);

#ifdef BITONIC_SORT
	dobitonicsort((record_t*)data, tuples);
#else
	throw UnknownAlgorithmException();
#endif
}
