
/*
 * Copyright 2012, Pythia authors (see AUTHORS file).
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

/* 
 * THIS IS ABANDONED CODE.
 *
 * This was meant to be the produce-sorted partitions heavyweight, but I stop
 * maintaining it as MPSM doesn't need it, and it's unclear whether there's any
 * benefit at combining the two functions in one operator.
 */

#include "operators.h"
#include "operators_priv.h"
#include "../rdtsc.h"
#include "../util/numaallocate.h"

using std::make_pair;

static Operator::Page* NullPage = 0;

void 
SortAndRangePartitionOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	// Attribute to sort and partition on, starting from 0. Must be numeric.
	//
	attribute = node["attr"];
	switch(schema.getColumnType(attribute))
	{
		case CT_LONG:
			break;
		case CT_DATE:
			// DATE is locked down because range only accepts integers, not
			// strings. The partitionining algorithm will work out of the box
			// if a correct int is constructed for a desired date, but the
			// code here doesn't (yet) do that.
			//
		default:
			throw NotYetImplemented();
	};

	// Total threads involved. This is also the number of output partitions.
	//
	unsigned int ihatelibconfig = node["threads"];
	threads = ihatelibconfig;
	barrier.init(threads);

	// Key range. This is used to compute per-partition ranges.
	//
	mininclusive.reserve(threads);
	maxexclusive.reserve(threads);
	unsigned long minkey = node["keyrange"][0];
	unsigned long maxkey = node["keyrange"][1];
	unsigned long step = (maxkey - minkey + 1)/threads;
	for (unsigned int i=0; i<threads-1; ++i)
	{
		mininclusive.push_back(i*step+minkey);
		maxexclusive.push_back((i+1)*step+minkey);
	}	
	mininclusive.push_back((threads-1)*step+minkey);
	maxexclusive.push_back(maxkey);

	// Compute max size of staging area used for sorting inputs.
	//
	int maxtuples;
	maxtuples = node["maxtuples"];

	// Allow for a per-thread variance of 20 buffers + 10% of input size.
	// This is an ad-hoc metric that worked for SortMergeJoinOp, just
	// replicating it here for consistency. :)
	//
	perthreadtuples = 20 * buffsize/nextOp->getOutSchema().getTupleSize() 
		+ (maxtuples * 1.1 / threads);

	// Is input already sorted?
	//
	string str = node["presorted"];
	presorted = (str == "yes");
	
	// Create state, output and build/probe staging areas.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		partitionstate.push_back(NULL);
		input.push_back(NULL);
	}
}

SortAndRangePartitionOp::PartitionState::PartitionState()
	: sortcycles(0), usedbytes(0)
{
}

void 
SortAndRangePartitionOp::threadInit(unsigned short threadid)
{
	void* space;

	space = numaallocate_local("SRPs", sizeof(PartitionState), this);
	memset(space, 0xAB, sizeof(PartitionState));
	partitionstate[threadid] = new (space) PartitionState();

	unsigned int tuplesize;
	tuplesize = nextOp->getOutSchema().getTupleSize();

	space = numaallocate_local("SRPi", sizeof(Page), this);
	input[threadid] = new (space) 
		Page(perthreadtuples*tuplesize, tuplesize, this);
	
	space = numaallocate_local("SRPo", sizeof(Page), this);
	output[threadid] = new (space) Page(buffsize, schema.getTupleSize(), this);
}

void 
SortAndRangePartitionOp::threadClose(unsigned short threadid)
{
	if (partitionstate[threadid]) 
	{
		numadeallocate(partitionstate[threadid]);
	}
	partitionstate[threadid] = NULL;

	if (input[threadid]) 
	{
		numadeallocate(input[threadid]);
	}
	input[threadid] = NULL;

	if (output[threadid]) 
	{
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;
}

// The following functions are reused in join.cpp, so hiding from global
// namespace here.
//
namespace {

/**
 * Copies all tuples from source operator \a op into staging area \a page.
 * Assumes operator has scan-started successfully for this threadid.
 * Error handling is non-existant, asserts if anything is not expected.
 */
void copySourceIntoPage(Operator* op, Operator::Page* page, unsigned short threadid)
{
	Operator::GetNextResultT result;
	result.first = Operator::Ready;

	while (result.first == Operator::Ready)
	{
		result = op->getNext(threadid);
		assert(result.first != Operator::Error);

		void* datastart = result.second->getTupleOffset(0);

		if (datastart == 0)
			continue;

		unsigned int datasize = result.second->getUsedSpace();
		void* space = page->allocate(datasize);
		assert(space != NULL);
		memcpy(space, datastart, datasize);
	}

	assert(result.first == Operator::Finished);
}

void verifysorted(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	void* tup1 = 0;
	void* tup2 = 0;
	Operator::Page::Iterator it = page->createIterator();
	Comparator comp = Schema::createComparator(
			schema, joinattr,
			schema, joinattr,
			Comparator::LessEqual);

	tup1 = it.next();
	if (tup1 == NULL)
		return;

	while ( (tup2 = it.next()) )
	{
		assert(comp.eval(tup1, tup2));
		tup1=tup2;
	}
}

/**
 * Sorts all tuples in given page.
 */
void sortAllInPage(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	unsigned int keyoffset = (unsigned long long)schema.calcOffset(0, joinattr);

	switch(schema.getColumnType(joinattr))
	{
		case CT_INTEGER:
			page->sort<CtInt>(keyoffset);
			break;
		case CT_LONG:
		case CT_DATE:
			page->sort<CtLong>(keyoffset);
			break;
		case CT_DECIMAL:
			page->sort<CtDecimal>(keyoffset);
			break;
		default:
			throw NotYetImplemented();
	}
}

};

Operator::ResultCode 
SortAndRangePartitionOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	PartitionState* state = partitionstate[threadid];

	// Copy chunks into staging area input[threadid].
	//
	assert(Ready == nextOp->scanStart(threadid, indexdatapage, indexdataschema));
	copySourceIntoPage(nextOp, input[threadid], threadid);
	assert(Ready == nextOp->scanStop(threadid));
	state->usedbytes = input[threadid]->getUsedSpace();
	
	// Sort build side.
	//
	startTimer(&state->sortcycles);
	if (presorted == false)
	{
		sortAllInPage(input[threadid], nextOp->getOutSchema(), attribute);
	}
	stopTimer(&state->sortcycles);
#ifdef DEBUG
	verifysorted(input[threadid], nextOp->getOutSchema(), attribute);
#endif
	
	// Wait on barrier.
	//
	barrier.Arrive();

	// Now all inputs sorted. Compute partition ranges.
	// Find locations of all partition minimums.
	// TODO
	throw NotYetImplemented();

	return Ready;
}

Operator::ResultCode 
SortAndRangePartitionOp::scanStop(unsigned short threadid)
{
	// Wait on barrier for threads that are working on this thread's input.
	//
	barrier.Arrive();

	// Forget data in staging area for thread. 
	// scanStop does not free memory, this is done at threadClose.
	// 
	input[threadid]->clear();
	
	return Ready;
}

Operator::GetNextResultT 
SortAndRangePartitionOp::getNext(unsigned short threadid)
{
	return make_pair(Error, NullPage);
}
