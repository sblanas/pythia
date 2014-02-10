
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

#include "operators.h"
#include "operators_priv.h"
#include "../rdtsc.h"
#include "../util/numaallocate.h"

using std::make_pair;

static Operator::Page* NullPage = 0;

void 
PartitionOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	// Attribute to sort and partition on, starting from 0. Must be numeric.
	//
	attribute = node["attr"];

	// Hash function & total threads involved. 
	// This is also the number of output partitions.
	//
	node.add("field", libconfig::Setting::TypeInt) = (int) attribute;
	node.add("fn", libconfig::Setting::TypeString) = "exactrange";
	hashfn = TupleHasher::create(schema, node);
	node.remove("fn");
	node.remove("field");
	assert(hashfn.buckets() < MAX_THREADS);
	barrier.init(hashfn.buckets());

	// Compute max size of staging area used for buffering input.
	// Allow for a per-thread variance of 20 buffers + 30% of input size.
	//
	unsigned long maxtuples;
	if (node.exists("maxtuplesinM"))
	{
		maxtuples = node["maxtuplesinM"];
		maxtuples *= 1024 * 1024;
	}
	else
	{
		maxtuples = node["maxtuples"];
	}
	perthreadtuples = 20 * buffsize/nextOp->getOutSchema().getTupleSize() 
		+ (maxtuples * 1.3 / hashfn.buckets());

	// Sorting output?
	//
	string ihatelibconfig = node["sort"];
	sortoutput = (ihatelibconfig == "yes");

	if (sortoutput)
	{
		sortattribute = attribute;
		node.lookupValue("sortattr", sortattribute);
	}
	else
	{
		sortattribute = 0xFFFF;
	}

	// Create state, output and build/probe staging areas.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		partitionstate.push_back(NULL);
		input.push_back(NULL);
	}
}

PartitionOp::PartitionState::PartitionState()
	: bufferingcycles(0), sortcycles(0), usedtuples(0), 
	  outputloc(0), trueoutput(EmptyPage)
{
	for (unsigned short t=0; t<MAX_THREADS; ++t)
	{
		tuplesforpartition[t] = 0;
		idxstart[t] = 0;
	}
}

void 
PartitionOp::threadInit(unsigned short threadid)
{
	void* space;

	space = numaallocate_local("PRTs", sizeof(PartitionState), this);
	memset(space, 0xAB, sizeof(PartitionState));
	partitionstate[threadid] = new (space) PartitionState();

	unsigned int tuplesize;
	tuplesize = nextOp->getOutSchema().getTupleSize();

	space = numaallocate_local("PRTi", sizeof(Page), this);
	input[threadid] = new (space) 
		Page(perthreadtuples*tuplesize, tuplesize, this, "PRTi");
	
	output[threadid] = NULL;
}

void 
PartitionOp::threadClose(unsigned short threadid)
{
	if (partitionstate[threadid]) 
	{
		numadeallocate(partitionstate[threadid]);
	}
	partitionstate[threadid] = NULL;

	assert(partitionstate[threadid] == NULL);

	if (output[threadid]) 
	{
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;
}

// The following functions are reused in sortandrangepartition.cpp and
// join.cpp, so hiding from global namespace here.
//
namespace {	

/**
 * Copies all tuples from source operator \a op into staging area \a page, and
 * popoulates histogram \a hist by hashing values using a \a hashfn.
 * Assumes operator has scan-started successfully for this threadid.
 * Error handling is non-existant, asserts if anything is not expected.
 */
void copySourceIntoPageAndPopulateHistogram(Operator* op, Operator::Page* page, 
		unsigned short threadid, unsigned int* hist, TupleHasher& hashfn)
{
	Operator::Page::Iterator it = EmptyPage.createIterator();
	Schema& s = op->getOutSchema();

	Operator::GetNextResultT result;
	result.first = Operator::Ready;

	while (result.first == Operator::Ready)
	{
		result = op->getNext(threadid);
		assert(result.first != Operator::Error);

		void* tup = NULL;
		it.place(result.second);

		while( (tup = it.next()) )
		{
			// Hash tup, update histogram.
			//
			unsigned int h = hashfn.hash(tup);
			dbgassert(h < hashfn.buckets());
			++hist[h];

			// Copy tup into page.
			//
			void* space = page->allocateTuple();
			assert(space != NULL);
			s.copyTuple(space, tup);
		}
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

void
repartition (Schema& schema, Operator::Page* in, 
		unsigned int* idxstart, vector<Operator::Page*>& out, TupleHasher& hashfn)
{
	Operator::Page::Iterator it = in->createIterator();
	void* tup = NULL;
	while ( (tup = it.next()) )
	{
		// Hash tup, update histogram.
		//
		unsigned int h = hashfn.hash(tup);
		dbgassert(h < hashfn.buckets());

		// Copy tup into out[idxstart[h]], increment idxstart[h].
		//
		void* dest = out[h]->getTupleOffset(idxstart[h]);
		dbgassert(dest != NULL);
		++idxstart[h];
		schema.copyTuple(dest, tup);
	}
}

Operator::ResultCode 
PartitionOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	PartitionState* state = partitionstate[threadid];

	// Copy chunks into staging area input[threadid].
	//
	assert(Ready == nextOp->scanStart(threadid, indexdatapage, indexdataschema));
	startTimer(&state->bufferingcycles);
	copySourceIntoPageAndPopulateHistogram(nextOp, input[threadid], 
			threadid, state->tuplesforpartition, hashfn);
	stopTimer(&state->bufferingcycles);
	assert(Ready == nextOp->scanStop(threadid));
	state->usedtuples = input[threadid]->getUsedSpace() / schema.getTupleSize();
	
	// Wait on barrier for all histograms to be built. 
	// Combine histograms to compute output target. Each thread computes the
	// targets for all other threads in its output partition. 
	//
	barrier.Arrive();
	for (unsigned int i=1; i < hashfn.buckets(); ++i)
	{
		unsigned int j = threadid;
		dbgassert(j >= 0);
		dbgassert(j < hashfn.buckets());
		dbgassert(partitionstate[0]->idxstart[j] == 0);

		// Thread i start point for partition j is thread i-1 start
		// point for j plus thread i-1 tuples for j.
		//
		partitionstate[i]->idxstart[j] = 
			partitionstate[i-1]->idxstart[j] +
			partitionstate[i-1]->tuplesforpartition[j];
	}

	// Wait on barrier for output targets for all partitions. 
	// Compute how much space is needed for this thread's output, then
	// allocate.
	//
	barrier.Arrive();
	unsigned long long tuplesinthispartition = 
		partitionstate[hashfn.buckets()-1]->idxstart[threadid] +
		partitionstate[hashfn.buckets()-1]->tuplesforpartition[threadid];
	void* space;
	space = numaallocate_local("PRTo", sizeof(Page), this);
	output[threadid] = new (space) Page(
			tuplesinthispartition * schema.getTupleSize(), schema.getTupleSize(), this, "PRTo");
	space = output[threadid]->allocate(tuplesinthispartition * schema.getTupleSize());
	assert(space != NULL);

	// Wait on barrier for allocation to complete. Then repartition.
	// 
	barrier.Arrive();
	repartition(schema, input[threadid], state->idxstart, output, hashfn);

	// Release unneeded memory.
	//
	if (input[threadid]) 
	{
		numadeallocate(input[threadid]);
	}
	input[threadid] = NULL;

	// Wait for other threads to complete writes to this thread's output.
	// If sorting, do it now; no need to syncrhonize.
	//
	barrier.Arrive();
	if (sortoutput)
	{
		startTimer(&state->sortcycles);
#ifdef BITONIC_SORT
//		system("./systemstats-pre.sh");
		output[threadid]->bitonicsort();
//		system("./systemstats-post.sh");
#else
		sortAllInPage(output[threadid], schema, sortattribute);
#endif
		stopTimer(&state->sortcycles);
#ifdef DEBUG
		verifysorted(output[threadid], schema, sortattribute);
#endif
	}

	// All set.
	//
	return Ready;
}

Operator::ResultCode 
PartitionOp::scanStop(unsigned short threadid)
{
	// Wait on barrier for threads that are working on this thread's input.
	//
	barrier.Arrive();

	// Forget data in staging area for thread. 
	// scanStop does not free memory, this is done at threadClose.
	// 
	output[threadid]->clear();
	
	return Ready;
}

Operator::GetNextResultT 
PartitionOp::getNext(unsigned short threadid)
{
	unsigned int tupsz = schema.getTupleSize();
	unsigned int maxtuplesout = buffsize / tupsz;
	PartitionState* state = partitionstate[threadid];

	void* start = output[threadid]->getTupleOffset(state->outputloc);

	// Output minimum of (maxtupleout, remainingtups) tuples.
	// Check condition with if to return Finished or Ready as needed.
	//
	dbgassert(state->outputloc <= output[threadid]->getUsedSpace() / tupsz);
	unsigned int remainingtups = (output[threadid]->getUsedSpace() / tupsz) - state->outputloc;

	if (remainingtups <= maxtuplesout)
	{
		if (start == NULL)
			state->trueoutput = EmptyPage;
		else
			state->trueoutput = Page(start, remainingtups * tupsz, NULL, tupsz);
		state->outputloc += remainingtups;
		return make_pair(Finished, &state->trueoutput);
	}

	state->trueoutput = Page(start, maxtuplesout * tupsz, NULL, tupsz);
	state->outputloc += maxtuplesout;
	return make_pair(Ready, &state->trueoutput);
}
