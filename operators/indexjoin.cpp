
/*
 * Copyright 2013, Pythia authors (see AUTHORS file).
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

#define TRACE( x )

void IndexHashJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	HashJoinOp::init(root, node);

	// Find and store index data page schema.
	//
	idxdataschema.add(buildOp->getOutSchema().getColumnType(joinattr1));

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		idxdatapage.push_back(NULL);
	}
}

void IndexHashJoinOp::threadInit(unsigned short threadid)
{
	HashJoinOp::threadInit(threadid);

	// Find groupno and allocate buffer with size twice as big as the expected
	// number of keys in the table (calculated as buckets * tuplesperbucket).
	//
	const unsigned short groupno = threadgroups.at(threadid);
	const unsigned long long idxdatasize = 
		2 * hashtable[groupno].getNumberOfBuckets() 
		* buildpagesize/sbuild.getTupleSize() * sbuild.getColumnWidth(joinattr1);
	
	void* space = numaallocate_local("iHJd", sizeof(Page), this);
	idxdatapage[threadid] = new (space) Page(idxdatasize, 
			idxdataschema.getTupleSize(), this, "iHJd");
}

Operator::ResultCode IndexHashJoinOp::scanStart(unsigned short threadid,
		Page* thrudatapage, Schema& thrudataschema)
{
	dbgassert(threadid < threadgroups.size());
	const unsigned short groupno = threadgroups[threadid];
	dbgassert(groupno < barriers.size());

	TRACE('1');

	// Build hash table.
	//
	GetNextResultT result;
	result.first = Operator::Ready;
	ResultCode rescode;

	rescode = buildOp->scanStart(threadid, thrudatapage, thrudataschema);
	if (rescode == Operator::Error) {
		return Error;
	}

	Schema& buildschema = buildOp->getOutSchema();
	while (result.first == Operator::Ready) 
	{
		result = buildOp->getNext(threadid);

		if (result.first == Operator::Error)
			return Error;

		Page* page = result.second;
		void* tup = NULL;
		unsigned int hashbucket;

		Page::Iterator it = page->createIterator();
		while( (tup = it.next()) ) 
		{
			// Find destination bucket.
			hashbucket = buildhasher.hash(tup);
			void* joinkey = buildschema.calcOffset(tup, joinattr1);

			// Copy key to idxdata page.
			void* idxdatatup = idxdatapage[threadid]->allocateTuple();
			assert(idxdatatup != NULL);
			idxdataschema.writeData(idxdatatup, 0, joinkey);

			// Project on build, copy result to target.
			void* target = hashtable[groupno].atomicAllocate(hashbucket, this);
			sbuild.writeData(target, 0, joinkey);
			for (unsigned int j=0, buildattrtarget=0; j<projection.size(); ++j) 
			{
				if (projection[j].first != BuildSide)
					continue; 

				unsigned int attr = projection[j].second;
				sbuild.writeData(target, buildattrtarget+1,	// dest, col in output
						buildschema.calcOffset(tup, attr));	// src 
				buildattrtarget++;
			}
		}
	}

	if (result.first == Operator::Error) {
		return Error;
	}

	rescode = buildOp->scanStop(threadid);
	if (rescode == Operator::Error) {
		return Error;
	}

	TRACE('2');

	// This thread is complete. Wait for other threads in the same group (ie.
	// partition) before you continue, or this thread might lose data.
	//
	barriers[groupno].Arrive();

	TRACE('3');

	// Hash table is complete now, every thread can proceed.
	// Handle first call: 
	// 0. Start scan on probe.
	// 1. Make state.location point at first output tuple of probeOp->GetNext.
	// 2. Place htiter and pgiter.
	
	rescode = probeOp->scanStart(threadid, idxdatapage[threadid], idxdataschema);
	if (rescode == Operator::Error) {
		return Error;
	}

	void* tup2;
	tup2 = readNextTupleFromProbe(threadid);
	hashjoinstate[threadid]->location = tup2;

	if (tup2 != NULL) {
		unsigned int bucket = probehasher.hash(tup2);
		hashtable[groupno].placeIterator(hashjoinstate[threadid]->htiter, bucket);
	} else {
		// Probe is empty?!
		rescode = Finished;
	}

	TRACE('4');

	return rescode;
}

Operator::ResultCode IndexHashJoinOp::scanStop(unsigned short threadid) 
{
	ResultCode ret = probeOp->scanStop(threadid);

	// Clear index data page.
	//
	idxdatapage[threadid]->clear();

	return ret;
}

void IndexHashJoinOp::threadClose(unsigned short threadid)
{
	HashJoinOp::threadClose(threadid);

	// Deallocate index data page.
	//
	if (idxdatapage[threadid]) {
		numadeallocate(idxdatapage[threadid]);
	}
	idxdatapage[threadid] = NULL;
}
