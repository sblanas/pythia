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
#include "../util/atomics.h"

#include <iostream>
using std::cerr;
using std::endl;

unsigned long readImm(volatile unsigned long* ptr)
{
	return *ptr;
}

static const char* state2error[3] = {
	"ThreadUninitialized",
	"ThreadInitialized",
	"ScanStarted"
};

void CallStateChecker::atomicallyTransitionTo(const unsigned short threadid, 
		const unsigned long oldstate,
		const unsigned long newstate)
{
	assert(objstate == ObjStateInitialized);

	unsigned long ret;

	ret = atomic_compare_and_swap(&threadstate[threadid], oldstate, newstate);
	if (ret != oldstate)
	{
		cerr << "Old state expected: " << state2error[oldstate] 
			<< ", Old state found: " << state2error[ret]
		    << ", Target state: " << state2error[newstate] << endl;
		assert(!"Illegal state found.");
	}

	assert(objstate == ObjStateInitialized);
}

void CallStateChecker::init(libconfig::Config& root, libconfig::Setting& node)
{
	assert(objstate == ObjStateUninitialized);

	SingleInputOp::init(root, node);
	schema = nextOp->getOutSchema();

	unsigned long long v = ThreadStateUninitialized;
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		threadstate.push_back(v);
	}

	if (ObjStateUninitialized != atomic_compare_and_swap(&objstate, ObjStateUninitialized, ObjStateInitialized))
		assert(!"Operator::init() called on operator that has been initialized already.");
}

void CallStateChecker::threadInit(unsigned short threadid)
{
	atomicallyTransitionTo(threadid, ThreadStateUninitialized, ThreadStateInitialized);
}

CallStateChecker::ResultCode CallStateChecker::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	atomicallyTransitionTo(threadid, ThreadStateInitialized, ThreadStateScanStarted);
	return nextOp->scanStart(threadid, indexdatapage, indexdataschema);
}

CallStateChecker::GetNextResultT CallStateChecker::getNext(unsigned short threadid)
{
	GetNextResultT ret;
	if (readImm(&threadstate[threadid]) == ThreadStateGetNextReturnedFinished)
	{
		for (int i=0; i<10; ++i)
		{
			// Assert that successive calls return Finished, and page is empty.
			//
			ret = nextOp->getNext(threadid);
			assert(ret.first == Finished);
			assert(ret.second->getTupleOffset(0) == NULL);
		}
		atomicallyTransitionTo(threadid, ThreadStateGetNextReturnedFinished, ThreadStateGetNextReturnedFinished);
	}
	else
	{
		atomicallyTransitionTo(threadid, ThreadStateScanStarted, ThreadStateScanStarted);
		ret = nextOp->getNext(threadid);
		if (ret.first != Error)
			assert(ret.second != NULL);
		if (ret.first == Finished)
			atomicallyTransitionTo(threadid, ThreadStateScanStarted, ThreadStateGetNextReturnedFinished);
	}
	return ret;
}

CallStateChecker::ResultCode CallStateChecker::scanStop(unsigned short threadid)
{
	if (readImm(&threadstate[threadid]) == ThreadStateGetNextReturnedFinished)
		atomicallyTransitionTo(threadid, ThreadStateGetNextReturnedFinished, ThreadStateInitialized);
	else
		atomicallyTransitionTo(threadid, ThreadStateScanStarted, ThreadStateInitialized);
	return nextOp->scanStop(threadid);
}

void CallStateChecker::threadClose(unsigned short threadid)
{
	atomicallyTransitionTo(threadid, ThreadStateInitialized, ThreadStateUninitialized);
}

void CallStateChecker::destroy()
{
	assert(objstate == ObjStateInitialized);

	assert(threadstate.size() == MAX_THREADS);
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		if (readImm(&threadstate[i]) != ThreadStateUninitialized)
			assert(!"Operator::destroy() called before all threads unregistered.");
	}

	threadstate.clear();

	if (ObjStateInitialized != atomic_compare_and_swap(&objstate, ObjStateInitialized, ObjStateUninitialized))
		assert(!"Operator::destroy() called on operator that had been uninitialized already.");
}

