
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

#include "operators.h"
#include "operators_priv.h"

using std::make_pair;

static unsigned short INVALIDENTRY = static_cast<unsigned short>(-1);

void ParallelScanOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	PartitionedScanOp::init(root, cfg);

	// Parameter not exists?
	//
	if (!cfg.exists("mapping"))
		throw MissingParameterException("ParallelScanOp needs `mapping' parameter.");

	libconfig::Setting& mapgrp = cfg["mapping"];
	unsigned int size = mapgrp.getLength();

	// Parameter not list/array?
	//
	if (size == 0)
		throw MissingParameterException("ParallelScanOp `mapping' parameter cannot have a length of zero.");

	// Parameter not of equal size to files vector?
	//
	if (size != vec_tbl.size())
		throw InvalidParameter();
	
	// Parse input, create vectors.
	//
	unsigned short maxtid = 0;
	for (unsigned int i=0; i<size; ++i)
	{
		libconfig::Setting& threadlist = mapgrp[i];
		vector<unsigned short> v;
		for (int k=0; k<threadlist.getLength(); ++k)
		{
			unsigned int tid = threadlist[k];
			maxtid = tid > maxtid ? tid : maxtid;
			v.push_back(tid);
		}
		vec_grouptothreadlist.push_back(v);

		PThreadLockCVBarrier barrier(v.size());
		vec_barrier.push_back(barrier);
	}

	vec_threadtogroup.resize(maxtid+1, INVALIDENTRY);
	for (unsigned int i=0; i<size; ++i)
	{
		libconfig::Setting& threadlist = mapgrp[i];
		for (int k=0; k<threadlist.getLength(); ++k)
		{
			unsigned int tid = threadlist[k];
			vec_threadtogroup.at(tid) = i;
		}
	}

}

void ParallelScanOp::threadInit(unsigned short threadid)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];

	// The first thread in each group is the unlucky one to do the allocation.
	//
	if (vec_grouptothreadlist[groupno][0] == threadid)
	{
		PartitionedScanOp::threadInit(groupno);
	}

	vec_barrier[groupno].Arrive();
}

Operator::ResultCode ParallelScanOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];

	// The first thread in each group is the unlucky one to do the load.
	//
	ResultCode res = Ready;
	if (vec_grouptothreadlist[groupno][0] == threadid)
	{
		res = PartitionedScanOp::scanStart(groupno, indexdatapage, indexdataschema);
	}	

	vec_barrier[groupno].Arrive();

	return res;
}

Operator::ResultCode ParallelScanOp::scanStop(unsigned short threadid)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];

	vec_barrier[groupno].Arrive();

	// The first thread in each group is the unlucky one to do the unload.
	//
	ResultCode res = Ready;
	if (vec_grouptothreadlist[groupno][0] == threadid)
		res = PartitionedScanOp::scanStop(groupno);

	return res;
}

void ParallelScanOp::threadClose(unsigned short threadid)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];

	vec_barrier[groupno].Arrive();

	// The first thread in each group is the unlucky one to do the deallocation.
	//
	if (vec_grouptothreadlist[groupno][0] == threadid)
		PartitionedScanOp::threadClose(groupno);
}

void ParallelScanOp::destroy()
{
	PartitionedScanOp::destroy();

	vec_threadtogroup.clear();
	vec_grouptothreadlist.clear();
	vec_barrier.clear();
}

Operator::GetNextResultT ParallelScanOp::getNext(unsigned short threadid)
{
	dbgassert(vec_threadtogroup.at(threadid) != INVALIDENTRY);
	dbgassert(vec_threadtogroup.at(threadid) < vec_tbl.size());

	unsigned short groupno = vec_threadtogroup[threadid];
	dbgassert(vec_tbl.at(groupno) != NULL);
	TupleBuffer* ret = vec_tbl[groupno]->atomicReadNext();

	if (ret == NULL) {
		return make_pair(Operator::Finished, &EmptyPage);
	} else {
		return make_pair(Operator::Ready, ret);
	}
}
