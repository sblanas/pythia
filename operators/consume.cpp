
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

#include "../util/numaallocate.h"
#include "../util/numaasserts.h"

void ConsumeOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);
	schema.add(CT_INTEGER);

	assert(nextOp->getOutSchema().getTupleSize() >= 4);
	static_assert(sizeof(CtInt) == 4);

	for (int i=0; i<128; ++i)
		vec.push_back(NULL);
}

void ConsumeOp::threadInit(unsigned short threadid)
{
	void* space = numaallocate_local("Cons", sizeof(Page), this);
	vec.at(threadid) = new(space) Page(buffsize, schema.getTupleSize(), this);
	assertaddresslocal(vec[threadid]);
}

Operator::GetNextResultT ConsumeOp::getNext(unsigned short threadid)
{
	CtInt val = 0;
	dbgassert(vec.at(threadid) != NULL);
	Page* out = vec[threadid];
	GetNextResultT result;

	assertaddresslocal(&val);

	const int tupw = nextOp->getOutSchema().getTupleSize();

	do {
		result = nextOp->getNext(threadid);
		dbgassert(result.first != Error);

		Page* in = result.second;
		Page::Iterator it = in->createIterator();

		void* tuple;
		while ( (tuple = it.next()) ) 
		{
			CtInt* casttuple = (CtInt*) tuple;
			for (int i=0; i<(tupw/4); ++i)
			{
				val ^= casttuple[i];
			}
		}
	} while(result.first == Operator::Ready);

	assertaddresslocal(&val);

	void* dest = out->allocateTuple();
	schema.writeData(dest, 0, &val);

	return make_pair(Finished, out);
}

void ConsumeOp::threadClose(unsigned short threadid)
{
	numadeallocate(vec.at(threadid));
}

