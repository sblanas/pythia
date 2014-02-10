
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

using std::make_pair;

static Operator::Page* NullPage = 0;

void ThreadIdPrependOp::mapinit(Schema& schema)
{
	schema.add(CT_INTEGER);
	Schema& inschema = nextOp->getOutSchema();
	unsigned int cols = inschema.columns();
	for (unsigned int pos=0; pos<cols; ++pos)
	{
		schema.add(inschema.getColumnType(pos));
	}

	description = "ThreadIdPrepend: Prepends thread id in every tuple.";
}

void ThreadIdPrependOp::map(unsigned short threadid, void* tuple, Page* out, Schema& schema)
{
	void* dest = out->allocateTuple();
	dbgassert(out->isValidTupleAddress(dest));
	dbgassert(schema.getTupleSize() == nextOp->getOutSchema().getTupleSize() + sizeof(CtInt));

	CtInt tid = threadid;
	schema.writeData(dest, 0, &tid);
	nextOp->getOutSchema().copyTuple(schema.calcOffset(dest, 1), tuple);
}

// Copy-paste from MapWrapper, passing thread id to map().
//
Operator::GetNextResultT ThreadIdPrependOp::getNext(unsigned short threadid)
{
	Page* in;
	Operator::ResultCode rc;
	unsigned int tupoffset;

	Page* out = output[threadid];
	out->clear();

	// Recover state information and start.
	// 
	in = state[threadid].input;
	rc = state[threadid].prevresult;
	tupoffset = state[threadid].prevoffset;

	while (rc != Error) 
	{
		void* tuple;

		dbgassert(rc != Error);
		dbgassert(in != NULL);
		dbgassert(tupoffset >= 0);
		dbgassert(tupoffset <= (buffsize / nextOp->getOutSchema().getTupleSize()) + 1);

		while ( (tuple = in->getTupleOffset(tupoffset++)) ) 
		{
			// User code call.
			//
			map(threadid, tuple, out, schema);

			// If output buffer full, record state and return.
			// 
			if (!out->canStoreTuple()) 
			{
				state[threadid] = State(in, Ready, tupoffset);
				return make_pair(Ready, out);
			}
		}

		// If input source depleted, remove state information and return.
		//
		if (rc == Finished) 
		{
			state[threadid] = State(&EmptyPage, Finished, 0);
			return make_pair(Finished, out);
		}

		// Read more input.
		//
		Operator::GetNextResultT result = nextOp->getNext(threadid);
		rc = result.first;
		in = result.second;
		tupoffset = 0;
	}

	state[threadid] = State(NullPage, Error, 0);
	return make_pair(Error, NullPage);	// Reached on Error
}
