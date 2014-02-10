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

#include <sstream>

static Operator::Page* NullPage = 0;

void BitEntropyPrinter::init(libconfig::Config& root, libconfig::Setting& node)
{
	MapWrapper::init(root, node);

	fieldno = (int) node["field"];

	ostringstream ss;
	ss << "Prints times bit is 0 or 1 for the first 64 bits of fieldno=" 
		<< fieldno + 1 << ".";
	description = ss.str();

	// Assert if output doesn't fit at least one record for each bit.
	//
	dbgassert(schema.getTupleSize() * sizeof(CtLong) * 8 >= buffsize);
}

void BitEntropyPrinter::mapinit(Schema& schema)
{
	schema.add(CT_INTEGER);
	schema.add(CT_INTEGER);
	schema.add(CT_LONG);
	schema.add(CT_LONG);
}

void populateOutputPage(Operator::Page* dest, Schema& schema, unsigned short threadid)
{
	CtInt cttid = threadid;
	CtLong ctzero = 0;

	for (unsigned int i=0; i<sizeof(CtLong)*8; ++i)
	{
		CtInt ctbit = i;
		void* desttup = dest->allocateTuple();
		dbgassert(desttup != NULL);

		schema.writeData(desttup, 0, &cttid);
		schema.writeData(desttup, 1, &ctbit);
		schema.writeData(desttup, 2, &ctzero);
		schema.writeData(desttup, 3, &ctzero);
	}
}

void addStatsToPage(Operator::Page* dest, Schema& schema, CtLong val)
{
	for (unsigned int i=0; i<sizeof(CtLong)*8; ++i)
	{
		void* desttup = dest->getTupleOffset(i);
		dbgassert(desttup != NULL);
		int offset = 2 + ((val >> i) & 0x1uLL);
		CtLong oldval = schema.asLong(desttup, offset);
		oldval += 1;
		schema.writeData(desttup, offset, &oldval);
	}
}

Operator::GetNextResultT BitEntropyPrinter::getNext(unsigned short threadid)
{
	Page* in;
	Operator::ResultCode rc;
	unsigned int tupoffset;

	Page* out = output[threadid];
	out->clear();

	// Populate output.
	//
	populateOutputPage(out, schema, threadid);

	// Do first read.
	//
	Operator::GetNextResultT result = nextOp->getNext(threadid);
	rc = result.first;
	in = result.second;
	tupoffset = 0;

	while (rc != Error) 
	{
		void* tuple;

		dbgassert(rc != Error);
		dbgassert(in != NULL);
		dbgassert(tupoffset >= 0);
		dbgassert(tupoffset <= (buffsize / nextOp->getOutSchema().getTupleSize()) + 1);

		while ( (tuple = in->getTupleOffset(tupoffset++)) ) 
		{
			// Calculate entropy.
			//
			CtLong val = *(CtLong*)(nextOp->getOutSchema().calcOffset(tuple, fieldno));
			addStatsToPage(out, schema, val);
		}

		// If input source depleted, remove state information and return.
		//
		if (rc == Finished) 
		{
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
