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

void IntGeneratorOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	ZeroInputOp::init(root, node);

	// Read parameters, make sure they make sense.
	//
	int tuples, width;

	tuples = node["sizeinmb"];
	width = node["width"];

	tuplewidth = width;
	totaltuples = tuples * 1024ull* 1024ull / tuplewidth;

	if (tuplewidth < sizeof(CtInt))
		throw InvalidParameter();

	// Populate schema.
	//
	schema.add(CT_INTEGER);
	schema.add(CT_CHAR, tuplewidth - sizeof(CtInt));
	assert(schema.getTupleSize() == tuplewidth);

	// Populate private data structures.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		scratchspace.push_back(NULL);
		output.push_back(NULL);
		producedsofar.push_back(0);
	}
}

void IntGeneratorOp::threadInit(unsigned short threadid)
{
	static const char* dummy = "The Past performance does not guarantee future results. We provide self-directed users with data services, and do not make recommendations or offer legal or other advice. You alone are responsible for evaluating the merits and risks associated with the use of our systems, services or products.";

	scratchspace.at(threadid) = new char[schema.getTupleSize()];
	dbgassert(scratchspace.at(threadid) != NULL);
	schema.copyTuple(scratchspace.at(threadid), dummy);

	output.at(threadid) = new Page(buffsize, schema.getTupleSize(), this);
	producedsofar.at(threadid) = 0;
}

void* IntGeneratorOp::produceOne(unsigned short threadid)
{
	dbgassert(scratchspace.at(threadid) != NULL);
	void* tuple = scratchspace[threadid];

	CtLong* curval = &producedsofar[threadid];

	if (*curval >= totaltuples)
		return NULL;

	++(*curval);
	schema.writeData(tuple, 0, curval);

	return tuple;
}

Operator::GetNextResultT IntGeneratorOp::getNext(unsigned short threadid)
{
	dbgassert(output.at(threadid) != NULL);
	Page* out = output[threadid];
	out->clear();

	while (out->canStoreTuple())
	{
		void* tuple = produceOne(threadid);
		if (tuple == NULL)
			return make_pair(Finished, out);

		void* target = out->allocateTuple();
		dbgassert(target != NULL);

		schema.copyTuple(target, tuple);
	}

	return make_pair(Ready, out);
}

void IntGeneratorOp::threadClose(unsigned short threadid)
{
	if (output.at(threadid))
	{
		delete output.at(threadid);
	}
	output.at(threadid) = NULL;

	if (scratchspace.at(threadid))
	{
		delete[] scratchspace.at(threadid);
	}
	scratchspace.at(threadid) = NULL;

	producedsofar.at(threadid) = 0;
}
