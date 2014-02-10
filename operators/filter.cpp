
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

void Filter::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	MapWrapper::init(root, cfg);	//< calls Filter::mapinit below

	fieldno = cfg["field"];

	// Read column spec from input and create comparator. 
	//
	ColumnSpec cs = schema.get(fieldno);
	string tmpstr = cfg["op"];
	opstr = tmpstr;
	Comparator::Comparison compop = Comparator::parseString(opstr);
	comparator = Schema::createComparator(schema, fieldno, cs, compop);
	
	// Create dummy schema and parse input to create comparator.
	//
	const char* inputval = cfg["value"];
	Schema dummyschema;
	dummyschema.add(cs);
	dbgassert(sizeof(value) == FILTERMAXWIDTH);
	dbgassert(dummyschema.getTupleSize() <= sizeof(value));
	dbgassert(dummyschema.columns() == 1);
	dummyschema.parseTuple(value, &inputval);
}

void Filter::mapinit(Schema& schema)
{
	schema = nextOp->getOutSchema();

	// We don't bother with setting a description, because Filter is a
	// first-class citizen for the engine and is recognized by the visitor
	// classes for pretty-printing.
}

/**
 * Filter and do copy.
 */
void Filter::map(void* tuple, Page* out, Schema& schema) 
{
	if (comparator.eval(tuple, &value))
	{
		void* dest = out->allocateTuple();
		dbgassert(out->isValidTupleAddress(dest));
		schema.copyTuple(dest, tuple);
	}
}
