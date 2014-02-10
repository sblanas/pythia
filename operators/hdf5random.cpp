
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

#include "../util/numaallocate.h"
#include "../util/numaasserts.h"
#include "../util/reentrant_random.h"

void RandomLookupsHdf5Op::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ScanHdf5Op::init(root, cfg);

	assert(totalpartitions == 1);
	assert(thispartition == 0);

	hdf5length = totaltuples;
	totaltuples = 0;

	randomlookups = (unsigned long) cfg["randomlookups"];
}

Operator::ResultCode RandomLookupsHdf5Op::scanStart(unsigned short threadid,
		Page* indexdata, Schema& indexdataschema)
{
	dbgCheckSingleThreaded(threadid);

	// Reset memspace that may have shrunk from last index lookup.
	// 
	sizeintup = buffsize/schema.getTupleSize();
	hsize_t zero = 0;
	H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);

	// Generate random ids.
	//
	ReentrantRandom rndgen;
	rndgen.init(0);
	rowstoaccess.reserve(randomlookups);
	for (unsigned int i=0; i<randomlookups; ++i)
		rowstoaccess.push_back(rndgen.next() % hdf5length);

	return Ready;
}
