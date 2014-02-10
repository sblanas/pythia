
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

#include "ibis.h"
static const char* nullstr = 0;

using std::make_pair;

void IndexHdf5Op::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ScanHdf5Op::init(root, cfg);

	assert(totalpartitions == 1);
	assert(thispartition == 0);

	totaltuples = 0;

	indexdirectory = (const char*) root.getRoot()["path"];
	indexdirectory += "/";
	indexdirectory += (const char*) cfg["indexdirectory"];

	indexdataset = (const char*) cfg["indexdataset"];
}

void IndexHdf5Op::destroy()
{
	ScanHdf5Op::destroy();
}

void IndexHdf5Op::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);

	void* space = numaallocate_local("iHdO", sizeof(Page), this);
	output = new(space) Page(buffsize, schema.getTupleSize(), this);
	assertaddresslocal(output);

	staging = numaallocate_local("iHdS", 
			sizeintup * maxOutputColumnWidth(), this);
	assertaddresslocal(staging);
}

void IndexHdf5Op::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	if (output) 
	{
		numadeallocate(output);
	}
	output = NULL;

	if (staging) 
	{
		numadeallocate(staging);
	}
	staging = NULL;
}

Operator::ResultCode IndexHdf5Op::scanStart(unsigned short threadid,
		Page* indexdata, Schema& indexdataschema)
{
	dbgCheckSingleThreaded(threadid);

	// Reset memspace that may have shrunk from last index lookup.
	// 
	sizeintup = buffsize/schema.getTupleSize();
	hsize_t zero = 0;
	H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);

	// Find elements in input, and allocate arrays.
	// Buffer keys to pass to FastBit.
	//
	vector<unsigned int> lookupkeys;
	const unsigned long long lookups = indexdata->getNumTuples();
	lookupkeys.reserve(lookups);

	assert(indexdataschema.columns() == 1);
	switch(indexdataschema.getColumnType(0))
	{
		case CT_INTEGER:
		{
			CtInt* arr = (CtInt*) indexdata->getTupleOffset(0);
			for (unsigned int i=0; i<lookups; ++i)
			{
				lookupkeys.push_back(arr[i]);
			}
			break;
		}
		case CT_LONG:
		{
			CtLong* arr = (CtLong*) indexdata->getTupleOffset(0);
			for (unsigned int i=0; i<lookups; ++i)
			{
				lookupkeys.push_back(arr[i]);
			}
			break;
		}
		default:
			throw IllegalSchemaDeclarationException();
	};

	// Pass keys to FastBit, return answer.
	//
	ibis::part part(indexdirectory.c_str(), nullstr);
	ibis::query query(ibis::util::userName(), &part);
	ibis::qDiscreteRange where = ibis::qDiscreteRange(indexdataset.c_str(), lookupkeys);
	query.setWhereClause(&where);

	int ret = query.evaluate();
	assert(ret >= 0);

	currentoffset = 0;
	vector<unsigned int> temp;
	query.getHitRows(temp);
	rowstoaccess.reserve(temp.size());
	for (unsigned int i=0; i<temp.size(); ++i)
		rowstoaccess.push_back(temp[i]);

	return Ready;
}

Operator::ResultCode IndexHdf5Op::scanStop(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	sizeintup = 0;
	rowstoaccess.clear();
	currentoffset = 0;

	return Ready;
}

Operator::GetNextResultT IndexHdf5Op::getNext(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	output->clear();

	unsigned long long remaintups = rowstoaccess.size()-currentoffset;
	if (sizeintup > remaintups)
	{
		// Less than sizeintup tuples remaining.
		// Change sizeintup to reflect this, and shrink memory space.
		//
		sizeintup = remaintups;
		if (sizeintup == 0)
			return make_pair(Operator::Finished, output);
		hsize_t zero = 0;
		H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &zero, NULL, &sizeintup, NULL);
	}

	output->allocate(sizeintup * schema.getTupleSize());

	for (unsigned int i=0; i<hdf5sets.size(); ++i)
	{
		// Add keys to selection.
		//
		H5Sselect_elements(hdf5space[i], H5S_SELECT_SET, 
				sizeintup, &rowstoaccess[currentoffset]);

		copySpaceIntoOutput(i);
	}
	currentoffset += sizeintup;
	totaltuples += sizeintup;

	if (currentoffset == rowstoaccess.size()) {
		return make_pair(Operator::Finished, output);
	} else {
		return make_pair(Operator::Ready, output);
	}
}
