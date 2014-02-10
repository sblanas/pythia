
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
#include "../util/static_assert.h"
#include "../util/atomics.h"

#include "ibis.h"
static const char* nullstr = 0;

using std::make_pair;

void FastBitScanOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ZeroInputOp::init(root, cfg);

	totaltuples = 0;
	totalkeylookups = 0;
	schema = Schema::create(cfg["schema"]);

	// Initialize column names.
	//
	libconfig::Setting& picknode = cfg["pick"];
	assert(picknode.isAggregate());
	for (int i = 0; i<picknode.getLength(); ++i)
	{
		string s = picknode[i];
		col_names.push_back(s);
	}

	assert(schema.columns() == col_names.size());

	// Initialize FastBit directories, filter condition and ibis::part object.
	//
	indexdirectory = (const char*) root.getRoot()["path"];
	indexdirectory += "/";
	indexdirectory += (const char*) cfg["indexdirectory"];

	indexdataset = (const char*) cfg["indexdataset"];

	conditionstr.clear();
	cfg.lookupValue("condition", conditionstr);

	void* space;
	space = numaallocate_local("FBtP", sizeof(ibis::part), this);
	assert(space != NULL);
	ibispart = new(space) ibis::part(indexdirectory.c_str(), nullstr);

	// Post-init
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		ibisquery.push_back(NULL);
		depleted.push_back(true);
	}
}

void FastBitScanOp::threadInit(unsigned short threadid)
{
	void* space;
	space = numaallocate_local("FBtQ", sizeof(ibis::query), this);
	ibisquery[threadid] = new(space) 
		ibis::query(ibis::util::userName(), ibispart);
}

Operator::ResultCode FastBitScanOp::scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema)
{
	// Construct FastBit objects, pass in query string.
	//
	ibis::part* part = ibispart;
	ibis::query* query = ibisquery[threadid];
	dbgassert(part != NULL);
	dbgassert(query != NULL);

	query->setWhereClause(conditionstr.c_str());

	// If indexdatapage is empty, or indexdataschema is empty, or 
	// indexdataset is empty, don't pass in qDiscreteRange.
	//
	if (   (indexdataset.empty() == false)
		&& (indexdataschema.columns() == 1)
		&& (indexdatapage->getUsedSpace() != 0)
	   )
	{
		// Find elements in input, and allocate arrays.
		// Buffer keys to pass to FastBit.
		//
		vector<unsigned int> lookupkeys;
		unsigned long long lookups = indexdatapage->getNumTuples();
		atomic_increment(&totalkeylookups, lookups);
		lookupkeys.reserve(lookups);

		switch(indexdataschema.getColumnType(0))
		{
			case CT_INTEGER:
			{
				CtInt* arr = (CtInt*) indexdatapage->getTupleOffset(0);
				for (unsigned int i=0; i<lookups; ++i)
				{
					lookupkeys.push_back(arr[i]);
				}
				break;
			}
			case CT_LONG:
			{
				CtLong* arr = (CtLong*) indexdatapage->getTupleOffset(0);
				for (unsigned int i=0; i<lookups; ++i)
				{
					lookupkeys.push_back(arr[i]);
				}
				break;
			}
			default:
				throw IllegalSchemaDeclarationException();
		};

		ibis::qDiscreteRange onlyfetch = 
			ibis::qDiscreteRange(indexdataset.c_str(), lookupkeys);
		query->addConditions(&onlyfetch);
	}

	// Evaluate query, find total tuples. 
	//
	int ret = query->evaluate();
	assert(ret >= 0);

	// Create output page big enough to hold all results.
	//
	unsigned long long tuples = query->getNumHits();
	unsigned int tupsz = schema.getTupleSize();
	atomic_increment(&totaltuples, tuples);

	void* space;
	space = numaallocate_local("FBtO", sizeof(Page), this);
	output[threadid] = new(space) Page(tuples * tupsz, tupsz, this);
	assertaddresslocal(output[threadid]);
	output[threadid]->allocate(tuples * tupsz);
	depleted[threadid] = false;

	return Ready;
}

/** 
 * Copies all elements from a FastBit \a array into a particular \a column 
 * of the \a output buffer. The output buffer's schema is given in parameter 
 * \a schema.
 */
template <typename KeyT>
void populateFromIbisArray(Operator::Page* output, Schema& schema, 
		unsigned int column, ibis::array_t<KeyT>* array)
{
	KeyT* staging = array->begin();
	size_t nelem = array->size();

	// Fast path if only one column.
	//
	if (schema.columns() == 1)
	{
		assert(column==0);
		void* out = output->getTupleOffset(0);
		memcpy(out, staging, nelem * sizeof(KeyT));

		return;
	}

	// Slow path, stitch vertical layout into tuples.
	//
	dbgassert(output->getTupleOffset(nelem-1) != NULL);
	const size_t offset = (size_t) schema.calcOffset(0, column);

	for (unsigned int i=0; i<nelem; ++i)
	{
		char* tup = (char*) output->getTupleOffset(i);
		KeyT* dest = (KeyT*) (tup+offset);
		*dest = staging[i];
	}
}

void populateFromStringVector(Operator::Page* output, Schema& schema, 
		unsigned int column, vector<string>* array)
{
	size_t nelem = array->size();

	// S-L-O-W due to pointer chasing and strncpy.
	//
	dbgassert(output->getTupleOffset(nelem-1) != NULL);
	const size_t offset = (size_t) schema.calcOffset(0, column);
	const unsigned int width = schema.getColumnWidth(column);

	for (unsigned int i=0; i<nelem; ++i)
	{
		char* target = ((char*) output->getTupleOffset(i)) + offset;
		strncpy(target, (*array)[i].c_str(), width-1);
		target[width-1] = 0;
	}
}

void copyIntoOut(Operator::Page* out, Schema& schema, unsigned int column,
		ibis::query* query, const char* colname)
{
	switch(schema.getColumnType(column))
	{
		case CT_INTEGER:
		{
			static_assert(sizeof(CtInt) == sizeof(int32_t));
			ibis::array_t<int32_t>* arr;
			arr = query->getQualifiedInts(colname);
			assert(arr != NULL);
			populateFromIbisArray(out, schema, column, arr);
			delete arr;
			break;
		}
		case CT_LONG:
		case CT_DATE:
		{
			static_assert(sizeof(CtLong) == sizeof(int64_t));
			static_assert(sizeof(CtDate) == sizeof(int64_t));
			ibis::array_t<int64_t>* arr;
			arr = query->getQualifiedLongs(colname);
			assert(arr != NULL);
			populateFromIbisArray(out, schema, column, arr);
			delete arr;
			break;
		}
		case CT_DECIMAL:
		{
			static_assert(sizeof(CtDecimal) == sizeof(double));
			ibis::array_t<double>* arr;
			arr = query->getQualifiedDoubles(colname);
			assert(arr != NULL);
			populateFromIbisArray(out, schema, column, arr);
			delete arr;
			break;
		}
		case CT_CHAR:
		{
			vector<string>* arr;
			arr = query->getQualifiedStrings(colname);
			assert(arr != NULL);
			populateFromStringVector(out, schema, column, arr);
			delete arr;
			break;
		}
		default:
			throw NotYetImplemented();
	}
}

Operator::GetNextResultT FastBitScanOp::getNext(unsigned short threadid)
{
	Page* out = output[threadid];
	ibis::query* query = ibisquery[threadid];
	assert(out != NULL);
	assert(query != NULL);

	if (depleted[threadid])
	{
		out->clear();
		return make_pair(Operator::Finished, out);
	}

	for (unsigned int i=0; i<col_names.size(); ++i)
	{
		copyIntoOut(out, schema, i, query, col_names[i].c_str());
	}

	depleted[threadid] = true;

	return make_pair(Operator::Finished, out);
}

Operator::ResultCode FastBitScanOp::scanStop(unsigned short threadid)
{
	// Free page.
	//
	if (output[threadid]) 
	{
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;

	return Ready;
}

void FastBitScanOp::threadClose(unsigned short threadid)
{
	// Destroy FastBit query object.
	//
	if (ibisquery[threadid])
	{
		ibisquery[threadid]->~query();
		numadeallocate(ibisquery[threadid]);
	}
	ibisquery[threadid] = NULL;
}

void FastBitScanOp::destroy()
{
	ZeroInputOp::destroy();

	// Destroy FastBit part object.
	//
	if (ibispart)
	{
		ibispart->~part();
		numadeallocate(ibispart);
	}
	ibispart = NULL;

	output.clear();
	ibisquery.clear();
	col_names.clear();
	conditionstr.clear();
	indexdirectory.clear();
	indexdataset.clear();
}

