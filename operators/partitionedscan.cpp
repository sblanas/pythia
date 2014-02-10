
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

#include <cassert>

#include "operators.h"
#include "operators_priv.h"

#include "../util/numaallocate.h"

using std::make_pair;

void PartitionedScanOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	cfg.add("file", libconfig::Setting::TypeString) = "NOTHING";
	ScanOp::init(root, cfg);
	cfg.remove("file");

	vec_filename.clear();
	vec_tbl.clear();

	std::string path = (const char*) root.getRoot()["path"];
	libconfig::Setting& filegrp = cfg["files"];
	int size = filegrp.getLength();

	assert( size != 0 );

	for (int i=0; i<size; ++i) {
		std::string filename;

		filename = path + "/";
		filename += (const char*) filegrp[i];

		vec_filename.push_back(filename);
		vec_tbl.push_back(NULL);	// threadInit populates this
	}
}

void PartitionedScanOp::threadInit(unsigned short threadid)
{
	assert( vec_tbl.size() == vec_filename.size() );
	assert( threadid < vec_tbl.size() );

	Table* tbl = NULL;

	if (parsetext)
	{
		void* space = numaallocate_local("ScPt", sizeof(PreloadedTextTable), this);
		PreloadedTextTable* tbl1 = new(space) PreloadedTextTable();
		dbgassert(tbl1 != NULL);
		tbl1->init(&schema, buffsize);
		tbl = tbl1;
	}
	else
	{
		void* space = numaallocate_local("ScPt", sizeof(MemMappedTable), this);
		MemMappedTable* tbl2 = new(space) MemMappedTable();
		dbgassert(tbl2 != NULL);
		tbl2->init(&schema);
		tbl = tbl2;
	}

	vec_tbl[threadid] = tbl;

	// Load table, and be verbose if and only if threadid == 0. Other threads
	// are non-verbose, or they will clobber stdout.
	//
	Table::LoadErrorT res = 
		vec_tbl[threadid]->load(vec_filename[threadid], separators, 
				threadid == 0 ? verbose : Table::SilentLoad, globparam);
	assert(res == Table::LOAD_OK);
}

Operator::ResultCode PartitionedScanOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	assert( vec_tbl.size() == vec_filename.size() );
	assert( threadid < vec_tbl.size() );
	dbgassert(vec_tbl.at(threadid) != NULL);

	return Ready;
}

Operator::ResultCode PartitionedScanOp::scanStop(unsigned short threadid)
{
	assert( vec_tbl.size() == vec_filename.size() );
	assert( threadid < vec_tbl.size() );
	dbgassert(vec_tbl.at(threadid) != NULL);

	return Ready;
}

void PartitionedScanOp::threadClose(unsigned short threadid)
{
	dbgassert(vec_tbl.at(threadid) != NULL);

	vec_tbl[threadid]->close();
	numadeallocate(vec_tbl[threadid]);
	vec_tbl[threadid] = NULL;
}

void PartitionedScanOp::destroy()
{
	for (unsigned int i=0; i<vec_tbl.size(); ++i)
		dbgassert(vec_tbl[i] == NULL);
	ScanOp::destroy();
}

Operator::GetNextResultT PartitionedScanOp::getNext(unsigned short threadid)
{
	dbgassert(vec_tbl.at(threadid) != NULL);
	TupleBuffer* ret = vec_tbl[threadid]->readNext();

	if (ret == NULL) {
		return make_pair(Operator::Finished, &EmptyPage);
	} else {
		return make_pair(Operator::Ready, ret);
	}
}
