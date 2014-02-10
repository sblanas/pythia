
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

#include "../util/numaallocate.h"

using std::make_pair;

void ScanOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	ZeroInputOp::init(root, cfg);

	schema = Schema::create(cfg["schema"]);

	string filename;
	filename = (const char*) root.getRoot()["path"];
	filename += "/";
	filename += (const char*) cfg["file"];
	vec_filename.push_back(filename);

	if (cfg.exists("filetype"))
	{
		string filetype = cfg["filetype"];
		parsetext = (filetype == "text");
	}
	else
	{
		parsetext = false;
	}

	if (cfg.exists("verbose"))
	{
		verbose = Table::VerboseLoad;
	}

	if (cfg.exists("sorted"))
	{
		globparam = Table::SortFiles;
	}

	cfg.lookupValue("separators", separators);

	vec_tbl.push_back(NULL);

	dbgassert(vec_filename.size() == 1);
	dbgassert(vec_tbl.size() == 1);
}	

void ScanOp::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);

	Table* tbl;
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

	vec_tbl[0] = tbl;

	Table::LoadErrorT res = vec_tbl[0]->load(vec_filename[0], separators, verbose, globparam);
	assert(res == Table::LOAD_OK);
}

Operator::ResultCode ScanOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgCheckSingleThreaded(threadid);
	dbgassert(vec_tbl.at(0) != NULL);

	return Ready;
}

Operator::ResultCode ScanOp::scanStop(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);
	dbgassert(vec_tbl.at(0) != NULL);

	return Ready;
}

void ScanOp::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);
	dbgassert(vec_tbl.at(0) != NULL);

	vec_tbl[0]->close();
	numadeallocate(vec_tbl[0]);
	vec_tbl[0] = NULL;
}

void ScanOp::destroy()
{
	dbgassert(vec_tbl.at(0) == NULL);
	vec_filename.clear();
	vec_tbl.clear();
}

Operator::GetNextResultT ScanOp::getNext(unsigned short threadid)
{
	// Assert that we are called single-threaded. Other classes to use:
	// * ParallelScanOp is thread-safe scan over multiple sources sharing
	//   one schema.
	// * PartitionedScanOp is single-threaded scan over multiple sources
	//   sharing one schema.
	//
	dbgCheckSingleThreaded(threadid);
	dbgassert(vec_tbl.at(0) != NULL);

	TupleBuffer* ret = vec_tbl[0]->readNext();

	if (ret == NULL) {
		return make_pair(Operator::Finished, &EmptyPage);
	} else {
		return make_pair(Operator::Ready, ret);
	}
}
