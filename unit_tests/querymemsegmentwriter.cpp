/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
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

#include "libconfig.h++"

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"
const char* tempfilename = "artzimpourtzikaioloulas.tmp";

#include <sys/mman.h>

// #define VERBOSE

const int TUPLES = 1000;
const int FILTERVAL = 60;

using namespace std;
using namespace libconfig;

Query q;

void cleanup()
{
	shm_unlink("/memsegmentwritetest.numa0.0000000");
	shm_unlink("/memsegmentwritetest.numa1.0000000");
}

void checkedcleanup()
{
	assert(shm_unlink("/memsegmentwritetest.numa0.0000000") == 0);
	assert(shm_unlink("/memsegmentwritetest.numa1.0000000") == 0);
}

void verify()
{
	int verify[FILTERVAL];

	for (int i=0; i<FILTERVAL; ++i) 
	{
		verify[i] = 0;
	}

	Schema s;
	s.add(CT_LONG);
	s.add(CT_LONG);

	MemMappedTable table;
	Table::LoadErrorT error;

	table.init(&s);
	error = table.load("/dev/shm/memsegmentwritetest.numa0.*", "", Table::SilentLoad, Table::PermuteFiles);
	if (error != MemMappedTable::LOAD_OK)
	{
		fail("Load error.");
	}

	error = table.load("/dev/shm/memsegmentwritetest.numa1.*", "", Table::SilentLoad, Table::PermuteFiles);
	if (error != MemMappedTable::LOAD_OK)
	{
		fail("Load error.");
	}

	TupleBuffer* bf;
	void* tuple;
	while ( (bf = table.readNext()) )
	{
		TupleBuffer::Iterator it = bf->createIterator();
		while ( (tuple = it.next()) )
		{
#ifdef VERBOSE
			cout << s.prettyprint(tuple, ' ') << endl;
#endif
			long long v = s.asLong(tuple, 0);
			if (v <= 0)
			{
				fail("Values that never were generated appear in the output stream.");
			}
			if (v >= FILTERVAL)
			{
				fail("Read values that filter should have eliminated.");
			}
			if (verify[v-1] != 0)
			{
				fail("Aggregation group appears twice.");
			}
			if (s.asLong(tuple, 1) != 1)
			{
				fail("Aggregation value is wrong.");
			}
			verify[v-1]++;
		}
	}

	for (int i=1; i<FILTERVAL; ++i) 
	{
		if (verify[i-1] != 1)
		{
			fail("An aggregation value does not appear exactly once.");
		}
	}
}


void compute() 
{
	q.threadInit();

	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

	while(result.first == Operator::Ready) {
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
			fail("No output should have been generated.");
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

int main()
{
	cleanup();

	const int aggbuckets = 1;	// 16;
	const int buffsize = 1 << 4;// 20;

	createfile(tempfilename, TUPLES);

	MemSegmentWriter node0;
	AggregateCount node1;
	Filter node2;
	ScanOp node3;

	Config cfg;

	// init node0
	Setting& memwritenode = cfg.getRoot().add("memwriter", Setting::TypeGroup);
	memwritenode.add("size", Setting::TypeInt) = 512;
	memwritenode.add("policy", Setting::TypeString) = "round-robin";
	Setting& numalist = memwritenode.add("numanodes", Setting::TypeList);
	numalist.add(Setting::TypeInt) = 0;
	numalist.add(Setting::TypeInt) = 0;
	Setting& writepaths = memwritenode.add("paths", Setting::TypeList);
	writepaths.add(Setting::TypeString) = "/dev/shm/memsegmentwritetest.numa0.";
	writepaths.add(Setting::TypeString) = "/dev/shm/memsegmentwritetest.numa1.";

	// init node1
	Setting& aggnode = cfg.getRoot().add("aggcount", Setting::TypeGroup);
	aggnode.add("field", Setting::TypeInt) = 0;
	Setting& agghashnode = aggnode.add("hash", Setting::TypeGroup);
	agghashnode.add("fn", Setting::TypeString) = "modulo";
	agghashnode.add("buckets", Setting::TypeInt) = aggbuckets;
	agghashnode.add("field", Setting::TypeInt) = 0;
	
	// init node2
	ostringstream oss;
	oss << FILTERVAL;

	Setting& filternode = cfg.getRoot().add("filter", Setting::TypeGroup);
	filternode.add("field", Setting::TypeInt) = 0;
	filternode.add("op", Setting::TypeString) = "<";
	filternode.add("value", Setting::TypeString) = oss.str();

	// init node3
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	scannode.add("file", Setting::TypeString) = tempfilename;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// build plan tree
	q.tree = &node0;
	node0.nextOp = &node1;
	node1.nextOp = &node2;
	node2.nextOp = &node3;

	// initialize each node
	node3.init(cfg, scannode);
	node2.init(cfg, filternode);
	node1.init(cfg, aggnode);
	node0.init(cfg, memwritenode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	q.destroynofree();

	deletefile(tempfilename);

	verify();
	checkedcleanup();

	return 0;
}
