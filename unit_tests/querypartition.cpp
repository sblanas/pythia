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

// #define VERBOSE

const int TUPLES=128*1024;
const int MAXTESTTHREADS=0xF;

using namespace std;
using namespace libconfig;

Query q;

int truecount[TUPLES];
int tuplecount[TUPLES];
int tuplesource[TUPLES];

void compute(const int threads) 
{
	for (int i=0; i<TUPLES; ++i) 
	{
		tuplecount[i] = 0;
		tuplesource[i] = -1;
	}

	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

	while(result.first == Operator::Ready) 
	{
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			long long v = q.getOutSchema().asLong(tuple, 1);
			assertmsg(v > 0 && v <= TUPLES, 
				"Values that never were generated appear in the output stream.");
			assertmsg((q.getOutSchema().asLong(tuple, 2) ^ 0xABCDEFll) == v, 
				"Values that never were generated appear in the output stream.");

			tuplecount[v-1]++;
			CtInt t = q.getOutSchema().asInt(tuple, 0);
			if (tuplesource[v-1] == -1)
				tuplesource[v-1] = t;

			assertmsg(t >= 0 && t < threads, 
				"Tuples with wrong thread IDs were produced.");
			assertmsg(tuplesource[v-1] == t,
				"Output not partitioned: found same tuple in different partitions.");
		}
	}

	assert(result.first != Operator::Error);

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}
}

void createrandomfile(const char* filename, const unsigned int maxnum)
{
	for (unsigned int i=0; i<maxnum; ++i) 
	{
		truecount[i] = 0;
	}

	std::ofstream of(filename);
	for (unsigned int i=0; i<2*maxnum; ++i)
	{
		unsigned long val = 1 + (lrand48() % maxnum);
		of << val << "|" << (val ^ 0xABCDEFul) << std::endl;
		++truecount[val-1];
	}
	of.close();
}


void test(const int threads)
{
	ParallelScanOp node1;
	PartitionOp node2;
	ThreadIdPrependOp node3;
	MergeOp node4;

	const int buffsize = 20;

	createrandomfile(tempfilename, TUPLES);

	Config cfg;

	// init node1
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& files = scannode.add("files", Setting::TypeList);
	files.add(Setting::TypeString) = tempfilename;
	Setting& mapping1 = scannode.add("mapping", Setting::TypeList);
	Setting& mapping1group0 = mapping1.add(Setting::TypeList);
	for (int i=0; i<threads; ++i)
		mapping1group0.add(Setting::TypeInt) = i;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// init node2
	Setting& sortpartnode = 
		cfg.getRoot().add("repartition", Setting::TypeGroup);
	sortpartnode.add("attr", Setting::TypeInt) = 0;
	sortpartnode.add("maxtuples", Setting::TypeInt) = 2 * TUPLES * threads;

	Setting& keyrange = sortpartnode.add("range", Setting::TypeArray);
	keyrange.add(Setting::TypeInt) = 1;
	keyrange.add(Setting::TypeInt) = TUPLES;
	sortpartnode.add("buckets", Setting::TypeInt) = threads;

	sortpartnode.add("sort", Setting::TypeString) = "no";

	// init node4
	Setting& mergenode = cfg.getRoot().add("merge", Setting::TypeGroup);
	mergenode.add("threads", Setting::TypeInt) = threads;

	// build plan tree
	q.tree = &node4;
	node4.nextOp = &node3;
	node3.nextOp = &node2;
	node2.nextOp = &node1;

	// initialize each node
	node1.init(cfg, scannode);
	node2.init(cfg, sortpartnode);
	node3.init(cfg, scannode /* ignored */);
	node4.init(cfg, mergenode);

	q.threadInit();

	PrettyPrinterVisitor ppv;
#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute(threads);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	q.threadClose();

	for (int i=0; i<TUPLES; ++i) 
	{
		if (tuplecount[i] < truecount[i])
			fail("Tuples are missing from output.");
		if (tuplecount[i] > truecount[i])
			fail("Extra tuples are in output.");
	}
	for (int i=1; i<TUPLES; ++i) 
	{
		assertmsg((tuplesource[i] >= tuplesource[i-1]) || (tuplesource[i] == -1), 
			"Output is not range partitioned.");
	}

	q.destroynofree();

	deletefile(tempfilename);
}

int main()
{
	int retries = 5;
	srand48(time(0));
	for (int i=0; i<retries; ++i) {
#ifdef VERBOSE
		cout << "Iteration " << i << endl;
#endif
		test((lrand48() & (MAXTESTTHREADS-1)) + 1);
	}
	return 0;
}
