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

const int TUPLES=200;
const int MAXTESTTHREADS=0xF;

using namespace std;
using namespace libconfig;

Query q;

int verify[MAXTESTTHREADS][TUPLES];

void compute(const int threads) 
{
	for (int t=0; t<MAXTESTTHREADS; ++t) 
	{
		for (int i=0; i<TUPLES; ++i) {
			verify[t][i] = 0;
		}
	}

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
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			long long v = q.getOutSchema().asLong(tuple, 1);
			if (v <= 0 || v > TUPLES || q.getOutSchema().asLong(tuple, 2) != v)
				fail("Values that never were generated appear in the output stream.");
			CtInt t = q.getOutSchema().asInt(tuple, 0);
			if (t < 0 || t >= threads)
				fail("Tuples with wrong thread IDs were produced.");
			verify[t][v-1]++;
		}
	}

	assert(result.first != Operator::Error);

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

void test(const int threads)
{
	PartitionedScanOp node1;
	ThreadIdPrependOp node2;
	MergeOp node3;

	const int buffsize = 20;

	createfile(tempfilename, TUPLES);

	Config cfg;

	// init node1
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& files = scannode.add("files", Setting::TypeList);
	for (int i=0; i<threads; ++i) {
		files.add(Setting::TypeString) = tempfilename;
	}
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// init node3
	Setting& mergenode = cfg.getRoot().add("merge", Setting::TypeGroup);
	mergenode.add("threads", Setting::TypeInt) = threads;

	// build plan tree
	q.tree = &node3;
	node3.nextOp = &node2;
	node2.nextOp = &node1;

	// initialize each node
	node1.init(cfg, scannode);
	node2.init(cfg, scannode /* ignored */);
	node3.init(cfg, mergenode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute(threads);

	for (int t=0; t<threads; ++t) 
	{
		for (int i=0; i<TUPLES; ++i) 
		{
#ifdef VERBOSE
			cout << "threads:" << threads 
				<< " verify[" << t << "][" << i << "]:" << verify[t][i] << endl;
#endif
			if (verify[t][i] < 1)
				fail("Tuples are missing from output.");
			if (verify[t][i] > 1)
				fail("Extra tuples are in output.");

			verify[t][i] = 0;
		}
	}
	for (int t=0; t<MAXTESTTHREADS; ++t) 
	{
		for (int i=0; i<TUPLES; ++i) 
		{
			if (verify[t][i] != 0)
				fail("Tuples with wrong thread IDs were produced.");
		}
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
