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
#include <cmath>

#include "common.h"
const char* tempfilename = "artzimpourtzikaioloulas.tmp";

// #define VERBOSE

const int TUPLES = 20;
const int THREADS = 4;

using namespace std;
using namespace libconfig;

Query q;

void compute() 
{
	int verify[TUPLES];

	for (int i=0; i<TUPLES; ++i) 
	{
		verify[i] = 0;
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
			long long v = q.getOutSchema().asLong(tuple, 0);
			if (v <= 0 || v > TUPLES)
				fail("Values that never were generated appear in the output stream.");
			if (verify[v-1] != 0)
				fail("Aggregation group appears twice.");
			if (lrint(q.getOutSchema().asDecimal(tuple, 1)) != (v*(v+1)/2))
				fail("Aggregated value is wrong.");
			verify[v-1]++;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

void createaggfile(const char* filename, const unsigned int maxnum)
{
	std::ofstream of(filename);
	for (unsigned int i=1; i<(maxnum+1); ++i)
	{
		for (unsigned int k=1; k<(i+1); ++k)
		{
			of << i << "|" << k << std::endl;
		}
	}
	of.close();
}

int main()
{
	const int aggbuckets = 4;
	const int buffsize = 16;

	createaggfile(tempfilename, TUPLES);

	AggregateSum node1;
	MergeOp mergeop;
	AggregateSum node2;
	ParallelScanOp node3;

	Config cfg;

	// init node1
	Setting& aggnode1 = cfg.getRoot().add("aggsumpre", Setting::TypeGroup);
	aggnode1.add("field", Setting::TypeInt) = 0;
	aggnode1.add("sumfield", Setting::TypeInt) = 1;
	Setting& agghashnode1 = aggnode1.add("hash", Setting::TypeGroup);
	agghashnode1.add("fn", Setting::TypeString) = "modulo";
	agghashnode1.add("buckets", Setting::TypeInt) = aggbuckets;
	agghashnode1.add("field", Setting::TypeInt) = 0;
	
	// init mergeop
	Setting& mergenode = cfg.getRoot().add("merge", Setting::TypeGroup);
	mergenode.add("threads", Setting::TypeInt) = THREADS;

	// init node2
	Setting& aggnode2 = cfg.getRoot().add("aggsumpost", Setting::TypeGroup);
	aggnode2.add("field", Setting::TypeInt) = 0;
	aggnode2.add("sumfield", Setting::TypeInt) = 1;
	Setting& agghashnode2 = aggnode2.add("hash", Setting::TypeGroup);
	agghashnode2.add("fn", Setting::TypeString) = "modulo";
	agghashnode2.add("buckets", Setting::TypeInt) = aggbuckets;
	agghashnode2.add("field", Setting::TypeInt) = 0;
	
	// init node3
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& files = scannode.add("files", Setting::TypeList);
	files.add(Setting::TypeString) = tempfilename;
	Setting& mapping = scannode.add("mapping", Setting::TypeList);
	Setting& mappinggroup0 = mapping.add(Setting::TypeList);
	for (int i=0; i<THREADS; ++i)
		mappinggroup0.add(Setting::TypeInt) = i;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "dec";

	// build plan tree
	q.tree = &node1;
	node1.nextOp = &mergeop;
	mergeop.nextOp = &node2;
	node2.nextOp = &node3;

	// initialize each node
	node3.init(cfg, scannode);
	node2.init(cfg, aggnode2);
	mergeop.init(cfg, mergenode);
	node1.init(cfg, aggnode1);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	q.destroynofree();

	deletefile(tempfilename);

	return 0;
}
