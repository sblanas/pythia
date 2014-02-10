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

#include <iostream>
#include <fstream>
#include <utility>
#include <set>
#include "libconfig.h++"

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"

// #define VERBOSE

using namespace std;
using namespace libconfig;

Query q;

ScanOp node1a;
IndexHdf5Op node1b;
IndexHashJoinOp node2;

static const char* KEYSFILE   = "unit_tests/data/ptfascii/indexedhashjoin/keys";
static const char* VERIFYFILE = "unit_tests/data/ptfascii/indexedhashjoin/payload";

static const char* HDF5FILE  = "unit_tests/data/ptf_small.h5";
static const char* DIRECTORY = "unit_tests/data/ptfascii/candidate/";
static const char* DATASET = "id";

typedef pair<long long, int> PairT;
set<PairT> correctresult;

void populatecorrect()
{
	ifstream f(VERIFYFILE);
	long long c1;
	int c2;

	f >> c1 >> c2;
	while(f)
	{
		if(correctresult.insert(make_pair(c1, c2)).second == false)
			fail("Duplicate tuple detected in input; test results are invalid.");
		f >> c1 >> c2;
	}

	f.close();
}

void compute() 
{
	unsigned int tuplesout = 0;

	q.threadInit();

	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	if (q.scanStart() == Operator::Error) {
		fail("Scan initialization failed.");
	}

	while(result.first == Operator::Ready) {
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) 
		{
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			long long v1 = q.getOutSchema().asLong(tuple, 0);
			int v2 = q.getOutSchema().asInt(tuple, 1);

			PairT temp = make_pair(v1, v2);
			if (correctresult.find(temp) == correctresult.end())
				fail("Wrong tuple in query output.");
			++tuplesout;
		}
	}

	if (tuplesout != correctresult.size())
		fail("Duplicate results detected: query output size is not correct.");

	if (q.scanStop() == Operator::Error) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

int test()
{
	const int buffsize = 32;
	const int threads = 1;

	Config cfg;

	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// Init node1a
	Setting& scannode1 = cfg.getRoot().add("scan1", Setting::TypeGroup);
	scannode1.add("filetype", Setting::TypeString) = "text";
	scannode1.add("file", Setting::TypeString) = KEYSFILE;
	Setting& schemanode1 = scannode1.add("schema", Setting::TypeList);
	schemanode1.add(Setting::TypeString) = "long";

	// Init node1b
	Setting& hdf5node = cfg.getRoot().add("hdf5index", Setting::TypeGroup);
	hdf5node.add("file", Setting::TypeString) = HDF5FILE;
	Setting& projattrnode = hdf5node.add("pick", Setting::TypeArray);
	projattrnode.add(Setting::TypeString) = "/candidate/id";
	projattrnode.add(Setting::TypeString) = "/candidate/sub_id";
	projattrnode.add(Setting::TypeString) = "/candidate/number";
	hdf5node.add("indexdirectory", Setting::TypeString) = DIRECTORY;
	hdf5node.add("indexdataset", Setting::TypeString) = DATASET;

	// Init node2
	Setting& joinnode = cfg.getRoot().add("join", Setting::TypeGroup);

	// Hash tree, with data properties of hash function.
	Setting& joinhashnode = joinnode.add("hash", Setting::TypeGroup);
	joinhashnode.add("fn", Setting::TypeString) = "knuth";
	joinhashnode.add("buckets", Setting::TypeInt) = 128;

	// Partition group tree.
	Setting& pgnode = joinnode.add("threadgroups", Setting::TypeList);
	Setting& singlepart = pgnode.add(Setting::TypeArray);
	for (int i=0; i<threads; ++i)
		singlepart.add(Setting::TypeInt) = i;

	joinnode.add("tuplesperbucket", Setting::TypeInt) = 2;
	joinnode.add("allocpolicy", Setting::TypeString) = "local";

	// Join attribute and projection tree.
	joinnode.add("buildjattr", Setting::TypeInt) = 0;
	joinnode.add("probejattr", Setting::TypeInt) = 0;

	Setting& projectnode = joinnode.add("projection", Setting::TypeList);
	projectnode.add(Setting::TypeString) = "P$1";
	projectnode.add(Setting::TypeString) = "P$2";

//	cfg.write(stdout);

	// build plan tree
	q.tree = &node2;
	node2.buildOp = &node1a;
	node2.probeOp = &node1b;

	// initialize each node
	node1a.init(cfg, scannode1);
	node1b.init(cfg, hdf5node);
	node2.init(cfg, joinnode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	q.destroynofree();

	return 0;
}

int main()
{
	populatecorrect();
	test();
	return 0;
}
