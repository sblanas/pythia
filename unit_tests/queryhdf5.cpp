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
const char* hdf5filename = "unit_tests/data/ptf_small.h5";
const char* validatefilename = "unit_tests/data/ptfascii/candidate.first3proj";

// #define VERBOSE
// #define VERBOSE2

using namespace std;
using namespace libconfig;

Query q;

struct DataT
{
	long long v1;
	long long v2;
	int v3;
};

vector<DataT> finalresult;

void compute() 
{
	q.threadInit();

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
		while ( (tuple = it.next()) ) 
		{
#ifdef VERBOSE2
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif

			DataT d;
			d.v1 = q.getOutSchema().asLong(tuple, 0);
			d.v2 = q.getOutSchema().asLong(tuple, 1);
			d.v3 = q.getOutSchema().asInt(tuple, 2);
			finalresult.push_back(d);
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

int run(int buffsize, int partitionid, int totalpartitions)
{
	ScanHdf5Op node1;

	Config cfg;

	// init node1
	Setting& hdf5node = cfg.getRoot().add("hdf5scan", Setting::TypeGroup);
	hdf5node.add("file", Setting::TypeString) = hdf5filename;
	Setting& projattrnode = hdf5node.add("pick", Setting::TypeArray);
	projattrnode.add(Setting::TypeString) = "/candidate/id";
	projattrnode.add(Setting::TypeString) = "/candidate/sub_id";
	projattrnode.add(Setting::TypeString) = "/candidate/number";
	hdf5node.add("thispartition", Setting::TypeInt) = partitionid;
	hdf5node.add("totalpartitions", Setting::TypeInt) = totalpartitions;

	// init node2
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// build plan tree
	q.tree = &node1;

	// initialize each node
	node1.init(cfg, hdf5node);

	compute();

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	q.destroynofree();

	return 0;
}

void verify(vector<DataT>& array)
{
	unsigned int tuplesout = 0;
	ifstream f(validatefilename);

	for (unsigned int i = 0; i < array.size(); ++i)
	{
		long long c1;
		long long c2;
		int c3;

		f >> c1 >> c2 >> c3;

		if (array.at(i).v1 != c1)
			fail("Values for column 1 differ!");
		if (array.at(i).v2 != c2)
			fail("Values for column 2 differ!");
		if (array.at(i).v3 != c3)
			fail("Values for column 3 differ!");

		++tuplesout;
	}

	f.close();
	if (tuplesout != 999)
		fail("Wrong number of tuples read; expected 999.");

	array.clear();
}

int main()
{
	run(32, 0, 1);
	verify(finalresult);
	run(64, 0, 1);
	verify(finalresult);
	run(1<<10, 0, 1);
	verify(finalresult);
	run(1<<16, 0, 1);
	verify(finalresult);

	int retries = 10;
	srand48(time(0));
	for (int i=0; i<retries; ++i) 
	{
		int totalpartitions = (lrand48() & 0xFF) + 1;
		for (int j=0; j<totalpartitions; ++j)
		{
			run(1<<10, j, totalpartitions);
		}
		verify(finalresult);
	}

	return 0;
}
