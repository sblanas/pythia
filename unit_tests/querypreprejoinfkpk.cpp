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
#include "libconfig.h++"

#include "../query.h"
#include "../operators/operators.h"
#include "../visitors/allvisitors.h"

#include "common.h"

// #define VERBOSE

const int TUPLES = 20000;
const int THREADS = 4;

using namespace std;
using namespace libconfig;

Query q;

PartitionedScanOp node1a;
PartitionedScanOp node1b;
PresortedPrepartitionedMergeJoinOp node2;
MergeOp node3;

int verify[TUPLES*THREADS];

void compute() 
{
	for (int i=0; i<TUPLES*THREADS; ++i) {
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
			if (v <= 0 || v > TUPLES*THREADS)
				fail("Values that never were generated appear in the output stream.");
			double d1 = q.getOutSchema().asDecimal(tuple, 1);
			double d2 = v + 0.1;
			if (d1 != d2)
				fail("Wrong tuple detected at join output.");
			verify[v-1]++;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

void createfileint(const char* filename, int thread, const unsigned int maxnum)
{
	std::ofstream of(filename);
	for (unsigned int i=thread*maxnum+1; i<((thread+1)*maxnum+1); ++i)
	{
		of << i << "|" << i << std::endl;
		of << i << "|" << i << std::endl;
		of << i << "|" << i << std::endl;
		of << i << "|" << i << std::endl;
	}
	of.close();
}

void createfiledouble(const char* filename, int thread, const unsigned int maxnum)
{
	ofstream of(filename);
	for (unsigned int i=thread*maxnum+1; i<((thread+1)*maxnum+1); ++i)
	{
		of << i << "|" << fixed << setprecision(1) << i + 0.1 << endl;
	}
	of.close();
}




int main()
{
	const int buffsize = 1 << 4;
	const int threads = THREADS;

	vector<string> tmpfileint;
	vector<string> tmpfiledouble;

	for (int i=0; i<threads; ++i)
	{
		ostringstream oss;
		oss << "testfileinttoint" << setfill('0') << setw(2) << i << ".tmp";
		tmpfileint.push_back(oss.str());
		createfileint(oss.str().c_str(), i, TUPLES);
	}
	for (int i=0; i<threads; ++i)
	{
		ostringstream oss;
		oss << "testfileinttodouble" << setfill('0') << setw(2) << i << ".tmp";
		tmpfiledouble.push_back(oss.str());
		createfiledouble(oss.str().c_str(), i, TUPLES);
	}

	Config cfg;

	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// Init node1a
	Setting& scannode1 = cfg.getRoot().add("scan1", Setting::TypeGroup);
	scannode1.add("filetype", Setting::TypeString) = "text";
	Setting& files1 = scannode1.add("files", Setting::TypeList);
	Setting& mapping1 = scannode1.add("mapping", Setting::TypeList);
	for (int i=0; i<threads; ++i)
	{
		files1.add(Setting::TypeString) = tmpfileint.at(i);
		Setting& mapping1group = mapping1.add(Setting::TypeList);
		mapping1group.add(Setting::TypeInt) = i;
	}
	Setting& schemanode1 = scannode1.add("schema", Setting::TypeList);
	schemanode1.add(Setting::TypeString) = "long";
	schemanode1.add(Setting::TypeString) = "long";

	// Init node1b
	Setting& scannode2 = cfg.getRoot().add("scan2", Setting::TypeGroup);
	scannode2.add("filetype", Setting::TypeString) = "text";
	Setting& files2 = scannode2.add("files", Setting::TypeList);
	Setting& mapping2 = scannode2.add("mapping", Setting::TypeList);
	for (int i=0; i<threads; ++i)
	{
		files2.add(Setting::TypeString) = tmpfiledouble.at(i);
		Setting& mapping2group = mapping2.add(Setting::TypeList);
		mapping2group.add(Setting::TypeInt) = i;
	}
	Setting& schemanode2 = scannode2.add("schema", Setting::TypeList);
	schemanode2.add(Setting::TypeString) = "long";
	schemanode2.add(Setting::TypeString) = "dec";

	// Init node2
	Setting& joinnode = cfg.getRoot().add("join", Setting::TypeGroup);

	joinnode.add("mostfreqbuildkeyoccurances", Setting::TypeInt) = 4;

	// Partition group tree.
	Setting& pgnode = joinnode.add("threadgroups", Setting::TypeList);
	for (int i=0; i<threads; ++i)
	{
		Setting& singlepart = pgnode.add(Setting::TypeArray);
		singlepart.add(Setting::TypeInt) = i;
	}

	// Join attribute and projection tree.
	joinnode.add("buildjattr", Setting::TypeInt) = 0;
	joinnode.add("probejattr", Setting::TypeInt) = 0;

	Setting& projectnode = joinnode.add("projection", Setting::TypeList);
	projectnode.add(Setting::TypeString) = "B$1";
	projectnode.add(Setting::TypeString) = "P$1";

	// Init node3
	Setting& mergenode = cfg.getRoot().add("merge", Setting::TypeGroup);
	mergenode.add("threads", Setting::TypeInt) = threads;

//	cfg.write(stdout);

	// build plan tree
	q.tree = &node3;
	node3.nextOp = &node2;
	node2.buildOp = &node1a;
	node2.probeOp = &node1b;

	// initialize each node
	node1a.init(cfg, scannode1);
	node1b.init(cfg, scannode2);
	node2.init(cfg, joinnode);
	node3.init(cfg, mergenode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	for (int i=0; i<TUPLES*THREADS; ++i) 
	{
		const int correctcount = 4;
		if (verify[i] < correctcount)
			fail("Tuples are missing from output.");
		if (verify[i] > correctcount)
			fail("Extra tuples are in output.");
	}

	q.destroynofree();

	for (int i=0; i<threads; ++i)
	{
		deletefile(tmpfileint.at(i).c_str());
		deletefile(tmpfiledouble.at(i).c_str());
	}

	return 0;
}
