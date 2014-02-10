
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

const int TUPLES = 20;
const int FILTERVAL = 10;

using namespace std;
using namespace libconfig;

Query q;

void compute() 
{
	int verify[FILTERVAL];

	for (int i=0; i<FILTERVAL; ++i) 
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
			if (v <= 0)
				fail("Values that never were generated appear in the output stream.");
			if (v >= FILTERVAL)
				fail("Read values that filter should have eliminated.");
			if (verify[v-1] != 0)
				fail("Tuple appears twice.");
			if (q.getOutSchema().columns() != 1)
				fail("Output has more columns than expected.");
			verify[v-1]++;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

int main()
{
	const int buffsize = 1 << 4;// 20;

	createfile(tempfilename, TUPLES);

	CallStateChecker check1;
	CallStateChecker check2;
	CallStateChecker check3;
	Project node1;
	Filter node2;
	ScanOp node3;

	Config cfg;

	// init node1
	Setting& projectnode = cfg.getRoot().add("project", Setting::TypeGroup);
	Setting& projattrnode = projectnode.add("projection", Setting::TypeArray);
	projattrnode.add(Setting::TypeString) = "$1";

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
	q.tree = &check1;
	check1.nextOp = &node1;
	node1.nextOp = &check2;
	check2.nextOp = &node2;
	node2.nextOp = &check3;
	check3.nextOp = &node3;

	// initialize each node
	node3.init(cfg, scannode);
	check3.init(cfg, /* unused */ cfg.getRoot());
	node2.init(cfg, filternode);
	check2.init(cfg, /* unused */ cfg.getRoot());
	node1.init(cfg, projectnode);
	check1.init(cfg, /* unused */ cfg.getRoot());

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
