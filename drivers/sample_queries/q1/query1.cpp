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

#include "../../../query.h"
#include "../../../visitors/allvisitors.h"
#include "../../../rdtsc.h"

// #define VERBOSE
#define QUERYPLAN

using namespace std;
using namespace libconfig;

class TestFailException { };

void fail(const char* explanation) {
	std::cout << " ** Test FAILED: " << explanation << std::endl;
	throw TestFailException();
}

unsigned long long skata1;
double skata2;

class Query1Eval : public MapWrapper {
	public:
		virtual void mapinit(Schema& schema)
		{
			schema.add(CT_LONG);
			schema.add(CT_DECIMAL);

			description = "Query1Eval: Evaluates col3-col2, writes it as new col2";
		}

		virtual void map(void* tuple, Page* out, Schema& schema) 
		{
			void* dest = out->allocateTuple();
			dbgassert(out->isValidTupleAddress(dest));
			schema.writeData(dest, 0, tuple);
			double val = nextOp->getOutSchema().asDecimal(tuple, 2) 
					- nextOp->getOutSchema().asDecimal(tuple, 1);
			schema.writeData(dest, 1, &val);
		}

};

Query q;

unsigned long long timer;

void compute() 
{
	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	startTimer(&timer);
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
			skata1 += *(unsigned long long*)tuple;
			skata2 += *((double*)tuple+1);
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}
	stopTimer(&timer);
}


int main(int argc, char** argv)
{
	skata1 = 0;
	skata2 = 0.0;

	Query1Eval* eval = new Query1Eval();
	dbgassert(eval != 0);

	Config cfg;

	if (argc < 2) {
		cout << "ERROR: Configuration file not specified." << endl;
		cout << "Usage: " << argv[0] << " conf-file" << endl;
		return 2;
	}

	cfg.readFile(argv[1]);

	Query::UserDefinedOpMapT opmap;
	opmap["q1eval"] = eval;

	dbgassert(opmap.find("q1eval") != opmap.end());
	dbgassert(opmap.find("q1eval")->second == eval);

	// build plan tree
	q.create(cfg, opmap);

	dbgassert(opmap.find("q1eval") != opmap.end());
	dbgassert(opmap.find("q1eval")->second == 0);

#ifdef QUERYPLAN
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	int threads = cfg.getRoot()["merge"]["threads"];

	q.threadInit();

	compute();

#ifdef QUERYPLAN
	cout << "---------- QUERY PLAN START ----------" << endl;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	q.threadClose();

	q.destroy();

	cout << "Threads: " << threads << endl;
	cout << "Cycles: " << timer << endl;
	cout << "Checksum: (" << skata1 << ", " << fixed << skata2 << ")" << endl;

	return 0;
}
