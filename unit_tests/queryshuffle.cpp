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

//#define VERBOSE

const int TUPLES = 50;
const int FILTERVAL = 1000;

using namespace std;
using namespace libconfig;

Query q;

void createfile_dec(const char* filename, const unsigned int maxnum)
{
	std::ofstream of(filename);
	for (unsigned int i=1; i<(maxnum+1); ++i)
	{
		of << i << "|" << i << "|" << (i+.01) << std::endl;  
	}
	of.close();
}

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

#ifdef VERBOSE
    char* c = new char[100];
    char* c2 = new char[100];
#endif

    int totaltups = 0;


	while(result.first == Operator::Ready) {
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
#ifdef VERBOSE
            q.getOutSchema().serialize(c,tuple);
			//cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
			//cout << q.getOutSchema().prettyprint(reinterpret_cast<void *> (c), ' ') << endl;
            //q.getOutSchema().serialize(c2,reinterpret_cast<void*> (c));
			cout << q.getOutSchema().prettyprint(reinterpret_cast<void *> (c), ' ') << endl;
#endif
            totaltups += 1;
            
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

    if (totaltups != TUPLES){
        fail ("lost tuples");
    }

	q.threadClose();
}

int main()
{
	const int buffsize = 24 * 10;//1 << 4;// 20;

	createfile_dec(tempfilename, TUPLES);

	ShuffleOp node2;
	ScanOp node3;

	Config cfg;

    // init node2
	Setting& shufflenode = cfg.getRoot().add("shuffle", Setting::TypeGroup);

	shufflenode.add("value", Setting::TypeInt) = FILTERVAL;
    shufflenode.add("destIPs", Setting::TypeString) = "127.0.0.1";
    shufflenode.add("incomingIPs", Setting::TypeString) = "127.0.0.1";
	shufflenode.add("netDEV", Setting::TypeString) = "eth1";
	shufflenode.add("myIP", Setting::TypeString) = "127.0.0.1";
    shufflenode.add("incomingBasePort", Setting::TypeInt) = 5490;

    Setting& shufflehashnode = shufflenode.add("hash", Setting::TypeGroup);
    shufflehashnode.add("fn", Setting::TypeString) = "willis";
    Setting& shufflehashnoderange = shufflehashnode.add("range", Setting::TypeArray);
    shufflehashnoderange.add(Setting::TypeInt) = 0;
    shufflehashnoderange.add(Setting::TypeInt) = TUPLES;
    shufflehashnode.add("buckets", Setting::TypeInt) = 1;
    //partition attribute field (starting at 0)
	shufflehashnode.add("field", Setting::TypeInt) = 1;



	// init node3
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	scannode.add("file", Setting::TypeString) = tempfilename;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "dec";


	// build plan tree
	//q.tree = &node1;
	//node1.nextOp = &node2;
	//node2.nextOp = &node3;
    q.tree = &node2;
    node2.nextOp = &node3;

	// initialize each node
	node3.init(cfg, scannode);
	node2.init(cfg, shufflenode);

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
