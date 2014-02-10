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

const int TUPLES = 20;
const int FILTERVAL = TUPLES;

using namespace std;
using namespace libconfig;

Query q;

void compute() 
{
	q.threadInit();

	Operator::Page* out;
	Operator::GetNextResultT result; 
	
	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

    //dexc
    long f[5] = {22, 22, 17, 15, 14};
    long s[5] = {12, 2, 7, 15, 14};
    //asc
    //long f[5] = {1, 1, 1, 2, 3};
    //long s[5] = {4, 19, 21, 20, 13};

    int c  = 0;

	while(result.first == Operator::Ready) {
		result = q.getNext();

		out = result.second;

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
#ifdef VERBOSE
			cout << q.getOutSchema().prettyprint(tuple, ' ') << endl;
#endif
			long v = q.getOutSchema().asLong(tuple, 0);
			long v2 = q.getOutSchema().asLong(tuple, 1);
            if (f[c] != v || s[c] != v2) 
                fail("didn't sort desired order");
            c += 1;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();
}

void createfilesort(const char* filename, const unsigned int maxnum)
{
	std::ofstream of(filename);
of<<"3|13"<<std::endl;
of<<"6|16"<<std::endl;

of<<"1|21"<<std::endl;

of<<"1|19"<<std::endl;

of<<"22|12"<<std::endl;

of<<"14|14"<<std::endl;

of<<"8|8"<<std::endl;

of<<"22|2"<<std::endl;

of<<"3|13"<<std::endl;

of<<"10|5"<<std::endl;

of<<"14|4"<<std::endl;

of<<"15|15"<<std::endl;

of<<"7|7"<<std::endl;

of<<"9|9"<<std::endl;

of<<"1|4"<<std::endl;

of<<"5|45"<<std::endl;

of<<"17|7"<<std::endl;

of<<"5|6"<<std::endl;

of<<"11|1"<<std::endl;

of<<"2|20"<<std::endl;

	of.close();

}


int main()
{
	const int buffsize = 1 << 6;// 20;

	createfilesort(tempfilename, TUPLES);

	ScanOp node2;
    SortLimit node1;

	Config cfg;

    //init node1
    Setting& sortnode = cfg.getRoot().add("sort", Setting::TypeGroup);
	Setting& sortnodeattr = sortnode.add("by", Setting::TypeArray);
	sortnodeattr.add(Setting::TypeString) = "$0";
	sortnodeattr.add(Setting::TypeString) = "$1";
    Setting& sortnodeasc = sortnode.add("asc", Setting::TypeArray);
    sortnodeasc.add(Setting::TypeInt) = 0;
    Setting& sortnodelimit = sortnode.add("limit", Setting::TypeArray);
    sortnodelimit.add(Setting::TypeInt) = 5;


	// init node2
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	scannode.add("file", Setting::TypeString) = tempfilename;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// build plan tree
	q.tree = &node1;
	node1.nextOp = &node2;

	// initialize each node
	node2.init(cfg, scannode);
	node1.init(cfg, sortnode);

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
