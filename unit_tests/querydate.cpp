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
const char* tempfilenamefeb = "artzimpourtzikaioloulasfeb.tmp";
const char* tempfilenamemar = "artzimpourtzikaioloulasmar.tmp";

// #define VERBOSE

const int TUPLES = 20;
const char* FILTERVAL = "12/12/1980";

using namespace std;
using namespace libconfig;

Query q;

void createfile_date_mar(const char* filename, const unsigned int maxnum)
{
	std::ofstream of(filename);
	for (unsigned int i=1; i<(maxnum+1); ++i)
	{
		of << i << "|" << "14/3/" << i+1969 << std::endl;  
	}
	of.close();
}

void createfile_date_feb(const char* filename, const unsigned int maxnum)
{
	std::ofstream of(filename);
	for (unsigned int i=1; i<(maxnum+1); ++i)
	{
		of << "14/2/" << i+1969 << "|" << i+1 << std::endl;  
	}
	of.close();
}

void compute() 
{
	q.threadInit();

	unsigned int count = 0;

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
			long long v1 = q.getOutSchema().asLong(tuple, 1);
			long long v2 = q.getOutSchema().asLong(tuple, 2);
			if (v1 <= 0 || v1 > TUPLES)
				fail("Values that never were generated appear in the output stream.");
			if (v2 <= 1 || v2 > TUPLES+1)
				fail("Values that never were generated appear in the output stream.");
			if (v1 + 1 != v2)
				fail("Output is corrupted, col3 should have been col2 + 1.");

			// Convert col2 from epoch to tm struct, check it's February.
			//
			struct tm timetmp;
			CtDate val = q.getOutSchema().asDate(tuple, 0);
			val.produceTM(&timetmp);
			if (timetmp.tm_mon != 1)
				fail("Output data is corrupted, month not February.");
			if (timetmp.tm_year + 1900 > 1980)
				fail("Output data is corrupted, year past 1980 found.");

			count++;
		}
	}

	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	if (count < 11)
		fail("Less output than expected.");
	if (count > 11)
		fail("More output than expected.");

	q.threadClose();
}

/* 
 * Subtracts one month from col2.
 */
class TestMapper : public MapWrapper {
	public:
		virtual void mapinit(Schema& schema)
		{
			schema.add(CT_LONG);
			schema.add(CT_DATE, "%d/%m/%Y");

			description = "TestWrapper: subtracts a month from col2";
		}

		virtual void map(void* tuple, Page* out, Schema& schema) 
		{
			long long v = schema.asLong(tuple, 0);
			if (v <= 0 || v > TUPLES)
				fail("Values that never were generated appear in the output stream.");

			void* dest = out->allocateTuple();
			if (!out->isValidTupleAddress(dest))
				fail("Destination address in output buffer is invalid.");

			schema.writeData(dest, 0, tuple);

			// Convert col2 from epoch to tm struct, subtract one 
			// month and convert back to epoch to put in col2.
			//
			struct tm timetmp;
			CtDate val = schema.asDate(tuple, 1);
			val.produceTM(&timetmp);
			if (timetmp.tm_mon != 2)
				fail("Input data is corrupted, month not March.");
			timetmp.tm_mon--;
			val.setFromTM(&timetmp);
			schema.writeData(dest, 1, &val);
		}

};

int main()
{
	const int buffsize = 32;

	createfile_date_mar(tempfilenamemar, TUPLES);
	createfile_date_feb(tempfilenamefeb, TUPLES);

	ScanOp node1;
	TestMapper node2;
	Filter node3;
	ScanOp node4;
	HashJoinOp node5;

	Config cfg;

	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	// init scanner for March input
	Setting& scannode = cfg.getRoot().add("scanMar", Setting::TypeGroup);
	scannode.add("file", Setting::TypeString) = tempfilenamemar;
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "date (%d/%m/%Y)";

	// filter everything less than FILTERVAL
	Setting& filternode = cfg.getRoot().add("filter", Setting::TypeGroup);
	filternode.add("field", Setting::TypeInt) = 1;
	filternode.add("op", Setting::TypeString) = "<";
	filternode.add("value", Setting::TypeString) = FILTERVAL;

	// Init hash join tree. Specify hash function first.
	//
	Setting& joinnode = cfg.getRoot().add("join", Setting::TypeGroup);

	Setting& joinhashnode = joinnode.add("hash", Setting::TypeGroup);
	joinhashnode.add("fn", Setting::TypeString) = "modulo";
	Setting& joinhashnoderange = joinhashnode.add("range", Setting::TypeArray);
	joinhashnoderange.add(Setting::TypeInt) = 0;
	joinhashnoderange.add(Setting::TypeInt) = 0x7FFFFFFFl;
	joinhashnode.add("buckets", Setting::TypeInt) = 16;

	// Join config.
	joinnode.add("tuplesperbucket", Setting::TypeInt) = 2;
	joinnode.add("buildjattr", Setting::TypeInt) = 1;
	joinnode.add("probejattr", Setting::TypeInt) = 0;

	// Partition group tree.
	Setting& pgnode = joinnode.add("threadgroups", Setting::TypeList);
	Setting& singlepart = pgnode.add(Setting::TypeArray);
	singlepart.add(Setting::TypeInt) = 0;
	joinnode.add("allocpolicy", Setting::TypeString) = "local";

	// Join projection list.
	Setting& projectnode = joinnode.add("projection", Setting::TypeList);
	projectnode.add(Setting::TypeString) = "B$1";
	projectnode.add(Setting::TypeString) = "B$0";
	projectnode.add(Setting::TypeString) = "P$1";

	// init scanner for February input
	Setting& scanfebnode = cfg.getRoot().add("scanFeb", Setting::TypeGroup);
	scanfebnode.add("filetype", Setting::TypeString) = "text";
	scanfebnode.add("file", Setting::TypeString) = tempfilenamefeb;
	Setting& schemafebnode = scanfebnode.add("schema", Setting::TypeList);
	schemafebnode.add(Setting::TypeString) = "date (%d/%m/%Y)";
	schemafebnode.add(Setting::TypeString) = "long";

	// build plan tree
	q.tree = &node5;
	node5.probeOp = &node4;
	node5.buildOp = &node3;
	node3.nextOp = &node2;
	node2.nextOp = &node1;

	// initialize each node
	node1.init(cfg, scannode);
	node2.init(cfg, /* unused */ cfg.getRoot());
	node3.init(cfg, filternode);
	node4.init(cfg, scanfebnode);
	node5.init(cfg, joinnode);

#ifdef VERBOSE
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	compute();

	q.destroynofree();

	deletefile(tempfilenamemar);
	deletefile(tempfilenamefeb);

	return 0;
}
