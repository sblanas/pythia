
/*
 * Copyright 2012, Spyros Blanas.
 */

#include <iostream>
#include <cstdlib>
#include "libconfig.h++"

#include "../query.h"
#include "../visitors/allvisitors.h"
#include "../rdtsc.h"

#define QUERYPLAN

using namespace std;
using namespace libconfig;

void fail(const char* explanation) {
	std::cout << " ** FAILED: " << explanation << std::endl;
	throw QueryExecutionError();
}

extern size_t TotalBytesAllocated;

#ifdef STATS_ALLOCATE
void dbgPrintAllocations(Query& q);
#endif

Query q;

void compute() 
{
	unsigned long long cycles = 0;
	Operator::GetNextResultT result; 
	result.first = Operator::Ready;
	
	startTimer(&cycles);

	if (q.scanStart() == Operator::Error)
		fail("Scan initialization failed.");

	while(result.first == Operator::Ready) {
		result = q.getNext();

		if (result.first == Operator::Error)
			fail("GetNext returned error.");

		Operator::Page::Iterator it = result.second->createIterator();
		void* tuple;
		while ((tuple = it.next()) ) {
			cout << q.getOutSchema().prettyprint(tuple, '|') << endl;
		}
	}

	if (q.scanStop() == Operator::Error) 
		fail("Scan stop failed.");

	stopTimer(&cycles);

#warning Cycles to seconds conversion is hardcoded for our prototype system
	cout << "ResponseTimeInSec: " << cycles/1000./1000./2000. << endl;
}


int main(int argc, char** argv)
{
	Config cfg;

	if (argc < 2) {
		cout << "ERROR: Configuration file not specified." << endl;
		cout << "Usage: " << argv[0] << " conf-file" << endl;
		return 2;
	}

	cfg.readFile(argv[1]);
	q.create(cfg);

	q.threadInit();

	compute();

#ifdef QUERYPLAN
	cout << "---------- QUERY PLAN START ----------" << endl;
	PrettyPrinterVisitor ppv;
	q.accept(&ppv);
	cout << "----------- QUERY PLAN END -----------" << endl;
#endif

	cout << "Max Memory Allocated (bytes): " << TotalBytesAllocated << endl;

#ifdef STATS_ALLOCATE
	dbgPrintAllocations(q);
#endif

	q.threadClose();

	q.destroy();

	return 0;
}
