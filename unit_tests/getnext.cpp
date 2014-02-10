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

using namespace std;
using namespace libconfig;

class FakeScan : public ScanOp {
	public:
		FakeScan() : ScanOp(), calls(0) { }

		void init(libconfig::Config& root, libconfig::Setting& node) 
		{ 
			// init FullPage, EmptyPage
			EmptyPage = new Page(0, 0, this);
			FullPage = new Page(16, 8, this);
		}

		void threadInit(unsigned short threadid) { }

		ResultCode scanStart(unsigned short threadid,
				Page* indexdatapage, Schema& indexdataschema)
		{
			void* tmp1 = FullPage->allocateTuple();
			void* tmp2 = FullPage->allocateTuple();
			strncpy((char*)tmp1, "TUPLE01", 8);
			strncpy((char*)tmp2, "TUPLE02", 8);
			return Ready;
		}

		GetNextResultT getNext(unsigned short threadid)
		{
			int curcall;
			GetNextResultT ret;

			lock.lock();
			curcall = calls;
			calls = calls + 1;
			lock.unlock();

			if (curcall < SOURCESIZE) {
				ret.first = Operator::Ready;
				ret.second = FullPage;
			} else if (curcall == SOURCESIZE) {
				ret.first = Operator::Finished;
				ret.second = FullPage;
			} else {
				ret.first = Operator::Finished;
				ret.second = EmptyPage;
			}

			return ret;
		}

		ResultCode scanStop(unsigned short threadid)
		{
			return Ready;
		}

		void threadClose(unsigned short threadid) { }

		void destroy()
		{
			delete FullPage;
			delete EmptyPage;
		}

	private:
		static const int SOURCESIZE = 100;

		Page* FullPage;
		Page* EmptyPage;

		int calls;
		Lock lock;
};

void runtest(Query& q) 
{
	Operator::Page* out;
	Operator::GetNextResultT result; 

	bool sawFinished = false;
	
	q.threadInit();

	// Scan init.
	//
	if (q.scanStart() != Operator::Ready) {
		fail("Scan initialization failed.");
	}

	// Picking a large threshhold for testing.
	//
	for (int i=0; i < 5000; ++i) {
		int iter=0;

		result = q.getNext();

		out = result.second;

		if (result.first != Operator::Ready
				&& result.first != Operator::Finished) {
			throw TestFailException();
		}

		Operator::Page::Iterator it = out->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
			++iter;
		}

#ifdef VERBOSE
		cout << "Iteration " << i << " returned " << result.first
			<< " and " << iter << " tuples." << endl;
#endif

		if (sawFinished) {
			// Once we have seen Finished:
			// 1. GetNext will always return Finished.
			if (result.first != Operator::Finished)
				throw TestFailException();

			// 2. GetNext will always return a valid, empty page.
			if (iter != 0)
				throw TestFailException();

		} else {

			if (result.first == Operator::Finished) {
				// First "Finished", iter can be anything.
				sawFinished = true;
			}
		}
	}

	// Stop scan.
	//
	if (q.scanStop() != Operator::Ready) {
		fail("Scan stop failed.");
	}

	q.threadClose();

	q.destroynofree();
}

void testFakeScan()
{
	Query q;
	Config cfg;
	FakeScan node;

	q.tree = &node;

	node.init(cfg, cfg.getRoot());

	runtest(q);
}

void testScanOp() 
{
	const int tuples = 199;
	const int buffsize = 1 << 5;

	Config cfg;
	Query q;
	ScanOp node;

	createfile(tempfilename, tuples);

	// config
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("file", Setting::TypeString) = tempfilename;
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// init
	q.tree = &node;

	node.init(cfg, scannode);

	runtest(q);

	deletefile(tempfilename);
}

void testParallelScanOp()
{
	const int tuples = 199;
	const int buffsize = 1 << 5;

	Config cfg;
	Query q;
	ParallelScanOp node;

	createfile(tempfilename, tuples);

	// config
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& files = scannode.add("files", Setting::TypeList);
	files.add(Setting::TypeString) = tempfilename;
	Setting& mapping = scannode.add("mapping", Setting::TypeList);
	Setting& mappinggroup0 = mapping.add(Setting::TypeList);
	mappinggroup0.add(Setting::TypeInt) = 0;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// init
	q.tree = &node;

	node.init(cfg, scannode);

	runtest(q);

	deletefile(tempfilename);
}

void testPartitionedScanOp()
{
	const int tuples = 199;
	const int buffsize = 1 << 5;

	Config cfg;
	Query q;
	PartitionedScanOp node;

	createfile(tempfilename, tuples);

	// config
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = buffsize;

	Setting& scannode = cfg.getRoot().add("scan", Setting::TypeGroup);
	scannode.add("filetype", Setting::TypeString) = "text";
	Setting& files = scannode.add("files", Setting::TypeList);
	files.add(Setting::TypeString) = tempfilename;
	Setting& schemanode = scannode.add("schema", Setting::TypeList);
	schemanode.add(Setting::TypeString) = "long";
	schemanode.add(Setting::TypeString) = "long";

	// init
	q.tree = &node;

	node.init(cfg, scannode);

	runtest(q);

	deletefile(tempfilename);
}

int main()
{
	// Check test class for sanity.
	testFakeScan();

	// Check scanners for sanity.
	testScanOp();
	testParallelScanOp();
	testPartitionedScanOp();

	// TODO: Check more complex operations when fed from test class.
	
	return 0;
}
