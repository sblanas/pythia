
/*
 * Copyright 2013, Pythia authors (see AUTHORS file).
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
#include <cassert>
#include <algorithm>
using namespace std;

#include "libconfig.h++"
using namespace libconfig;

#include "common.h"
#include "../operators/operators.h"

const char* HDF5FILE = "unit_tests/data/ptf_small.h5";
const char* DIRECTORY = "unit_tests/data/ptfascii/candidate/";
const char* DATASET = "id";
const char* CORRECT = "unit_tests/data/ptfascii/candidate.first3proj";

int test()
{
	vector<unsigned int> valuespicked;
	vector<unsigned int> correctvalues;
	vector<unsigned int> output;

	// Scan over CORRECT, and pick lines at random for querying.
	// For each line picked, remember line number to compare with index offset.
	//
	ifstream correct(CORRECT);
	int a, b, c;
	int line = 0;
	correct >> a >> b >> c;
	while(correct)
	{
		if ((lrand48() & 0x3) == 0)
		{
			valuespicked.push_back(a);
			correctvalues.push_back(b);
		}
		++line;
		correct >> a >> b >> c;
	}
	correct.close();

	// Lookup all values in valuespicked array.
	//
	assert(sizeof(unsigned int) == sizeof(CtInt));
	Operator::Page datain(&valuespicked[0], sizeof(CtInt)*valuespicked.size(),
		   	0, sizeof(CtInt));
	Schema schemain;
	schemain.add(CT_INTEGER);

	// Create config object for parameters.
	//
	Config cfg;

	// init node1
	Setting& hdf5node = cfg.getRoot().add("hdf5index", Setting::TypeGroup);
	hdf5node.add("file", Setting::TypeString) = HDF5FILE;
	Setting& projattrnode = hdf5node.add("pick", Setting::TypeArray);
	projattrnode.add(Setting::TypeString) = "/candidate/sub_id";
	hdf5node.add("indexdirectory", Setting::TypeString) = DIRECTORY;
	hdf5node.add("indexdataset", Setting::TypeString) = DATASET;

	// init node2
	cfg.getRoot().add("path", Setting::TypeString) = "./";
	cfg.getRoot().add("buffsize", Setting::TypeInt) = 64;
	

	// Create operator, initialize appropriately.
	//
	IndexHdf5Op op;
	op.init(cfg, hdf5node);
	op.threadInit(0);

	assert(op.getOutSchema().columns() == 1);
	assert(op.getOutSchema().getColumnType(0) == CT_LONG);

	if (op.scanStart(0, &datain, schemain) == Operator::Error)
		fail("Error in scanStart()");

	Operator::GetNextResultT result; 
	result.first = Operator::Ready;

	while(result.first == Operator::Ready)
	{
		result = op.getNext(0);

		if (result.first == Operator::Error)
			fail("GetNext returned error.");

		Operator::Page::Iterator it = result.second->createIterator();
		void* tuple;
		while ( (tuple = it.next()) )
		{
			CtLong v = *(CtLong*)tuple;
			output.push_back(static_cast<int>(v));
		}
	}

	if (op.scanStop(0) == Operator::Error)
		fail("Error in scanStop()");

	op.threadClose(0);
	op.destroy();

	// Make sure output and correctvalues are the same.
	// The following works because the test file contains no duplicates.
	//
	assert(output.size() == correctvalues.size());
	sort(output.begin(), output.end());
	sort(correctvalues.begin(), correctvalues.end());
	for (unsigned int i=0; i<output.size(); ++i)
	{
		assert(output.at(i) == correctvalues.at(i));
	}

	return 0;
}

int main()
{
	int retries = 10;
	srand48(time(0));
	for (int i=0; i<retries; ++i) 
	{
		test();
	}

	return 0;
}
