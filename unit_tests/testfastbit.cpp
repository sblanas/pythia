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

#include "ibis.h"

static const char* nullstr = 0;

const char* DIRECTORY = "unit_tests/data/ptfascii/candidate/";
const char* DATASET = "id";
const char* CORRECT = "unit_tests/data/ptfascii/candidate.first3proj";

int test()
{
	vector<unsigned int> correctidxpos;
	vector<unsigned int> valuespicked;
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
			correctidxpos.push_back(line);
		}
		++line;
		correct >> a >> b >> c;
	}
	correct.close();

	// Lookup all values in valuespicked array.
	//
	ibis::part part(DIRECTORY, nullstr);
	ibis::query query(ibis::util::userName(), &part);
	ibis::qDiscreteRange where = ibis::qDiscreteRange(DATASET, valuespicked);
	query.setWhereClause(&where);

	int ret = query.evaluate();
	assert(ret > 0);

	query.getHitRows(output);

	// Make sure output and correctidxpos are the same.
	// No need to sort correctidxpos, as we generated it in sorted order.
	//
	assert(output.size() == correctidxpos.size());
	sort(output.begin(), output.end());
	for (unsigned int i=0; i<output.size(); ++i)
	{
		assert(output.at(i) == correctidxpos.at(i));
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
