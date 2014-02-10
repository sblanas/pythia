
/*
 * Copyright 2009, Pythia authors (see AUTHORS file).
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
using namespace std;

#include "common.h"

#include "../operators/loaders/loader.h"
#include "../operators/loaders/table.h"
#include "../schema.h"

// #define VERBOSE

#ifdef VERBOSE
string formatout(const vector<string>& str) {
	string ret = "|";
	for (vector<string>::const_iterator it=str.begin(); it!=str.end(); ++it)
		ret += *it + '|';
	return ret;
}
#endif

int work(const char* filename)
{
	Schema s;
	s.add(CT_INTEGER);
	s.add(CT_INTEGER);

	PreloadedTextTable out;
	out.init(&s, 1024);

	Loader l("|");
	l.load(filename, out, false);

	TupleBuffer* b;
	void* tuple;

	while ( (b = out.readNext()) ) 
	{
		int i = 0;
		while( (tuple = b->getTupleOffset(i++)) ) 
		{
#ifdef VERBOSE
			cout << formatout(out.schema()->outputTuple(tuple)) << endl;
#endif
			if (s.asInt(tuple, 0) != i)
				fail("First value in tuple is wrong.");
			if (s.asInt(tuple, 1) != i)
				fail("Second value in tuple is wrong.");
		}
	}

	return 0;
}

int main()
{
	work("unit_tests/loadertest/test.tbl");
	work("unit_tests/loadertest/test.tbl.bz2");
	return 0;
}
