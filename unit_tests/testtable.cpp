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

#include "../schema.h"
#include "../operators/loaders/table.h"

#include "common.h"

using namespace std;

const int TESTS=10;

#ifdef VERBOSE
string formatout(const vector<string>& str) {
	string ret = "|";
	for (vector<string>::const_iterator it=str.begin(); it!=str.end(); ++it)
		ret += *it + '|';
	return ret;
}
#endif

void testshallowcopy(const unsigned int tuples) 
{
	void* tupleptrs[tuples];

	Schema s;
	s.add(CT_INTEGER);
	s.add(CT_CHAR, 25);
	s.add(CT_INTEGER);

	PreloadedTextTable wr1;
	wr1.init(&s, 128);

	for (unsigned int i=0; i<tuples; ++i) {
		vector<string> v;
		stringstream ss;
		ss << i;
		v.push_back(ss.str());
		v.push_back("Hello worldie!");
		v.push_back(ss.str());
		wr1.append(v);
	}

	PreloadedTextTable wr2(wr1);
	TupleBuffer* b;
	void* tuple;
	int k = 0;

	while ( (b = wr1.readNext()) ) 
	{
		int i = 0;
		while ( (tuple = b->getTupleOffset(i++)) )
	   	{
			tupleptrs[k] = tuple;
			k++;
#ifdef VERBOSE
			cout << formatout(wr1.schema()->outputTuple(tuple)) << '\n';
#endif
		}
	}

	k = 0;

#ifdef VERBOSE
	cout << "----CONTENTS-OF-SHALLOW-COPY----" << endl;
#endif

	while ( (b = wr2.readNext()) )
   	{
		int i = 0;
		while ( (tuple = b->getTupleOffset(i++)) )
	   	{
			if (tupleptrs[k] != tuple)
				fail("Shallow table copy points to different data!");
			k++;
#ifdef VERBOSE
			cout << formatout(wr2.schema()->outputTuple(tuple)) << '\n';
#endif
		}
	}

	// Closing wr2 is double-free!
	wr1.close();
}

void testntiactuallywrites() 
{
	long long values[] = { 0xDEADDEADDEADDEADull, 0xBEEFBEEFBEEFBEEFull };
	Schema s;
	s.add(CT_LONG);
	s.add(CT_LONG);

	PreloadedTextTable t;
	t.init(&s, 128);
	t.nontemporalappend16(&values);

	void* dest = t.readNext()->getTupleOffset(0);
	if(values[0] != s.asLong(dest, 0))
		fail("non-temporal append failed to write at field 1.");
	if(values[1] != s.asLong(dest, 1))
		fail("non-temporal append failed to write at field 2.");
}

int main()
{
	srand48(time(NULL));
	testntiactuallywrites();
	for (int i=0; i<TESTS; ++i)
	{
		testshallowcopy(lrand48()%1000);
	}
	return 0;
}
