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
using namespace std;

#include "common.h"

#include "../util/hashtable.h"

const int TESTS=10;
const char* HASHTABLESERIALIZATIONFILENAME = "hashtable.test";

void testiterator(const int numtuples) {
	int valid[numtuples];
	for (int i=0; i<numtuples; ++i)
	{
		valid[i] = 0;
	}

	HashTable ht;
	ht.init(1, 2*sizeof(int), sizeof(int), vector<char>(), 0);
	ht.bucketclear(0, 1);

	for (int i=0; i<numtuples; ++i) 
	{
		*(int*)ht.allocate(0, 0) = i;
	}

	ht.prefetch(0);

	HashTable::Iterator it = ht.createIterator();
	ht.placeIterator(it, 0);

	void* tup;
	while( (tup = it.next()) )
	{
		int v = *(int*)tup;
#ifdef VERBOSE
		cout << v << endl;
#endif
		if (v < 0 || v >= numtuples)
			fail("Value outside generated range");
		valid[v]++;
	}

	for (int i=0; i<numtuples; ++i)
	{
		if (valid[i] != 1)
			fail("A value does not appear exactly once");
	}

	ht.bucketclear(0, 1);
	ht.destroy();
}

void testserialize(const int numtuples)
{
	int valid[numtuples];
	for (int i=0; i<numtuples; ++i)
	{
		valid[i] = 0;
	}

	HashTable ht;
	ht.init(numtuples, sizeof(int), sizeof(int), vector<char>(), 0);
	ht.bucketclear(0,1);

	for (int i=0; i<numtuples; ++i) 
	{
		*(int*)ht.allocate(i, 0) = i;
	}

	HashTable::Iterator it = ht.createIterator();

	for (int i=0; i<numtuples; ++i) 
	{
		void* tup;
		ht.placeIterator(it, i);

		while( (tup = it.next()) )
		{
			int v = *(int*)tup;
#ifdef VERBOSE
			cout << v << endl;
#endif
			if (v < 0 || v >= numtuples)
				fail("Value outside generated range");
			valid[v]++;
		}
	}

	for (int i=0; i<numtuples; ++i)
	{
		if (valid[i] != 1)
			fail("A value does not appear exactly once");
	}

	ht.serialize(HASHTABLESERIALIZATIONFILENAME, 0);

	ht.bucketclear(0,1);
	ht.destroy();
}

void testdeserialize(const int numtuples)
{
	int valid[numtuples];
	for (int i=0; i<numtuples; ++i)
	{
		valid[i] = 0;
	}

	HashTable ht;
	ht.init(numtuples, sizeof(int), sizeof(int), vector<char>(), 0);
	ht.deserialize(HASHTABLESERIALIZATIONFILENAME, 0);

	HashTable::Iterator it = ht.createIterator();

	for (int i=0; i<numtuples; ++i) 
	{
		void* tup;
		ht.placeIterator(it, i);

		while( (tup = it.next()) )
		{
			int v = *(int*)tup;
#ifdef VERBOSE
			cout << v << endl;
#endif
			if (v < 0 || v >= numtuples)
				fail("Value outside generated range");
			valid[v]++;
		}
	}

	for (int i=0; i<numtuples; ++i)
	{
		if (valid[i] != 1)
			fail("A value does not appear exactly once");
	}

	ht.bucketclear(0,1);
	ht.destroy();
}

void testserialization()
{
	long v = lrand48() % 10000;
	testserialize(v);
	testdeserialize(v);
	deletefile(HASHTABLESERIALIZATIONFILENAME);
}

int main()
{
	srand48(time(NULL));
	for (int i=0; i<TESTS; ++i)
	{
		testiterator(lrand48()%10000);
		testserialization();
	}
	return 0;
}
