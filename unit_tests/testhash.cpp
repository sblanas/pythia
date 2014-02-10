/*
 * Copyright 2007, Pythia authors (see AUTHORS file).
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

#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <sstream>

#include "../hash.h"

#include "common.h"

// declaring what exists in ../hash.cpp for testing
unsigned int getlogarithm(unsigned int k);

//#define VERBOSE

using namespace std;

const int TESTS = 20000;

void testGenerate() {
	ParameterizedModuloValueHasher mhf(0, 1024, 2);
	vector<HashFunction*> ret = mhf.generate(3);

	if (	(mhf.buckets() != (1<<10))	
			|| (ret[0]->buckets() != (1<<3))
			|| (ret[1]->buckets() != (1<<3))
			|| (ret[2]->buckets() != (1<<4)) 
		) {
		fail("Unexpected result when creating multi-pass hash function.");
	}

	for (unsigned int i=0; i<ret.size(); ++i) 
	{
		delete ret[i];
	}
}

void testGetLogarithm() 
{
	// Why the hell ceil() and floor() return doubles?

#ifdef VERBOSE
	cout << "Testing " << TESTS << " getlogarithm()... ";
#endif
	int a[TESTS];
	for (int i=0; i<TESTS; ++i)
		a[i] = lrand48()%(1<<10)+1;

	for (int i=0; i<TESTS; ++i) {
		int res = getlogarithm(a[i]);
		int exp = (int)ceil(log(a[i])/log(2));
#ifdef VERBOSE
		cout << "getlogarithm(" << a[i] << ")=" << res << endl;
#endif
		if (res != exp) {
			ostringstream oss;
			oss << "getlogarithm(" << a[i] << ")=" << res << " (expected " << exp << ")";
			fail(oss.str().c_str());
		}
	}
}

void testModuloBounds() 
{
#ifdef VERBOSE
	cout << "Testing " << TESTS << " times that hash() <= k... ";
#endif
	ModuloValueHasher a(1024);
	for (int i=0; i<=TESTS; ++i) {
		if (a.hash(lrand48()%10001) >= 1024)
			fail("Hashed value out of bounds.");
	}
}

void testAlwaysZeroFn()
{
	AlwaysZeroHasher azh;
	for (int i=0; i<=TESTS; ++i) 
	{
		if (azh.buckets() != 1)
			fail("AlwaysZeroHasher doesn't have exactly one bucket.");
		if (azh.hash( (void*) lrand48(), (size_t) lrand48()) != 0)
			fail("AlwaysZeroHasher returns non-zero hash value.");
	}
}

void testExactRange()
{
	for (int i=0; i<=TESTS; ++i)
	{
		long mx = lrand48() & (~1);
		long mn = 0;
		do
		{
			mn = lrand48();
		} while(mn >= mx);

		ExactRangeValueHasher erv(mn, mx, 80);

		if (erv.buckets() != 80)
			fail("ExactRangeValueHasher doesn't have requested number of buckets.");
		if (erv.hash(mx) > 79)
			fail("ExactRangeValueHasher returns bucket number higher than max bucket for max range.");

		for (unsigned int j=1; j<80; ++j)
		{
			assertmsg(erv.hash(erv.minimumforbucket(j)-1) == j-1, "Value lower than minimum hashes to same bucket.");
			assertmsg(erv.hash(erv.minimumforbucket(j)) == j, "Value equal to minimum hashes to different bucket.");
		}
	}
}


int main() {
	srand48(time(NULL));
	testGenerate();
	testGetLogarithm();
	testModuloBounds();
	testAlwaysZeroFn();
	testExactRange();
	return 0;
}
