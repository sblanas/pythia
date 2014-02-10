/*
 * Copyright 2012, Pythia authors (see AUTHORS file).
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
#include "../util/buffer.h"

#include "common.h"

using namespace std;

const int TESTS=10;
const unsigned int TUPLESIZE = 16;

void randompopulate(TupleBuffer* tb, unsigned int elements)
{
	for (unsigned int i=0; i<elements; ++i)
	{
		void* space = tb->allocateTuple();
		assertmsg(space != NULL, "Not enough space in buffer.");

		char* spacechar = (char*) space;
		for (unsigned int i=0; i<TUPLESIZE; ++i)
		{
			spacechar[i] = 'a' + i;
		}

		*(unsigned int*)(spacechar+4) = lrand48();
	}
}

void verifypayloadintact(void* tup)
{
	char* tupchar = (char*) tup;
	assertmsg(*(tupchar+ 0) == 'a' +  0, "Non-key data have been modified.");
	assertmsg(*(tupchar+ 1) == 'a' +  1, "Non-key data have been modified.");
	assertmsg(*(tupchar+ 2) == 'a' +  2, "Non-key data have been modified.");
	assertmsg(*(tupchar+ 3) == 'a' +  3, "Non-key data have been modified.");
	assertmsg(*(tupchar+ 8) == 'a' +  8, "Non-key data have been modified.");
	assertmsg(*(tupchar+ 9) == 'a' +  9, "Non-key data have been modified.");
	assertmsg(*(tupchar+10) == 'a' + 10, "Non-key data have been modified.");
	assertmsg(*(tupchar+11) == 'a' + 11, "Non-key data have been modified.");
	assertmsg(*(tupchar+12) == 'a' + 12, "Non-key data have been modified.");
	assertmsg(*(tupchar+13) == 'a' + 13, "Non-key data have been modified.");
	assertmsg(*(tupchar+14) == 'a' + 14, "Non-key data have been modified.");
	assertmsg(*(tupchar+15) == 'a' + 15, "Non-key data have been modified.");
}

unsigned int getkey(void* tup)
{
	char* tupchar = (char*) tup;
	return *(unsigned int*)(tupchar+4);
}


void verifysorted(TupleBuffer* tb, unsigned int elements)
{
	TupleBuffer::Iterator it = tb->createIterator();

	unsigned int min = 0;

	for (unsigned int i=0; i<elements; ++i)
	{
		void* tup = it.next();
		assertmsg(tup != NULL, "Fewer elements than expected.");

		verifypayloadintact(tup);

		unsigned int newval = getkey(tup);
		assertmsg(newval >= min, "Output not sorted.");
		min = (newval > min) ? newval : min;
	}
}

void sortthis(TupleBuffer* tb)
{
	tb->sort<unsigned int>(4);
}

void testfindsmallest(TupleBuffer* tb, unsigned int elements)
{
	for (unsigned int i=0; i<elements*TESTS; ++i)
	{
		// Lookup random unsigned int.
		//
		unsigned int randomkey = lrand48();
		unsigned int idx = tb->findsmallest<unsigned int>(4, randomkey);

		assertmsg(idx <= elements, "Index value returned from findsmallest() larger than array.");
		
		// If index != 0, assert that key at index-1 is less than randomkey.
		//
		if (idx != 0)
		{
			char* tup1 = (char*) tb->getTupleOffset(idx-1);
			assertmsg(tup1 != NULL, "Previous tuple invalid, but not at beginning of array.");
			verifypayloadintact(tup1);
			assertmsg(getkey(tup1) < randomkey, "Previous key not less than random key.");
		}

		// If index != end, assert that key at index is equal or greater than randomkey.
		//
		if (idx != elements)
		{
			char* tup2 = (char*) tb->getTupleOffset(idx);
			assertmsg(tup2 != NULL, "Current tuple invalid, but not past the end of array.");
			verifypayloadintact(tup2);
			assertmsg(randomkey <= getkey(tup2), "Current key not equal or greater than random key.");
		}
	}
}

void testpagesort(unsigned int elements)
{
	TupleBuffer tb((elements + lrand48() % 10) * TUPLESIZE, TUPLESIZE, NULL);

	randompopulate(&tb, elements);

	sortthis(&tb);

	verifysorted(&tb, elements);

	testfindsmallest(&tb, elements);
}

int main()
{
	srand48(time(NULL));
	for (int i=0; i<TESTS; ++i)
	{
		testpagesort(lrand48()%10000);
	}
	return 0;
}
