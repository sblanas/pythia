
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

#include <pthread.h>

#include "common.h"
#include "../util/parallelqueue.h"
#include "../Barrier.h"

const unsigned long long ITEMSPERTHREAD = 1024*128;	//< this MUST be a power of two
typedef ParallelQueue<unsigned int, 32> TestQueueT;

struct ThreadArg {
	int threadid;
	TestQueueT* queue;
	unsigned long long threadsum;
};

PThreadLockCVBarrier* barrier;

void* consume(void* threadarg)
{
	TestQueueT* queue = ((ThreadArg*)threadarg)->queue;
	unsigned long long* threadsum = &((ThreadArg*)threadarg)->threadsum;

	unsigned int val = 0;

	while (queue->pop(&val) != TestQueueT::Rundown)
	{
		(*threadsum) += val;
	}

	for (int i=0; i<1024; ++i)
	{
		if (queue->pop(&val) != TestQueueT::Rundown)
			fail("Expected Rundown, but pop() succeded.");
	}

	return NULL;
}

void* produce(void* threadarg)
{
	int threadid = ((ThreadArg*)threadarg)->threadid;
	TestQueueT* queue = ((ThreadArg*)threadarg)->queue;

	for (unsigned int i=0; i<ITEMSPERTHREAD; ++i)
	{
		if (queue->push(&i) == TestQueueT::Rundown)
			fail("Expected push() to succeded, but received Rundown.");
	}

	barrier->Arrive();

	if (threadid == 0)
		queue->signalRundown();

	barrier->Arrive();

	for (unsigned int i=0; i<1024; ++i)
	{
		if (queue->push(&i) != TestQueueT::Rundown)
			fail("Expected Rundown, but push() succeded.");
	}

	return NULL;
}

void test(const int prodthreads, const int consthreads)
{
	const int threads = prodthreads + consthreads;
	ThreadArg ta[threads];
	pthread_t threadpool[threads];

	barrier = new PThreadLockCVBarrier(prodthreads);
	TestQueueT queue;

	for (int i=0; i<threads; ++i) 
	{
		ta[i].threadid = i;
		ta[i].queue = &queue;
		ta[i].threadsum = 0;
	}

	for (int i=0; i<prodthreads; ++i) 
	{
		assert(!pthread_create(&threadpool[i], NULL, produce, &ta[i]));
	}

	for (int i=0; i<consthreads; ++i) 
	{
		assert(!pthread_create(&threadpool[i+prodthreads], NULL, consume, &ta[i+prodthreads]));
	}

	for (int i=0; i<threads; ++i) 
	{
		assert(!pthread_join(threadpool[i], NULL));
	}

	delete barrier;

	// Check that the sum of all sums is equal to the maxrange * producers.
	//
	unsigned long long sumofsums = 0;
	for (int i=0; i<threads; ++i) 
	{
		sumofsums += ta[i].threadsum;
	}

	if (sumofsums != (prodthreads * (ITEMSPERTHREAD / 2uLL) * (ITEMSPERTHREAD - 1)))
		fail("Produced sum different than consumed sum."); 
}

int main()
{
	int retries = 10;
	srand48(time(0));
	for (int i=0; i<retries; ++i) 
	{
		test((lrand48() & 0x7F) + 1, (lrand48() & 0x7F) + 1);
	}
	return 0;
}
