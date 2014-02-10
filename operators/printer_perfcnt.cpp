
/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
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

#include "operators.h"
#include "operators_priv.h"

#include "../util/static_assert.h"

/**
 * Returns number of performance counters.
 */
unsigned short numberofcounters()
{
	static_assert(sizeof(unsigned int) == 4);

	unsigned int eax, ebx, ecx, edx;
	unsigned int maxid, leafid;
	unsigned int numgp;

	// Read max value for next tests.
	//
	leafid = 0x0;
	__asm__ ("cpuid" \
			: "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) \
			: "a" (leafid)	\
			);

	maxid = eax;
	leafid = 0xA;

	if (maxid < leafid) 
		return 0;

	__asm__ ("cpuid" \
			: "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) \
			: "a" (leafid)	\
			);

	numgp = (eax & 0x0000FF00ul) >>  8;

	return static_cast<unsigned short>(numgp);
}

/**
 * Returns performance counter for \a counterid.
 * 32-bit (fast) read.
 */
unsigned int fastreadpmc(unsigned int counterid) 
{
	dbgassert(counterid < numberofcounters());
	static_assert(sizeof(unsigned int) == 4);

	unsigned int lo;

	// Bit 30 is cleared to indicate general-purpose counter.
	//
	counterid &= 0xBFFFFFFFu;

	// Bit 31 is set to indicate 32-bit (fast) read.
	//
	counterid |= 0x80000000u;

	__asm__ __volatile__ ("rdpmc" 		\
			: "=a" (lo)	\
			: "c" (counterid)		\
			: "edx" );
	return lo;
}

/**
 * Returns performance counter for \a counterid.
 * 48-bit (slow) read.
 */
unsigned long long slowreadpmc(unsigned int counterid) 
{
	dbgassert(counterid < numberofcounters());
	static_assert(sizeof(unsigned int) == 4);

	unsigned int hi, lo;

	// Bit 30 is cleared to indicate general-purpose counter.
	// Bit 31 is cleared to indicate full-width (slow) read.
	//
	counterid &= 0x3FFFFFFFu;

	__asm__ __volatile__ ("rdpmc" 		\
			: "=d" (hi), "=a" (lo)	\
			: "c" (counterid)		\
			);
	return (((unsigned long long) hi) << 32) | lo;
}

void PerfCountPrinter::init(libconfig::Config& root, libconfig::Setting& node)
{
	SingleInputOp::init(root, node);

	schema = nextOp->getOutSchema();

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		events.push_back(EventsPerOp());
	}
}

enum NumericOperation
{
	Add,
	Subtract
};

template <NumericOperation T>
void allCounters(unsigned long long* location);

template <>
inline
void allCounters<Add>(unsigned long long* location)
{
	unsigned short total = numberofcounters();

	dbgassert(total < PerfCountPrinter::MAX_COUNTERS);

	for (unsigned short i=0; i<total; ++i)
		location[i] += slowreadpmc(i);
}

template <>
inline
void allCounters<Subtract>(unsigned long long* location)
{
	unsigned short total = numberofcounters();

	dbgassert(total < PerfCountPrinter::MAX_COUNTERS);

	for (unsigned short i=0; i<total; ++i)
		location[i] -= slowreadpmc(i);
}

Operator::ResultCode PerfCountPrinter::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	ResultCode res;
	allCounters<Subtract>(events[threadid].ScanStartCnt);
	res = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	allCounters<Add>(events[threadid].ScanStartCnt);
	return res;
}

Operator::GetNextResultT PerfCountPrinter::getNext(unsigned short threadid)
{
	GetNextResultT res;
	allCounters<Subtract>(events[threadid].GetNextCnt);
	res = nextOp->getNext(threadid);
	allCounters<Add>(events[threadid].GetNextCnt);
	return res;
}

Operator::ResultCode PerfCountPrinter::scanStop(unsigned short threadid)
{
	ResultCode res;
	allCounters<Subtract>(events[threadid].ScanStopCnt);
	res = nextOp->scanStop(threadid);
	allCounters<Add>(events[threadid].ScanStopCnt);
	return res;
}
