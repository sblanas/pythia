
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

#include "perfcounters.h"

#if defined(__sparc__)
#include <iostream>
using namespace std;
#include <libcpc.h>
#include <errno.h>
#include "exceptions.h"
#endif

void PerfCounters::init() 
{
#if defined(__i386__) || defined(__x86_64__)
	// Nothing to do, `wrmsr` must be called from ring 0.
	//
	
#elif defined(__sparc__)
	if ((cpc = cpc_open(CPC_VER_CURRENT)) == NULL) {
		cout << "perf counters unavailable: " << strerror(errno) << endl;
		throw PerfCountersException();
	}

#else
#error Performance counters not known for this architecture.
#endif
}

void PerfCounters::destroy() 
{
#if defined(__i386__) || defined(__x86_64__)
	// Nothing to do, `wrmsr` must be called from ring 0.
	//
	
#elif defined(__sparc__)
	cpc_close(cpc);

#else
#error Performance counters not known for this architecture.
#endif
}
void PerfCounters::threadinit() 
{
#if defined(__i386__) || defined(__x86_64__)
	// Nothing to do, `wrmsr` must be called from ring 0.
	//
	
#elif defined(__sparc__)
	char *event0 = NULL, *event1 = NULL;
	cpc_set_t *set;

	if ((event0 = getenv("EVENT0")) == NULL) {
		event0 = "L2_dmiss_ld";
	}

	if ((event1 = getenv("EVENT1")) == NULL) {
		event1 = "Instr_cnt";
	}

	if ((set = cpc_set_create(cpc)) == NULL) {
		cout << "could not create set: " << strerror(errno) << endl;
		throw PerfCountersException();
	}

	if (cpc_set_add_request(cpc, set, event0, 0, CPC_COUNT_USER, 0, NULL) == -1) {
		cout << "could not add first request: " << strerror(errno) << endl;
		throw PerfCountersException();
	}

	if (cpc_set_add_request(cpc, set, event1, 0, CPC_COUNT_USER, 0, NULL) == -1) {
		cout << "could not add second request: " << strerror(errno) << endl;
		throw PerfCountersException();
	}

	if (cpc_bind_curlwp(cpc, set, 0) == -1) {
		cout << "cannot bind lwp: " << strerror(errno) << endl;
		throw PerfCountersException();
	}

#else
#error Performance counters not known for this architecture.
#endif
}
