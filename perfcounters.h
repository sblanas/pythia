
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

#ifdef __sparc__
#include <libcpc.h>
#endif

class PerfCounters {
	public:
		void init();
		void threadinit();
		void destroy();

		inline void writeCounters(unsigned long long* counter1, unsigned long long* counter2)
		{
#ifdef PERFCOUNT
#if defined(__i386__) || defined(__x86_64__)
			*counter1 = readpmc(0);
			*counter2 = readpmc(1);
#elif defined(__sparc__)
			unsigned long long val;
			__asm__ __volatile__ (
					"rd %%pic, %0"
					: "=r" (val) /* output */
					);
			*counter1 = val >> 32;
			*counter2 = val & 0xFFFFFFFFull;
#else
#error Performance counters not known for this architecture.
#endif
#endif
		}

	private:
#if defined(__i386__) || defined(__x86_64__)
		inline unsigned long long readpmc(unsigned int counterid) {
			unsigned long hi, lo;
			__asm__ __volatile__ ("rdpmc"        \
					: "=d" (hi), "=a" (lo)  \
					: "c" (counterid)       \
					);
			return (((unsigned long long) hi) << 32) | lo;
		}

#elif defined(__sparc__)
		cpc_t* cpc;

#endif

};
