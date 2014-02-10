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

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(__i386__) && !defined(__x86_64__) && !defined(__sparc__)
#warning No supported architecture found -- timers will return junk.
#endif

static __inline__ unsigned long long curtick() {
	unsigned long long tick;
#if defined(__i386__)
	unsigned long lo, hi;
	__asm__ __volatile__ (".byte 0x0f, 0x31" : "=a" (lo), "=d" (hi));
	tick = (unsigned long long) hi << 32 | lo;
#elif defined(__x86_64__)
	unsigned long lo, hi;
	__asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
	tick = (unsigned long long) hi << 32 | lo;
#elif defined(__sparc__)
	__asm__ __volatile__ ("rd %%tick, %0" : "=r" (tick));
#endif
	return tick;
}

static __inline__ void startTimer(unsigned long long* t) {
	*(volatile unsigned long long*)t = curtick();
}

static __inline__ void stopTimer(unsigned long long* t) {
	*(volatile unsigned long long*)t = curtick() - *(volatile unsigned long long*)t;
}

#ifdef __cplusplus
}
#endif
