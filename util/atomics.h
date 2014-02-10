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

#ifndef __MYATOMICS__
#define __MYATOMICS__

#include "../exceptions.h"

inline 
void* atomic_compare_and_swap_pointer(void** ptr, void* oldval, void* newval)
{
#if defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_2) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_1)
	newval = __sync_val_compare_and_swap(ptr, oldval, newval);
#elif defined(__sparcv9)
	__asm__ __volatile__ ( \
			"casx [%3], %2, %0   \n\t" \
:           "+r" (newval), "+m" (*ptr)     /* output */ \
:           "r" (oldval), "r" (ptr) /* input */ \
:           "memory"    /* clobber */ \
	);
#elif defined(__sparcv8)
	__asm__ __volatile__ ( \
			"cas [%3], %2, %0   \n\t" \
:           "+r" (newval), "+m" (*ptr)     /* output */ \
:           "r" (oldval), "r" (ptr) /* input */ \
:           "memory"    /* clobber */ \
	);
#elif defined(__i386__) 
	__asm__ __volatile__ ( \
			"lock cmpxchgl %2, %1   \n\t" \
:           "=a" (newval), "+m" (*ptr)     /* output */ \
:           "r" (newval), "a" (oldval) /* input */ \
	);
#elif defined(__x86_64__)
	__asm__ __volatile__ ( \
			"lock cmpxchgq %q2, %1   \n\t" \
:           "=a" (newval), "+m" (*ptr)     /* output */ \
:           "r" (newval), "a" (oldval) /* input */ \
	);
#else
#error CAS instruction not known for this architecture.
#endif
	return newval;
}

/**
 * Atomic compare-and-swap: If the current value of \a *ptr is \a oldval, then
 * write \a newval into \a *ptr.
 * @return the contents of *ptr before the operation. If return value is equal
 * to \a oldval, this means that the operation was successful.
 */
template <typename T>
inline
T atomic_compare_and_swap(volatile T* ptr, T oldval, T newval)
{
#if defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_2) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_1)
	return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
	switch (sizeof(T))
	{
		case sizeof(void*):
			return (T) atomic_compare_and_swap_pointer((void**)ptr, (void*)oldval, (void*)newval);

		default:
			throw NotYetImplemented();
	}
#endif
}

/**
 * Atomic increment, to value pointed to by \a ptr, and return old value.
 */
template <typename T>
inline
T atomic_increment(volatile T* ptr, T by=1)
{
	T ret;
#if defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_2) || defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_1)
	ret = __sync_fetch_and_add(ptr, by);
#else
	T oldval;
	do
	{
		oldval = *ptr;
		ret = atomic_compare_and_swap(ptr, oldval, oldval+by);
	} while (oldval != ret);
#endif
	return ret;
}

#endif
