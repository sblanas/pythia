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

#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cassert>
#ifdef ENABLE_NUMA
#include <numaif.h>
#endif

#ifdef ENABLE_NUMA
#define NUMA_VERSION1_COMPATIBILITY //< v1 is widely available 
#include <numa.h>
#endif

#include <vector>
#include <iostream>
using namespace std;

#include "custom_asserts.h"

/**
 * Returns NUMA node for \a p.
 */
int numafromaddress(void* p)
{
#ifdef ENABLE_NUMA
	int node = -1;
	assert(0 == get_mempolicy(&node, NULL, 0, p, MPOL_F_ADDR | MPOL_F_NODE));
	return node;
#else
	return 0;
#endif
}

/**
 * Returns a vector of CPUs for the particular NUMA \a node.
 * TODO Actual topology knowledge in in affinitizer, should be leveraged.
 */
vector<unsigned short> cpusfornode(int node)
{
	vector<unsigned short> ret;
	
	// Hacking CPU-to-node mapping, as numa_node_to_cpus doesn't appear to be
	// multi-threaded.
#warning CPU-to-node mapping is a hack; may not work with this machine.
	int sockets = 1;
#ifdef ENABLE_NUMA
	sockets = numa_max_node() + 1;
#endif
	for (int i = node; i<80; i+=sockets)
		ret.push_back(i);

	return ret;
}

/**
 * Local NUMA "discovery". Currently hardcoded.
 * If thread is not affinitized, favors NUMA node 0.
 * TODO Actual topology knowledge in in affinitizer, should be leveraged.
 */
int localnumanode()
{
#warning CPU-to-node mapping is a hack; may not work with this machine.
	int socketmask = 0;
#ifdef ENABLE_NUMA
	socketmask = numa_max_node();
	assertpowerof2(socketmask+1);
#endif

	cpu_set_t mask;
	CPU_ZERO(&mask);

	int tid = syscall(SYS_gettid);
	assert(sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == 0);

	for (unsigned int i=0; i<80; ++i)
	{
		assert(i < CPU_SETSIZE);
		if (CPU_ISSET(i, &mask))
			return i & socketmask;
	}

	assert(false);	// Not a single bit set in mask?!
	return 0;
}

/**
 * Checks if this thread is affinitized to at least one of the CPUs of the
 * NUMA node where \a address is homed.
 */
bool checkifaddresslocal(void* address)
{
#ifdef ENABLE_NUMA
	// Get node for address.
	// 
	int node = numafromaddress(address);
	
	// Get CPUs for address.
	//
	vector<unsigned short> cpus = cpusfornode(node);
	
	// Check that at least one CPU is contained in the affinity mask.
	//
	cpu_set_t mask;
	CPU_ZERO(&mask);

	int tid = syscall(SYS_gettid);
	assert(sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == 0);

	for (unsigned int i=0; i<cpus.size(); ++i)
	{
		assert(cpus[i] < CPU_SETSIZE);
		if (CPU_ISSET(cpus[i], &mask))
			return true;
	}

	return false;
#else
	// If not NUMA, everything is local.
	//
	return true;
#endif
}

void dbgprintcalleraffinity()
{
	// Check that at least one CPU is contained in the affinity mask.
	//
	cpu_set_t mask;
	CPU_ZERO(&mask);

	int tid = syscall(SYS_gettid);
	assert(sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == 0);

	for (unsigned int i=0; i<CPU_SETSIZE; ++i)
	{
		if (CPU_ISSET(i, &mask))
			cout << i << " ";
	}
	cout << endl;
}

/**
 * Assert that this address is local.
 */
void assertaddresslocal(void* address)
{
#ifdef ENABLE_NUMA
	assert(checkifaddresslocal(address));
#endif
}

/**
 * Check if this address is on specific NUMA node.
 */
bool checkifaddressonnuma(void* address, int numa)
{
	return (numafromaddress(address) == numa);
}

/**
 * Assert that this address is on specific NUMA node.
 */
void assertaddressonnuma(void* address, int numa)
{
	assert(checkifaddressonnuma(address, numa));
}
