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

#include <iostream>
#include <string>
#include <cassert>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <algorithm>

#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifdef ENABLE_NUMA
#define NUMA_VERSION1_COMPATIBILITY
#include <numa.h>
#ifndef LIBNUMA_API_VERSION
#define LIBNUMA_API_VERSION 1
#endif
#endif

#include "../exceptions.h"
#include "custom_asserts.h"
#include "affinitizer.h"

using namespace std;

/** 
 * Mapping from (socket,core) -> logical cpu id.
 */
typedef vector<vector<vector<unsigned short> > > ComputeTopologyT;

/** 
 * Mapping from (numa) -> logical cpu id.
 */
typedef vector<vector<unsigned short> > MemoryTopologyT;

struct NodeT;

typedef set<NodeT*> ChildrenT;
typedef map<int, NodeT*> LevelT;
typedef vector<LevelT> TreeT;

struct NodeT 
{
	NodeT() 
		: type(-1), visited(false), logicalid(-1), apicid(-1) 
	{ }

	char type;
	ChildrenT children;
	bool visited;

	short logicalid;
	short apicid;
};

void throwerror(const char* msg) 
{
	throw AffinitizationException(msg);
}

/**
 * Caller is affinitized to particular CPU.
 */
void affinitize(int offset)
{
#ifdef linux
	// Affinitize this thread at CPU \a offset.
	//
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(offset, &mask);

	int tid = syscall(SYS_gettid);
	if (sched_setaffinity(tid, sizeof(cpu_set_t), &mask) == -1) 
	{
		throwerror("Error in setting affinity");
	}
#else
#error Affinitization not implemented yet!
#endif
}

/**
 * Marks all nodes under \a pnode as "visited".
 */
void startDfs(NodeT* pnode) 
{
	for (ChildrenT::iterator it = pnode->children.begin();
			it != pnode->children.end(); ++it) {

		startDfs(*it);
	}

	pnode->visited = true;
}

ComputeTopologyT enumerateComputeTopology() 
{
	ComputeTopologyT ret;

	unsigned int leafid;
	unsigned int eax, ebx, ecx, edx;

	/* Read max value for next tests. */
	leafid = 0x0;
	__asm__ ("cpuid" \
			: "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) \
			: "a" (leafid)	\
			);

	unsigned int maxid = eax;

	ostringstream oss;
	oss << string((char*) &ebx, 4) 
		<< string((char*) &edx, 4) 
		<< string((char*) &ecx, 4);
	if (oss.str() != "GenuineIntel")
	{
		throwerror("CPU doesn't appear to be Intel; don't know how to enumerate topology.");
	}


	/* Read topology info. */
	leafid = 0xB;
	if (maxid < leafid) 
	{
		throwerror("CPUID leaf not supported; CPU too old.");
	}

	TreeT tree;

	tree.push_back(LevelT());

	cpu_set_t mask;

	int tid = syscall(SYS_gettid);
	if (sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == -1) 
	{
		throwerror("Error in getting affinity");
	}

	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
	{
		if (!CPU_ISSET(cpu, &mask))
			continue;

		affinitize(cpu);

		int ecxin = 0;
		int leveltype = 0;

		__asm__ ("cpuid" \
				: "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) \
				: "a" (leafid), "c" (ecxin)	\
				);
		leveltype = ((ecx >> 8) & 0xFF);

#ifdef VERBOSE
		cout << "Logical processor " << cpu << " ";
		cout << "(APIC ID: " << edx << ")" << endl;
#endif

		unsigned int level, nextid;

		NodeT* pnode = new NodeT();
		pnode->logicalid = cpu;
		pnode->apicid = edx;
		pnode->type = leveltype;

		level = 0;
		nextid = cpu;

		tree[level][nextid] = pnode;

		do 
		{
			level = (ecx & 0xFF);
			nextid = (edx >> (eax % 0x1F));

			if (tree.size() <= level + 1)
				tree.push_back(LevelT());

#ifdef VERBOSE
			cout << "Level: " << level << endl;

			unsigned int siblings = (ebx & 0xFFFF);
			cout << "Siblings at this level: " << siblings << endl;

			switch (leveltype) 
			{
				case 1:
					cout << "Level type: SMT" << endl;
					break;
				case 2:
					cout << "Level type: Core" << endl;
					break;
			}
			cout << "Unique next topology id: " << nextid << endl;
#endif

			if (tree[level+1].find(nextid) == tree[level+1].end())
				tree[level+1][nextid] = new NodeT();
			tree[level+1][nextid]->children.insert(pnode);
			pnode = tree[level+1][nextid];

			ecxin++;

			__asm__ ("cpuid" \
					: "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) \
					: "a" (leafid), "c" (ecxin)	\
					);
			leveltype = ((ecx >> 8) & 0xFF);

			tree[level+1][nextid]->type = leveltype;

		} while (leveltype != 0);

	}

	if (sched_setaffinity(tid, sizeof(cpu_set_t), &mask) == -1) 
	{
		throwerror("Error in resetting affinity");
	}

	int socket = 0;
	int core = 0;
	int context = 0;
	
	for (LevelT::iterator it = tree[tree.size()-1].begin();
			it != tree[tree.size()-1].end(); ++it) 
	{
		NodeT* pnode = it->second;

		assert(pnode->type == 0); /* assert it's unset, aka socket */
		ret.push_back(vector<vector<unsigned short> >());

		for (ChildrenT::iterator it2 = pnode->children.begin();
				it2 != pnode->children.end(); ++it2) 
		{
			assert((*it2)->type == 2); /* assert it's core */
			ret[socket].push_back(vector<unsigned short>());

			for (ChildrenT::iterator it3 = (*it2)->children.begin();
					it3 != (*it2)->children.end(); ++it3) 
			{
				assert((*it3)->type == 1); /* assert it's context */
				ret[socket][core].push_back((*it3)->logicalid);
				++context;
			}
			++core;
			context = 0;
		}

		++socket;
		core = 0;

		startDfs(it->second);
	}

	for (int level = tree.size()-1; level >= 0; --level) 
	{
		for (LevelT::iterator it = tree[level].begin();
				it != tree[level].end(); ++it) 
		{
			if (it->second->visited == false)
				throwerror("Topology is not hierarchical: "
						"A tree is rooted below the first level.");

			delete it->second;
		}
	}

	return ret;
}

MemoryTopologyT enumerateMemoryTopology()
{
	MemoryTopologyT ret;

#ifdef ENABLE_NUMA
	// NUMA enabled. Enumerate NUMA node to CPU mapping.
	//

	// Due to bug in the libnuma2.0.3 result caching mechanism, any other
	// value but 64 for BUFFERLEN will cause numa_node_to_cpus to fail in
	// subsequent calls.
	const int BUFFERLEN=64;	
	unsigned long buffer[BUFFERLEN];
	const int maxnode = numa_max_node();

	for (int i=0; i<=maxnode; ++i)
	{
		int res = numa_node_to_cpus(i, buffer, sizeof(buffer));
		assert(res == 0);
		ret.push_back(vector<unsigned short>());
#ifdef VERBOSE
		cout << "NUMA node " << i << " is logical CPUs: [";
#endif
		const int bitsperentry = (sizeof(buffer[0])*8);
		for (unsigned short cpu=0; cpu<sizeof(buffer)*8; ++cpu)
		{
			int skip = cpu / bitsperentry;
			int offset = cpu - (skip * bitsperentry);
			if (buffer[skip] & (1ull << offset))
			{
				ret[i].push_back(cpu);
#ifdef VERBOSE
				cout << cpu << ", ";
#endif
			}
		}
#ifdef VERBOSE
		cout << "\b\b]" << endl;
#endif
	}

#else
	// NUMA information unavailable. Assume all CPUs belong to one NUMA node.
	//
	ret.push_back(vector<unsigned short>());

	cpu_set_t mask;
	int tid = syscall(SYS_gettid);
	if (sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == -1) 
	{
		throwerror("Error in getting affinity");
	}

	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
	{
		if (!CPU_ISSET(cpu, &mask))
			continue;

		ret[0].push_back(cpu);
	}
#endif

	return ret;
}

/**
 * Check if two topologies contain the same logical CPUs.
 */
bool sameLogicalCPUs(const ComputeTopologyT& ctop, const MemoryTopologyT& mtop)
{
	// Create a set of all CPUs in compute topology.
	//
	multiset<unsigned short> ccpus;

	for (unsigned int socket = 0; socket < ctop.size(); ++socket)
	{
		for (unsigned int core = 0; core < ctop[socket].size(); ++core)
		{
			for (unsigned int context = 0; context < ctop[socket][core].size(); ++context)
			{
				ccpus.insert(ctop[socket][core][context]);
			}
		}
	}
	
	// Create a set of all CPUs in memory topology.
	//
	multiset<unsigned short> mcpus;
	for (unsigned int numa = 0; numa < mtop.size(); ++numa)
	{
		for (unsigned int cpu = 0; cpu < mtop[numa].size(); ++cpu)
		{
			mcpus.insert(mtop[numa][cpu]);
		}
	}
	
	// Check if equal.
	//
	if (ccpus.size() != mcpus.size())
		return false;
	return equal(ccpus.begin(), ccpus.end(), mcpus.begin());
}

/**
 * Assumes all CPUs belong to one socket. Used for when topology information is
 * unavailable.
 */
ComputeTopologyT allCPUsAreCores()
{
	ComputeTopologyT ret;

	ret.push_back(vector<vector<unsigned short> >());

	cpu_set_t mask;
	int tid = syscall(SYS_gettid);
	if (sched_getaffinity(tid, sizeof(cpu_set_t), &mask) == -1) 
	{
		throwerror("Error in getting affinity");
	}

	for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
	{
		if (!CPU_ISSET(cpu, &mask))
			continue;

		vector<unsigned short> v;
		v.push_back(cpu);
		ret[0].push_back(v);
	}

	return ret;
}

/**
 * Return CPU -> socket mapping.
 */
map<unsigned short, unsigned short> calcCPUtoSocketMapping(
		const ComputeTopologyT& topology)
{
	map<unsigned short, unsigned short> ret;
	
	for (unsigned int socket = 0; socket < topology.size(); ++socket)
	{
		for (unsigned int core = 0; core < topology[socket].size(); ++core)
		{
			for (unsigned int context = 0; context < topology[socket][core].size(); ++context)
			{
				unsigned short logCpuId = topology[socket][core][context];

				// Assert no double inserts; each logical CPU exists once in
				// the topology.
				//
				if (ret.find(logCpuId) != ret.end())
				{
					throwerror("A logical CPU appears more than once in the topology.");
				}

				ret[logCpuId] = socket;
			}
		}
	}
	return ret;
}

/**
 * Map sockets to NUMA nodes.
 */
vector<unsigned short> computeSocketToNumaMapping(
	   const ComputeTopologyT& ctop, const MemoryTopologyT& mtop)
{
	const unsigned short InvalidNuma = (unsigned short) -1;

	map<unsigned short, unsigned short> cpu2socket = calcCPUtoSocketMapping(ctop);

	vector<unsigned short> socket2numa 
		= vector<unsigned short>(ctop.size(), InvalidNuma);

	for (unsigned int numa = 0; numa < mtop.size(); ++numa)
	{
		for (unsigned int cpu = 0; cpu < mtop[numa].size(); ++cpu)
		{
			unsigned short logicalCpuId = mtop[numa][cpu];
			dbgassert(cpu2socket.find(logicalCpuId) != cpu2socket.end());
			unsigned short socket = cpu2socket[logicalCpuId];

			unsigned short curentry = socket2numa[socket];
			if (curentry == InvalidNuma)
			{
				// Entry is untouched, write value.
				//
				socket2numa[socket] = numa;
			}
			else if (curentry == numa)
			{
				// This mapping has been previously defined, do nothing.
				//
			}
			else
			{
				// Mapping has been previously discovered. This can only be the
				// case if one socket maps to more than one NUMA nodes.
				//
				throwerror("A socket was found to map to more than one NUMA nodes.");
			}
		}
	}

	return socket2numa;
}

Affinitizer::TopologyT combineTopologies(
		const ComputeTopologyT& ctop, const MemoryTopologyT& mtop)
{
	// Make returned array to have as many entries as NUMA nodes.
	//
	Affinitizer::TopologyT ret(mtop.size(), ComputeTopologyT());

	// Check that we found the same CPUs from both.
	//
	if (sameLogicalCPUs(ctop, mtop) == false)
	{
		throwerror("Compute and memory topologies returned different logical CPUs.");
	}

	// Make socket -> NUMA mapping.
	//
	vector<unsigned short> socket2numa = computeSocketToNumaMapping(ctop, mtop);

	for(unsigned int socket=0; socket<socket2numa.size(); ++socket)
	{
		unsigned short targetnuma = socket2numa.at(socket);
		ret.at(targetnuma).push_back(ctop.at(socket));
	}

	return ret;
}


/** 
 * Best-effort topology enumeration.
 */
Affinitizer::TopologyT enumerateTopology()
{
	// Enumerate topologies.
	//
	ComputeTopologyT ctop;
	try 
	{
		ctop = enumerateComputeTopology();
	} 
	catch (AffinitizationException e)
	{
		cerr << " ** Affinitization WARNING: " << e.what() << endl;
		cerr << " ** Affinitization WARNING: Assuming 1-socket machine, all CPUs are cores...." << endl;
		ctop = allCPUsAreCores();
	}

	MemoryTopologyT mtop = enumerateMemoryTopology();

	// Combine topolgies.
	//
	Affinitizer::TopologyT ret = combineTopologies(ctop, mtop);

	return ret;
}


void Affinitizer::init(libconfig::Setting& node)
{
	const unsigned short MAX_THREADS = 128;

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		mapping.push_back(Binding());
	}

	// Parse mapping.
	//
	if (!node.exists("affinitize"))
		return;

	topology = enumerateTopology();

	libconfig::Setting& mapnode = node["affinitize"];
	dbgassert(mapnode.isList());

	for (int idx = 0; idx < mapnode.getLength(); ++idx)
	{
		libconfig::Setting& threadspecnode = mapnode[idx];
		dbgassert(threadspecnode.getLength() >= 2);
		dbgassert(threadspecnode.exists("threadid"));
		dbgassert(threadspecnode.exists("bindto"));

		int threadid = threadspecnode["threadid"];
		Binding& binding = mapping.at(threadid);

		libconfig::Setting& bnode = threadspecnode["bindto"];
		dbgassert(bnode.getLength() >= 4);

		unsigned int value; 

		value = bnode[0];
		binding.numa = value;

		value = bnode[1];
		binding.socket = value;

		value = bnode[2];
		binding.core = value;

		value = bnode[3];
		binding.context = value;
	}
}


void Affinitizer::affinitize(unsigned short threadid)
{
	dbgassert(threadid<mapping.size());

	ostringstream oss;
	oss << threadid;
	string threadidstr = oss.str();

	// If any value is invalid, throw.
	//
	Binding& binding = mapping[threadid];
	if ( binding.numa == Binding::InvalidBinding
			|| binding.socket == Binding::InvalidBinding
			|| binding.core == Binding::InvalidBinding
			|| binding.context == Binding::InvalidBinding )
	{
		throw AffinitizationException(
				"Undefined mapping for thread " + threadidstr + "."); 
	}

	// If any part of the mapping does not exist in the topology, throw.
	//
	if (binding.numa >= topology.size())
	{
		throw AffinitizationException("Thread " + threadidstr 
				+ " specified a NUMA node that doesn't exist.");
	}

	if (binding.socket >= topology[binding.numa].size())
	{
		throw AffinitizationException("Thread " + threadidstr 
				+ " specified a socket that doesn't exist.");
	}

	if (binding.core >= topology[binding.numa][binding.socket].size())
	{
		throw AffinitizationException("Thread " + threadidstr 
				+ " specified a core that doesn't exist.");
	}

	if (binding.context >= topology[binding.numa][binding.socket][binding.context].size())
	{
		throw AffinitizationException("Thread " + threadidstr 
				+ " specified a context that doesn't exist.");
	}

	// All clear, affinitize.
	//
	::affinitize(topology[binding.numa][binding.socket][binding.core][binding.context]);
}
