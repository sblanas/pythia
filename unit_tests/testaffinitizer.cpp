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

#include "common.h"
#include "../util/affinitizer.h"
#include "../exceptions.h"
#include <cassert>

using namespace std;

/** 
 * Mapping from (socket,core) -> logical cpu id.
 */
typedef vector<vector<vector<unsigned short> > > ComputeTopologyT;

/** 
 * Mapping from (numa) -> logical cpu id.
 */
typedef vector<vector<unsigned short> > MemoryTopologyT;

Affinitizer::TopologyT combineTopologies(
		const ComputeTopologyT& ctop, const MemoryTopologyT& mtop);

MemoryTopologyT enumerateMemoryTopology();
ComputeTopologyT enumerateComputeTopology();
ComputeTopologyT allCPUsAreCores();
Affinitizer::TopologyT enumerateTopology();

ComputeTopologyT fakeEnumerateComputeTopology() 
{
	ComputeTopologyT ret;

	for (int i=0; i<2; ++i)
	{
		ret.push_back(vector<vector<unsigned short> >());
		for (int j=0; j<6; ++j)
		{
			ret[i].push_back(vector<unsigned short>());
			for (int k=0; k<2; ++k)
			{
				ret[i][j].push_back(0);
			}
		}
	}

	ret[0][0][0]=1;
	ret[0][0][1]=13;
	ret[0][1][0]=3;
	ret[0][1][1]=15;
	ret[0][2][0]=5;
	ret[0][2][1]=17;
	ret[0][3][0]=7;
	ret[0][3][1]=19;
	ret[0][4][0]=9;
	ret[0][4][1]=21;
	ret[0][5][0]=11;
	ret[0][5][1]=23;
	ret[1][0][0]=0;
	ret[1][0][1]=12;
	ret[1][1][0]=2;
	ret[1][1][1]=14;
	ret[1][2][0]=4;
	ret[1][2][1]=16;
	ret[1][3][0]=6;
	ret[1][3][1]=18;
	ret[1][4][0]=8;
	ret[1][4][1]=20;
	ret[1][5][0]=10;
	ret[1][5][1]=22;

	return ret;
}

MemoryTopologyT fakeEnumerateMemoryTopology()
{
	MemoryTopologyT ret;
	ret.push_back(vector<unsigned short>());
	ret.push_back(vector<unsigned short>());

	ret[0].push_back(0);
	ret[0].push_back(2);
	ret[0].push_back(4);
	ret[0].push_back(6);
	ret[0].push_back(8);
	ret[0].push_back(10);
	ret[0].push_back(12);
	ret[0].push_back(14);
	ret[0].push_back(16);
	ret[0].push_back(18);
	ret[0].push_back(20);
	ret[0].push_back(22);

	ret[1].push_back(1);
	ret[1].push_back(3);
	ret[1].push_back(5);
	ret[1].push_back(7);
	ret[1].push_back(9);
	ret[1].push_back(11);
	ret[1].push_back(13);
	ret[1].push_back(15);
	ret[1].push_back(17);
	ret[1].push_back(19);
	ret[1].push_back(21);
	ret[1].push_back(23);

	return ret;
}

void prettyPrintTopology(const Affinitizer::TopologyT& topology)
{
	for (unsigned int numa = 0; numa < topology.size(); ++numa)
	{
		for (unsigned int socket = 0; socket < topology[numa].size(); ++socket)
		{
			for (unsigned int core = 0; core < topology[numa][socket].size(); ++core)
			{
				for (unsigned int context = 0; context < topology[numa][socket][core].size(); ++context)
				{
					cout << numa << '\t' << socket << '\t' << core << '\t' 
						<< context << '\t' << ": " 
						<< topology[numa][socket][core][context] << endl;
				}
			}
		}
	}
}

void prettyPrintComputeTopology(const ComputeTopologyT& topology)
{
	for (unsigned int socket = 0; socket < topology.size(); ++socket)
	{
		for (unsigned int core = 0; core < topology[socket].size(); ++core)
		{
			for (unsigned int context = 0; context < topology[socket][core].size(); ++context)
			{
				cout << socket << '\t' << core << '\t' << context << '\t' 
					<< ": " << topology[socket][core][context] << endl;
			}
		}
	}
}

void prettyPrintMemoryTopology(const MemoryTopologyT& topology)
{
	for (unsigned int numa = 0; numa < topology.size(); ++numa)
	{
		for (unsigned int cpu = 0; cpu < topology[numa].size(); ++cpu)
		{
			cout << numa << '\t' << ": " << topology[numa][cpu] << endl;
		}
	}
}


int main()
{
	ComputeTopologyT ctopology = fakeEnumerateComputeTopology();
#ifdef VERBOSE
	prettyPrintComputeTopology(ctopology);
#endif 

	MemoryTopologyT mtopology = fakeEnumerateMemoryTopology();
#ifdef VERBOSE
	prettyPrintMemoryTopology(mtopology);
#endif

	Affinitizer::TopologyT topology = combineTopologies(ctopology, mtopology);
#ifdef VERBOSE
	prettyPrintTopology(topology);
#endif

	assert(topology[1][0][0][0]==1);
	assert(topology[1][0][0][1]==13);
	assert(topology[1][0][1][0]==3);
	assert(topology[1][0][1][1]==15);
	assert(topology[1][0][2][0]==5);
	assert(topology[1][0][2][1]==17);
	assert(topology[1][0][3][0]==7);
	assert(topology[1][0][3][1]==19);
	assert(topology[1][0][4][0]==9);
	assert(topology[1][0][4][1]==21);
	assert(topology[1][0][5][0]==11);
	assert(topology[1][0][5][1]==23);
	assert(topology[0][0][0][0]==0);
	assert(topology[0][0][0][1]==12);
	assert(topology[0][0][1][0]==2);
	assert(topology[0][0][1][1]==14);
	assert(topology[0][0][2][0]==4);
	assert(topology[0][0][2][1]==16);
	assert(topology[0][0][3][0]==6);
	assert(topology[0][0][3][1]==18);
	assert(topology[0][0][4][0]==8);
	assert(topology[0][0][4][1]==20);
	assert(topology[0][0][5][0]==10);
	assert(topology[0][0][5][1]==22);

	// Real topology now. Make sure nothing asserts.
	//
	try 
	{
		ctopology = enumerateComputeTopology();
	} 
	catch (AffinitizationException e)
	{
		cerr << " ** Affinitization WARNING: " << e.what() << endl;
		cerr << " ** Affinitization WARNING: Assuming 1-socket machine, all CPUs are cores...." << endl;
		ctopology = allCPUsAreCores();
	}
#ifdef VERBOSE
	prettyPrintComputeTopology(ctopology);
#endif

	mtopology = enumerateMemoryTopology();
#ifdef VERBOSE
	prettyPrintMemoryTopology(mtopology);
#endif

	topology = combineTopologies(ctopology, mtopology);
#ifdef VERBOSE
	prettyPrintTopology(topology);
#endif

	return 0;
}
