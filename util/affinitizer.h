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

#ifndef __MYAFFINITIZER__
#define __MYAFFINITIZER__

#include <vector>
using std::vector;
#include "libconfig.h++"

/**
 * Affinitizes each caller of affinitize() to a particular logical processor,
 * as specified by the configuration parameter \a affinitize. The operator strives
 * to be topology-aware, transparently converting higher level abstractions
 * like NUMA node and socket into logical processor IDs. It is an error to
 * leave threads unbound and call affinitize().
 *
 * A binding is four numbers, each denoting the numa, socket, core and context.
 *
 * binding := [ <number>, <number>, <number>, <number> ] 
 *
 *
 * A thread-spec specifies which thread will be affinitized according to this
 * binding.
 *
 * threadspec := { threadid = <number>, bindto = <binding> }
 *
 *
 * The mapping parameter is simply an array of thread-specs.
 *
 * affinitize := [ <threadspec>, <threadspec>, ... ]
 *
 */
class Affinitizer
{
	public:
		void init(libconfig::Setting& node);

		void affinitize(unsigned short threadid);

		/** 
		 * Mapping from (numa,socket,core) -> logical cpu id.
		 */
		typedef vector<vector<vector<vector<unsigned short> > > > TopologyT;

		struct Binding
		{
			static const unsigned short InvalidBinding = (unsigned short) -1;

			Binding()
				: numa(InvalidBinding), socket(InvalidBinding),
				core(InvalidBinding), context(InvalidBinding)
			{ }

			unsigned short numa;
			unsigned short socket;
			unsigned short core;
			unsigned short context;
		};

		vector<Binding> mapping;
		TopologyT topology;
};

#endif 
