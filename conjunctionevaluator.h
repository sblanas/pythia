
/*
 * Copyright 2010, Pythia authors (see AUTHORS file).
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

#include "schema.h"
#include "comparator.h"
#include <string>
#include <vector>

/**
 * Class evaluates a special form of conjunction, where every clause is of the
 * form X op Y.
 */
class ConjunctionEvaluator 
{
	public:
		void init(Schema& s1, Schema& s2, 
				vector<unsigned short>& attr1, vector<unsigned short>& attr2,
				vector<Comparator::Comparison>& op);
		inline bool eval(void* tup1, void* tup2);

	private:
		vector<Comparator> comps;
};


bool ConjunctionEvaluator::eval(void* tup1, void* tup2) {
	for (unsigned int i=0; i<comps.size(); ++i)
	{
		if (comps[i].eval(tup1, tup2) == false) {
			return false;
		}
	}
	return true;
}



/**
 * Class evaluates a special form of conjunction, where every clause is of the
 * form X==Y.
 */
class ConjunctionEqualsEvaluator : public ConjunctionEvaluator 
{
	public:
		void init(Schema& s1, Schema& s2, 
				vector<unsigned short>& attr1, vector<unsigned short>& attr2);
};
