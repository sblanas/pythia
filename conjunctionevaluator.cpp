
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

#include "conjunctionevaluator.h"
using std::string;

/**
 * Specifies the expression the object is evaluating. 
 *
 * The expression evaluated when \a eval is called is:
 * expr =  (s1[attr1[0]] op[0] s2[attr2[0]])
 *		&& (s1[attr1[1]] op[1] s2[attr2[1]])
 *		&& (s1[attr1[2]] op[2] s2[attr2[2]])
 *		&& ... 
 * where s1[0] is the first column of schema s1.
 *
 * If the input vectors are empty, expression always evaluates to true. 
 * This is useful for comparison expressions with empty predicates (eg.
 * aggregation without a GROUP BY clause).
 */
void ConjunctionEvaluator::init(Schema& s1, Schema& s2,
		vector<unsigned short>& attr1, vector<unsigned short>& attr2,
		vector<Comparator::Comparison>& op)
{
	comps.clear();
	//dbgassert(op.size() == attr1.size());
	//dbgassert(attr1.size() == attr2.size());

    //WILLIS CHANGED:
    //WE GO BY OP SIZE SO THAT THIS NEED NOT EVAL ON THE ENTIRE TUPLE BY MAYBE A PORTION OF IT
	for (unsigned int i=0; i<op.size(); ++i)
	{
		comps.push_back(Schema::createComparator(
					s1, attr1[i], s2, attr2[i], op[i]));
	}
}


/**
 * Specifies the expression the object is evaluating. 
 *
 * The expression evaluated when \a eval is called is:
 * expr =  (s1[attr1[0]] == s2[attr2[0]])
 *		&& (s1[attr1[1]] == s2[attr2[1]]) 
 *		&& (s1[attr1[2]] == s2[attr2[2]]) 
 *		&& ... 
 * where s1[0] is the first column of schema s1.
 *
 * If the input vectors are empty, expression always evaluates to true. 
 * This is useful for comparison expressions with empty predicates (eg.
 * aggregation without a GROUP BY clause).
 */
void ConjunctionEqualsEvaluator::init(Schema& s1, Schema& s2, 
				vector<unsigned short>& attr1, vector<unsigned short>& attr2)
{
	vector<Comparator::Comparison> op(attr1.size(), Comparator::Equal);
	ConjunctionEvaluator::init(s1, s2, attr1, attr2, op);
}
