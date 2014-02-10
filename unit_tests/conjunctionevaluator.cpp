/*
 * Copyright 2014, Pythia authors (see AUTHORS file).
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

#include "../schema.h"
#include "../comparator.h"
#include "../conjunctionevaluator.h"
#include "common.h"

int main() {
	char tup1[16];
	char tup2[16];
	char tup3[16];

	int idummy = 0;
	int ival1 = 4;
	long long lval1 = 4;
	long long lval2 = 5;
	double dval = 4.5;

	Schema s1;
	s1.add(CT_INTEGER);
	s1.add(CT_INTEGER);
	s1.writeData(tup1, 0, &idummy);
	s1.writeData(tup1, 1, &ival1);

	Schema s2;
	s2.add(CT_DECIMAL);
	s2.add(CT_INTEGER);
	s2.writeData(tup2, 0, &dval);
	s2.writeData(tup2, 1, &ival1);

	Schema s3;
	s3.add(CT_LONG);
	s3.add(CT_LONG);
	s3.writeData(tup3, 0, &lval1);
	s3.writeData(tup3, 1, &lval2);

	bool result;
	bool realresult;

	// Test ConjunctionEvaluator.
	//
	{
		ConjunctionEvaluator evaluator;
		vector <unsigned short> v2;
		vector <unsigned short> v3;
		vector <Comparator::Comparison> op;
	   
		// Evaluate "tup2[1] >= tup3[0]"
		//
		v2.push_back(1);
		op.push_back(Comparator::GreaterEqual);
		v3.push_back(0);

		evaluator.init(s2, s3, v2, v3, op);
		result = evaluator.eval(tup2, tup3);
		realresult = (ival1 >= lval1);
		dbgassert(realresult == true);
		if (result != realresult) 
		{
			fail("Evaluation wrong.");
		}

		// Evaluate "tup2[1] >= tup3[0] && tup2[0] < tup3[1]"
		//
		v2.push_back(0);
		op.push_back(Comparator::Less);
		v3.push_back(1);
		evaluator.init(s2, s3, v2, v3, op);
		result = evaluator.eval(tup2, tup3);
		realresult = ((ival1 >= lval1) && (dval < lval2));
		dbgassert(realresult == true);
		if (result != realresult)
		{
			fail("Evaluation wrong.");
		}
	}	


	// Test ConjunctionEquals.
	//
	{
		ConjunctionEqualsEvaluator equalityevaluator;
		vector<unsigned short> v1;
		vector<unsigned short> v2;

		// Evaluate "tup1[1] == tup3[0]"
		//
		v1.push_back(1);
		v2.push_back(0);

		equalityevaluator.init(s1, s3, v1, v2);
		result = equalityevaluator.eval(tup1, tup3);
		realresult = (ival1 == lval1);
		dbgassert(realresult == true);
		if (result != realresult)
		{
			fail("Evaluation wrong.");
		}

		// Evaluate "tup1[1] == tup3[0] && tup3[0] == tup1[0]"
		//
		v1.push_back(0);
		v2.push_back(0);

		equalityevaluator.init(s1, s3, v1, v2);
		result = equalityevaluator.eval(tup1, tup3);
		realresult = ((ival1 == lval1) && (lval1 == idummy));
		dbgassert(realresult == false);
		if (result != realresult)
		{
			fail("Evaluation wrong.");
		}
	}


	return 0;
}
