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

#include <iostream>
#include <vector>
#include <string>

#include "../schema.h"

#include "common.h"

using namespace std;

int main() {
	Schema s;
	s.add(CT_INTEGER);
	s.add(CT_CHAR, 25);
	s.add(CT_INTEGER);

	char a[42] = {
		0x00, 0x01, 0x00, 0x00, 
		'T', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', 
		't', 'e', 's', 't', ' ', 's', 't', 'r', 'i', 'n', 
		'g', '.', 0x00, 0x00, 0x00, 
		0xFF, 0xFF, 0xFF, 0xFF
	};

	{
		// Testing asX methods
		//

		if (string(s.asString(a, 1)) != "This is a test string.")
			fail("asString does not return what was expected.");
		if (s.asInt(a, 2) != -1)
			fail("asInt does not return what was expected.");
		if (s.asInt(a, 0) != 256) // 256 on LittleE machines, 65536 on BigE
			fail("asInt does not return what was expected, or machine is big-endian.");
		if (s.getTupleSize() != (sizeof(CtInt)+25+sizeof(CtInt)))
			fail("getTupleSize does not return what was expected.");
	}


	{
		// Testing outputTuple method
		//

		vector<string> str = s.outputTuple(a);
		if (str.size() != 3)
			fail("outputTuple returns different number of strings than expected.");
		if (str[0] != "256") // 256 on LittleE machines, 65536 on BigE
			fail("outputTuple[0] not what expected, or machine is big-endian.");
		if (str[1] != "This is a test string.")
			fail("outputTuple[1] not what expected.");
		if (str[2] != "-1")
			fail("outputTuple[2] not what expected.");
	}

	/* Next test case. */

	s.add(CT_DECIMAL);

	{
		// Testing writeData
		//

		int val1 = 25;
		const char* val2 = "Hello, world!";
		int val3 = 256;
		double val4 = 3.14159;
		s.writeData(a, 0, &val1);
		s.writeData(a, 1, val2);
		s.writeData(a, 2, &val3);
		s.writeData(a, 3, &val4);

		if (s.asInt(a, 0) != val1)
			fail("Result in column 1 after writeData is not expected.");
		if (string(s.asString(a,1)) != string(val2))
			fail("Result in column 2 after writeData is not expected.");
		if (s.asInt(a, 2) != val3)
			fail("Result in column 3 after writeData is not expected.");
		if (s.asDecimal(a,3) != val4)
			fail("Result in column 4 after writeData is not expected.");
	}

	/* Next test case. */

	{
		// Testing parseTuple
		//
		vector<string> asd;
		asd.push_back("122");
		asd.push_back("Hello good lady!");
		asd.push_back("42");
		asd.push_back("3.14159");
		s.parseTuple(a, asd);
		
		int exp1 = 122;
		const char* exp2 = "Hello good lady!";
		int exp3 = 42;
		double exp4 = 3.14159;

		if (s.asInt(a,0) != exp1)
			fail("Result in column 1 after parseTuple is not expected.");
		if (string(s.asString(a,1)) != string(exp2))
			fail("Result in column 2 after parseTuple is not expected.");
		if (s.asInt(a,2) != exp3)
			fail("Result in column 3 after parseTuple is not expected.");
		if (s.asDecimal(a,3) != exp4)
			fail("Result in column 4 after parseTuple is not expected.");
	}
	/* Next test case. */
	{
		char b[8] = {
			0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
		};
		Schema test;
		test.add(CT_POINTER);

		if (test.asPointer(b,0) != (void*)0x100)
			fail("asPointer does not return what is expected, or machine is big-endian.");
	
		if (test.calcOffset(b,0) != b)
			fail("Field zero is not the first in the tuple.");
	}
	return 0;
}
