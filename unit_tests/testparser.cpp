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
#include "../operators/loaders/parser.h"
using namespace std;

#include "common.h"

const unsigned int MAX_COL=12;

void validate(Parser& p, char* input, vector<string>& expected)
{
	if (expected.size() > MAX_COL)
		fail("Caller expects more output fields than MAX_COL.");

	ostringstream oss;
	oss << "Parsing \"" << input << "\" ";
	const char* result[MAX_COL];
	unsigned short fields = p.parseLine(input, result, MAX_COL);
	if (expected.size() != fields)
	{
		oss << "returns " << fields << " fields, " << expected.size() 
			<< " expected." << endl;
	   	fail(oss.str().c_str());
	}
	for (unsigned int i=0; i<expected.size(); ++i)
	{
		string res(result[i]);
		if (expected[i] != res)
		{
			oss << "field " << i+1 << " returns \"" << res << "\", expected \""
				<< expected[i] << "\".";
			fail(oss.str().c_str());
		}
	}
}

int main() {
	Parser p("|");
	{
		char a[120] = "Hello|World!|";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		validate(p, a, expected);
	}
	/* next exp */
	{
		char a[120] = "Hello|World!";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		validate(p, a, expected);
	}
	/* next exp */
	{
		char a[120] = "Hello|||World!";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		validate(p, a, expected);
	}
	/* next exp */
	{
		char a[120] = "|Hello|World!";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		validate(p, a, expected);
	}
	/* next exp */
	{
		char a[120] = "|Hello|||World!";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		validate(p, a, expected);
	}
	/* next exp */
	{
		char a[120] = "|Hello|World!|123testing|asdf||";
		vector<string> expected;
		expected.push_back("Hello");
		expected.push_back("World!");
		expected.push_back("123testing");
		expected.push_back("asdf");
		validate(p, a, expected);
	}
	return 0;
}
