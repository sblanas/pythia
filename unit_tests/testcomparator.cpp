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

int main() {
	char tup1a[16];
	char tup1b[16];
	char tup2 [16];

	int idummy = 0;
	int ival1 = 4;
	int ival2 = 5;
	double dval = 4.5;

	Schema s1;
	s1.add(CT_INTEGER);
	s1.add(CT_INTEGER);

	s1.writeData(tup1a, 0, &idummy);
	s1.writeData(tup1a, 1, &ival1);
	s1.writeData(tup1b, 0, &idummy);
	s1.writeData(tup1b, 1, &ival2);

	Schema s2;
	s2.add(CT_DECIMAL);

	s2.writeData(tup2, 0, &dval);

	Comparator comp1 = Schema::createComparator(s1, 1, s2, 0, Comparator::Less);
	Comparator comp2 = Schema::createComparator(s1, 1, s2, 0, Comparator::Greater);

	assert(comp1.eval(tup1a, tup2));
	assert(comp2.eval(tup1b, tup2));
}
