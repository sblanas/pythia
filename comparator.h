
/*
 * Copyright 2009, Pythia authors (see AUTHORS file).
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

#ifndef __MY_COMPARATOR__
#define __MY_COMPARATOR__

#include "schema.h"

/**
 * Helper object to simplify comparisons of known data types.
 */
class Comparator {
	public:

		enum Comparison {
			Equal,
			Less,
			LessEqual,
			Greater,
			GreaterEqual,
			NotEqual
		};

		/**
		 * Parses the following: "<", "<=", "=", "==", "<>", "!=", ">=", ">".
		 * @return The appropriate Comparison value for the input string.
		 * @throws UnknownComparisonException Input string not recognized.
		 */
		static Comparison parseString(const string& opstr);

		Comparator() : loffset(0), roffset(0), size(0), fn(NULL) { }
		
		/**
		 * Initializes comparator object.
		 * @param lct ColumnSpec in left tuple.
		 * @param loff Offset in left tuple.
		 * @param rct ColumnSpec in right tuple.
		 * @param roff Offset in right tuple.
		 * @param op Comparison operation to perform.
		 */
		void init(ColumnSpec lct, unsigned int loff, 
				ColumnSpec rct, unsigned int roff, 
				Comparison op);

		inline bool eval(void* ltup, void* rtup) {
			char* lreal = (char*)ltup + loffset;
			char* rreal = (char*)rtup + roffset;
			return fn(lreal, rreal, size);
		}

	private:
		int loffset, roffset;
		int size;	// passed to strncmp for CT_CHAR comparisons
		bool (*fn)(void*, void*, int);
};

#endif
