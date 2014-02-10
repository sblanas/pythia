
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

#include <cstring>

#include "rawcompfns.h"
#include "exceptions.h"

bool MakesNoSense(void* lhs, void* rhs, int n)
{
	throw UnknownComparisonException();
}

// Compare Int with {Int, Long, Double}
//

bool IntIntEqual(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) == (*(int*)rhs);
}

bool IntIntLess(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) < (*(int*)rhs);
}

bool IntIntLessEqual(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) <= (*(int*)rhs);
}

bool IntIntGreater(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) > (*(int*)rhs);
}

bool IntIntGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) >= (*(int*)rhs);
}

bool IntIntNotEqual(void* lhs, void* rhs, int n)
{
	return (*(int*)lhs) != (*(int*)rhs);
}

bool IntLongEqual(void* lhs, void* rhs, int n)
{
	return ((long long)(*(int*)lhs)) == (*(long long*)rhs);
}

bool IntLongLess(void* lhs, void* rhs, int n)
{
	return (long long)(*(int*)lhs) < (*(long long*)rhs);
}

bool IntLongLessEqual(void* lhs, void* rhs, int n)
{
	return (long long)(*(int*)lhs) <= (*(long long*)rhs);
}

bool IntLongGreater(void* lhs, void* rhs, int n)
{
	return (long long)(*(int*)lhs) > (*(long long*)rhs);
}

bool IntLongGreaterEqual(void* lhs, void* rhs, int n)
{
	return (long long)(*(int*)lhs) >= (*(long long*)rhs);
}

bool IntLongNotEqual(void* lhs, void* rhs, int n)
{
	return (long long)(*(int*)lhs) != (*(long long*)rhs);
}

bool IntDoubleEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) == (*(double*)rhs);
}

bool IntDoubleLess(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) < (*(double*)rhs);
}

bool IntDoubleLessEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) <= (*(double*)rhs);
}

bool IntDoubleGreater(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) > (*(double*)rhs);
}

bool IntDoubleGreaterEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) >= (*(double*)rhs);
}

bool IntDoubleNotEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(int*)lhs) != (*(double*)rhs);
}

// Compare Long with {Int, Long, Double}
//

bool LongIntEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) == (long long)(*(int*)rhs);
}

bool LongIntLess(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) < (long long)(*(int*)rhs);
}

bool LongIntLessEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) <= (long long)(*(int*)rhs);
}

bool LongIntGreater(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) > (long long)(*(int*)rhs);
}

bool LongIntGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) >= (long long)(*(int*)rhs);
}

bool LongIntNotEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) != (long long)(*(int*)rhs);
}

bool LongLongEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) == (*(long long*)rhs);
}

bool LongLongLess(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) < (*(long long*)rhs);
}

bool LongLongLessEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) <= (*(long long*)rhs);
}

bool LongLongGreater(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) > (*(long long*)rhs);
}

bool LongLongGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) >= (*(long long*)rhs);
}

bool LongLongNotEqual(void* lhs, void* rhs, int n)
{
	return (*(long long*)lhs) != (*(long long*)rhs);
}

bool LongDoubleEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) == (*(double*)rhs);
}

bool LongDoubleLess(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) < (*(double*)rhs);
}

bool LongDoubleLessEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) <= (*(double*)rhs);
}

bool LongDoubleGreater(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) > (*(double*)rhs);
}

bool LongDoubleGreaterEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) >= (*(double*)rhs);
}

bool LongDoubleNotEqual(void* lhs, void* rhs, int n)
{
	return (double)(*(long long*)lhs) != (*(double*)rhs);
}

// Compare Double with {Int, Long, Double}
//

bool DoubleIntEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) == (double)(*(int*)rhs);
}

bool DoubleIntLess(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) < (double)(*(int*)rhs);
}

bool DoubleIntLessEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) <= (double)(*(int*)rhs);
}

bool DoubleIntGreater(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) > (double)(*(int*)rhs);
}

bool DoubleIntGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) >= (double)(*(int*)rhs);
}

bool DoubleIntNotEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) != (double)(*(int*)rhs);
}

bool DoubleLongEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) == (double)(*(long long*)rhs);
}

bool DoubleLongLess(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) < (double)(*(long long*)rhs);
}

bool DoubleLongLessEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) <= (double)(*(long long*)rhs);
}

bool DoubleLongGreater(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) > (double)(*(long long*)rhs);
}

bool DoubleLongGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) >= (double)(*(long long*)rhs);
}

bool DoubleLongNotEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) != (double)(*(long long*)rhs);
}

bool DoubleDoubleEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) == (*(double*)rhs);
}

bool DoubleDoubleLess(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) < (*(double*)rhs);
}

bool DoubleDoubleLessEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) <= (*(double*)rhs);
}

bool DoubleDoubleGreater(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) > (*(double*)rhs);
}

bool DoubleDoubleGreaterEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) >= (*(double*)rhs);
}

bool DoubleDoubleNotEqual(void* lhs, void* rhs, int n)
{
	return (*(double*)lhs) != (*(double*)rhs);
}

// Char to char comparison
// FIXME Buggy behavior if comapring "AB" with "ABCD" and n==2. 
// Strings will be reported as equal, while they are not.
//

bool CharCharEqual(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) == 0;
}

bool CharCharLess(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) < 0;
}

bool CharCharLessEqual(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) <= 0;
}

bool CharCharGreater(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) > 0;
}

bool CharCharGreaterEqual(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) >= 0;
}

bool CharCharNotEqual(void* lhs, void* rhs, int n)
{
	return strncmp((char*)lhs, (char*)rhs, n) != 0;
}

// Pointer to pointer comparison
//

bool PointerPointerEqual(void* lhs, void* rhs, int n)
{
	return (*(void**)lhs) == (*(void**)rhs);
}

bool PointerPointerNotEqual(void* lhs, void* rhs, int n)
{
	return (*(void**)lhs) != (*(void**)rhs);
}
