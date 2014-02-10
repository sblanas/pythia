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

/*
 * Inlines for schema.h.
 */

#ifdef DEBUG2
#include "util/static_assert.h"
#endif

unsigned int Schema::getTupleSize() 
{
	return totalsize;
}

void Schema::writeData(void* dest, unsigned int pos, const void* const data) 
{
	dbg2assert(pos<columns());
	dbg2assert(sizeof(void*)==sizeof(long));
	void* d = reinterpret_cast<char*>(dest)+voffset[pos];

	// copy
	switch (vct[pos]) {
		case CT_POINTER: {
			static_assert(sizeof(void*) == sizeof(long*));
			const long* val = reinterpret_cast<const long*>(data);
			*reinterpret_cast<long*>(d) = *val;
			break;
		}
		case CT_INTEGER: {
			const int* val = reinterpret_cast<const int*>(data);
			*reinterpret_cast<int*>(d) = *val;
			break;
		}
		case CT_LONG: {
			const long long* val = reinterpret_cast<const long long*>(data);
			*reinterpret_cast<long long*>(d) = *val;
			break;
		}
		case CT_DECIMAL: {
			const double* val2 = reinterpret_cast<const double*>(data);
			*reinterpret_cast<double*>(d) = *val2;
			break;
		}
		case CT_CHAR: {
			const char* p = reinterpret_cast<const char*>(data);
			char* t = reinterpret_cast<char*>(d);

			const unsigned int length = getColumnWidth(pos);
			strncpy(t, p, length);

			break;
		}
		case CT_DATE: {
			const CtDate* val = reinterpret_cast<const CtDate*>(data);
			*reinterpret_cast<CtDate*>(d) = *val;
			break;
		}

	}

}

void Schema::copyTuple(void* dest, const void* const src) 
{
	memcpy(dest, src, totalsize);
}

unsigned int Schema::columns()
{
	dbg2assert(vct.size()==voffset.size());
	return vct.size();
}

ColumnType Schema::getColumnType(unsigned int pos) 
{
	dbg2assert(pos<columns());
	return vct[pos];
}

ColumnSpec Schema::get(unsigned int pos) 
{
	dbg2assert(pos<columns());
	unsigned int val1 = voffset[pos];
	unsigned int val2 = (pos != columns()-1) ? voffset[pos+1] : totalsize;
	short idx = vmetadataidx[pos];

	// Format string must exist if type is date.
	//
	dbgassertimplies(vct[pos] == CT_DATE, idx != -1);

	const string& formatstr = 
		(idx == -1) 
		? UninitializedFormatString 
		: vformatstr[idx];
	return ColumnSpec(vct[pos], val2-val1, formatstr);
}

const CtChar* Schema::asString(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	if (vct[pos]!=CT_CHAR)
		throw IllegalConversionException();
#endif
	char* d = reinterpret_cast<char*> (data);
	return reinterpret_cast<const CtChar*> (d+voffset[pos]);
}

const CtLong Schema::asLong(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	static_assert(sizeof(CtDate) == sizeof(CtLong));
	if (vct[pos]!=CT_LONG && vct[pos]!=CT_DATE)
		throw IllegalConversionException();
#endif
	char* d = reinterpret_cast<char*> (data);
	return *reinterpret_cast<const CtLong*> (d+voffset[pos]);
}

const CtInt Schema::asInt(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	if (vct[pos]!=CT_INTEGER)
		throw IllegalConversionException();
#endif
	char* d = reinterpret_cast<char*> (data);
	return *reinterpret_cast<int*> (d+voffset[pos]);
}

const CtDate Schema::asDate(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	if (vct[pos]!=CT_DATE)
		throw IllegalConversionException();
#endif
	char* d = reinterpret_cast<char*> (data);
	return *reinterpret_cast<CtDate*> (d+voffset[pos]);
}

const CtDecimal Schema::asDecimal(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	if (vct[pos]!=CT_DECIMAL)
		throw IllegalConversionException();
#endif
	char* d = reinterpret_cast<char*> (data);
	return *reinterpret_cast<double*> (d+voffset[pos]);
}

const CtPointer Schema::asPointer(void* data, unsigned int pos) 
{
#ifdef DEBUG2
	assert(pos<columns());
	if (vct[pos]!=CT_POINTER)
		throw IllegalConversionException();
	assert(sizeof(void*)==sizeof(long));
#endif
	char* d = reinterpret_cast<char*> (data);
	return reinterpret_cast<void*>(*reinterpret_cast<long*> (d+voffset[pos]));
}

void* Schema::calcOffset(void* data, unsigned int pos) 
{
	dbg2assert(pos<columns());
	return reinterpret_cast<char*>(data)+voffset[pos];
}

unsigned int Schema::getColumnWidth(unsigned int pos)
{
	dbg2assert(pos<columns());
	if (pos == (columns() - 1))
		return getTupleSize() - voffset[pos];
	dbg2assert(pos+1<columns());
	return voffset[pos+1] - voffset[pos];
}
