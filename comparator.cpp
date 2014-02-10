
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

#include "schema.h"	// for ColumnType
#include "util/static_assert.h"

#include "rawcompfns.h"

// Comparator class code.
//

void Comparator::init(ColumnSpec lct, unsigned int loff, 
					   ColumnSpec rct, unsigned int roff, 
					   Comparison op) 
{
	loffset = loff;
	roffset = roff;
	size = 0;

	switch(lct.type) {

		// First argument is int.
		//
		case CT_INTEGER:
			switch(rct.type) {
				case CT_INTEGER:
					switch(op) {
						case Equal:
							fn = IntIntEqual;
							break;
						case NotEqual:
							fn = IntIntNotEqual;
							break;
						case Less:
							fn = IntIntLess;
							break;
						case LessEqual:
							fn = IntIntLessEqual;
							break;
						case Greater:
							fn = IntIntGreater;
							break;
						case GreaterEqual:
							fn = IntIntGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_LONG:
					switch(op) {
						case Equal:
							fn = IntLongEqual;
							break;
						case NotEqual:
							fn = IntLongNotEqual;
							break;
						case Less:
							fn = IntLongLess;
							break;
						case LessEqual:
							fn = IntLongLessEqual;
							break;
						case Greater:
							fn = IntLongGreater;
							break;
						case GreaterEqual:
							fn = IntLongGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_DECIMAL:
					switch(op) {
						case Equal:
							fn = IntDoubleEqual;
							break;
						case NotEqual:
							fn = IntDoubleNotEqual;
							break;
						case Less:
							fn = IntDoubleLess;
							break;
						case LessEqual:
							fn = IntDoubleLessEqual;
							break;
						case Greater:
							fn = IntDoubleGreater;
							break;
						case GreaterEqual:
							fn = IntDoubleGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_CHAR:
				case CT_POINTER:
				default:
					throw UnknownComparisonException();
					break;
			}
			break;

		// First argument is long.
		//
		case CT_LONG:
			switch(rct.type) {
				case CT_INTEGER:
					switch(op) {
						case Equal:
							fn = LongIntEqual;
							break;
						case NotEqual:
							fn = LongIntNotEqual;
							break;
						case Less:
							fn = LongIntLess;
							break;
						case LessEqual:
							fn = LongIntLessEqual;
							break;
						case Greater:
							fn = LongIntGreater;
							break;
						case GreaterEqual:
							fn = LongIntGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_LONG:
					switch(op) {
						case Equal:
							fn = LongLongEqual;
							break;
						case NotEqual:
							fn = LongLongNotEqual;
							break;
						case Less:
							fn = LongLongLess;
							break;
						case LessEqual:
							fn = LongLongLessEqual;
							break;
						case Greater:
							fn = LongLongGreater;
							break;
						case GreaterEqual:
							fn = LongLongGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_DECIMAL:
					switch(op) {
						case Equal:
							fn = LongDoubleEqual;
							break;
						case NotEqual:
							fn = LongDoubleNotEqual;
							break;
						case Less:
							fn = LongDoubleLess;
							break;
						case LessEqual:
							fn = LongDoubleLessEqual;
							break;
						case Greater:
							fn = LongDoubleGreater;
							break;
						case GreaterEqual:
							fn = LongDoubleGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_CHAR:
				case CT_POINTER:
				default:
					throw UnknownComparisonException();
					break;
			}
			break;

		// First argument is double.
		//
		case CT_DECIMAL:
			switch(rct.type) {
				case CT_INTEGER:
					switch(op) {
						case Equal:
							fn = DoubleIntEqual;
							break;
						case NotEqual:
							fn = DoubleIntNotEqual;
							break;
						case Less:
							fn = DoubleIntLess;
							break;
						case LessEqual:
							fn = DoubleIntLessEqual;
							break;
						case Greater:
							fn = DoubleIntGreater;
							break;
						case GreaterEqual:
							fn = DoubleIntGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_LONG:
					switch(op) {
						case Equal:
							fn = DoubleLongEqual;
							break;
						case NotEqual:
							fn = DoubleLongNotEqual;
							break;
						case Less:
							fn = DoubleLongLess;
							break;
						case LessEqual:
							fn = DoubleLongLessEqual;
							break;
						case Greater:
							fn = DoubleLongGreater;
							break;
						case GreaterEqual:
							fn = DoubleLongGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_DECIMAL:
					switch(op) {
						case Equal:
							fn = DoubleDoubleEqual;
							break;
						case NotEqual:
							fn = DoubleDoubleNotEqual;
							break;
						case Less:
							fn = DoubleDoubleLess;
							break;
						case LessEqual:
							fn = DoubleDoubleLessEqual;
							break;
						case Greater:
							fn = DoubleDoubleGreater;
							break;
						case GreaterEqual:
							fn = DoubleDoubleGreaterEqual;
							break;
						default:
							fn = MakesNoSense;
							break;
					}
					break;
				case CT_CHAR:
				case CT_POINTER:
				default:
					throw UnknownComparisonException();
					break;
			}
			break;

		// First argument is pointer.
		//
		// Only equals comparisons with other pointers make sense.
		//
		case CT_POINTER:
			if (rct.type != CT_POINTER)
				throw UnknownComparisonException();

			switch(op) {
				case Equal:
					fn = PointerPointerEqual;
					break;
				case NotEqual:
					fn = PointerPointerNotEqual;
					break;
				case Less:
				case LessEqual:
				case Greater:
				case GreaterEqual:
				default:
					fn = MakesNoSense;
					break;
			}
			break;

		// First argument is string.
		//
		case CT_CHAR:
			if (rct.type != CT_CHAR)
				throw UnknownComparisonException();

			switch(op) {
				case Equal:
					fn = CharCharEqual;
					break;
				case NotEqual:
					fn = CharCharNotEqual;
					break;
				case Less:
					fn = CharCharLess;
					break;
				case LessEqual:
					fn = CharCharLessEqual;
					break;
				case Greater:
					fn = CharCharGreater;
					break;
				case GreaterEqual:
					fn = CharCharGreaterEqual;
					break;
				default:
					fn = MakesNoSense;
					break;
			}
			size = min(lct.size, rct.size);
			break;

		// First argument is a date.
		//
		// Byte-wise this is the same as CtLong, but we treat it separately to
		// avoid ambiguous comparisons (eg. comparison of date with double).
		//
		case CT_DATE:
			if (rct.type != CT_DATE)
				throw UnknownComparisonException();

			static_assert(sizeof(CtDate) == sizeof(CtLong));

			switch(op) {
				case Equal:
					fn = LongLongEqual;
					break;
				case NotEqual:
					fn = LongLongNotEqual;
					break;
				case Less:
					fn = LongLongLess;
					break;
				case LessEqual:
					fn = LongLongLessEqual;
					break;
				case Greater:
					fn = LongLongGreater;
					break;
				case GreaterEqual:
					fn = LongLongGreaterEqual;
					break;
				default:
					fn = MakesNoSense;
					break;
			}
			break;

		// Huh, missing type?
		//
		default:
			throw UnknownComparisonException();
			break;
	}
}

Comparator::Comparison Comparator::parseString(const string& opstr)
{
	Comparison ret;

		 if (opstr == "<" ) ret = Comparator::Less;
	else if (opstr == "<=") ret = Comparator::LessEqual;
	else if (opstr == "=" ) ret = Comparator::Equal;
	else if (opstr == "==") ret = Comparator::Equal;
	else if (opstr == "<>") ret = Comparator::NotEqual;
	else if (opstr == "!=") ret = Comparator::NotEqual;
	else if (opstr == ">=") ret = Comparator::GreaterEqual;
	else if (opstr == ">" ) ret = Comparator::Greater;
	else 
		throw UnknownComparisonException();

	return ret;
}
