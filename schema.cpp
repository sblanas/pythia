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

#include <sstream>
#include <cassert>
#include <cstdlib>
#include "schema.h"
#include <iomanip>
#include <algorithm>

using namespace std;

const string Schema::UninitializedFormatString = "Uninitialized format string.";

/** 
 * Adds a column of the specified description. It is symmetrical to @a
 * Schema::get and quite slow.  
 * @param desc Column specification.
 */
void Schema::add(ColumnSpec desc) {
	if (desc.type != CT_DATE)
	{
		add(desc.type, desc.size);
	} 
	else
	{
		add(desc.type, desc.formatstr);
	}
}

/** 
 * Adds a column of type CT_DATE. 
 * @param ct Column type, must be CT_DATE.
 * @param formatstr Format string, a new string is generated to hold value.
 */
void Schema::add(ColumnType ct, const string& formatstr) 
{
	dbgassert(ct == CT_DATE);
	vct.push_back(ct);
	voffset.push_back(totalsize);
	vmetadataidx.push_back(vformatstr.size());
	vformatstr.push_back(formatstr.substr(0, formatstr.find(')')));
	totalsize += sizeof(CtDate);
}

/** 
 * Adds a column of any type except CT_DATE. 
 * @param ct Column type, must not be CT_DATE.
 * @param size Size of column in bytes. It must be provided for CT_CHAR,
 * optional for other types.
 */
void Schema::add(ColumnType ct, unsigned int size /* = 0 */) {
	dbgassert(ct != CT_DATE);
	vct.push_back(ct);
	voffset.push_back(totalsize);
	vmetadataidx.push_back(-1);

	int s=-1;
	switch (ct) {
		case CT_INTEGER:
			s = sizeof(int);
			break;
		case CT_LONG:
			s = sizeof(long long);
			break;
		case CT_DECIMAL:
			s = sizeof(double);
			break;
		case CT_CHAR:
			// Zero-sized character array doesn't make sense. 
			// Assert is here to catch a call to add CT_CHAR and not passing
			// the field's length.
			dbgassert(size != 0);
			// One-sized character array doesn't make sense either, as at least
			// one byte is needed for zero-padding.
			dbgassert(size != 1);
			s = size;
			break;
		case CT_POINTER:
			s = sizeof(void*);
			break;
		default:
			throw IllegalSchemaDeclarationException();
	}
	dbgassert(s!=-1);
	totalsize+=s;
}

void Schema::parseTuple(void* dest, const vector<string>& input) {
	dbg2assert(input.size()>=columns());
	const char** data = new const char*[columns()];
	for (unsigned int i=0; i<columns(); ++i) {
		data[i] = input[i].c_str();
	}
	parseTuple(dest, data);
	delete[] data;
}

void Schema::parseTuple(void* dest, const char** input) {
	for (unsigned int i=0; i<columns(); ++i) {
		switch (vct[i]) {
			case CT_INTEGER: {
				int val;
				val = atoi(input[i]);
				writeData(dest, i, &val);
				break;
			}
			case CT_LONG: 
				long long val3;
				val3 = atoll(input[i]);
				writeData(dest, i, &val3);
				break;
			case CT_DECIMAL:
				double val2;
				val2 = atof(input[i]);
				writeData(dest, i, &val2);
				break;
			case CT_CHAR:
				writeData(dest, i, input[i]);
				break;
			case CT_DATE: {
				CtDate val;
				struct tm intm;
				int idx = vmetadataidx[i];
				dbg2assert(idx != -1);
				memset(&intm, 0, sizeof(intm));

				strptime(input[i], vformatstr[idx].c_str(), &intm);
				val.setFromTM(&intm);
				writeData(dest, i, &val);
				break;
			}
			case CT_POINTER:
				throw IllegalConversionException();
				break;
		}
	}
}

vector<string> Schema::outputTuple(void* data) {
	vector<string> ret;
	for (unsigned int i=0; i<columns(); ++i) {
		ostringstream oss;
		switch (vct[i]) {
			case CT_INTEGER: 
				oss << asInt(data, i);
				break;
			case CT_LONG: 
				oss << asLong(data, i);
				break;
			case CT_DECIMAL:
				// every decimal in TPC-H has 2 digits of precision
				oss << setiosflags(ios::fixed) << setprecision(2) << asDecimal(data, i);
				break;
			case CT_CHAR:
				oss << string(asString(data, i), 0, getColumnWidth(i));
				break;
			case CT_DATE: {
				const int outbufmax = 128;
				char outbuf[outbufmax];
				struct tm outtm;
				int idx = vmetadataidx[i];
				dbg2assert(idx != -1);
				CtDate val = asDate(data, i);

				val.produceTM(&outtm);
				strftime(outbuf, outbufmax, vformatstr[idx].c_str(), &outtm);
				oss << outbuf;
				break;
			}
			case CT_POINTER:
				throw IllegalConversionException();
				break;
		}
		ret.push_back(oss.str());
	}
	return ret;
}

string Schema::prettyprint(void* tuple, char sep)
{
	string ret;
	const vector<string>& tokens = outputTuple(tuple);
	for (unsigned int i=0; i<tokens.size()-1; ++i)
		ret += tokens[i] + sep;
	if (tokens.size() >= 1)
		ret += tokens[tokens.size()-1];
	return ret;
}

Schema Schema::create(const libconfig::Setting& line) {
	Schema ret;
	for (int i=0; i<line.getLength(); ++i) {
		string val = line[i];

		// Only convert to lowercase up to character '(', or end of string.
		//
		string::size_type ndx = val.find('(');
		string::iterator itend;
		itend = (ndx == string::npos) ? val.end() : (val.begin() += ndx);
		transform(val.begin(), itend, val.begin(), ::tolower);

		if (val.find("int")==0) {
			ret.add(CT_INTEGER);
		} else if (val.find("long")==0) {
			ret.add(CT_LONG);
		} else if (val.find("char")==0) {
			// char, check for integer
			string::size_type c = val.find("(");
			if (c==string::npos)
				throw IllegalSchemaDeclarationException();
			istringstream iss(val.substr(++c));
			int len;
			iss >> len;
			ret.add(CT_CHAR, len+1);	// compensating for \0
		} else if (val.find("dec")==0) {
			ret.add(CT_DECIMAL);
		} else if (val.find("date")==0) {
			// date, check for format string
			string::size_type c = val.find("(");
			if (c==string::npos)
				throw IllegalSchemaDeclarationException();
			ret.add(CT_DATE, val.substr(++c));
		} else {
			throw IllegalSchemaDeclarationException();
		}
	}
	return ret;
}

Comparator Schema::createComparator(Schema& lhs, unsigned int lpos, Schema& rhs, unsigned int rpos, Comparator::Comparison op)
{
	Comparator c;
	c.init(lhs.get(lpos), lhs.voffset[lpos], 
			rhs.get(rpos), rhs.voffset[rpos],
			op);
	return c;
}

Comparator Schema::createComparator(Schema& lhs, unsigned int lpos, ColumnSpec& rhs, Comparator::Comparison op)
{
	Comparator c;
	c.init(lhs.get(lpos), lhs.voffset[lpos], 
			rhs, 0,
			op);
	return c;
}

Comparator Schema::createComparator(ColumnSpec& lhs, Schema& rhs, unsigned int rpos, Comparator::Comparison op)
{
	Comparator c;
	c.init(lhs, 0, 
			rhs.get(rpos), rhs.voffset[rpos],
			op);
	return c;
}
