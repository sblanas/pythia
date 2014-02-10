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

#include "parser.h"

#include "../../util/custom_asserts.h"

/**
 * Returns true if character \a c exists in \a str.
 */
inline bool notExistsIn(const char c, const string& str)
{
	return str.find(c) == string::npos;
}

Parser::Parser (const string& separator)
	: _sep(separator) 
{ 
	dbg2assert(notExistsIn('\0', _sep));
}

unsigned short Parser::parseLine(char* line, const char** result, const short maxfields) 
{
	char* s=line; /**< Points to beginning of token. */
	char* p=line; /**< Points to end of token. */
	unsigned short ret = 0;

	while (*s) 
	{
		// While not separator or end-of-line, advance end-of-token pointer.
		while (notExistsIn(*p, _sep) && *p)
		{	
			p++;
		}

		// If an empty field, skip over it.
		//
		if (s!=p)
		{
			// If there's space in result array, store start of token in result.
			//
			if (ret < maxfields)
			{
				result[ret++]=s;
			}
			// Otherwise return, there's no more space in the output array.
			//
			else
			{
				break;
			}
		}

		// If stopped because of separator, replace it with \0, so that output
		// array contains null-terminated strings.
		//
		if (*p)
		{	
			(*p)=0;
			s=++p;
		}
		// Otherwise return, we stopped because of end of string.
		// (Normal loop exit point.)
		//
		else
		{
			break;
		}
	}

	dbg2assert(ret <= maxfields);
	return ret;
}
