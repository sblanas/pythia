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


#ifndef __MYEXCEPTIONS__
#define __MYEXCEPTIONS__

#include <exception>
#include <string>

class InvalidParameter { };

class IllegalConversionException { };

class IllegalSchemaDeclarationException { };

class UnknownComparisonException { };

class UnknownAlgorithmException { };

class UnknownPartitionerException { };

class UnknownHashException { };

class QueryExecutionError { };

class UnknownCommand : public QueryExecutionError { };

class PageFullException {
	public:
		PageFullException(int b) : value(b) { }
		int value;
};

class LoadBZ2Exception { };

class PerfCountersException { };

class AffinitizationException : public std::exception
{ 
    public:
        AffinitizationException(const std::string c)
            : desc(c)
        { }

		virtual ~AffinitizationException() throw () { }

        virtual const char* what() { return desc.c_str(); }

    private:
		std::string desc;
};

class FileNotFoundException { };

class NotYetImplemented { };

class MissingParameterException : public std::exception
{ 
	public:
		MissingParameterException()
		{ }

		MissingParameterException(const std::string c)
			: desc(c)
		{ }

		virtual ~MissingParameterException() throw () { }

        virtual const char* what() { return desc.c_str(); }

    private:
		std::string desc;
};

class SingleThreadedOnly : public QueryExecutionError { };

class CreateSegmentFailure { };

#endif
