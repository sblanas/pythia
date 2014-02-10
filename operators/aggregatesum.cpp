
/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
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

#include "operators.h"

Schema& AggregateSum::foldinit(libconfig::Config& root, libconfig::Setting& cfg)
{
	sumfieldno = cfg["sumfield"];
	inschema = nextOp->getOutSchema();

	// Assert we sum numeric types.
	//
	assert(inschema.getColumnType(sumfieldno) == CT_DECIMAL
			|| inschema.getColumnType(sumfieldno) == CT_INTEGER
			|| inschema.getColumnType(sumfieldno) == CT_LONG);

	aggregateschema = Schema();
	aggregateschema.add(inschema.getColumnType(sumfieldno));
	return aggregateschema;
}

void AggregateSum::foldstart(void* output, void* tuple)
{
	switch (aggregateschema.getColumnType(0))
	{
		case CT_INTEGER:
		{
			CtInt val = inschema.asInt(tuple, sumfieldno);
			*(CtInt*)output = val;
			break;
		}
		case CT_LONG:
		{
			CtLong val = inschema.asLong(tuple, sumfieldno);
			*(CtLong*)output = val;
			break;
		}
		case CT_DECIMAL:
		{
			CtDecimal val = inschema.asDecimal(tuple, sumfieldno);
			*(CtDecimal*)output = val;
			break;
		}
		default:
			throw NotYetImplemented();
			break;
	};
}

void AggregateSum::fold(void* partialresult, void* tuple)
{
	switch (aggregateschema.getColumnType(0))
	{
		case CT_INTEGER:
		{
			CtInt nextval = inschema.asInt(tuple, sumfieldno);
			*(CtInt*)partialresult += nextval;
			break;
		}
		case CT_LONG:
		{
			CtLong nextval = inschema.asLong(tuple, sumfieldno);
			*(CtLong*)partialresult += nextval;
			break;
		}
		case CT_DECIMAL:
		{
			CtDecimal nextval = inschema.asDecimal(tuple, sumfieldno);
			*(CtDecimal*)partialresult += nextval;
			break;
		}
		default:
			throw NotYetImplemented();
			break;
	};
}
