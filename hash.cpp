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

#include <string>
using std::string;

#include "hash.h"
#include "exceptions.h"

#include "util/static_assert.h"

const CtLong ByteHasher::FNV_64_OFFSET = 14695981039346656037ull;

/**
 * Returns the base 2 logarithm of the next higher power of two.
 */
unsigned int getlogarithm(unsigned int k) 
{
	--k;
	int m = 1 << (sizeof(int)*8-1);
	for (unsigned int i=0; i<sizeof(int)*8; ++i) {
		if ((m >> i) & k)
			return sizeof(int)*8-i;
	}
	return 0;
}


HashFunction::HashFunction(unsigned int buckets) 
{
	if (buckets == 0)
	{
		throw MissingParameterException("Number of hash buckets cannot be zero.");
	}

	_k = getlogarithm(buckets);
}

/**
 * Factory method for creating a TupleHasher.
 *
 * Takes a configuration node with the following structure:
 *
 * <fn-name> = "bytes" | "modulo" | "range" | "exactrange"| "parammodulo" 
 * 		| "knuth" | "tpchorderkey" | "willis" | "alwayszero"
 *
 * <field-spec> = field = <number>; | fieldrange = ( <number>, <number> );
 *
 * hash :
 * {
 *   <field-spec>
 *   buckets = <number>;
 *   fn = <fn-name>;
 *   (hash-function specific options)
 * }
 *
 * If fn = "alwayszero", the buckets and field-spec options are ignored.
 *
 * There are no function-specific options, apart from the following:
 *
 * If fn = "range" | fn = "exactrange", the extra options are:
 *  range = (<scalar>, <scalar>)
 *
 * If fn = "parammodulo" | fn = "knuth", the extra options are:
 *  offset = <scalar>
 *  skipbits = <scalar>
 *
 */
TupleHasher TupleHasher::create(Schema& schema, const libconfig::Setting& node)
{
	HashFunction* hashfn;

	string hashfnname = node["fn"];

	// If no hashing is desired, don't bother checking further.
	//
	if (hashfnname == "alwayszero")
	{
		hashfn = new AlwaysZeroHasher();
		return TupleHasher(0, 0, hashfn);
	}

	int fieldmin, fieldmax;
	int buckets = node["buckets"];

	// Read list with hash fields, either:
	// * "field", a scalar value, or
	// * "fieldrange", an aggregate that has exactly two numeric values
	//
	if (node.exists("fieldrange"))
	{
		libconfig::Setting& field = node["fieldrange"];
	    dbgassert(field.isAggregate());
		dbgassert(field.getLength() == 2);
		fieldmin = field[0];
		fieldmax = field[1];
	}
	else
	{
		libconfig::Setting& field = node["field"];
		dbgassert(field.isNumber());
		fieldmin = field;
		fieldmax = fieldmin;
	}


	// Construct object, and check preconditions.
	//
	if (hashfnname == "bytes")
	{
		hashfn = new ByteHasher(buckets);
	}
	else if (hashfnname == "tpchq1magic")
	{
		hashfn = new TpchQ1MagicByteHasher();
	}
	else
	{
		// A ValueHasher has been requested. 
		// Make sure there's a single hash attribute and its type is numeric.
		//
		if (fieldmin != fieldmax)
		{
			// It's a composite hash.
			throw IllegalSchemaDeclarationException();
		}

		switch(schema.get(fieldmin).type) {
			case CT_INTEGER:
			case CT_LONG:
			case CT_DATE:
				static_assert(sizeof(CtDate) == sizeof(CtLong));
				break;

			default:
				// It's not a numeric type.
				throw IllegalSchemaDeclarationException();
		}

		// Preconditions okay. Initialize appropriate function.
		//
		if (hashfnname == "modulo")
		{
			hashfn = new ModuloValueHasher(buckets);
		}
		else if (hashfnname == "range")
		{
			int min = node["range"][0];
			int max = node["range"][1];
			hashfn = new RangeValueHasher(min, max, buckets);
		}
		else if (hashfnname == "exactrange")
		{
			int min = node["range"][0];
			int max = node["range"][1];
			hashfn = new ExactRangeValueHasher(min, max, buckets);
		}
		else if (hashfnname == "parammodulo") 
		{
			unsigned int skipbits = 0;
			unsigned int offset = 0;
			node.lookupValue("skipbits", skipbits);
			node.lookupValue("offset", offset);
			hashfn = new ParameterizedModuloValueHasher(offset, buckets, skipbits);
		} 
		else if (hashfnname == "knuth") 
		{
			unsigned int skipbits = 0;
			unsigned int offset = 0;
			node.lookupValue("skipbits", skipbits);
			node.lookupValue("offset", offset);
			hashfn = new KnuthValueHasher(offset, buckets, skipbits);
		} 
		else if (hashfnname == "tpchorderkey")
		{
			hashfn = new TpchMagicValueHasher(buckets);
		} 
		else if (hashfnname == "willis")
		{
			hashfn = new WillisValueHasher(buckets);
		} 
		else 
		{
			throw UnknownHashException();
		}
	}

	// Calculate offset and size.
	//
	// Offset is offset of fieldmin.
	// Size is size of fields from fieldmin to fieldmax.
	//
	unsigned long long lloffset 
		= reinterpret_cast<unsigned long long>(schema.calcOffset(0, fieldmin));
	unsigned short offset = static_cast<unsigned short>(lloffset);
	unsigned short size = 0;

	for (int i=fieldmin; i<=fieldmax; ++i)
	{
		size += schema.get(i).size;
	}

	return TupleHasher(offset, size, hashfn);
}


vector<HashFunction*> ParameterizedModuloValueHasher::generate(unsigned int passes)
{
	vector<HashFunction*> ret;

	unsigned int totalbitsset = getlogarithm(buckets()-1);
	unsigned int bitsperpass = totalbitsset / passes;

	for (unsigned int i=0; i<passes-1; ++i) {
		ret.push_back(new ParameterizedModuloValueHasher(_min, 
					(1 << bitsperpass), 
					_skipbits + totalbitsset - ((i+1)*bitsperpass)));
	}

	bitsperpass = totalbitsset - ((passes-1) * bitsperpass);
	ret.push_back(new ParameterizedModuloValueHasher(_min, 
				(1 << bitsperpass), 
				_skipbits));

#ifdef DEBUG
	unsigned int kfinal = dynamic_cast<ParameterizedModuloValueHasher*>(ret[0])->_k;
	for (unsigned int i=1; i<ret.size(); ++i) {
		kfinal |= dynamic_cast<ParameterizedModuloValueHasher*>(ret[i])->_k;
		assert( (dynamic_cast<ParameterizedModuloValueHasher*>(ret[i])->_k 
					& dynamic_cast<ParameterizedModuloValueHasher*>(ret[i-1])->_k) 
				== 0 );
	}
	assert(kfinal==this->_k);
#endif
	
	return ret;
}
