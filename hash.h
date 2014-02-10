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


#ifndef __MYHASHFUNCTION__
#define __MYHASHFUNCTION__

#include <vector>
#include "libconfig.h++"

#include "schema.h"

using std::vector;

unsigned int getlogarithm(unsigned int k);

/**
 * Base class for all hash functions. Default implementation hashes to values
 * that are power of 2, but this can be overriden by subclasses.
 */
class HashFunction 
{
	public:
		/**
		 * Returns the domain size of the hash function. 
		 * If ret is the return value, this function hashes from zero to ret-1.
		 */
		virtual inline unsigned int buckets() {
			return 1 << _k;
		}

		virtual unsigned int hash(void* start, size_t size) = 0;

		virtual ~HashFunction() { }
		
	protected:
		/**
		 * Creates a new hashing object, rounding the number of buckets
		 * \a k to the next power of two. Formally \a k will become
		 * \f$ k = 2^{\lceil{\log_2{k}}\rceil} \f$
		 * @param buckets Number of buckets. Value will be rounded to the next
		 * higher power of two.
		 */
		HashFunction(unsigned int buckets);

		/**
		 * Creates a new object and sets _k to zero, so that subclasses do not
		 * have to treat \a _k as log2(buckets).
		 */
		HashFunction() : _k(0) { }

		CtLong _k;	/**< \f$ \_k=log_2(buckets) \f$ */
};

class TupleHasher
{
	public:
		friend class PrettyPrinterVisitor;

		TupleHasher() 
			: offset(0), size(0), fn(0)
		{ }

		static TupleHasher create(
				Schema& schema, const libconfig::Setting& node);

		inline unsigned int hash(void* tuple)
		{
			return fn->hash(static_cast<char*>(tuple) + offset, size);
		}

		inline void destroy()
		{
			if (fn != 0)
			{
				delete fn;
				fn = 0;
			}
		}

		inline unsigned int buckets()
		{
			return fn->buckets();
		}

	private:
		TupleHasher(unsigned short of, unsigned short sz, HashFunction* f) 
			: offset(of), size(sz), fn(f)
		{ }

		unsigned short offset;
		unsigned short size;
		HashFunction* fn;
};

/**
 * Class compares bytes that form the group by in TPC-H Q1.
 */
class TpchQ1MagicByteHasher : public HashFunction
{
	public:
		TpchQ1MagicByteHasher()
			: HashFunction(4)
		{ }

		inline unsigned int hash(void* start, size_t dummy)
		{
			unsigned int v = * reinterpret_cast<unsigned int*>(start);
			return (((v>>4) | (v>>16)) & 0x1u) | ((v>>1) & 0x2u);
		}
};

/**
 * Class compares bytes using FNV-1a function.
 *
 * Bad choice for comparisons that operate on interpreted values. Examples of
 * cases where byte comparisons don't make sense is comparing a CtInt with a
 * CtLong, or comparing two strings of different locales.
 */
class ByteHasher : public HashFunction
{
	public:
		ByteHasher(unsigned int buckets)
			: HashFunction(buckets)
		{ }

		inline unsigned int hash(void* start, size_t size)
		{
			CtLong hash = FNV_64_OFFSET;
			char* vcur = static_cast<char*>(start); 
			char* vend = static_cast<char*>(start) + size;

			// Not supporting hashing on nothing (when size==0) removes two
			// jumps from the generated assembly code.  The do-while loop
			// should become a while loop (ie. check the guard at the beginning
			// as well), if hashing on nothing is a desired feauture of this
			// hash function. 
			//
			dbgassert(vcur != vend);

			do
			{
				// XOR the bottom with the current byte.
				//
				hash ^= (CtLong) *vcur++;

				// Multiply by the 64 bit FNV magic prime. 
				//
				// FNV_64_PRIME = 2**40 + 0x01B3, so the multiplication
				// can be computed as the following shifts.
				//
				hash += (hash << 1) + (hash << 4) + (hash << 5) +
					(hash << 7) + (hash << 8) + (hash << 40);

			} while (vcur < vend);

			hash = ((hash >> _k) ^ hash) & ((1ull << _k) - 1);
			return static_cast<unsigned int>(hash);
		}

	private:
		static const CtLong FNV_64_OFFSET;
};

class ValueHasher : public HashFunction
{
	public:
		ValueHasher(unsigned int buckets)
			: HashFunction(buckets)
		{ }

		virtual ~ValueHasher() { }
		
		/**
		 * Returns the bucket number \f$n\in[0,k)\f$ for this \a value.
		 * @param value Value to hash.
		 * @return Bucket number.
		 */
		virtual unsigned int hash(CtLong value) = 0;

	protected:
		/**
		 * Returns a numerical CtLong for the memory pointed to by the pointer.
		 */
		inline CtLong numericalize(void* start, size_t size)
		{
			CtLong ret = 0;

			dbgassert( sizeof(CtDate) == sizeof(CtInt) 
					|| sizeof(CtDate) == sizeof(CtLong));

			switch(size)
			{
				case sizeof(CtInt):
					ret = *(CtInt*)start;
					break;
				case sizeof(CtLong):
					ret = *(CtLong*)start;
					break;
				default:
					throw IllegalConversionException();
			}
			return ret;
		}
};

/**
 * Range partitioning value hasher.
 */
class RangeValueHasher : public ValueHasher 
{
	public:
		RangeValueHasher(CtLong min, CtLong max, unsigned int buckets)
			: ValueHasher(buckets), _min(min), _max(max) 
		{ }

		inline unsigned int hash(CtLong value) 
		{
			CtLong val = (value-_min);
			val <<= _k;
			return val / (_max-_min+1);
		}

		inline unsigned int hash(void* start, size_t size)
		{
			return RangeValueHasher::hash(numericalize(start, size));
		}

	protected:
		CtLong _min, _max;
};

/**
 * Fast value hasher where modulo is a power of two.
 */
class ModuloValueHasher : public ValueHasher 
{
	public:
		ModuloValueHasher(unsigned int buckets)
			: ValueHasher(buckets)
		{ 
			_k = ((1 << _k) - 1);	// _k is used as the modulo mask
		}

		/** Return h(x) = x mod k. */
		inline unsigned int hash(CtLong value) 
		{
			return (value & _k);
		}

		inline unsigned int hash(void* start, size_t size)
		{
			return ModuloValueHasher::hash(numericalize(start, size));
		}

		/**
		 * Returns the domain size of the hash function. 
		 * If ret is the return value, this function hashes from zero to ret-1.
		 */
		inline unsigned int buckets() {
			return _k + 1;
		}
};

/**
 * Parameterized value hasher where modulo is a power of two. Two additional
 * parameters to (1) control number of least significant bits to discard, and
 * (2) specify a starting offset for the value domain so that key ``offset''
 * hashes to zero.
 */
class ParameterizedModuloValueHasher : public ModuloValueHasher 
{
	public:
		/**
		 * Skipbits parameter defines number of least-significant bits which
		 * will be discarded before computing the hash.
		 */
		ParameterizedModuloValueHasher(CtLong offset, unsigned int buckets, unsigned char skipbits)
			: ModuloValueHasher(buckets), _min(offset), _skipbits(skipbits) 
		{ 
			_k <<= _skipbits;	// _k is used as the modulo mask
		}

		/** Return h(x) = x mod k. */
		inline unsigned int hash(CtLong value) {
			return ((value-_min) & _k) >> _skipbits;
		}

		inline unsigned int hash(void* start, size_t size)
		{
			return ParameterizedModuloValueHasher::hash(numericalize(start, size));
		}

		/**
		 * Returns the domain size of the hash function. 
		 * If ret is the return value, this function hashes from zero to ret-1.
		 */
		inline unsigned int buckets() {
			return (_k >> _skipbits) + 1;
		}

		/** Generate set of hash functions to be used for multiple passes. */
		vector<HashFunction*> generate(unsigned int passes);

	protected:
		CtLong _min; 
		unsigned char _skipbits;
};

/**
 * Uses Knuth's hash function that multiplies with 2,654,435,761 (a prime
 * number, very close to 2^32 / golden ratio). This implementation then
 * discards \a skipbits least significant bits, and uses the remaining \a
 * log2(buckets) least significant bits as the hash value.
 * Works well for sequential numbers like primary keys, and, unlike modulo, it
 * does not produce an artificially sequential memory access pattern. 
 */
class KnuthValueHasher : public ParameterizedModuloValueHasher 
{
	public:
		/**
		 * Skipbits parameter defines number of least-significant bits which
		 * will be discarded before computing the hash.
		 */
		KnuthValueHasher(CtLong offset, unsigned int buckets, unsigned char skipbits)
			: ParameterizedModuloValueHasher(offset, buckets, skipbits) 
		{ 
		}

		/** Return h(x) = x mod k. */
		inline unsigned int hash(CtLong value) {
			return ((value*2654435761uL) & _k) >> _skipbits;
		}

		inline unsigned int hash(void* start, size_t size)
		{
			return KnuthValueHasher::hash(numericalize(start, size));
		}
};

/** 
 * The domain of o_orderkey in TPC-H has the weird property that it 
 * sets least signifiant bits 3 and 4 to zero. This hash function 
 * has been designed to work around this peculiarity. 
 *
 * NOTE: Taking advantage of this is illegal, per TPC-H specs. If you're 
 * using this hash function to claim good TPC-H numbers, you're crazy.
 */
class TpchMagicValueHasher : public ModuloValueHasher 
{
	public:
		TpchMagicValueHasher(unsigned int buckets)
			: ModuloValueHasher(buckets) { }

		inline unsigned int hash(void* start, size_t size)
		{
			return TpchMagicValueHasher::hash(numericalize(start, size));
		}

		inline unsigned int hash(long long value) {
			return ( ( (value >> 2) & ~7L ) | (value & 7) ) & _k;
		}
};

class WillisValueHasher : public ModuloValueHasher
{
    public:
        WillisValueHasher (unsigned int buckets)
			: ModuloValueHasher(buckets)
		{ }

		inline unsigned int hash(void* start, size_t size)
		{
			return WillisValueHasher::hash(numericalize(start, size));
		}

        inline unsigned int hash (CtLong value)
		{
            CtLong l = value;
            l = (~l) + (l << 21); // l = (l << 21) - l - 1;
            l = l ^ (l >> 24);
            l = (l + (l << 3)) + (l << 8); // l * 265
            l = l ^ (l >> 14);
            l = (l + (l << 2)) + (l << 4); // l * 21
            l = l ^ (l >> 28);
            l = l + (l << 31);
            l = (l > 0) ? l : -l;
            return (l & _k);
        }
};

/**
 * Pseudo function when no hashing is desired (eg. aggregation with no GROUP BY
 * clause). Hashes to zero and contains one bucket.
 */
class AlwaysZeroHasher : public HashFunction 
{
	public:
		AlwaysZeroHasher()
			: HashFunction(1) 
		{ }

		virtual unsigned int hash(void* start, size_t size) 
		{ 
			return 0; 
		}

		virtual ~AlwaysZeroHasher() { }
};

/**
 * Function that range partitions into exactly as many buckets as specified.
 */
class ExactRangeValueHasher : public RangeValueHasher
{
	public:
		ExactRangeValueHasher(CtLong min, CtLong max, unsigned int buckets)
			: RangeValueHasher(min, max, buckets)
		{ 
			_k = buckets;
			_bucketrange = (_max-_min+_k)/_k;
		}

		inline unsigned int hash(CtLong value) 
		{
			CtLong val = (value-_min);
			return val / _bucketrange;
		}

		inline unsigned int hash(void* start, size_t size)
		{
			return ExactRangeValueHasher::hash(numericalize(start, size));
		}

		inline CtLong minimumforbucket(unsigned int bucket)
		{
			if (bucket == _k)
				return _max + 1;
			return bucket*_bucketrange + _min;
		}

		inline unsigned int buckets()
		{
			return _k;
		}

	protected:
		CtLong _bucketrange;
};

#endif
