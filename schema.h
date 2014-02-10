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


#ifndef __MYSCHEMA__
#define __MYSCHEMA__

#include <vector>
#include <utility>
#include <string>
#include <cstring>
#include <cassert>
#include <ctime>
#include "libconfig.h++"

#include "util/custom_asserts.h"
#include "util/static_assert.h"
#include "exceptions.h"

using namespace std;

enum ColumnType {
	CT_INTEGER,	/**< Integer is sizeof(int), either 32 or 64 bits. */
	CT_LONG,	/**< Long is sizeof(long long), exactly 64 bits. */
	CT_DECIMAL,	/**< Decimal is sizeof(double). */
	CT_CHAR,	/**< Char has user-defined length, no zero padding is done in this class. */
	CT_DATE,	/**< Date is sizeof(CtLong), custom format (see DateT class). */
	CT_POINTER	/**< Pointer is sizeof(void*). */
};

typedef int CtInt;
typedef long long CtLong;
typedef double CtDecimal;
typedef char CtChar;
typedef void* CtPointer;

/** 
 * Class representing date objects. Bit structure:
 *
 *  Bit range | Number  | Discrete | 
 * (0 is LSB) | of bits |  values  |  Purpose
 * -----------+---------+----------+---------------
 *     00-09  |     10  |    1024  |  Microseconds
 *     10-19  |     10  |    1024  |  Milliseconds
 *     20-25  |      6  |      64  |  Seconds
 *     26-31  |      6  |      64  |  Minutes
 *     32-36  |      5  |      32  |  Hours
 *     37-41  |      5  |      32  |  Days
 *     42-45  |      4  |      16  |  Months
 *     46-63  |     18  |  262144  |  Years
 */ 
class DateT
{
	public:
		explicit DateT()
		{
			date = 0;
		}

		inline const DateT& operator=(const DateT& rhs) 
		{
			date = rhs.date; 
			return rhs;
		}

		inline void setFromTM(const struct tm* t)
		{
			date  = 0;
			date |= (t->tm_sec & MASKSEC)   << SHIFTSEC;
			date |= (t->tm_min & MASKMIN)   << SHIFTMIN;
			date |= (t->tm_hour & MASKHOUR) << SHIFTHOUR;
			date |= (t->tm_mday & MASKDAY)  << SHIFTDAY;
			date |= (t->tm_mon & MASKMONTH) << SHIFTMONTH;
			date |= (t->tm_year & MASKYEAR) << SHIFTYEAR;
		}

		inline void produceTM(struct tm* out)
		{
			memset(out, 0, sizeof(struct tm));
			out->tm_sec  = (date >> SHIFTSEC)   & MASKSEC;
			out->tm_min  = (date >> SHIFTMIN)   & MASKMIN;
			out->tm_hour = (date >> SHIFTHOUR)  & MASKHOUR;
			out->tm_mday = (date >> SHIFTDAY)   & MASKDAY;
			out->tm_mon  = (date >> SHIFTMONTH) & MASKMONTH;
			out->tm_year = (date >> SHIFTYEAR)  & MASKYEAR;
		}

	private:
		static const unsigned long long MASK4  = 0x0000F;
		static const unsigned long long MASK5  = 0x0001F;
		static const unsigned long long MASK6  = 0x0003F;
		static const unsigned long long MASK10 = 0x003FF;
		static const unsigned long long MASK18 = 0x3FFFF;

		static const unsigned long long MASKUSEC = MASK10;
		static const unsigned long long MASKMSEC = MASK10;
		static const unsigned long long MASKSEC  = MASK6;
		static const unsigned long long MASKMIN  = MASK6;
		static const unsigned long long MASKHOUR = MASK5;
		static const unsigned long long MASKDAY  = MASK5;
		static const unsigned long long MASKMONTH= MASK4;
		static const unsigned long long MASKYEAR = MASK18;

		static const unsigned long long SHIFTUSEC =  0;
		static const unsigned long long SHIFTMSEC = 10;
		static const unsigned long long SHIFTSEC  = 20;
		static const unsigned long long SHIFTMIN  = 26;
		static const unsigned long long SHIFTHOUR = 32;
		static const unsigned long long SHIFTDAY  = 37;
		static const unsigned long long SHIFTMONTH= 42;
		static const unsigned long long SHIFTYEAR = 46;

		CtLong date;
};

typedef class DateT CtDate;

struct ColumnSpec 
{
	ColumnSpec(ColumnType ct, unsigned int sz, const string& str) 
		: type(ct), size(sz), formatstr(str) 
	{ }

	ColumnType type;			/**< Type of field. */
	unsigned int size;			/**< Size of field, in bytes. */
	const string& formatstr;	/**< Format string, only valid for CT_DATE. */
};

#include "comparator.h"

class Schema {
	public:
		Schema() : totalsize(0) { }
		/**
		 * Add new column at the end of this schema.
		 * @param ct Column type.
		 * @param size Max characters (valid for CHAR(x) type only).
		 */
		void add(ColumnType ct, unsigned int size = 0);

		/**
		 * Add new column at the end of this schema.
		 * @param ct Column type -- date only.
		 * @param format Date format string.
		 */
		void add(ColumnType ct, const string& formatstr);

		void add(ColumnSpec desc);

		/** 
		 * Returns columnt type at position \a pos.
		 * @param pos Position in schema.
		 * @return Column type.
		 */
		inline
		ColumnType getColumnType(unsigned int pos);

		/** 
		 * Returns a \a ColumSpec.
		 * @param pos Position in schema.
		 * @return A \a ColumnSpec for this column.
		 */
		inline
		ColumnSpec get(unsigned int pos);

		/**
		 * Get width (in bytes) of column \a pos.
		 */
		inline
		unsigned int getColumnWidth(unsigned int pos);

		/**
		 * Get total number of columns.
		 * @return Total number of columns.
		 */
		inline
		unsigned int columns();

		/**
		 * Get a tuple size, in bytes, for this schema.
		 * @return Tuple size in bytes.
		 */
		inline
		unsigned int getTupleSize();

		/**
		 * Return a string representation of the data in column \a pos.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtChar* asString(void* data, unsigned int pos);

		/**
		 * Calculate the position of data item \a pos inside tuple \a data.
		 * @param data Tuple to work on.
		 * @param pos Position in tuple.
		 * @return Pointer to data of column \a pos.
		 */
		inline
		void* calcOffset(void* data, unsigned int pos);

		/**
		 * Return an integer representation of the data in column \a pos.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtInt asInt(void* data, unsigned int pos);

		/**
		 * Return a date representation (seconds since epoch) of the data in column \a pos.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtDate asDate(void* data, unsigned int pos);

		/**
		 * Return an integer representation of the data in column \a pos.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtLong asLong(void* data, unsigned int pos);

		/**
		 * Return a double representation of the data in column \a pos.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtDecimal asDecimal(void* data, unsigned int pos);

		/**
		 * Return the data in column \a pos as a pointer.
		 * @param data Tuple to work on.
		 * @param pos Position of column to parse.
		 * @throw IllegalConversionException.
		 */
		inline
		const CtPointer asPointer(void* data, unsigned int pos);

		/**
		 * Write the input \a data to the tuple pointed by \a dest.
		 * @pre Caller must have preallocated enough memory at \a dest.
		 * @param dest Destination tuple to write.
		 * @param pos Position in the tuple.
		 * @param data Data to write.
		 */
		inline
		void writeData(void* dest, unsigned int pos, const void* const data);

		/**
		 * Parse the input vector \a input, convert each element to
		 * the appropriate type (according to the schema) and 
		 * call @ref writeData on that.
		 * @pre Caller must have preallocated enough memory at \a dest.
		 * @param dest Destination tuple to write.
		 * @param input Vector of string inputs.
		 */
		void parseTuple(void* dest, const std::vector<std::string>& input);
		void parseTuple(void* dest, const char** input);

		/**
		 * Returns a string representation of each column in the tuple.
		 * @param data Tuple to parse.
		 * @return Vector of strings.
		 */
		std::vector<std::string> outputTuple(void* data);

		/**
		 * Copy a tuple from \a src to \a dest. The number of bytes
		 * copied is equal to \ref getTupleSize.
		 * @pre Caller must have preallocated enough memory at \a dest.
		 * @param dest Destination address.
		 * @param src Source address.
		 */
		inline
		void copyTuple(void* dest, const void* const src);

		/** 
		 * Pretty-print the tuple, using character \a sep as separator.
		 */
		string prettyprint(void* tuple, char sep);

		/**
		 * Create Schema object from configuration node.
		 */
		static Schema create(const libconfig::Setting& line);

		/**
		 * Create Comparator object.
		 */
		static Comparator createComparator(Schema& lhs, unsigned int lpos, 
										Schema& rhs, unsigned int rpos,
										Comparator::Comparison op);

		static Comparator createComparator(Schema& lhs, unsigned int lpos, 
										ColumnSpec& rhs,
										Comparator::Comparison op);

		static Comparator createComparator(ColumnSpec& lhs, 
										Schema& rhs, unsigned int rpos,
										Comparator::Comparison op);

		static const string UninitializedFormatString;

	private:
		vector<ColumnType> vct;
		vector<unsigned short> voffset;
		vector<short> vmetadataidx;
		vector<string> vformatstr;
		int totalsize;
};

#include "schema.inl"

#endif
