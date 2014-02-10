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

#ifndef __MYTABLE__
#define __MYTABLE__

#include <string>
#include <vector>
#include "../../schema.h"
#include "../../util/buffer.h"
#include "../../lock.h"

class TupleBufferCursor 
{
	public:
		/**
		 * Returns the next non-requested page, or NULL if no next page exists.
		 * Not necessarily thread-safe!!!
		 */
		virtual TupleBuffer* readNext() { return atomicReadNext(); }

		/**
		 * Returns the next non-requested page, or NULL if no next page exists.
		 * A synchronized version of readNext().
		 */
		virtual TupleBuffer* atomicReadNext() = 0;

		/** Return \ref Schema object for all pages. */
		virtual Schema* schema() = 0;

		/**
		 * Resets the reading point to the start of the table.
		 */
		virtual void reset() { }

		/**
		 * Closes the cursor.
		 */
		virtual void close() { }

		virtual ~TupleBufferCursor() { }

};

/**
 * Container for a linked list of TupleBuffer objects.
 */
class Table : public TupleBufferCursor {
	public:
		/**
		 * Constructs a new table.
		 * @param s Reference to the schema of the new table.
		 */
		Table() : _schema(NULL), data(NULL), cur(NULL) { }
		virtual ~Table() { }

		enum GlobParamT
		{
			PermuteFiles = 0,
			SortFiles
		};

		enum VerbosityT
		{
			SilentLoad = 0,
			VerboseLoad
		}
		;
		enum LoadErrorT
		{
			LOAD_OK = 0,
			GLOB_FAILED,
			OPEN_FAILED,
			FSTAT_FAILED,
			MMAP_FAILED
		};

		virtual LoadErrorT load(const string& filepattern, 
				const string& separators, 
				VerbosityT verbose, 
				GlobParamT globparam) = 0;

		/**
		 * Initializes a new table.
		 * @param s The schema of the new table.
		 */
		void init(Schema* s);

		/**
		 * Returns the next non-requested bucket, or NULL if no next bucket exists.
		 * Not thread-safe!!!
		 */
		inline TupleBuffer* readNext()
		{
			TupleBuffer* ret = cur;
			if (cur)
				cur = cur->getNext();
			return ret;
		}


		/**
		 * Returns the next non-requested bucket, or NULL if no next bucket exists.
		 * A synchronized version of readNext().
		 */
		inline LinkedTupleBuffer* atomicReadNext()
		{
			LinkedTupleBuffer* oldval;
			LinkedTupleBuffer* newval;

			newval = cur;

			do
			{
				if (newval == NULL)
					return NULL;

				oldval = newval;
				newval = oldval->getNext();
				newval = atomic_compare_and_swap(&cur, oldval, newval);

			} while (newval != oldval);

			return newval;
		}

		/**
		 * Resets the reading point to the start of the table.
		 */
		inline void reset()
		{
			cur = data;
		}

		/**
		 * Close the table, ie. destroy all data associated with it.
		 * Not closing the table will result in a memory leak.
		 */
		virtual void close();

		/** Return associated \ref Schema object. */
		Schema* schema() 
		{ 
			return _schema;
		}

	protected:
		Schema* _schema;
		LinkedTupleBuffer* data;
		/* volatile */ LinkedTupleBuffer* cur;
};

class PreloadedTextTable : public Table {
	public:
		PreloadedTextTable() : last(NULL), size(0) { }
		virtual ~PreloadedTextTable() { }

		void init(Schema* s, unsigned int size);

		/**
		 * Loads a single text file, where each line is a tuple and each field
		 * is separated by any character in the \a separators string.
		 */
		LoadErrorT load(const string& filepattern, const string& separators, 
				VerbosityT verbose, GlobParamT globparam);

		/** 
		 * Appends the \a input at the end of this table, creating new
		 * buckets as necessary.
		 */
		virtual void append(const vector<string>& input);
		virtual void append(const char** data, unsigned int count);
		virtual void append(const void* const src);
		void nontemporalappend16(const void* const src);

		/**
		 * Appends the table to this table.
		 * PRECONDITION: Caller must check that schemas are same.
		 */
		void concatenate(const PreloadedTextTable& table);

		/** 
		 * Allocates one tuple at \last.
		 */
		void* allocateTuple();

	protected:
		LinkedTupleBuffer* last;
		unsigned int size;
};

class MemMappedTable : public Table
{
	public:
		/**
		 * Loads files matching the file pattern at the end of the linked
		 * list. Current read position is reset on load.
		 * @param filepattern File pattern that will be expanded according to
		 * the rules used by the shell. \a glob(3) is used to perform the
		 * expansion. Patterns that start with "/dev/shm/" are treated
		 * as shared memory segments regardless of the OS.
		 */
		LoadErrorT load(const string& filepattern, const string& separators, 
				VerbosityT verbose, GlobParamT globparam);

		void close();

	protected:
		/**
		 * Performs the load on files that match the pattern.
		 */
		LoadErrorT doload(const string& filepattern, int openflags, 
				int memoryprotection, int mmapflags, int globflags);
		
};
#endif
