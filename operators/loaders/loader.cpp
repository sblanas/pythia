
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

#include <iostream>
#include <fstream>
#include <iomanip>
using namespace std;

#include "bzlib.h"
#include <pthread.h>

#include "loader.h"
#include "parser.h"
#include "../../util/parallelqueue.h"

const int ParseThreads = 10;

struct ParseWorkT
{
	ParseWorkT()
	{
		for (unsigned int i=0; i<PARSE_BATCH; ++i)
			WorkUnit[i].target = 0;
	}

	static const unsigned int PARSE_BATCH = 1024;
	struct ParseWorkUnitT
	{
		char input[Loader::MAX_LINE];
		void* target;
	};

	ParseWorkUnitT WorkUnit[PARSE_BATCH];
};

const int QueueSize = 128;
typedef ParallelQueue<ParseWorkT*, QueueSize> WorkQueueT;

struct ThreadArg
{
	Schema* schema;
	Parser* parser;
	WorkQueueT* queue;
	WorkQueueT* emptyqueue;
};

void* parse(void* arg)
{
	unsigned int parseresultcount;
	const char* parseresult[Loader::MAX_COL];
	ParseWorkT* work = NULL;

	Schema* schema = ((ThreadArg*) arg)->schema;
	Parser* parser = ((ThreadArg*) arg)->parser;
	WorkQueueT* queue = ((ThreadArg*) arg)->queue;
	WorkQueueT* emptyqueue = ((ThreadArg*) arg)->emptyqueue;

	while(queue->pop(&work) != WorkQueueT::Rundown)
	{
		for (unsigned int i=0; i<ParseWorkT::PARSE_BATCH; ++i)
		{
			if (work->WorkUnit[i].target == 0)
				break;

			parseresultcount = parser->parseLine(work->WorkUnit[i].input, parseresult, Loader::MAX_COL);
			assert(parseresultcount == schema->columns());
			schema->parseTuple(work->WorkUnit[i].target, parseresult);
		}

		// Push structure back to producer.
		//
		emptyqueue->push(&work);
	}

	return NULL;
};

class ProgressBar
{
	public:
		ProgressBar(long long maxwork, char width) 
			: maxwork(maxwork), width(width), firsttime(true), value(0)
		{ }

		void update(long long work);

	private:
		long long maxwork;
		char width;
		bool firsttime;
		long long value;
};

void ProgressBar::update(long long work)
{
	if (firsttime)
	{
		cout << '[';
		for (char i=0; i<width; ++i)
		{
			cout << ' ';
		}
		cout << "] ";
		cout << "  0%" << flush;
		firsttime = false;
	}

	long long newvalue = work * 100. / maxwork;
	if (newvalue == value)
		return;

	value = newvalue;
	cout << "\b\b\b\b\b\b";
	for (char i=0; i<width; ++i)
		cout << '\b';
	for (char i=0; i<width*value/100; ++i)
		cout << '#';
	for (char i=width*value/100; i<width; ++i)
		cout << ' ';
	cout << "] ";
	cout << setw(3) << value << '%' << flush;
}

Loader::Loader(const string& separator)
	: sep(separator) 
{
}

/**
 * Loads \a filename, parses it and outputs it at the PreloadedTextTable \a output.
 * Automatically checks if input is a BZ2 file and uncompresses on the fly,
 * using libbz2.
 */
void Loader::load(const string& filename, PreloadedTextTable& output, bool verbose)
{
	const char* parseresult[MAX_COL];
	int parseresultcount;

	Parser parser(sep);

	if (verbose)
		cout << "Loading file \"" << filename << "\"..." << endl;

	ifstream f(filename.c_str(), ifstream::in);

	if (!f)
		throw FileNotFoundException();

	f.seekg(0, ios::end);
	ProgressBar progressbar(f.tellg(), 60);
	f.seekg(0, ios::beg);

	if (!isBz2(filename))
	{
		ParseWorkT* work; 

		WorkQueueT queue;
		WorkQueueT emptyqueue;
		ThreadArg targ;
		targ.schema = output.schema();
		targ.parser = &parser;
		targ.queue = &queue;
		targ.emptyqueue = &emptyqueue;

		for (int i=0; i<QueueSize-1; ++i)	// size-1 can be pushed without blocking
		{
			work = new ParseWorkT();
			emptyqueue.push(&work);
		}

		pthread_t threadpool[ParseThreads];

		// Start threads.
		//
		for (int i=0; i<ParseThreads; ++i) 
		{
			assert(!pthread_create(&threadpool[i], NULL, parse, &targ));
		}
	
		// Main control loop.
		//
		do
		{
			// Grab new work block.	
			//
			assert(emptyqueue.pop(&work) != WorkQueueT::Rundown);

			for (unsigned int i = 0; i<ParseWorkT::PARSE_BATCH; ++i)
			{
				f.getline(work->WorkUnit[i].input, MAX_LINE);

				if (!f)
				{
					work->WorkUnit[i].target = 0;
					break;
				}

				if (verbose)
					progressbar.update(f.tellg());

				work->WorkUnit[i].target = output.allocateTuple();
			}

			// Push work block in queue.
			//
			assert(queue.push(&work) != WorkQueueT::Rundown);

		} while(f);

		f.close();

		// Deallocate WorkUnits and terminate threads.
		//
		queue.signalRundown();
		emptyqueue.signalRundown();

		while (emptyqueue.pop(&work) != WorkQueueT::Rundown)
			delete work;

		for (int i=0; i<ParseThreads; ++i) 
		{
			assert(!pthread_join(threadpool[i], NULL));
		}

	} 
	else 
	{
		f.close();

		FILE*   f;
		BZFILE* b;
		int     bzerror;
		int     nBuf;

		int 	unused = 0;

		char* 	decbuf;
		const int decbufsize = 1024*1024;
		decbuf = new char[decbufsize];

		f = fopen (filename.c_str(), "rb");
		if (!f) {
			throw LoadBZ2Exception();
		}
		b = BZ2_bzReadOpen (&bzerror, f, 0, 0, NULL, 0);
		if (bzerror != BZ_OK) {
			BZ2_bzReadClose (&bzerror, b);
			throw LoadBZ2Exception();
		}

		bzerror = BZ_OK;

		while (bzerror == BZ_OK) 
		{
			if (verbose)
				progressbar.update(ftell(f));

			nBuf = BZ2_bzRead ( &bzerror, b, decbuf+unused, decbufsize-unused);

			// Check for error during decompression.
			//
			if (bzerror != BZ_OK && bzerror != BZ_STREAM_END) {
				BZ2_bzReadClose ( &bzerror, b );
				throw LoadBZ2Exception();
			}

			// No error; parse decompressed buffer.
			//
			char* p = decbuf;
			char* usablep = decbuf;
			assert(nBuf + unused <= decbufsize);
			while (p < decbuf + nBuf + unused) {
				p = readFullLine(usablep, decbuf, nBuf + unused);

				// Check if buffer didn't have a full line. If yes, break.
				//
				if (usablep == p) {
					break;
				}

				// We have a full line at usablep. Parse it.
				//
				parseresultcount = parser.parseLine(usablep, parseresult, MAX_COL);
				output.append(parseresult, parseresultcount);

				usablep = p;
			}

			// Copy leftovers to start of buffer and call bzRead so
			// that it writes output immediately after existing data. 
			unused = decbuf + nBuf + unused - p;
			assert(unused < p - decbuf);  // no overlapped memcpy
			memcpy(decbuf, p, unused);
		}

		assert (bzerror == BZ_STREAM_END);
		BZ2_bzReadClose ( &bzerror, b );

		delete[] decbuf;
	}

	if (verbose)
		cout << endl;
}

/** 
 * Reads one full line from buffer. 
 *
 * If buffer has at least one line, returns the start of the next line and 
 * \a cur points to a null-terminated string.
 *
 * Otherwise, returns \a cur.
 */
char* Loader::readFullLine(char* cur, const char* bufstart, const int buflen)
{
	char* oldcur = cur;

	while(cur >= bufstart && cur < (bufstart + buflen))
	{
		if ((*cur) == '\n')
		{
			*cur = 0;
			return ++cur;
		}

		cur++;
	}

	return oldcur;
}

/**
 * Returns true if input file is BZ2-compressed. 
 * Check is done by looking at the three first bytes of the file: if "BZh",
 * then it is assumed to be a valid file to be parsed by libbz2.
 */
bool Loader::isBz2(const string& filename)
{
	char header[4];

	ifstream f(filename.c_str(), ifstream::in);
	f.get(header, 4);
	f.close();

	return string(header) == "BZh";
}
