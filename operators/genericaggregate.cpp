
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
#include "operators_priv.h"

#include "../util/numaallocate.h"
#include "../util/numaasserts.h"

#ifdef ENABLE_NUMA
#include <numa.h>
#else
#include <iostream>
using std::cerr;
using std::endl;
#endif

using std::make_pair;

void GenericAggregate::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	Operator::init(root, cfg);

	// Read aggregation fields. This is either a number (if aggregating on a
	// single field) or a list of numbers (for aggregation on a composite key).
	// An empty vector indicates no grouping, ie. compute aggregation over 
	// entire input.
	//
	if (cfg.exists("fields"))
	{
		libconfig::Setting& field = cfg["fields"];
		dbgassert(field.isAggregate());

		for (int i=0; i<field.getLength(); ++i)
		{
			int fieldno = field[i];
			aggfields.push_back(fieldno);
		}
	}
	else
	{
		dbgassert(cfg.exists("field"));
		libconfig::Setting& field = cfg["field"];
		dbgassert(field.isNumber());
		int fieldno = field;
		aggfields.push_back(fieldno);
	}

	// If fields vector is empty, it is okay if hash function has not been specified.
	// pre-fill "alwayszero" hash function.
	//
	if (aggfields.empty() && (cfg.exists("hash") == false))
	{
		libconfig::Setting& hashnode = cfg.add("hash", libconfig::Setting::TypeGroup);
		hashnode.add("fn", libconfig::Setting::TypeString) = "alwayszero";
	}

	// Read user-defined schema.
	//
	Schema& uds = foldinit(root, cfg);
	dbgassert(uds.columns() != 0);

	// Get data type of aggregation attributes, then add user-defined schema.
	//
	for (unsigned int i=0; i<aggfields.size(); ++i)
	{
		ColumnSpec cs = nextOp->getOutSchema().get(aggfields[i]);
		schema.add(cs);
	}
	for (unsigned int i=0; i<uds.columns(); ++i)
	{
		schema.add(uds.get(i));
	}

	// Create object for comparisons.
	//
	vector <unsigned short> tempvec;	// [0,1,2,...]
	for (unsigned int i=0; i<aggfields.size(); ++i)
	{
		tempvec.push_back(i);
	}
	comparator.init(schema, nextOp->getOutSchema(), tempvec, aggfields);

	assert(aggregationmode == Unset);

	if (cfg.exists("presorted"))
	{
		aggregationmode = OnTheFly;
		throw NotYetImplemented();
	}
	else
	{
		hashfn = TupleHasher::create(nextOp->getOutSchema(), cfg["hash"]);
		if (cfg.exists("global"))
		{
			aggregationmode = Global;

			int thr = cfg["threads"];
			threads = thr;

			vector<char> allocpolicy;

#ifdef ENABLE_NUMA
			int maxnuma = numa_max_node() + 1;

			// Stripe on all NUMA nodes.
			//
			for (char i=0; i<maxnuma; ++i)
			{
				allocpolicy.push_back(i);
			}
#else
			cerr << " ** NUMA POLICY WARNING: Memory policy is ignored, "
				<< "NUMA disabled at compile." << endl;
#endif

			hashtable.push_back( HashTable() );
			hashtable[0].init(
				hashfn.buckets(),        // number of hash buckets
				schema.getTupleSize()*4, // space for each bucket
				schema.getTupleSize(),	 // size of each tuple
				allocpolicy,			 // stripe across all
				this);

			barrier.init(threads);
		}
		else
		{
			aggregationmode = ThreadLocal;
			for (int i=0; i<MAX_THREADS; ++i) 
			{
				hashtable.push_back( HashTable() );
			}
		}
	}

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		state.push_back( State(HashTable::Iterator()) );
	}
}

void GenericAggregate::threadInit(unsigned short threadid)
{
	void* space = numaallocate_local("GnAg", sizeof(Page), this);
	output[threadid] = new(space) Page(buffsize, schema.getTupleSize(), this);
	switch(aggregationmode)
	{
		case ThreadLocal:
			hashtable[threadid].init(
				hashfn.buckets(),        // number of hash buckets
				schema.getTupleSize()*4, // space for each bucket
				schema.getTupleSize(),	 // size of each tuple
				vector<char>(),          // always allocate locally
				this);
			hashtable[threadid].bucketclear(0, 1);
			break;

		case Global:
			hashtable[0].bucketclear(threadid, threads);
			barrier.Arrive();
			break;

		default:
			throw NotYetImplemented();
	}
	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}
	state[threadid] = State(hashtable[htid].createIterator());
}

void GenericAggregate::threadClose(unsigned short threadid)
{
	if (output[threadid]) {
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;

	switch(aggregationmode)
	{
		case ThreadLocal:
			hashtable[threadid].bucketclear(0, 1);
			hashtable[threadid].destroy();
			break;

		case Global:
			barrier.Arrive();
			hashtable[0].bucketclear(threadid, threads);
			break;

		default:
			throw NotYetImplemented();
	}
}

void GenericAggregate::destroy()
{
	if (aggregationmode == Global)
		hashtable[0].destroy();
	hashfn.destroy();
	hashtable.clear();
	aggregationmode = Unset;
}

void GenericAggregate::remember(void* tuple, HashTable::Iterator& it, unsigned short htid)
{
	void* candidate;
	int totalaggfields = aggfields.size();
	Schema& inschema = nextOp->getOutSchema();

	// Identify key and hash it to find the hashtable bucket.
	//
	unsigned int h = hashfn.hash(tuple);
	if (aggregationmode == Global)
	{
		hashtable[htid].lockbucket(h);
	}
	hashtable[htid].placeIterator(it, h);

	// Scan bucket.
	//
	while ( (candidate = it.next()) ) 
	{
		// Compare keys of tuple stored in hash chain with input tuple.
		// If match found, increment count and return immediately.
		//
		if (comparator.eval(candidate, tuple)) {
			fold(schema.calcOffset(candidate, totalaggfields), tuple);
			goto unlockandexit;
		}
	}

	// If no match found on hash chain, allocate space and add tuple.
	//
	candidate = hashtable[htid].allocate(h, this);
	for (int i=0; i<totalaggfields; ++i)
	{
		schema.writeData(candidate, i, inschema.calcOffset(tuple, aggfields[i]));
	}
	foldstart(schema.calcOffset(candidate, totalaggfields), tuple);

unlockandexit:
	if (aggregationmode == Global)
	{
		hashtable[htid].unlockbucket(h);
	}
}

Operator::ResultCode GenericAggregate::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	// Read and aggregate until source depleted.
	//
	Page* in;
	Operator::GetNextResultT result; 
	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}
	HashTable::Iterator htit = hashtable[htid].createIterator();
	ResultCode rescode;
	
	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	if (rescode != Operator::Ready) {
		return rescode;
	}

	do {
		result = nextOp->getNext(threadid);

		in = result.second;

		Page::Iterator it = in->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
			remember(tuple, htit, htid);
		}
	} while(result.first == Operator::Ready);

	rescode = nextOp->scanStop(threadid);

#ifdef ENABLE_NUMA
	unsigned int maxnuma = numa_max_node() + 1;
#else
	unsigned int maxnuma = 1;
#endif

	switch (aggregationmode)
	{
		case ThreadLocal:
			state[threadid].bucket = 0;
			break;

		case Global:
			{
			if (threads > maxnuma)
			{
				// Threads from this NUMA node participating 
				//
				unsigned int participants = threads/maxnuma;
				if ((threadid % maxnuma) < (threads % maxnuma))
				{
					participants++;
				}

				// Compute offset
				//
				unsigned int startoffset = threadid % maxnuma;
				startoffset += (threadid/maxnuma) 
					* (((hashfn.buckets()/maxnuma)/participants)*maxnuma);

				state[threadid].bucket = startoffset;
			}
			else
			{
				state[threadid].bucket = threadid;
			}

			barrier.Arrive();

			break;
			}

		default:
			throw NotYetImplemented();
	}
	state[threadid].startoffset = state[threadid].bucket;


	unsigned int step;
	switch (aggregationmode)
	{
		case Global:
			step = (threads > maxnuma) ? maxnuma : threads;
			break;

		default:
			step = 1;
			break;
	}
	state[threadid].step = step;


	unsigned int hashbuckets = hashfn.buckets();
	unsigned int endoffset;
	if (aggregationmode == Global)
	{
		if (threadid >= (threads - maxnuma))
		{
			endoffset = hashbuckets;
		}
		else
		{
			// Threads from this NUMA node participating 
			//
			unsigned int participants = threads/maxnuma;
			if ((threadid % maxnuma) < (threads % maxnuma))
			{
				participants++;
			}

			// Compute offset
			//
			endoffset = (threadid % maxnuma);
			endoffset += ((threadid+maxnuma)/maxnuma)
						* (((hashbuckets/maxnuma)/participants)*maxnuma);
		}
	}
	else
	{
		endoffset = hashbuckets;
	}
	state[threadid].endoffset = endoffset;


	hashtable[htid].placeIterator(state[threadid].iterator, state[threadid].bucket);

	// If scan failed, return Error. Otherwise return what scanClose returned.
	//
	return ((result.first != Operator::Error) ? rescode : Operator::Error);
}

Operator::GetNextResultT GenericAggregate::getNext(unsigned short threadid)
{
	void* tuple;

	// Restore iterator from saved state.
	//
	HashTable::Iterator& it = state[threadid].iterator;

	Page* out = output[threadid];
	out->clear();

	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}

	const unsigned int endoffset = state[threadid].endoffset;
	const unsigned int step = state[threadid].step;

	for (unsigned int i=state[threadid].bucket; i<endoffset; i+=step)
	{
		// Output aggregates, page-by-page.
		//
		while ( (tuple = it.next()) ) 
		{
#ifdef DEBUG2
#ifdef ENABLE_NUMA
#warning Hardcoded NUMA socket number.
			unsigned int maxnuma = 4;
// Disabled for perf.
//			unsigned int maxnuma = numa_max_node() + 1;
#else
			unsigned int maxnuma = 1;
#endif
			if (threads >= maxnuma)
			{
				assertaddresslocal(tuple);
			}
#endif

			// Do copy to output buffer.
			//
			void* dest = out->allocateTuple();
			dbgassert(out->isValidTupleAddress(dest));
			schema.copyTuple(dest, tuple);

			// If output buffer full, record state and return.
			// 
			if (!out->canStoreTuple()) {
				state[threadid].bucket = i; 
				return make_pair(Ready, out);
			}
		}

		// Making iterator wrap-around so as to avoid past-the-end placement.
		//
		if ((i+step) < endoffset)
		{
			hashtable[htid].placeIterator(it, (i+step));
		}
		else
		{
			hashtable[htid].placeIterator(it, 0);
		}
	}

	return make_pair(Finished, out); 
}

vector<unsigned int> GenericAggregate::statAggBuckets()
{
	vector<unsigned int> ret;

	for (unsigned int i=0; i<hashtable.size(); ++i)
	{
		vector<unsigned int> htstats = hashtable.at(i).statBuckets();
		const unsigned int htstatsize = htstats.size();

		if (htstatsize >= ret.size())
			ret.resize(htstatsize, 0);

		for (unsigned int j=0; j<htstatsize; ++j)
		{
			ret.at(j) += htstats.at(j);
		}
	}

	return ret;
}
