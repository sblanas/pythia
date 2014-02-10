
/*
 * Copyright 2010, Pythia authors (see AUTHORS file).
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

#ifdef ENABLE_NUMA
#define NUMA_VERSION1_COMPATIBILITY //< v1 is widely available 
#include <numa.h>
#endif

#include <vector>

#include "operators.h"
#include "operators_priv.h"
#include "../rdtsc.h"

#include "../util/numaallocate.h"

#include <sstream>
#include <iostream>
using std::istringstream;
using std::make_pair;
using std::cerr;
using std::endl;

// Enable define to turn on the tracing facility.
//
// The tracing facility records events at the global static TraceLog array.
// This simplifies allocation, reclamation and object-oriented clutter, at the
// cost of producing meaningless output if more than one HashJoins are being
// executed in parallel.
//
// #define TRACELOG

#ifdef TRACELOG
struct TraceEntry
{
	unsigned short threadid;
	unsigned char label;
	unsigned long tick;
};

static const unsigned long long TraceLogSize = 0x100000;
static TraceEntry TraceLog[TraceLogSize];
static volatile unsigned long TraceLogTail;

void append_to_log(unsigned char label, unsigned short threadid)
{
	static_assert(sizeof(unsigned long) == sizeof(void*));
	
	unsigned long oldval;
	unsigned long newval;

	newval = TraceLogTail;

	do
	{
		oldval = newval;
		newval = (oldval + 1) % TraceLogSize;
		newval = (unsigned long) atomic_compare_and_swap (
				(void**)&TraceLogTail, (void*)oldval, (void*)newval);

	} while (newval != oldval);

	TraceLog[newval].threadid = threadid;
	TraceLog[newval].label = label;
	TraceLog[newval].tick = curtick();
}

#include<fstream>

void dbgDumpTraceToFile(const char* filename)
{
	ofstream of(filename);
	for (unsigned long i=0; i<TraceLogSize; ++i)
	{
		if (TraceLog[i].threadid == static_cast<unsigned short>(-1))
			break;
		of << TraceLog[i].threadid << " ";
		of << TraceLog[i].label << " ";
		of << TraceLog[i].tick << endl;
	}
	of.close();
}

#define TRACE( x ) append_to_log( x , threadid )
#define TRACEID( x, y ) append_to_log( x , y )
#else
#define TRACE( x )
#define TRACEID( x, y )
#endif

int numafromaddress(void* p);
void dbgPrintNuma(vector<Operator::Page*> output)
{
	for (unsigned int i=0; i<output.size(); ++i)
	{
		if (output.at(i) == 0)
			continue;
		cout << i << "\t" << numafromaddress(output.at(i)) << endl;
	}
}

/**
 * Parses a Setting, that looks like: [ "B$0", "P$1", ... ]
 *
 * It first locates the '$' character. It reads one character before (if
 * possible), and expects either "B" or "P", to denote the input source. It
 * then tries to read an int after the '$' sign, and returns.
 * On failure, an exception is thrown.
 */
std::vector<JoinOp::JoinPrjT> createProjectionVector(const libconfig::Setting& line) {
	std::vector<JoinOp::JoinPrjT> ret;
	dbgassert(line.isAggregate());

	for (int i=0; i<line.getLength(); ++i) 
	{
		string s = line[i];
		size_t l = s.find('$');

		// Is '$' in an appropriate position?
		//
		if (l == string::npos || l == 0)
			throw InvalidParameter();

		string remainder = s.substr(l+1);
		unsigned int attribute;
		istringstream ss(remainder);
		ss >> attribute;

		// Does a number follow '$'?
		//
		if (!ss)
			throw InvalidParameter();

		char c = s.at(l-1);

		switch (c)
		{
			case 'B':
				ret.push_back(make_pair(JoinOp::BuildSide, attribute));
				break;
			case 'P':
				ret.push_back(make_pair(JoinOp::ProbeSide, attribute));
				break;
			default:
				// 'B' or 'P' doesn't precede '$', throw.
				//
				throw InvalidParameter();
		}
	}
	return ret;
}

void JoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	Operator::init(root, node);

#ifdef TRACELOG
	memset(TraceLog, -1, sizeof(TraceEntry)*TraceLogSize);
	TraceLogTail = 0;
#endif

	// Remember select and join attributes.
	projection = createProjectionVector(node["projection"]);
	joinattr1 = node["buildjattr"];
	joinattr2 = node["probejattr"];

	// Create partition groups, and initialize barriers.
	//
	libconfig::Setting& partnode = node["threadgroups"];
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		threadgroups.push_back(static_cast<unsigned short>(-1));
		threadposingrp.push_back(static_cast<unsigned short>(-1));
	}
	dbgassert(partnode.isAggregate());
	for (int i=0; i<partnode.getLength(); ++i)
	{
		dbgassert(partnode[i].isAggregate());

		// Fill thread->group mapping.
		//
		for (int j=0; j<partnode[i].getLength(); ++j)
		{
			int tid = partnode[i][j];
			// Check that same thread does not exist in two groups.
			assert(threadgroups.at(tid) == static_cast<unsigned short>(-1));
			assert(threadposingrp.at(tid) == static_cast<unsigned short>(-1));

			threadgroups.at(tid) = i;
			threadposingrp.at(tid) = j;
		}

		// Fill group->threadlead mapping.
		//
		int leadtid = partnode[i][0];
		groupleader.push_back(leadtid);

		// Fill group->groupsize mapping.
		//
		groupsize.push_back(partnode[i].getLength());

		// Fill group->barrier mapping.
		// 
		barriers.push_back(PThreadLockCVBarrier());
		dbgassert(static_cast<unsigned int>(i) == (barriers.size() - 1));
		barriers.at(i).init(groupsize.at(i));
	}
	dbgassert(barriers.size() == groupleader.size());

	// Compute and store output schema.
	for (unsigned int i=0; i<projection.size(); ++i)
	{
		unsigned int attr = projection[i].second;

		switch (projection[i].first)
		{
			case BuildSide:
				dbgassert(attr < buildOp->getOutSchema().columns());
				schema.add(buildOp->getOutSchema().get(attr));
				break;
			case ProbeSide:
				dbgassert(attr < probeOp->getOutSchema().columns());
				schema.add(probeOp->getOutSchema().get(attr));
				break;
			default:
				throw InvalidParameter();
		}
	}
}

HashJoinOp::HashJoinState::HashJoinState() 
	: location(NULL), pgiter(EmptyPage.createIterator()), probedepleted(false) 
{ 
}

/**
 * Hash join operator initialization.
 *
 * In configuration object, "hash" node picks up "field" from the join
 * attributes. 
 */
void HashJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	JoinOp::init(root, node);

	// Compute and store build schemas.
	sbuild.add(buildOp->getOutSchema().get(joinattr1));
	for (unsigned int i=0; i<projection.size(); ++i) 
	{
		if (projection[i].first != BuildSide)
			continue; 

		ColumnSpec ct = buildOp->getOutSchema().get(projection[i].second);
		sbuild.add(ct);
	}

	// Initialize hash functions.
	//
	dbgassert(!node["hash"].exists("field"));

	node["hash"].add("field", libconfig::Setting::TypeInt) = (int) joinattr1;
	buildhasher = TupleHasher::create(buildOp->getOutSchema(), node["hash"]);
	node["hash"].remove("field");

	node["hash"].add("field", libconfig::Setting::TypeInt) = (int) joinattr2;
	probehasher = TupleHasher::create(probeOp->getOutSchema(), node["hash"]);
	node["hash"].remove("field");

	dbgassert(buildhasher.buckets() == probehasher.buckets());

	// Do hashtable-related init.
	//
	// Join attribute is written first in hashtable during build, thus
	// comparison on build side is always on first column.
	//
	keycomparator = Schema::createComparator(
			sbuild, 0,
			probeOp->getOutSchema(), joinattr2,
			Comparator::NotEqual);

	buildpagesize = node["tuplesperbucket"];
	buildpagesize *= sbuild.getTupleSize();

	// Create hash table objects, to be initialized by each thread group
	// leader when calling \a threadInit.
	//
	for (unsigned int i=0; i<groupleader.size(); ++i)
	{
		hashtable.push_back(HashTable());
	}

	// Create and populate NUMA allocation policy object. This could be done
	// per-group, but for now we use a blanket allocation policy.
	//
	string policystr = node["allocpolicy"];
	if (policystr == "striped")
	{
#ifdef ENABLE_NUMA
		int maxnuma = numa_max_node() + 1;
		if (node.exists("stripeon"))
		{
			// Stripe data on user-specified NUMA nodes.
			//
			libconfig::Setting& stripenode = node["stripeon"];
			for (int i=0; i<stripenode.getLength(); ++i)
			{
				int v = stripenode[i];
				assert(0 <= v);
				assert(v < maxnuma);
				allocpolicy.push_back(static_cast<char>(v));
			}
		}
		else
		{
			// No constraint specified, stripe on all NUMA nodes.
			//
			for (char i=0; i<maxnuma; ++i)
			{
				allocpolicy.push_back(i);
			}
		}
#else
		cerr << " ** NUMA POLICY WARNING: Memory policy is ignored, "
			<< "NUMA disabled at compile." << endl;
#endif
	}

	// Create state and output tables.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		hashjoinstate.push_back(NULL);
	}
}

void HashJoinOp::threadInit(unsigned short threadid)
{
	void* space2 = numaallocate_local("HJst", sizeof(HashJoinState), this);
	hashjoinstate[threadid] = new (space2) HashJoinState();

	const unsigned short groupno = threadgroups.at(threadid);
	if (groupleader.at(groupno) == threadid)
	{
		hashtable[groupno].init(buildhasher.buckets(), buildpagesize, 
				sbuild.getTupleSize(), allocpolicy, this);
	}

	// Wait for hashtable init before clearing bucket space and creating
	// iterator.
	//
	barriers[groupno].Arrive();
	hashtable[groupno].bucketclear(threadposingrp.at(threadid), groupsize.at(groupno));

	barriers[groupno].Arrive();
	hashjoinstate[threadid]->htiter = hashtable[groupno].createIterator();

	void* space = numaallocate_local("HJpg", sizeof(Page), this);
	output[threadid] = new (space) Page(buffsize, schema.getTupleSize(), this, "HJpg");
}

/**
 * BUG: On error, other threads will get stuck at the barrier.
 */
Operator::ResultCode HashJoinOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < threadgroups.size());
	const unsigned short groupno = threadgroups[threadid];
	dbgassert(groupno < barriers.size());

	TRACE('1');

	// Build hash table.
	//
	GetNextResultT result;
	ResultCode rescode;

	rescode = buildOp->scanStart(threadid, indexdatapage, indexdataschema);
	if (rescode == Operator::Error) {
		return Error;
	}

	while (result.first == Operator::Ready) {
		result = buildOp->getNext(threadid);
		buildFromPage(result.second, groupno);
	}

	if (result.first == Operator::Error) {
		return Error;
	}

	rescode = buildOp->scanStop(threadid);
	if (rescode == Operator::Error) {
		return Error;
	}

	TRACE('2');

	// This thread is complete. Wait for other threads in the same group (ie.
	// partition) before you continue, or this thread might lose data.
	//
	barriers[groupno].Arrive();

	TRACE('3');

	// Hash table is complete now, every thread can proceed.
	// Handle first call: 
	// 0. Start scan on probe.
	// 1. Make state.location point at first output tuple of probeOp->GetNext.
	// 2. Place htiter and pgiter.
	
	rescode = probeOp->scanStart(threadid, indexdatapage, indexdataschema);
	if (rescode == Operator::Error) {
		return Error;
	}

	void* tup2;
	tup2 = readNextTupleFromProbe(threadid);
	hashjoinstate[threadid]->location = tup2;

	if (tup2 != NULL) {
		unsigned int bucket = probehasher.hash(tup2);
		hashtable[groupno].placeIterator(hashjoinstate[threadid]->htiter, bucket);
	} else {
		// Probe is empty?!
		rescode = Finished;
	}

	TRACE('4');

	return rescode;
}

/**
 * Evaluates \a projection on the two input tuples, writing result to
 * \a output. No test is done to see that the tuples match.
 * Overrides JoinOp::constructOutputTuple to use hashtable schema \a sbuild.
 * @param tupbuild The build tuple.
 * @param tupprobe The probe tuple.
 * @param output The output tuple. Caller must have preallocated enough
 * space as described by this object's \a getOutSchema().
 */
void HashJoinOp::constructOutputTuple(void* tupbuild, void* tupprobe, void* output)
{
	void* tupattr;
	Schema& probeschema = probeOp->getOutSchema();

	// Copy each column to destination. buildFromPage scans projection
	// sequentially, so we repeat buildattr starts from 1, because the join
	// key is attr 0.
	for (unsigned int j=0, buildattr=1; j<projection.size(); ++j) 
	{
		if (projection[j].first == BuildSide)
		{
			dbg2assert(sbuild.getColumnType(buildattr) == schema.getColumnType(j));
			dbg2assert(sbuild.getColumnWidth(buildattr) <= schema.getColumnWidth(j));
			tupattr = sbuild.calcOffset(tupbuild, buildattr);
			schema.writeData(output, j, tupattr);
			buildattr++;
		}
		else
		{
			dbg2assert(projection[j].first == ProbeSide);
			unsigned int attr = projection[j].second;
			tupattr = probeschema.calcOffset(tupprobe, attr);
			schema.writeData(output, j, tupattr);
		}
	}
}

Operator::GetNextResultT HashJoinOp::getNext(unsigned short threadid)
{
	void* tup1;
	void* tup2;

	TRACE('G');

	Page* out = output[threadid];
	HashJoinState* state = hashjoinstate[threadid];
	HashTable& ht = hashtable[threadgroups[threadid]];

	out->clear();
	tup2 = state->location;

	// Reposition based on iterators.
	while(1) {
		// Finish joining last tuple.
		while ( (tup1 = state->htiter.next()) ) {
			void* target;

			if (keycomparator.eval(tup1, tup2)) {
				continue;
			}

			// Keys equal, join tup1 with tup2 and copy at output buffer.
			target = out->allocateTuple();
			dbg2assert(target!=NULL);

			constructOutputTuple(tup1, tup2, target);

			// If buffer full, return with Ready. 
			// Nothing to remember, as htiter is part of the state already.
			if (!out->canStoreTuple()) {
				TRACE('R');
				return make_pair(Ready, out);
			}
		}
	
		// Old tuple done. Read new tuple from probe side.
		tup2 = readNextTupleFromProbe(threadid);

		// Find bucket in hashtable and remember in state.
		// readNextTupleFromProbe() places pgiter, so no need to worry here.
		state->location = tup2;
		if (tup2 != NULL) {
			// hash tup2 to place htiter.
			unsigned int bucket = probehasher.hash(tup2);
			ht.placeIterator(state->htiter, bucket);
		} else {
			state->htiter = ht.createIterator();
			TRACE('F');
			return make_pair(Operator::Finished, out);
		}

	}

	TRACE('E');
	// How the hell did we get here?
	return make_pair(Operator::Error, &EmptyPage);
}

/**
 * Reads next tuple from probe. WARNING: non-existent error-handling.
 * As a side-effect, it sets the \a pgiter in the \a hashjoinstate for the
 * current thread.
 * @return Next tuple if available, otherwise NULL.
 * @bug Remove exception, make proper returns with error checking.
 */
void* HashJoinOp::readNextTupleFromProbe(unsigned short threadid) {
	void* ret;
	GetNextResultT result;

	// Scan forward from pgit. 
	Page::Iterator& pgit = hashjoinstate[threadid]->pgiter;
	ret = pgit.next();

	// If valid tuple, return it.
	if (ret != NULL)
		return ret;

	// If probe depleted, nothing more to do here.
	if (hashjoinstate[threadid]->probedepleted)
		return NULL;

	// Request next page and place iterator.
	result = probeOp->getNext(threadid);

	if (result.first == Operator::Error) {
		throw QueryExecutionError();
	}

	pgit.place(result.second);
	
	// If return code was Finished, return tuple. Otherwise, recurse. 
	// (Rationale for recursion: Perhaps it's an empty page in a Ready stream.)
	if (result.first == Operator::Finished) {
		hashjoinstate[threadid]->probedepleted = true;
		return pgit.next();
	}
	return readNextTupleFromProbe(threadid);
}

void HashJoinOp::threadClose(unsigned short threadid)
{
	if (hashjoinstate[threadid]) {
		numadeallocate(hashjoinstate[threadid]);
	}
	hashjoinstate[threadid] = NULL;

	if (output[threadid]) {
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;

	// Wait for all threads to finish, then clear buckets and destroy hashtable.
	//
	const unsigned short groupno = threadgroups.at(threadid);

	barriers[groupno].Arrive();
	hashtable[groupno].bucketclear(threadposingrp.at(threadid), groupsize.at(groupno));

	barriers[groupno].Arrive();
	if (groupleader.at(groupno) == threadid)
	{
		hashtable[groupno].destroy();
	}
}

void HashJoinOp::destroy()
{
	buildhasher.destroy();
	probehasher.destroy();

#ifdef TRACELOG
	dbgDumpTraceToFile("hjtrace");
#endif
}

void HashJoinOp::buildFromPage(Page* page, unsigned short groupno)
{
	void* tup = NULL;
	void* target = NULL;
	unsigned int hashbucket;
	Schema& buildschema = buildOp->getOutSchema();

	Page::Iterator it = page->createIterator();
	while( (tup = it.next()) ) {
		// Find destination bucket.
		hashbucket = buildhasher.hash(tup);
		target = hashtable[groupno].atomicAllocate(hashbucket, this);

		// Project on build, copy result to target.
		sbuild.writeData(target, 0, buildschema.calcOffset(tup, joinattr1));
		for (unsigned int j=0, buildattrtarget=0; j<projection.size(); ++j) 
		{
			if (projection[j].first != BuildSide)
				continue; 

			unsigned int attr = projection[j].second;
			sbuild.writeData(target, buildattrtarget+1,	// dest, col in output
					buildschema.calcOffset(tup, attr));	// src 
			buildattrtarget++;
		}
	}
}

/**
 * Scan on buildOp is started and stopped inside \a scanStart.
 */
Operator::ResultCode HashJoinOp::scanStop(unsigned short threadid) 
{ 
	return probeOp->scanStop(threadid);
}

void SortMergeJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	JoinOp::init(root, node);

	// Populate group->thread mapping.
	//
	libconfig::Setting& partnode = node["threadgroups"];
	dbgassert(partnode.isAggregate());
	for (int i=0; i<partnode.getLength(); ++i)
	{
		dbgassert(partnode[i].isAggregate());

		grouptothreads.push_back(vector<unsigned short>());

		for (int j=0; j<partnode[i].getLength(); ++j)
		{
			int tid = partnode[i][j];
			grouptothreads.at(i).push_back(tid);
		}
	}

	// Compute max size of staging area used for sorting inputs/outputs.
	//
	unsigned long maxbuildtuples;
	if (node.exists("maxbuildtuplesinM"))
	{
		maxbuildtuples = node["maxbuildtuplesinM"];
		maxbuildtuples *= 1024 * 1024;
	}
	else
	{
		maxbuildtuples = node["maxbuildtuples"];
	}

	unsigned long maxprobetuples;
	if (node.exists("maxprobetuplesinM"))
	{
		maxprobetuples = node["maxprobetuplesinM"];
		maxprobetuples *= 1024 * 1024;
	}
	else
	{
		maxprobetuples = node["maxprobetuples"];
	}

	int totalthreads = 0;
	for (unsigned int i=0; i<groupsize.size(); ++i)
		totalthreads += groupsize[i];

	// Allow for a per-thread variance of 20 buffers + 30% of input size.
	//
	perthreadbuildtuples = 20 * buffsize/buildOp->getOutSchema().getTupleSize() 
		+ (maxbuildtuples * 1.3 / totalthreads);
	perthreadprobetuples = 20 * buffsize/probeOp->getOutSchema().getTupleSize()
		+ (maxprobetuples * 1.3 / totalthreads);

	// Create comparators.
	//
	probekeylessthanbuildkey = Schema::createComparator(
			probeOp->getOutSchema(), joinattr2,
			buildOp->getOutSchema(), joinattr1,
			Comparator::Less);
	probekeyequalsbuildkey = Schema::createComparator(
			probeOp->getOutSchema(), joinattr2,
			buildOp->getOutSchema(), joinattr1,
			Comparator::Equal);
	buildkeyequalsbuildkey = Schema::createComparator(
			buildOp->getOutSchema(), joinattr1,
			buildOp->getOutSchema(), joinattr1,
			Comparator::Equal);

	// Is any input already sorted?
	//
	buildpresorted = false;
	if (node.exists("buildpresorted"))
	{
		string str = node["buildpresorted"];
		buildpresorted = (str == "yes");
	}
	
	probepresorted = false;
	if (node.exists("probepresorted"))
	{
		string str = node["probepresorted"];
		probepresorted = (str == "yes");
	}

	// Is build prepartitioned?
	//
	if (node.exists("buildprepartitioned"))
	{
		int min = node["buildprepartitioned"]["range"][0];
		int max = node["buildprepartitioned"]["range"][1];
		int buckets = node["buildprepartitioned"]["buckets"];
		prepartfn = ExactRangeValueHasher(min, max, buckets);
	}

	// Create state, output and build/probe staging areas.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		sortmergejoinstate.push_back(NULL);
		buildpage.push_back(NULL);
		probepage.push_back(NULL);
	}
}

void SortMergeJoinOp::threadInit(unsigned short threadid)
{
	void* space;

	space = numaallocate_local("SMJs", sizeof(SortMergeState), this);
	memset(space, 0xAB, sizeof(SortMergeState));
	sortmergejoinstate[threadid] = new (space) SortMergeState();

	unsigned int buildtuplesize;
	unsigned int probetuplesize;

	buildtuplesize = buildOp->getOutSchema().getTupleSize();
	probetuplesize = probeOp->getOutSchema().getTupleSize();


	space = numaallocate_local("SMJb", sizeof(Page), this);
	buildpage[threadid] = new (space) 
		Page(perthreadbuildtuples*buildtuplesize, buildtuplesize, this, "SMJb");
	
	space = numaallocate_local("SMJp", sizeof(Page), this);
	probepage[threadid] = new (space) 
		Page(perthreadprobetuples*probetuplesize, probetuplesize, this, "SMJp");
	
	space = numaallocate_local("SMJo", sizeof(Page), this);
	output[threadid] = new (space) Page(buffsize, schema.getTupleSize(), this, "SMJo");
}

void SortMergeJoinOp::threadClose(unsigned short threadid)
{
	if (sortmergejoinstate[threadid]) {
		numadeallocate(sortmergejoinstate[threadid]);
	}
	sortmergejoinstate[threadid] = NULL;

	if (buildpage[threadid]) {
		numadeallocate(buildpage[threadid]);
	}
	buildpage[threadid] = NULL;

	if (probepage[threadid]) {
		numadeallocate(probepage[threadid]);
	}
	probepage[threadid] = NULL;

	if (output[threadid]) {
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;
}

// The following functions are reused in sortandrangepartition.cpp, so 
// hiding from global namespace here.
//
namespace {	

/**
 * Copies all tuples from source operator \a op into staging area \a page.
 * Assumes operator has scan-started successfully for this threadid.
 * Error handling is non-existant, asserts if anything is not expected.
 */
void copySourceIntoPage(Operator* op, Operator::Page* page, unsigned short threadid)
{
	Operator::GetNextResultT result;
	result.first = Operator::Ready;

	while (result.first == Operator::Ready)
	{
		result = op->getNext(threadid);
		assert(result.first != Operator::Error);

		void* datastart = result.second->getTupleOffset(0);

		if (datastart == 0)
			continue;

		unsigned int datasize = result.second->getUsedSpace();
		void* space = page->allocate(datasize);
		assert(space != NULL);
		memcpy(space, datastart, datasize);
	}

	assert(result.first == Operator::Finished);
}

void verifysorted(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	void* tup1 = 0;
	void* tup2 = 0;
	Operator::Page::Iterator it = page->createIterator();
	Comparator comp = Schema::createComparator(
			schema, joinattr,
			schema, joinattr,
			Comparator::LessEqual);

	tup1 = it.next();
	if (tup1 == NULL)
		return;

	while ( (tup2 = it.next()) )
	{
		assert(comp.eval(tup1, tup2));
		tup1=tup2;
	}
}

/**
 * Sorts all tuples in given page.
 */
void sortAllInPage(Operator::Page* page, Schema& schema, unsigned int joinattr)
{
	unsigned int keyoffset = (unsigned long long)schema.calcOffset(0, joinattr);

	switch(schema.getColumnType(joinattr))
	{
		case CT_INTEGER:
			page->sort<CtInt>(keyoffset);
			break;
		case CT_LONG:
		case CT_DATE:
			page->sort<CtLong>(keyoffset);
			break;
		case CT_DECIMAL:
			page->sort<CtDecimal>(keyoffset);
			break;
		default:
			throw NotYetImplemented();
	}
}

/**
 * Returns smallest tuple index for given value. 
 * \pre Page must be sorted on \a joinattr.
 */
unsigned int findInPage(Operator::Page* page, Schema& schema, unsigned int joinattr, CtLong value)
{
#ifdef DEBUG
	verifysorted(page, schema, joinattr);
#endif
	unsigned int keyoffset = (unsigned long long)schema.calcOffset(0, joinattr);

	switch(schema.getColumnType(joinattr))
	{
		case CT_INTEGER:
			{
			CtInt val = value;
			return page->findsmallest<CtInt>(keyoffset, val);
			}
		case CT_LONG:
		case CT_DATE:
			static_assert(sizeof(CtDate) == sizeof(CtLong));
			return page->findsmallest<CtLong>(keyoffset, value);
		default:
			throw NotYetImplemented();
	}

	return 0;
}

};

void SortMergeJoinOp::BufferAndSort(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	SortMergeState* threadstate = sortmergejoinstate[threadid];

	// Copy build side chunks into staging area, buildpage[threadid].
	//
	assert(Ready == buildOp->scanStart(threadid, indexdatapage, indexdataschema));
	copySourceIntoPage(buildOp, buildpage[threadid], threadid);
	assert(Ready == buildOp->scanStop(threadid));
	threadstate->buildusedbytes = buildpage[threadid]->getUsedSpace();
	
	// Sort build side.
	//
	if (buildpresorted == false)
	{
		startTimer(&threadstate->buildsortcycles);
#ifdef BITONIC_SORT
		buildpage[threadid]->bitonicsort();
#else
		sortAllInPage(buildpage[threadid], buildOp->getOutSchema(), joinattr1);
#endif
		stopTimer(&threadstate->buildsortcycles);
	}
#ifdef DEBUG
	verifysorted(buildpage[threadid], buildOp->getOutSchema(), joinattr1);
#endif
	
	// Copy probe side chunks into staging area, probepage[threadid].
	//
	assert(Ready == probeOp->scanStart(threadid, indexdatapage, indexdataschema));
	copySourceIntoPage(probeOp, probepage[threadid], threadid);
	assert(Ready == probeOp->scanStop(threadid));
	threadstate->probeusedbytes = probepage[threadid]->getUsedSpace();

	// Sort probe side.
	//
	if (probepresorted == false)
	{
		startTimer(&threadstate->probesortcycles);
#ifdef BITONIC_SORT
		probepage[threadid]->bitonicsort();
#else
		sortAllInPage(probepage[threadid], probeOp->getOutSchema(), joinattr2);
#endif
		stopTimer(&threadstate->probesortcycles);
	}
#ifdef DEBUG
	verifysorted(probepage[threadid], probeOp->getOutSchema(), joinattr2);
#endif
}

Operator::ResultCode SortMergeJoinOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	SortMergeState* threadstate = sortmergejoinstate[threadid];

	// Buffer inputs and sort them.
	//
	BufferAndSort(threadid, indexdatapage, indexdataschema);
	
	// Wait on barrier.
	//
	const unsigned short groupno = threadgroups.at(threadid);
	barriers.at(groupno).Arrive();

	// Place iterators, and set current tuples.
	//
	startTimer(&threadstate->setitercycles);
	Page::Iterator iter(buildpage[threadid]->createIterator());
	threadstate->builditer = iter;
	threadstate->buildtup  = threadstate->builditer.next();
	threadstate->probepageidxmax = grouptothreads.at(groupno).size();

	threadstate->probetuplesread = 0;
	threadstate->probepageidx = 0;

	for (int i = 0; i < threadstate->probepageidxmax; ++i)
	{
		unsigned short destthread = grouptothreads.at(groupno).at(i);
		Page* p = probepage[destthread];

		if (prepartfn.buckets() > 1)
		{
			assert(prepartfn.buckets() == threadstate->probepageidxmax);
			dbgassert(threadid < prepartfn.buckets());

			CtLong minvalincl = prepartfn.minimumforbucket(threadid);
			CtLong maxvalexcl = prepartfn.minimumforbucket(threadid+1);
			unsigned int mintidincl = 
				findInPage(p, probeOp->getOutSchema(), joinattr2, minvalincl);
			unsigned int maxtidexcl = 
				findInPage(p, probeOp->getOutSchema(), joinattr2, maxvalexcl);
			threadstate->probetuplesread += (maxtidexcl-mintidincl);

			Page::SubrangeIterator iter(p->createSubrangeIterator(mintidincl, maxtidexcl));
			threadstate->probecuriters[i] = iter;
		}
		else
		{
			Page::SubrangeIterator iter(p->createSubrangeIterator());
			threadstate->probecuriters[i] = iter;
		}
		threadstate->probeolditers[i] = threadstate->probecuriters[i];
		threadstate->probetups[i] = threadstate->probecuriters[i].next();
	}
	stopTimer(&threadstate->setitercycles);

	return Ready;
}

Operator::GetNextResultT SortMergeJoinOp::getNext(unsigned short threadid)
{
	void* buildtup = NULL;

	Page* out = output[threadid];
	out->clear();

	SortMergeState* state = sortmergejoinstate[threadid];

	// Reposition based on stored state.
	//
	buildtup = state->buildtup;
	
	// Repeat until output buffer is filled, or build depleted.
	//
	while ( (out->canStoreTuple() == true) && (buildtup != NULL) )
	{
		bool advancebuild = true;

		// "Stand on" build tuple and iterate through probe staging areas. 
		//
		// Prefetching could help tremendously here, as the tuple from
		// probepage[i+1] can be prefetched while operating on probepage[i].
		//
		while (state->probepageidx < state->probepageidxmax)
		{
			unsigned short i = state->probepageidx;
			void* probetup = state->probetups[i];

			while ((probetup != NULL) 
					&& (probekeylessthanbuildkey.eval(probetup, buildtup)))
			{
				// Skip tuples if probe side key less than build side key. 
				//
				state->probeolditers[i] = state->probecuriters[i];
				probetup = state->probecuriters[i].next();
			}

			if ((probetup != NULL) 
					&& (probekeyequalsbuildkey.eval(probetup, buildtup)))
			{
				// If keys match, join tuples and write to the output.
				//
				void* target = out->allocateTuple();
				dbg2assert(target != NULL);
				constructOutputTuple(buildtup, probetup, target);

				// Advance probe iterator and remember state. 
				//
				state->probetups[i] = state->probecuriters[i].next();

				// This iterator now stands on a key that is either greater
				// than or equal to the build key, or we have finished with
				// this probe staging area.
				//
				dbgassert( (state->probetups[i] == NULL) 
						|| (probekeylessthanbuildkey.eval(state->probetups[i], buildtup) == false) );

				// Break to outer loop but do not advance build iterator. 
				// This will guard that there is enough space in the 
				// output buffer before continuing.
				//
				advancebuild = false;
				break;
			}
			else
			{
				// No match. Remember state for next build tuple.
				// Move on to next probe staging area.
				//
				state->probetups[i] = probetup;
				++state->probepageidx;
			}
		}

		if (advancebuild)
		{
			// All probe iterators now stand on a key that is greater than the
			// build key, or this probe staging area has been depleted.
			//
			// "Greater than" comparator has not been created, therefore we use 
			//    greater than = (NOT less than) AND (NOT equals)
			//
#ifdef DEBUG
			for (unsigned int i=0; i<state->probepageidxmax; ++i)
			{
				assert( (state->probetups[i] == NULL) 
						|| ( (probekeylessthanbuildkey.eval(state->probetups[i], buildtup) == false)
							&& (probekeyequalsbuildkey.eval(state->probetups[i], buildtup) == false) 
							) 
						);
			}
#endif

			// Advance build iterator.
			//
			void* oldbuildtup = buildtup;
			buildtup = state->builditer.next();
			state->probepageidx = 0;

			// If new key equals old key, reposition all current probe
			// iterators on the start of this key (old iterator set).
			//
			if ((buildtup != NULL) 
					&& (buildkeyequalsbuildkey.eval(oldbuildtup, buildtup)))
			{
				for (unsigned int i=0; i<state->probepageidxmax; ++i)
				{
					state->probecuriters[i] = state->probeolditers[i];
					state->probetups[i] = state->probecuriters[i].next();
				}
			}
		}
	}

	state->buildtup = buildtup;
	return make_pair(buildtup == NULL ? Operator::Finished : Operator::Ready, out);
}

Operator::ResultCode SortMergeJoinOp::scanStop(unsigned short threadid)
{
	// Wait on barrier.
	//
	const unsigned short groupno = threadgroups.at(threadid);
	barriers.at(groupno).Arrive();

	// Forget data in staging area for thread. 
	// scanStop does not free memory, this is done at threadClose.
	// 
	buildpage[threadid]->clear();
	probepage[threadid]->clear();
	
	return Ready;
}

void SortMergeJoinOp::destroy()
{
}

SortMergeJoinOp::SortMergeState::SortMergeState() 
	: buildsortcycles(0), buildusedbytes(0), 
	  probesortcycles(0), probeusedbytes(0), probetuplesread(0), 
	  setitercycles(0), buildtup(NULL), 
	  probepageidx(0), probepageidxmax(0)
{
}

/**
 * Evaluates \a projection on the two input tuples, writing result to
 * \a output. No test is done to see that the tuples match.
 * @param tupbuild The build tuple.
 * @param tupprobe The probe tuple.
 * @param output The output tuple. Caller must have preallocated enough
 * space as described by this object's \a getOutSchema().
 */
void JoinOp::constructOutputTuple(void* tupbuild, void* tupprobe, void* output)
{
	void* tupattr;
	Schema& probeschema = probeOp->getOutSchema();
	Schema& buildschema = buildOp->getOutSchema();

	// Copy each column to destination. 
	//
	for (unsigned int j=0; j<projection.size(); ++j) 
	{
		unsigned int attr = projection[j].second;
		if (projection[j].first == BuildSide)
		{
			dbg2assert(projection[j].first == BuildSide);
			dbg2assert(buildschema.getColumnType(attr) == schema.getColumnType(j));
			dbg2assert(buildschema.getColumnWidth(attr) == schema.getColumnWidth(j));
			tupattr = buildschema.calcOffset(tupbuild, attr);
		}
		else
		{
			dbg2assert(projection[j].first == ProbeSide);
			dbg2assert(probeschema.getColumnType(attr) == schema.getColumnType(j));
			dbg2assert(probeschema.getColumnWidth(attr) == schema.getColumnWidth(j));
			tupattr = probeschema.calcOffset(tupprobe, attr);
		}
		schema.writeData(output, j, tupattr);
	}
}

/**
 * Similar to SortMergeJoinOp::getNext, but joins each probe buffer sequentially.
 */
Operator::GetNextResultT OldMPSMJoinOp::getNext(unsigned short threadid)
{
	void* buildtup = NULL;

	Page* out = output[threadid];
	out->clear();

	SortMergeState* state = sortmergejoinstate[threadid];

	// Reposition based on stored state.
	//
	buildtup = state->buildtup;
	
	// Repeat until output buffer is filled, or input depleted.
	//
	while ( (out->canStoreTuple() == true) && (buildtup != NULL) )
	{
		unsigned short i = state->probepageidx;
		void* probetup = state->probetups[i];

		while ((probetup != NULL) 
				&& (probekeylessthanbuildkey.eval(probetup, buildtup)))
		{
			// Skip probe tuples if probe side key less than probe side key. 
			//
			state->probeolditers[i] = state->probecuriters[i];
			probetup = state->probecuriters[i].next();

			// Note that the saved probetups[i] state is now stale. 
			// This must be brought up-to-date at the end of this loop.
			//
		}

		if ((probetup != NULL) 
				&& (probekeyequalsbuildkey.eval(probetup, buildtup)))
		{
			// If keys match, join tuples and write to the output.
			//
			void* target = out->allocateTuple();
			dbg2assert(target != NULL);
			constructOutputTuple(buildtup, probetup, target);

			// Advance probe iterator and remember state. 
			//
			state->probetups[i] = state->probecuriters[i].next();

			// This iterator now stands on a key that is either greater
			// than or equal to the build key, or we have finished with
			// this probe staging area.
			//
			dbgassert( (state->probetups[i] == NULL) 
					|| (probekeylessthanbuildkey.eval(state->probetups[i], buildtup) == false) );

			// Break to outer loop to check that there is enough 
			// space in the output buffer before continuing.
			//
			continue;
		}

		// No match. 
		//
		// Skip build tuples if build side key less than probe side key.
		//
		// Three cases here: 
		// 1. If next build tuple has same key, probe iterator needs to be
		// repositioned, or we'll miss matching probe tuples.
		// 2. If either build or probe ares depleted, move on to the next 
		// one, or return.
		// 3. Else, write back state to make probetups[i] up-to-date, and
		// evaluate loop guard again.
		//
		void* oldbuildtup = buildtup;
		do
		{
			state->buildtup = state->builditer.next();
			buildtup = state->buildtup;
		} while ((probetup != NULL) 
				&& (buildtup != NULL)
				&& (buildkeyequalsbuildkey.eval(oldbuildtup, buildtup) == false)
				&& (buildkeylessthanprobekey.eval(buildtup, probetup))
				);

		if ((buildtup != NULL) 
				&& (buildkeyequalsbuildkey.eval(oldbuildtup, buildtup)))
		{
			// 1.
			// If new key equals old key, reposition current probe
			// iterator on the start of this key (old iterator).
			//
			state->probecuriters[i] = state->probeolditers[i];
			state->probetups[i] = state->probecuriters[i].next();
		}
		else if ((buildtup == NULL) || (probetup == NULL))
		{
			// 2.
			// Either probe or build areas are depleted, and there 
			// is no need to remember the start of the probe key range. 
			//
			if (state->probepageidx != state->probepageidxmax - 1)
			{
				// If more staging areas available, move on there.
				//
				state->probepageidx++;
				state->builditer.reset();
				state->buildtup = state->builditer.next();
			}
			else
			{
				// If no next staging area, we're done.
				//
				state->buildtup = NULL;
			}
			buildtup = state->buildtup;
		}
		else
		{
			// 3.
			// Build now stands on key that is either equal to or 
			// greater than probe key. Write back state, and check guard.
			//
			state->probetups[i] = probetup;
		}
	}

	state->buildtup = buildtup;
	return make_pair(buildtup == NULL ? Operator::Finished : Operator::Ready, out);
}

void OldMPSMJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SortMergeJoinOp::init(root, node);

	buildkeylessthanprobekey = Schema::createComparator(
			buildOp->getOutSchema(), joinattr1,
			probeOp->getOutSchema(), joinattr2,
			Comparator::Less);
}

void MPSMJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	SortMergeJoinOp::init(root, node);

	fakebuildop.schema = buildOp->getOutSchema();
	fakeprobeop.schema = probeOp->getOutSchema();
	mergejoinop.buildOp = &fakebuildop;
	mergejoinop.probeOp = &fakeprobeop;

	if (!node.exists("mostfreqbuildkeyoccurances"))
	{
		node.add("mostfreqbuildkeyoccurances", libconfig::Setting::TypeInt) = 1;
	}
	mergejoinop.init(root, node);
}

void MPSMJoinOp::threadInit(unsigned short threadid)
{
	SortMergeJoinOp::threadInit(threadid);
	void* space;

	space = numaallocate_local("PSMb", sizeof(FakeOp::FakeOpState), this);
	fakebuildop.fakeopstate[threadid] = new (space) FakeOp::FakeOpState();

	space =	numaallocate_local("PSMp", sizeof(FakeOp::FakeOpState), this);
	fakeprobeop.fakeopstate[threadid] = new (space) FakeOp::FakeOpState();

	for (unsigned int i=0; i<groupsize.at(threadgroups.at(threadid)); ++i)
	{
		fakeprobeop.fakeopstate[threadid]->input[i] = (Page*)
			numaallocate_local("PSMf", sizeof(Page), this);
	}

	mergejoinop.threadInit(threadid);
}

Operator::ResultCode MPSMJoinOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgassert(threadid < MAX_THREADS);
	SortMergeState* threadstate = sortmergejoinstate[threadid];

	this->indexdatapage = indexdatapage;
	this->indexdataschema = &indexdataschema;

	// Buffer inputs and sort them.
	//
	SortMergeJoinOp::BufferAndSort(threadid, indexdatapage, indexdataschema);

	// Wait on barrier.
	//
	const unsigned short groupno = threadgroups.at(threadid);
	barriers.at(groupno).Arrive();
	vector<unsigned short>& tids = grouptothreads.at(groupno);
	dbgassert(tids.size() == groupsize.at(groupno));

	// Create shallow pages to feed into fakebuildop, fakeprobeop
	//
	startTimer(&threadstate->setitercycles);
	threadstate->probetuplesread = 0;
	fakebuildop.fakeopstate[threadid]->maxidx = 1;
	fakebuildop.fakeopstate[threadid]->input[0] = buildpage[threadid];

	fakeprobeop.fakeopstate[threadid]->maxidx = tids.size();
	for (unsigned int i=0; i<tids.size(); ++i)
	{
		unsigned int tupsz = probeOp->getOutSchema().getTupleSize();
		unsigned short probeid = tids[i];
		Page* p = probepage[probeid];

		if (prepartfn.buckets() > 1)
		{
			assert(prepartfn.buckets() == tids.size());
			dbgassert(threadid < prepartfn.buckets());

			CtLong minvalincl = prepartfn.minimumforbucket(threadid);
			CtLong maxvalexcl = prepartfn.minimumforbucket(threadid+1);
			unsigned int mintidincl = 
				findInPage(p, probeOp->getOutSchema(), joinattr2, minvalincl);
			unsigned int maxtidexcl = 
				findInPage(p, probeOp->getOutSchema(), joinattr2, maxvalexcl);
			threadstate->probetuplesread += (maxtidexcl-mintidincl);

			void* start = p->getTupleOffset(mintidincl);
			void* end = p->getTupleOffset(maxtidexcl);
			unsigned int size;
			if (end == 0)
			{
				if (start == 0)
				{
					size = 0;
					tupsz = 0;
				} 
				else
				{
					size = p->getUsedSpace() - (((char*)start) - ((char*)p->getTupleOffset(0)));
				}
			}
			else
			{
				size = ((char*)end) - ((char*)start);
			}

			fakeprobeop.fakeopstate[threadid]->start[i] = start;
			fakeprobeop.fakeopstate[threadid]->size[i] = size;

			fakeprobeop.fakeopstate[threadid]->input[i] = 
				new (fakeprobeop.fakeopstate[threadid]->input[i]) Page(start, size, 0, tupsz);
		}
		else
		{
			void* start = p->getTupleOffset(0);
			unsigned int size = p->getUsedSpace();
			threadstate->probetuplesread += size/tupsz;
			if (size == 0)
			{
				tupsz = 0;
			}

			fakeprobeop.fakeopstate[threadid]->start[i] = start;
			fakeprobeop.fakeopstate[threadid]->size[i] = size;

			fakeprobeop.fakeopstate[threadid]->input[i] = 
				new (fakeprobeop.fakeopstate[threadid]->input[i]) Page(start, size, 0, tupsz);
		}
	}
	stopTimer(&threadstate->setitercycles);

	assert(Ready == mergejoinop.scanStart(threadid, indexdatapage, indexdataschema));
	startTimer(&fakeprobeop.fakeopstate[threadid]->counters[0]);
	return Ready;
}

Operator::GetNextResultT MPSMJoinOp::getNext(unsigned short threadid)
{
	GetNextResultT result = mergejoinop.getNext(threadid);

	// Is this fakeprobeop finished?
	if (result.first == Finished)
	{
		stopTimer(&fakeprobeop.fakeopstate[threadid]->counters[fakeprobeop.fakeopstate[threadid]->idx]);
		assert(Ready == mergejoinop.scanStop(threadid));

		// Move forward fakeprobeop for this thread, if more data available.
		//
		if (fakeprobeop.fakeopstate[threadid]->idx + 1 < fakeprobeop.fakeopstate[threadid]->maxidx)
		{
			++fakeprobeop.fakeopstate[threadid]->idx;
			result.first = Ready;
			assert(Ready == mergejoinop.scanStart(threadid, indexdatapage, *indexdataschema));
			startTimer(&fakeprobeop.fakeopstate[threadid]->counters[fakeprobeop.fakeopstate[threadid]->idx]);
		}
	}

	return result;
}

void MPSMJoinOp::threadClose(unsigned short threadid)
{
	for (unsigned int i=0; i<groupsize.at(threadgroups.at(threadid)); ++i)
	{
		if (fakeprobeop.fakeopstate[threadid]->input[i] != NULL)
			numadeallocate(fakeprobeop.fakeopstate[threadid]->input[i]);
		fakeprobeop.fakeopstate[threadid]->input[i] = NULL;
	}

	if (fakebuildop.fakeopstate[threadid] != NULL)
		numadeallocate(fakebuildop.fakeopstate[threadid]);
	fakebuildop.fakeopstate[threadid] = NULL;

	mergejoinop.threadClose(threadid);

	SortMergeJoinOp::threadClose(threadid);
}

void PresortedPrepartitionedMergeJoinOp::init(libconfig::Config& root, libconfig::Setting& node)
{
	JoinOp::init(root, node);

	/*
	 * Not true if called from within MPSM.
	 *
	// Verify that all threadgroups have exactly one thread.
	// Algorithm will not work otherwise.
	//
	for (unsigned int i=0; i<groupsize.size(); ++i)
	{
		if (groupsize.at(i) != 1)
			throw InvalidParameter();
	}
	*/

	// Parse MFBKO. 
	//
	unsigned int ihatelibconfig = node["mostfreqbuildkeyoccurances"];
	mostfreqbuildkeyoccurances = ihatelibconfig;
	
	// Create comparators.
	// 
	buildkeylessthanprobekey = Schema::createComparator(
			buildOp->getOutSchema(), joinattr1,
			probeOp->getOutSchema(), joinattr2,
			Comparator::Less);
	buildkeyequalsbuildkey = Schema::createComparator(
			buildOp->getOutSchema(), joinattr1,
			buildOp->getOutSchema(), joinattr1,
			Comparator::Equal);
	buildkeyequalsprobekey = Schema::createComparator(
			buildOp->getOutSchema(), joinattr1,
			probeOp->getOutSchema(), joinattr2,
			Comparator::Equal);

	// Create state, output and build/probe staging areas.
	//
	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		preprejoinstate.push_back(NULL);
		buildbuf.push_back(NULL);
	}
}

void PresortedPrepartitionedMergeJoinOp::threadInit(unsigned short threadid)
{
	void* space = NULL;
	unsigned int buildtupsz = 0;
	buildtupsz = buildOp->getOutSchema().getTupleSize();

	// Size build side buffer appropriately.
	//
	space = numaallocate_local("PPJb", sizeof(Page), this);
	buildbuf[threadid] = new (space) Page(
			buildtupsz*mostfreqbuildkeyoccurances, buildtupsz, this, "PPJb");

	space = numaallocate_local("PPJs", sizeof(PrePreJoinState), this);
	preprejoinstate[threadid] = new (space) PrePreJoinState();

	space = numaallocate_local("PPJo", sizeof(Page), this);
	output[threadid] = new (space) Page(buffsize, schema.getTupleSize(), this, "PPJo");
}

void PresortedPrepartitionedMergeJoinOp::threadClose(unsigned short threadid)
{
	if (preprejoinstate[threadid]) {

		numadeallocate(preprejoinstate[threadid]);
	}
	preprejoinstate[threadid] = NULL;

	if (buildbuf[threadid]) {
		numadeallocate(buildbuf[threadid]);
	}
	buildbuf[threadid] = NULL;

	if (output[threadid]) {
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;
}

Operator::ResultCode PresortedPrepartitionedMergeJoinOp::scanStop(unsigned short threadid)
{
	// Forget data in staging area and output for thread, and reset state.
	// scanStop does not free memory, this is done at threadClose.
	// 
	buildbuf[threadid]->clear();
	preprejoinstate[threadid] = new (preprejoinstate[threadid]) PrePreJoinState();
	output[threadid]->clear();
	
	assert(Ready == buildOp->scanStop(threadid));
	assert(Ready == probeOp->scanStop(threadid));

	return Ready;
}

/**
 * Advances build iterator. It is safe to call this function again after false
 * has been returned; the call is idempotent. 
 * @return False if build side has been depleted, true otherwise.
 * @post If true is returned, buildpage.getTupleOffset(buildpos) is valid.
 */
inline
bool PresortedPrepartitionedMergeJoinOp::advanceBuild(unsigned short threadid)
{
	PrePreJoinState* state = preprejoinstate[threadid];

	++state->buildpos;

	// If this buildpage is NULL or depleted, read next page. NULL buildpage
	// will appear on first call, and after entire build side has been read.
	//
	while ( (state->buildpage == NULL) 
			|| (state->buildpage->getTupleOffset(state->buildpos) == NULL) )
	{
		// Depleted entire input? If yes, return false.
		//
		if (state->builddepleted)
		{
			state->buildpage = NULL;
			return false;
		}

		// Remember new page.
		//
		GetNextResultT result = buildOp->getNext(threadid);
		assert(result.first != Operator::Error);
		state->builddepleted = (result.first == Operator::Finished);
		state->buildpage = result.second;
		state->buildpos = 0;
	}

	dbgassert(state->buildpage->getTupleOffset(state->buildpos) != NULL);
	return true;
}

/**
 * Returns pointer to tuple under build "cursor".
 */
inline 
void* PresortedPrepartitionedMergeJoinOp::readBuildTuple(unsigned short threadid)
{
	PrePreJoinState* state = preprejoinstate[threadid];
	dbgassert(state != NULL);
	dbgassert(state->buildpage != NULL);
	void* ret = state->buildpage->getTupleOffset(state->buildpos);
	dbgassert(ret != NULL);
	return ret;
}

/**
 * Advances probe iterator. It is safe to call this function again after false
 * has been returned; the call is idempotent. 
 * @return False if probe side has been depleted, true otherwise.
 */
inline
bool PresortedPrepartitionedMergeJoinOp::advanceProbe(unsigned short threadid)
{
	PrePreJoinState* state = preprejoinstate[threadid];

	++state->probepos;

	// If this probepage is NULL or depleted, read next page. NULL probepage
	// will appear on first call, and after entire probe side has been read.
	//
	while ( (state->probepage == NULL) 
			|| (state->probepage->getTupleOffset(state->probepos) == NULL) )
	{
		// Depleted entire input? If yes, return false.
		//
		if (state->probedepleted)
		{
			state->probepage = NULL;
			return false;
		}

		// Remember new page.
		//
		GetNextResultT result = probeOp->getNext(threadid);
		assert(result.first != Operator::Error);
		state->probedepleted = (result.first == Operator::Finished);
		state->probepage = result.second;
		state->probepos = 0;
	}

	dbgassert(state->probepage->getTupleOffset(state->probepos) != NULL);
	return true;
}

/**
 * Returns pointer to tuple under probe "cursor".
 */
inline 
void* PresortedPrepartitionedMergeJoinOp::readProbeTuple(unsigned short threadid)
{
	PrePreJoinState* state = preprejoinstate[threadid];
	dbgassert(state != NULL);
	dbgassert(state->probepage != NULL);
	void* ret = state->probepage->getTupleOffset(state->probepos);
	dbgassert(ret != NULL);
	return ret;
}

/**
 * Buffers all tuples that match the key the build iterator is standing on.
 * Advances build iterator accordingly.
 * @return True if more tuples to read in build, false if build side depleted.
 * @pre Build cursor is positioned over a valid tuple.
 * @post The buffer has at least one tuple.
 * @post Tuples in buffer have same join key. 
 */
inline
bool PresortedPrepartitionedMergeJoinOp::populateBuffer(unsigned short threadid)
{
	bool ret = true;
	Page* buf = buildbuf[threadid];

	dbgassert(readBuildTuple(threadid) != NULL);

	buf->clear();
	void* src = readBuildTuple(threadid);

	do
	{
		void* dest = buf->allocateTuple();
		dbgassert(dest != NULL);
		buildOp->getOutSchema().copyTuple(dest, src);

		bool hasmore = advanceBuild(threadid);
		if (!hasmore)
		{
			ret = false;
			break;		// If build depleted, stop.
		}

		src = readBuildTuple(threadid);
	} 
	while (buildkeyequalsbuildkey.eval(buf->getTupleOffset(0), src));

#ifdef DEBUG
	assert(buf->getTupleOffset(0) != NULL);
	Page::Iterator it = buf->createIterator();
	void* tup;
	while ( (tup = it.next()) )
	{
		assert(buildkeyequalsbuildkey.eval(buf->getTupleOffset(0), tup));
	}
#endif
	return ret;
}

/**
 * Advances iterators and populates build side buffer if needed.
 * @return If false: No more keys to join, buffer is empty. True otherwise.
 * @post Probe iterator join key and buffer join key match, if return is true.
 */
inline
bool PresortedPrepartitionedMergeJoinOp::advanceIteratorsAndPopulateBuffer(unsigned short threadid)
{
	PrePreJoinState* state = preprejoinstate[threadid];
	Page* buf = buildbuf[threadid];
	void* build = NULL;
	void* probe = NULL;
	void* tupinbuf = NULL;
	bool hasmore = false;
	dbgassert(buf != NULL);

	// Advance probe.
	//
	hasmore = advanceProbe(threadid);
	if (!hasmore)
	{
		goto depleted;
	}
	probe = readProbeTuple(threadid);
	dbgassert(probe != NULL);

	// Is key the same as in build buffer? If yes, return true.
	//
	tupinbuf = buf->getTupleOffset(0);
	if ((tupinbuf != NULL) && (buildkeyequalsprobekey.eval(tupinbuf, probe)))
	{
		goto exit;
	}
	
	// Else, clear buffer.
	//
	buf->clear();
	if (state->buildpage != NULL)	// Guards against first and last iteration.
	{
		build = readBuildTuple(threadid);
	}

	if (build == NULL)
	{
		hasmore = advanceBuild(threadid);
		if (!hasmore)
		{
			goto depleted;
		}
		build = readBuildTuple(threadid);
	}

	// While keys are not equal, ...
	//
	while (buildkeyequalsprobekey.eval(build, probe) == false)
	{
		// ... advance either build or probe, depending which has lower value.
		// If either depleted, stop; we're done.
		//
		if (buildkeylessthanprobekey.eval(build, probe) == true)
		{
			hasmore = advanceBuild(threadid);
			if (!hasmore)
			{
				goto depleted;
			}
			build = readBuildTuple(threadid);
		}
		else
		{
			hasmore = advanceProbe(threadid);
			if (!hasmore)
			{
				goto depleted;
			}
			probe = readProbeTuple(threadid);
		}

	}

	// Now build and probe have equal keys. Populate buffer.
	//
	populateBuffer(threadid);
	
	// Call may mark build depleted. Even in this case, there might be more
	// keys to join from the probe side, so we should always return true here.
	//
	
exit:
#ifdef DEBUG
	tupinbuf = buf->getTupleOffset(0);
	probe = readProbeTuple(threadid);
	assert(tupinbuf != NULL);
	assert(probe != NULL);
	assert(buildkeyequalsprobekey.eval(tupinbuf, probe));
#endif
	return true;

depleted:
	state->builddepleted = true;
	state->buildpage = NULL;
	state->probedepleted = true;
	state->probepage = NULL;
	buf->clear();
	return false;
}

Operator::ResultCode PresortedPrepartitionedMergeJoinOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	assert(Ready == buildOp->scanStart(threadid, indexdatapage, indexdataschema));
	assert(Ready == probeOp->scanStart(threadid, indexdatapage, indexdataschema));

	return Ready;
}

Operator::GetNextResultT PresortedPrepartitionedMergeJoinOp::getNext(unsigned short threadid)
{
	dbgassert(buildbuf[threadid] != NULL);

	PrePreJoinState* state = preprejoinstate[threadid];
	Page* out = output[threadid];
	out->clear();

	dbgassert(out->canStoreTuple());	// It better fit a single tuple after clear().

	// Repeat until output buffer is filled.
	//
	while ( (out->canStoreTuple() == true) )
	{
		void* build = buildbuf[threadid]->getTupleOffset(state->bufidx);
		++state->bufidx;

		// If buffer depleted, find next match, populate and repeat.
		//
		if (build == NULL)
		{
			state->bufidx = 0;
			bool hasmore = advanceIteratorsAndPopulateBuffer(threadid);
			if (!hasmore)
			{
				return make_pair(Operator::Finished, out);
			}
			continue;
		}
		void* probe = readProbeTuple(threadid);

		// Keys must match (post-conditon of advanceIteratorsAndPopulateBuffer), 
		// so join tuples and write to the output.
		//
		void* target = out->allocateTuple();
		dbgassert(target != NULL);
		constructOutputTuple(build, probe, target);
	}
	return make_pair(Operator::Ready, out);
}
