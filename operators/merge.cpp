
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

#include "operators.h"
#include "operators_priv.h"
#include "../visitors/allvisitors.h"

#include "../util/numaasserts.h"

#include <unistd.h>
#include <sys/mman.h>

#ifdef ENABLE_NUMA
#define NUMA_VERSION1_COMPATIBILITY //< v1 is widely available 
#include <numa.h>
#endif

// Enable define to turn on the tracing facility.
//
// The tracing facility records events at the global static TraceLog array.
// This simplifies allocation, reclamation and object-oriented clutter, at the
// cost of producing meaningless output if more than one MergeOps are being
// executed in parallel.
//
// #define TRACELOG

#ifdef TRACELOG
struct TraceEntry
{
	unsigned short threadid;
	union {
		const void* address;
		const char* label;
	};
};

static const unsigned long long TraceLogSize = 0x10000;
static TraceEntry TraceLog[TraceLogSize];
static volatile unsigned long TraceLogTail;

void append_to_log(const void* addrorlabel, unsigned short threadid)
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
	TraceLog[newval].address = addrorlabel;
}

#define TRACE( x ) append_to_log( x , threadid )
#define TRACEID( x, y ) append_to_log( x , y )
#else
#define TRACE( x )
#define TRACEID( x, y )
#endif

using std::make_pair;

/*
 * XXX MergeOp is NOT REENTRANT. Only one calling thread/consumer. XXX
 */

const int ThreadStackSize = 10*1024*1024;

namespace MergeOpNS {

void* threntrypoint(void* param)
{
	MergeOp* obj = ((MergeOp::ParamObj*)param)->obj;
	unsigned short threadid = ((MergeOp::ParamObj*)param)->threadid;

	obj->realentry(threadid);

	return NULL;
}

};

void MergeOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	Operator::init(root, cfg);

#ifdef TRACELOG
	memset(TraceLog, -1, sizeof(TraceEntry)*TraceLogSize);
	TraceLogTail = 0;
#endif

	schema = Schema(nextOp->getOutSchema());

	spawnedthr = (int) cfg["threads"];
	remainingthr = spawnedthr;

	affinitizer.init(cfg);

	// init
	assert(!pthread_mutex_init(&consumerlock, NULL));
	assert(!pthread_cond_init(&consumercv, NULL));
	prevthread = spawnedthr-1;
	consumerwakeup = false;
	
	// ProducerInfo init
	producerinfo = new ProducerInfo[spawnedthr];
	dbgassert(producerinfo != NULL);
	for (int i=0; i<spawnedthr; ++i) {
		assert(!pthread_mutex_init(&producerinfo[i].producerlock, NULL));
		assert(!pthread_cond_init(&producerinfo[i].producercv, NULL));
		assert(!pthread_attr_init(&producerinfo[i].threadattr));
		producerinfo[i].threadstack = NULL;
		producerinfo[i].flag = ProducerEmpty;
		producerinfo[i].command = DoException;
		producerinfo[i].result = make_pair(Error, static_cast<Page*>(NULL));
	}
}

/** 
 * Deallocates stack. Must have been allocated with allocateStack.
 */
void deallocateStack(void* stack, const int stacksize)
{
	const int pagesize = getpagesize();

	// Go one page back, unmap region, of size stacksize + 2 pages.
	//
	int ret = munmap((char*)stack - pagesize, stacksize + 2*pagesize);
	assert(ret==0);
}

void MergeOp::threadClose(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	// Signal all workers to do threadClose, wait for all and check output.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		signalIdleWorker(i, DoThreadClose);
	}

	for (int i=0; i<spawnedthr; ++i) 
	{
		blockUntilWorkerDoneAndGetProducerLock(i);

		// Inspect and act on output.
		//
#ifdef DEBUG
		assert(producerinfo[i].flag == ProducerEmpty);
		assert(producerinfo[i].result.first == Error);
		assert(producerinfo[i].result.second == NULL);
#endif
		pthread_mutex_unlock(&producerinfo[i].producerlock);
	}

	// Join all threads: First change flag to stop, then wait until threads 
	// are finished.
	int result = 0;
	for (int i=0; i<spawnedthr; ++i) 
	{
		pthread_mutex_lock(&producerinfo[i].producerlock);
		assert(producerinfo[i].flag == ProducerEmpty);
		producerinfo[i].flag = ProducerStop;
		pthread_cond_signal(&producerinfo[i].producercv);
		pthread_mutex_unlock(&producerinfo[i].producerlock);

		result |= pthread_join(producerinfo[i].threadcontext, NULL);

		producerinfo[i].flag = ProducerEmpty;
		producerinfo[i].command = DoException;
		producerinfo[i].result = make_pair(Error, static_cast<Page*>(NULL));
	}
	assert(result == 0);

	// Destroy attr objects, and stacks (if custom-allocated).
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		dbgassert(producerinfo[i].threadstack != NULL);
		deallocateStack(producerinfo[i].threadstack, ThreadStackSize);
		producerinfo[i].threadstack = NULL;
	}
}
	
void MergeOp::destroy()
{
	// destruct consumerlock, consumercv
	assert(!pthread_mutex_destroy(&consumerlock));
	assert(!pthread_cond_destroy(&consumercv));
	
	// ProducerInfo destruct
	for (int i=0; i<spawnedthr; ++i) {
		assert(!pthread_mutex_destroy(&producerinfo[i].producerlock));
		assert(!pthread_cond_destroy(&producerinfo[i].producercv));
		assert(!pthread_attr_destroy(&producerinfo[i].threadattr));
	}
	delete[] producerinfo;
}

void MergeOp::signalIdleWorker(unsigned short threadid, ProducerCommand cmd)
{
	pthread_mutex_lock(&producerinfo[threadid].producerlock);
	TRACE("Consumer issues GO to idle thread");
	assert(producerinfo[threadid].flag == ProducerEmpty);
	producerinfo[threadid].command = cmd;
	producerinfo[threadid].flag = ProducerGo;
	TRACE("Consumer pre-signal to producer");
	pthread_cond_signal(&producerinfo[threadid].producercv);
	TRACE("Consumer post-signal to producer");
	pthread_mutex_unlock(&producerinfo[threadid].producerlock);
}


void MergeOp::blockUntilWorkerDoneAndGetProducerLock(unsigned short threadid)
{
	pthread_mutex_lock(&producerinfo[threadid].producerlock);

	TRACE("Consumer is willing to wait for producer");

	// If worker thread hasn't finished, wait for it.
	//
	while (producerinfo[threadid].flag == ProducerBusy 
			|| producerinfo[threadid].flag == ProducerGo)
	{
		TRACE("Consumer finds producer hasn't finished");
		pthread_mutex_lock(&consumerlock);
		pthread_mutex_unlock(&producerinfo[threadid].producerlock);

		consumerwakeup = false;

		// Q: Why do we turn down the consumerwakeup signal, regardless of who
		//    has signalled us?
		//
		// A: MergeOp supports a single thread as a consumer, therefore we are
		//    guaranteed that:
		//    (1) Threads with smaller threadids will not produce anything
		//        until this "round" is done, so there will be no lost output.
		//        (This is possible due to the sunchronous nature of the
		//        threadInit/scanStart/scanStop/threadClose calls in MergeOp.)
		//    (2) Threads with larger threadids will be inspected next, so it
		//        is ok if we turn off their notification now.
		//

		while (consumerwakeup == false) 
		{
			TRACE("Consumer sleeps for producer");
			pthread_cond_wait(&consumercv, &consumerlock);
			TRACE("Consumer wakes up");
		}
		consumerwakeup = false;
		pthread_mutex_unlock(&consumerlock);

		// Now no locks are held. 
		// We have to check that threadid woke us up and not some other worker.
		// 
		//
		pthread_mutex_lock(&producerinfo[threadid].producerlock);
	}
	TRACE("Consumer finished waiting for producer");
}


void MergeOp::threadInit(unsigned short threadid)
{
	dbgSetSingleThreaded(threadid);
	dbgCheckSingleThreaded(threadid);

	// Allocate attr objects, and (optionally) threads' stacks.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		dbgassert(producerinfo[i].threadstack == NULL);
		producerinfo[i].threadstack = allocateStack(i, ThreadStackSize);
		dbgassert(producerinfo[i].threadstack != NULL);
		pthread_attr_setstack(&producerinfo[i].threadattr, producerinfo[i].threadstack, ThreadStackSize);
	}

	// Spawn threads.
	for (int i=0; i<spawnedthr; ++i) {
		producerinfo[i].threadparams.obj = this;
		producerinfo[i].threadparams.threadid = i;
		TRACEID("Consumer creates thread", i);
		pthread_create(&producerinfo[i].threadcontext, &producerinfo[i].threadattr, 
				MergeOpNS::threntrypoint, &producerinfo[i].threadparams);
	}

	// Signal all workers to do threadInit, wait for all and check output.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		signalIdleWorker(i, DoThreadInit);
	}

	for (int i=0; i<spawnedthr; ++i) 
	{
		blockUntilWorkerDoneAndGetProducerLock(i);

		// Inspect and act on output.
		//
#ifdef DEBUG
		TRACEID("Consumer inspects output for threadInit", i);
		assert(producerinfo[i].flag == ProducerEmpty);
		assert(producerinfo[i].result.first == Error);
		assert(producerinfo[i].result.second == NULL);
#endif
		pthread_mutex_unlock(&producerinfo[i].producerlock);
	}
}

/**
 * @return Error if one or more of the worker \a scanStart calls has returned
 * Error. Ready otherwise.
 */
Operator::ResultCode MergeOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	dbgCheckSingleThreaded(threadid);

	this->indexdatapage=indexdatapage;
	this->indexdataschema=&indexdataschema;

	ResultCode ret = Ready;

	// Signal all workers to do scanStart, wait for all and check output.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		signalIdleWorker(i, DoScanStart);
	}

	for (int i=0; i<spawnedthr; ++i) 
	{
		blockUntilWorkerDoneAndGetProducerLock(i);

		// Inspect and act on output.
		//
		TRACEID("Consumer inspects output for scanStart", i);
		assert(producerinfo[i].flag == ProducerEmpty);
		assert(producerinfo[i].result.second == NULL);
		if (ret == Ready) 
		   ret = producerinfo[i].result.first;
		producerinfo[i].finished = false;
		pthread_mutex_unlock(&producerinfo[i].producerlock);
	}

	// Signal all workers to prefetch getNext work.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		signalIdleWorker(i, DoGetNext);
	}

	return ret;
}

Operator::ResultCode MergeOp::scanStop(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	this->indexdatapage=NULL;
	this->indexdataschema=NULL;

	ResultCode ret = Ready;

	// Signal all workers to do scanStop, wait for all and check output.
	//
	for (int i=0; i<spawnedthr; ++i) 
	{
		signalIdleWorker(i, DoScanStop);
	}

	for (int i=0; i<spawnedthr; ++i) 
	{
		blockUntilWorkerDoneAndGetProducerLock(i);

		// Inspect and act on output.
		//
		TRACEID("Consumer inspects output for scanStop", i);
		assert(producerinfo[i].flag == ProducerEmpty);
		assert(producerinfo[i].result.second == NULL);
		if (ret == Ready) 
		   ret = producerinfo[i].result.first;
		pthread_mutex_unlock(&producerinfo[i].producerlock);
	}

	return ret;
}

/**
 * Reads inputs until one output block has been filled.
 */
Operator::GetNextResultT MergeOp::getNext(unsigned short threadid)
{
	dbgCheckSingleThreaded(threadid);

	TRACE("Consumer enters getNext");

	// Forget all wakeup signals so far -- we will get to all the producers.
	//
	pthread_mutex_lock(&consumerlock);
	consumerwakeup = false;
	pthread_mutex_unlock(&consumerlock);

	// Signal previous thread to continue, if flag is Empty and output has 
	// been consumed (ie. result.second == NULL).
	//
	pthread_mutex_lock(&producerinfo[prevthread].producerlock);
	if (producerinfo[prevthread].flag == ProducerEmpty
			&& producerinfo[prevthread].result.second == NULL 
			&& producerinfo[prevthread].finished == false)	
	{
		producerinfo[prevthread].command = DoGetNext;
		producerinfo[prevthread].flag = ProducerGo;
		TRACEID("Consumer pre-signal getNext to producer", prevthread);
		pthread_cond_signal(&producerinfo[prevthread].producercv);
		TRACEID("Consumer post-signal getNext to producer", prevthread);
	}
	pthread_mutex_unlock(&producerinfo[prevthread].producerlock);
	
	// Look at each individual thread for output.
	// 
	while(1) {
		for (int i=0; i<spawnedthr; ++i) {
			// For each output location: lock, check thread output, unlock.
			//
			short curtid = (prevthread + 1 + i) % spawnedthr;

			// If producer is working or has finished, leave output alone.
			//
			pthread_mutex_lock(&producerinfo[curtid].producerlock);
			TRACEID("Consumer inspects producer", curtid);
			if (producerinfo[curtid].flag != ProducerEmpty 
					|| producerinfo[curtid].finished) 
			{
				TRACEID("Consumer is working or has no more data, leave alone", curtid);
				pthread_mutex_unlock(&producerinfo[curtid].producerlock);
				continue;
			}

			// Found a valid block: remember thread for subsequent call 
			// and return block.
			//
			prevthread = curtid;
			GetNextResultT res = producerinfo[curtid].result;
			producerinfo[curtid].result.second = NULL;

			if (res.first == Ready) {
				pthread_mutex_unlock(&producerinfo[curtid].producerlock);
				TRACEID("Consumer exits getNext and returns output from producer", curtid);
				return res;

			} else if (res.first == Finished) {
				// Convert Finished to Ready if a thread has finished
				// and is not the last one. If Finished but Empty, mark 
				// entry Stopped so that subsequent checks will skip it.
				//
				producerinfo[curtid].finished = true;
				pthread_mutex_unlock(&producerinfo[curtid].producerlock);
				producerinfo[curtid].result.first = Error;	// for safety in case of stray read

				--remainingthr;
				TRACEID("Consumer marks producer finished", curtid);

				if (remainingthr != 0) {
					TRACEID("Consumer exits getNext and returns output from producer", curtid);
					return make_pair(Ready, res.second);
				}

				TRACEID("Consumer exits getNext, signals Finished, and returns output from producer", curtid);
				return res;

			} else {
				// Error was returned. Stop thread and propagate error.
				//
				assert(res.first == Error);
				producerinfo[curtid].finished = true;
				pthread_mutex_unlock(&producerinfo[curtid].producerlock);
				--remainingthr;
				TRACEID("Consumer exits getNext with error from producer", curtid);
				return res;

			}

		}

		// Otherwise, sleep and wait for signal.
		//
		pthread_mutex_lock(&consumerlock);
		while(consumerwakeup == false) {
			TRACE("Consumer sleeps");
			pthread_cond_wait(&consumercv, &consumerlock);
			TRACE("Consumer wakeup");
		}

		// We'll repeat an entire round, so forget all wakeup signals.
		//
		consumerwakeup = false;
		pthread_mutex_unlock(&consumerlock);
	} 

	return make_pair(Error, static_cast<Page*>(NULL));
}

void MergeOp::realentry(unsigned short threadid)
{
	ProducerInfo& pi = producerinfo[threadid];

	ThreadInitVisitor initvisitor(threadid);
	ThreadCloseVisitor closevisitor(threadid);

	GetNextResultT result;

	if (affinitizer.mapping.at(threadid).core != Affinitizer::Binding::InvalidBinding)
		affinitizer.affinitize(threadid);

	pthread_mutex_lock(&pi.producerlock);
	do {
		TRACE("Producer wakeup");
		// if flag is Go, reset it and proceed
		if (pi.flag == ProducerGo) {
			dbgassert(pi.result.second == NULL);	// not overwriting output
			pi.flag = ProducerBusy;

		// if flag is Stop, return
		} else if (pi.flag == ProducerStop) {
			break;

		// if flag is Empty, it was a spurious wakeup, sleep again
		} else {
			assert(pi.flag != ProducerBusy);
			TRACE("Producer sleep (spurious wakeup)");
			continue;
		}

		// Do work.
		//
		assert(pi.flag == ProducerBusy);
		pthread_mutex_unlock(&pi.producerlock);
		switch(pi.command)
		{
			case DoThreadInit:
				TRACE("Producer calls threadInit");
				nextOp->accept(&initvisitor);
				TRACE("Producer threadInit returns");
				result.first = Error;
				result.second = NULL;
				break;

			case DoScanStart:
				TRACE("Producer calls scanStart");
				result.first = nextOp->scanStart(threadid, indexdatapage, *indexdataschema);
				TRACE("Producer scanStart returns");
				result.second = NULL;
				break;

			case DoGetNext:
				TRACE("Producer calls getNext");
				result = nextOp->getNext(threadid);
				TRACE("Producer getNext returns");
				break;

			case DoScanStop:
				TRACE("Producer calls scanStop");
				result.first = nextOp->scanStop(threadid);
				TRACE("Producer scanStop returns");
				result.second = NULL;
				break;

			case DoThreadClose:
				TRACE("Producer calls threadClose");
				nextOp->accept(&closevisitor);
				TRACE("Producer threadClose returns");
				result.first = Error;
				result.second = NULL;
				break;

			case DoException:
			default:
				TRACE("Producer throws exception");
				throw UnknownCommand();
		}
		pthread_mutex_lock(&pi.producerlock);
	
		// write result
		pi.result = result;

		TRACE("Producer pre-signal consumer");

		// signal blocked consumer (if any)
		pthread_mutex_lock(&consumerlock);
		consumerwakeup = true;
		pthread_cond_signal(&consumercv);
		pthread_mutex_unlock(&consumerlock);

		TRACE("Producer post-signal consumer");

		if (pi.flag == ProducerStop) {
			break;
		} else {
			pi.flag = ProducerEmpty;
		}

		TRACE("Producer sleep");

	} while( !pthread_cond_wait(&pi.producercv, &pi.producerlock) );

	pthread_mutex_unlock(&pi.producerlock);
}

/**
 * Allocates a stack of size \a stacksize from numa node \a numanode, and
 * returns a pointer to the lowest (closest to zero) address. 
 * Internally, the function "protects" the stack with two non-readable,
 * non-writeable pages, one on each boundary.
 */
void* allocateStackOnNode(const int numanode, const int stacksize)
{
	const int pagesize = getpagesize();
	int allocsize = stacksize + 2*pagesize;

	// Allocate size + 2 pages from specific numa node.
	//
	void* space = mmap(NULL, allocsize, PROT_READ | PROT_WRITE, 
			MAP_GROWSDOWN | MAP_STACK | MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
	assert(space != MAP_FAILED);

#ifdef ENABLE_NUMA
	if (numanode != Affinitizer::Binding::InvalidBinding)
	{
		// Specify particular numa policy.
		//
		numa_set_strict(1);
		numa_set_bind_policy(1);
		numa_tonode_memory(space, allocsize, numanode);
	}
#endif

	// Change protection in first & last pages.
	//
	int ret = -1;
	ret = mprotect(space, pagesize, PROT_NONE);
	assert(ret==0);
	ret = mprotect((char*)space + stacksize + pagesize, pagesize, PROT_NONE);
	assert(ret==0);

	// Write to prefault.
	//
	memset((char*)space + pagesize, 0xAC, stacksize);

	// Return writeable region.
	//
	return (char*)space + pagesize;
}

void* MergeOp::allocateStack(const int threadid, const int stacksize)
{
	// Find mapping for node, and call allocateStackOnNode
	//
	int numa = affinitizer.mapping.at(threadid).numa;
	void* ret = allocateStackOnNode(numa, stacksize);
	if (numa != Affinitizer::Binding::InvalidBinding)
	{
		// Assert first and last byte are affinitized correctly.
		//
		assertaddressonnuma(ret, numa);
		assertaddressonnuma((char*)ret+stacksize-1, numa);
	}
	return ret;
}
