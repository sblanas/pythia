
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

#ifndef __MYOPERATOR__
#define __MYOPERATOR__

#include <string>
#include <utility>
#include <pthread.h>

#include <sys/select.h>

#include "libconfig.h++"

#include "loaders/table.h"
#include "../util/buffer.h"
#include "../schema.h"
#include "../hash.h"
#include "../util/hashtable.h"
#include "../Barrier.h"
#include "../conjunctionevaluator.h"

#include "../visitors/visitor.h"
#include "../util/affinitizer.h"

#ifdef ENABLE_HDF5
#include "hdf5.h"
#endif

#ifdef ENABLE_FASTBIT
#include "ibis.h"
#endif

/** 
 * Maximum width of value to filter tuples with, in bytes. 
 * Anything bigger than that cannot be stored by Filter.
 */
static const int FILTERMAXWIDTH = 128;

/**
 * Maximum number of worker threads. 
 * Not a fundamental limitation; used to size "flat" C arrays when
 * preallocated memory is used. This is mainly to avoid the 
 * generic STL allocator used by "vector" and other STL containers.
 */
static const unsigned short MAX_THREADS = 128;

class Operator {
	public:

		Operator() 
			: buffsize(0)
#ifdef DEBUG
			, firstcaller(-1) 
#endif
		{ }

		enum ResultCode {
			Ready = 0,	// Ready for subsequent operations. Page* can be used.
			Finished,	// No more data left in source. Page* can be used.
			Error		// Error. Page* is not guaranteed to be safe.
		};

		typedef TupleBuffer Page;
		typedef std::pair<Operator::ResultCode, Page*> GetNextResultT;

		virtual ~Operator() { }

		/**
		 * Initializes operator. Called once. Tree structure (eg. nextOp,
		 * buildOp, etc.) must have been initialized before this call.
		 *
		 * After this call returns, calls to \a getOutSchema must return the
		 * schema of this operator.
		 */
		virtual void init(libconfig::Config& root, libconfig::Setting& node);

		/**
		 * Registers each thread with operator. Called once from each thread.
		 * Must not propagate call down; this is done automatically by the
		 * ThreadInitVisitor.
		 */
		virtual void threadInit(unsigned short threadid) { };

		/**
		 * Initializes a scan. Must be called before \a getNext.
		 * Must propagate call down.
		 */
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema) = 0;

		/**
		 * Gets next block of input.
		 * Once Finished is returned for a particular threadid, all subsequent
		 * calls for the same threadid must return with Finished as well.
		 * Finished returns a valid Page* to read from (which can be empty).
		 */
		virtual GetNextResultT getNext(unsigned short threadid) = 0;

		/**
		 * Terminates a scan. Signals that this thread will not call 
		 * \a getNext again, before re-initializing a scan via \a scanStart. 
		 * Must propagate call down.
		 */
		virtual ResultCode scanStop(unsigned short threadid) = 0;

		/**
		 * Signals that this threadid will not call \a getNext() any more. 
		 * Called once from each thread.
		 * Must not propagate call down; this is done automatically by the
		 * ThreadCloseVisitor.
		 */
		virtual void threadClose(unsigned short threadid) { };

		/**
		 * Destroys operator. Called once.
		 */
		virtual void destroy() { };

		/**
		 * Visitor entry point.
		 */
		virtual void accept(Visitor* v) = 0;

		virtual Schema& getOutSchema() { return schema; }

	protected:
		/**
		 * This operator's output schema.
		 */
		Schema schema;
		unsigned int buffsize;

		void dbgSetSingleThreaded(unsigned short threadid)
		{
#ifdef DEBUG
			static_assert(sizeof(long) == sizeof(void*));
			long tid = threadid;
			atomic_compare_and_swap((void**)&firstcaller, (void*)-1ll, (void*)tid);
#endif
		}

		void dbgCheckSingleThreaded(unsigned short threadid) 
		{
#ifdef DEBUG
			if (threadid != firstcaller)
				throw SingleThreadedOnly();
#endif
		}

	private:
#ifdef DEBUG
		long firstcaller;
#endif
};

class SingleInputOp : public virtual Operator {
	public:
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema)
		{ 
			return nextOp->scanStart(threadid, indexdatapage, indexdataschema); 
		}

		virtual ResultCode scanStop(unsigned short threadid) 
		{ 
			return nextOp->scanStop(threadid); 
		}


		SingleInputOp() : nextOp(0) { }
		virtual ~SingleInputOp() { }

		Operator* nextOp;
};

/**
 * Aggregation super-class.
 * 
 * Parameters:
 * \li \c field a number specifying the group by key. First attribute in tuple
 * is zero.
 * \li \c fields a vector specifying a composite group by key. First attribute
 * in tuple is zero.
 * \li \c presorted if config attribute exists, performs on-the-fly merge
 * aggreagation. Not yet implemented.
 * \li \c global if config attribute exists, performs hash-based aggregation on
 * a hash table shared by all threads.
 * \li \c threads (mandatory if "global" is set) specifies number of threads to
 * synchronize with on barriers.
 */
class GenericAggregate : public virtual SingleInputOp {
	public:
		friend class PrettyPrinterVisitor;

		GenericAggregate() 
			: aggregationmode(Unset), threads(0)
		{}
		virtual ~GenericAggregate() { }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);

		/**
		 * Scan is started and stopped inside \a scanStart.
		 */
		virtual ResultCode scanStop(unsigned short threadid)
		{
			return Ready;
		}

		virtual void threadClose(unsigned short threadid);
		virtual void destroy();
	
		virtual void accept(Visitor* v) { v->visit(this); }

		/**
		 * Returns reference to user-defined aggregation schema.
		 */
		virtual Schema& foldinit(libconfig::Config& root, libconfig::Setting& node) = 0;

		/**
		 * Called when a new fold starts. \a output is guaranteed to be as
		 * wide as the user-defined schema returned from \a foldinit.
		 */
		virtual void foldstart(void* output, void* tuple) = 0;

		/**
		 * Reads the partial result in \a partialresult and the current 
		 * \a tuple, computes user-defined function and writes result back to
		 * \a partialresult. \a partialresult is as wide as the user-defined
		 * schema returned from \a foldinit.
		 */
		virtual void fold(void* partialresult, void* tuple) = 0;

		/**
		 * Aggregates bucket utilization statistics from all hash tables, as
		 * reported by HashTable::statBuckets().
		 */
		vector<unsigned int> statAggBuckets();

		/**
		 * Aggregates hash table spill statistics from all hash tables, as
		 * reported by HashTable::statAggSpills().
		 */
		unsigned long statAggSpills();

	private:
		enum Mode 
		{
			Unset,
			OnTheFly,
			ThreadLocal,
			Global
		};

		void remember(void* tuple, HashTable::Iterator& it, unsigned short threadid);

		vector<unsigned short> aggfields;
		ConjunctionEqualsEvaluator comparator;
		TupleHasher hashfn;
		Mode aggregationmode;
		unsigned short threads;
		PThreadLockCVBarrier barrier;

		/**
		 * Either one hashtable per thread if thread-local aggregation, or 
		 * a single hashtable if global aggregation.
		 */
		vector<HashTable> hashtable;

		class State {
			public:
				State(HashTable::Iterator it)
					: iterator(it), bucket(0), startoffset(0), endoffset(0), step(0)
				{ }

				char padding1[64];
				HashTable::Iterator iterator;
				unsigned int bucket;

				unsigned int startoffset;
				unsigned int endoffset;
				unsigned int step;

				char padding2[64];
		};
		vector<State> state;

		/** Output buffers. Class owns the memory. */
		vector<Page*> output;
};

class AggregateSum : public GenericAggregate {
	public:
		friend class PrettyPrinterVisitor;

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual Schema& foldinit(libconfig::Config& root, libconfig::Setting& node);
		virtual void foldstart(void* output, void* tuple);
		virtual void fold(void* partialresult, void* tuple);

	private:
		Schema aggregateschema;
		unsigned int sumfieldno;
		Schema inschema;

};

class AggregateCount : public GenericAggregate {
	public:
		friend class PrettyPrinterVisitor;

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual Schema& foldinit(libconfig::Config& root, libconfig::Setting& node);
		virtual void foldstart(void* output, void* tuple);
		virtual void fold(void* partialresult, void* tuple);

	private:
		Schema aggregatecountschema;

};

class DualInputOp : public virtual Operator {
	public:
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual ResultCode scanStart(unsigned short threadid, 
			Page* indexdatapage, Schema& indexdataschema)
		{
			ResultCode rescode1;
			ResultCode rescode2;
			rescode1 = buildOp->scanStart(threadid, indexdatapage, indexdataschema);
			rescode2 = probeOp->scanStart(threadid, indexdatapage, indexdataschema);
			return ((rescode1==rescode2) ? rescode1 : Operator::Error);
		}

		virtual ResultCode scanStop(unsigned short threadid) 
		{
			ResultCode rescode1;
			ResultCode rescode2;
			rescode1 = buildOp->scanStop(threadid);
			rescode2 = probeOp->scanStop(threadid);
			return ((rescode1==rescode2) ? rescode1 : Operator::Error);
		}

		DualInputOp() : buildOp(0), probeOp(0) { }
		virtual ~DualInputOp() { }

		Operator* buildOp;
		Operator* probeOp;
};

class ZeroInputOp : public virtual Operator
{
	public:
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema)
		{ 
			return Ready;
		}

		virtual ResultCode scanStop(unsigned short threadid) 
		{ 
			return Ready;
		}

		virtual ~ZeroInputOp() { }
};

/**
 * Single-threaded operator for file scan.
 * Takes five parameters:
 * \li \c schema Describes the schema of the input.
 * \li \c file The input file name. The global configuration parameter \c path
 * is prepended to this string to get the filename that will be opened.
 * \li (Optional) \c filetype If this string is "text", the file will be treated as a
 * comma-separated file, with each line describing a single tuple. "text" can
 * also decode bzip2-ed files on the fly.  Default is "binary".
 * \li (Optional) \c separators If \c filetype is "text", this field specifies what characters
 * will separate the different fields in a single line. Each character in this
 * string will be treated as a separator. Default is the characters: comma,
 * '|', and tab.
 * \li (Optional) \c verbose If set, and the load operation is lengthy, a
 * progress bar will be displayed on stdout. Setting it on more than one scan
 * operators in a tree will clobber stdout with garbage.
 */
class ScanOp : public virtual ZeroInputOp 
{
	public:
		friend class PrettyPrinterVisitor;

		ScanOp()
			: parsetext(false), globparam(Table::PermuteFiles), 
				verbose(Table::SilentLoad), separators(",|\t")
		{ }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual ~ScanOp() { }

	protected:
		vector<std::string> vec_filename;
		vector<Table*> vec_tbl;
		bool parsetext;
		Table::GlobParamT globparam;
		Table::VerbosityT verbose;
		string separators;
};

/**
 * Partitioned scan operator; each threadid reads its own input table.
 * Wrapper over \a Table::readNext.
 */
class PartitionedScanOp : public ScanOp {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

		virtual void accept(Visitor* v) { v->visit(this); }

};

/**
 * Class allows multiple consumers for a file.  It accepts all configuration
 * parameters of PartitionedScanOp, with the addition of \a mapping.
 *
 * mapping := "(" thread-list ("," thread-list)* ")"
 * thread-list := thread | thread-list "," thread
 * thread := <integer representing threadid>
 *
 * \a mapping must contain as many thread-list elements as \a files (specified
 * in PartitionedScanOp).
 */
class ParallelScanOp : public PartitionedScanOp {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

		virtual void accept(Visitor* v) { v->visit(this); }

	protected:
		vector<vector<unsigned short> > vec_grouptothreadlist;
		vector<unsigned short> vec_threadtogroup;
		vector<PThreadLockCVBarrier> vec_barrier;
};

/**
 * Synchronization class: spawns more threads for the specified subtree.
 * Support for single-threaded consumer only, with threadid 0. 
 */
class MergeOp : public virtual SingleInputOp {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		
		/** 
		 * Method will return after entire subtree has been intialized for
		 * threadids from 1 to \a spawnedthr. Initializing threadid=0 is
		 * caller's responsibility.
		 */
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

		virtual void accept(Visitor* v) { v->visit(this); }

		/** Thread entry point. */
		void realentry(unsigned short threadid);

		struct ParamObj {
			MergeOp* obj;
			unsigned short threadid;
		};

	private:
		GetNextResultT realGetNext(unsigned short threadid);

		enum ProducerCommand {
			DoThreadInit,
			DoScanStart,
			DoGetNext,
			DoScanStop,
			DoThreadClose,
			DoException	//< Trigger error for sanity checking.
		};

		/**
		 * Signals an idle worker to wakeup and do \a cmd.
		 * @precondition Caller must pre-deterimne that the worker is idle and
		 * that no other threads will attempt to call this method concurrently.
		 */
		void signalIdleWorker(unsigned short threadid, ProducerCommand cmd);

		/**
		 * Call blocks until worker thread is done, and returns holding the
		 * producer lock.
		 */
		void blockUntilWorkerDoneAndGetProducerLock(unsigned short threadid);

		/**
		 * Allocates a stack at a specific NUMA node for this thread.
		 */
		void* allocateStack(const int threadid, const int stacksize);

		enum ProducerWakeUp {
			ProducerEmpty,
			ProducerBusy,
			ProducerGo,
			ProducerStop
		};

		struct ProducerInfo {
			char padding1[64];
			pthread_t threadcontext;
			ParamObj threadparams;
			pthread_mutex_t producerlock;
			pthread_cond_t producercv;
			volatile ProducerWakeUp flag; 
			volatile ProducerCommand command; 
			GetNextResultT result;
			bool finished;
			pthread_attr_t threadattr;
			void* threadstack;
			char padding2[64];
		};

		short spawnedthr;
		short remainingthr;

		pthread_mutex_t consumerlock;
		pthread_cond_t consumercv;
		short prevthread;
		volatile bool consumerwakeup;

		ProducerInfo* producerinfo;

		Affinitizer affinitizer;

		Page* indexdatapage;
		Schema* indexdataschema;
};

/**
  * Distributed exchange operator.
  * @author Willis Lang
  */
class ShuffleOp : public virtual SingleInputOp 
{
    public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
	
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void destroy();

        void produce(unsigned short threadid);

		class WillisBlock : public Page
		{
			public:
				WillisBlock(unsigned int size, unsigned int tuplesize)
					: Page(size, tuplesize, 0)
				{ }

				WillisBlock(void* data, unsigned int size, 
						void* free, unsigned int tuplesize)
					: Page(data, size, free, tuplesize)
				{ }

				inline unsigned int maxTuples() { return maxsize/tuplesize; }

				//return false if overflows, doesn't actually perform
				bool blockCopy(void * src, int len);

				//return false if overflows, doesn't actually perform
				bool blockCopy(WillisBlock * src, int len);

				//return false if overflows, doesn't actually perform, shift copies data to start of page (data) not free
				bool blockShift(WillisBlock * src, unsigned int len, int srcOffset);

				//return false if overflows, doesn't actually perform, shift copies data to start of page (data) not free
				bool blockShift(void * src, unsigned int len, int srcOffset);

				//return amount of bytes used
				inline unsigned int getFill() { return reinterpret_cast<char*>(free) - reinterpret_cast<char*>(data); }
		};

	private:
		unsigned int fieldno;	// for pretty printing only
		unsigned int pattr;	// partition attribute num (converted to starting from 0)
		TupleHasher hashfn;

		Comparator comparator;
		long long value;

		class State {
			public:
				State(WillisBlock* i, Operator::ResultCode r, unsigned int c) 
					: input(i), prevresult(r), prevoffset(c)
				{ }

				WillisBlock* input;
				Operator::ResultCode prevresult;
				unsigned int prevoffset;
		};


		/** Output buffers. Class owns the memory. */
		vector<WillisBlock*> output;

        //list of destIPs to send to
        vector<char*> destIPs;

        //list of incomingIPs to send to
        vector<char*> incomingIPs;

        //remember this for 'select'
        int lastIncomingSocket;

        char* myIP;
        //which position in destIPs
        int myDestOffset;
        //which position in incomingIPs
        int myIncomingOffset;

        int incomingBasePort;

        int * incomingSockets;
        int * destSockets;

        // output buffers for network, not threads
        vector<WillisBlock*> noutput;

        WillisBlock* nsendPage;

        // input buffers for network, not threads
        vector<WillisBlock*> ninput;


        //getnext flag
        bool producerStarted;

        //to accept connections
        fd_set sockSet;

        struct timeval selTimeout;

        //select return value
        int retval;
        //number of accepted (incoming) connections
        unsigned int num_accepted;

		/** 
		 * Remember state to resume on next call. 
		 * State is returned page, code and current tuple offset.
		 */
		vector<State> state;
};

/**
 * Generic join class.
 * No support for composite keys yet.
 *
 * Parameter \a projection :
 * projection := [ <join-attribute-proj>, <join-attribute-proj>, ... ]
 * join-attribute-proj := "<source>$<scalar>"
 * source := "B" | "P"
 * 
 * For example, if projection is [ "B$0", "P$1", "B$2" ], this means that the
 * output will have the first attribute from the build side ("B$0"), the second
 * attribute from the probe side ("P$1"), and the third attribute from the
 * build side ("B$0").
 *
 * Paramter \a buildjattr :
 * buildjattr := <scalar>
 *
 * Attribiute to join on on the build side.
 *
 * Paramter \a probejattr :
 * probejattr := <scalar>
 *
 * Attribiute to join on on the probe side.
 *
 * Paramter \a threadgroups :
 * threadgroups := [ <threadgroup>, <threadgroup>, ... ]
 * threadgroup := <list of thread ids>
 *
 * The \a threadgroups parameter specifies what other threads does a thread
 * need to wait for before continuing to the next phase of a join. For example,
 * if threadgroups = [ [1, 2], [3] ], this means that threads 1 and 2 work
 * on the same partition, and thus have to synchronize, while thread 3 works on
 * a separate partition.
 */
class JoinOp : public virtual DualInputOp {
	public:
		friend class PrettyPrinterVisitor;
		virtual ~JoinOp() { }

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);

		enum JoinSrcT { BuildSide, ProbeSide };
		typedef pair<JoinSrcT, unsigned int> JoinPrjT; //< <source, attribute> pair

	protected:
		void constructOutputTuple(void* tupbuild, void* tupprobe, void* output);

		vector<JoinPrjT> projection;
		unsigned int joinattr1, joinattr2;

		vector<unsigned short> threadgroups;  //< threadid->groupid
		vector<unsigned short> threadposingrp;//< threadid->position in group
		vector<unsigned short> groupleader;   //< groupid->leadthreadid
		vector<unsigned short> groupsize;     //< groupid->size
		vector<PThreadLockCVBarrier> barriers;//< threadid->barrier
};

/**
 * Hash join class. 
 *
 * Code will create as many hash tables as groups in threadgroups, each having
 * the parameters specified here. So, if "buckets" is 1024, and there are 4
 * thread groups, there will be 4 hash tables each having 1024 buckets, or 4096
 * buckets total.
 *
 * Parameter block \a algorithm :
 * tuplesperbucket = <size of each bucket, in tuples>
 *
 * allocpolicy = "local" | "striped"
 * If "local", hash table is local to the NUMA node of the first thread in the
 * threadgroup that calls \a threadInit.
 * If "striped", hash table will be striped. The NUMA node where each 
 * partition will reside in depends on the (optional) parameter "stripeon".
 *
 * stripeon = <list of NUMA nodes>
 * List of NUMA nodes hash table will be striped on. If "stripeon" is absent,
 * hash table will be striped across all NUMA nodes.
 */
class HashJoinOp : public JoinOp {
	public:
		friend class PrettyPrinterVisitor;

		HashJoinOp() : buildpagesize(0) { }

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

	protected:
		void constructOutputTuple(void* tupbuild, void* tupprobe, void* output);

		/**
		 * Inserts all the data items in the \a page in the hash table.
		 * @param page Page to insert from.
		 * @param groupno Hash table index to insert in.
		 */
		void buildFromPage(Page* page, unsigned short groupno);

		void* readNextTupleFromProbe(unsigned short threadid);

		vector<HashTable> hashtable;
		int buildpagesize;

		Schema sbuild;		///< join key + build projection

		struct HashJoinState {
			HashJoinState();

			char padding1[64];
			void* location;	///< Start from here.
			HashTable::Iterator htiter;	///< Current iterator on build.
			Page::Iterator pgiter;	///< Current iterator on probe.
			bool probedepleted; ///< Don't bother continuing the probe.
			char padding2[64];
		};
		vector<HashJoinState*> hashjoinstate;

		TupleHasher buildhasher;
		TupleHasher probehasher;

	private:
		vector<Page*> output;

		Comparator keycomparator;

		vector<char> allocpolicy;
};

/**
 * Class provides map-like functionality to derived classes. 
 *
 * Limitations: 
 * * Code outputs zero or one tuples per input.
 * * No side-effects allowed, ie. no accesses to variables living 
 *   outside the \a map function.
 */
class MapWrapper : public virtual SingleInputOp {
	public:
		friend class PrettyPrinterVisitor;
		virtual ~MapWrapper() { }

		void accept(Visitor* v) { v->visit(this); }

		void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

		/**
		 * Creates output schema for operator, and optionally changes the \a
		 * description parameter for pretty-printing.
		 * @param schema Output schema of this operator.
		 */
		virtual void mapinit(Schema& schema) = 0;
		virtual void map(void* tuple, Page* out, Schema& schema) = 0;

	protected:
		class State {
			public:
				State(Page* i, Operator::ResultCode r, unsigned int c) 
					: input(i), prevresult(r), prevoffset(c)
				{ }

				char padding1[64];
				Page* input;
				Operator::ResultCode prevresult;
				unsigned int prevoffset;
				char padding2[64];
		};

		vector<Page*> output;
		vector<State> state;
		string description;
};

/**
 * Sort-Merge join class. 
 *
 */
class SortMergeJoinOp : public JoinOp {
	public:
		friend class PrettyPrinterVisitor;

		SortMergeJoinOp() 
			: prepartfn(0, 0, 1) 
		{ }

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

	protected:
		struct SortMergeState {
			SortMergeState();

			char padding1[64];

			unsigned long long buildsortcycles;
			unsigned long long buildusedbytes;
			unsigned long long probesortcycles;
			unsigned long long probeusedbytes;
			unsigned long long probetuplesread;
			unsigned long long setitercycles;

			void* buildtup;		///< Current build tuple. NULL means depleted.
			Page::Iterator builditer;	///< Current iterator on build.

			unsigned short probepageidx;	///< Start from this probe page in probeiters.
			unsigned short probepageidxmax;	///< Last probe page in probeiters.
			// The next two arrays are indexed sequentially, they are not
			// indexed by threadid. 
			// For example, if this thread's threadgroup is [4, 8], the
			// slots 0 and 1 will be used, not 4 and 8.
			void* probetups[MAX_THREADS];	///< Current probe tuples. NULL means depleted.
			Page::SubrangeIterator probecuriters[MAX_THREADS];	///< Current iterators on probe side.
			/**
			 * If the build contains a second tuple with the same key, 
			 * each current iterator will have to be repositioned to the start
			 * of this key's range in each probe staging area.
			 * This set of iterators remembers that position.
			 */
			Page::SubrangeIterator probeolditers[MAX_THREADS];	///< Current iterators on probe side.

			char padding2[64];
		};
		vector<SortMergeState*> sortmergejoinstate;

		vector<vector<unsigned short> > grouptothreads;	//< groupid->vector of threadids

		vector<Page*> output;
		vector<Page*> buildpage;
		vector<Page*> probepage;

		Comparator probekeylessthanbuildkey;
		Comparator probekeyequalsbuildkey;
		Comparator buildkeyequalsbuildkey;

		unsigned long perthreadbuildtuples;
		unsigned long perthreadprobetuples;

		bool buildpresorted;
		bool probepresorted;

		ExactRangeValueHasher prepartfn;

		void BufferAndSort(unsigned short threadid,
				Page* indexdatapage, Schema& indexdataschema);
};

/**
 * Sort-merge join implementation that scans only two inputs at a time.  
 * Faithful to MPSM [1], at the cost of producing unsorted output. (The
 * output of each thread is sorted runs, not a globally sorted order.)
 *
 * [1]
 * Martina-Cezara Albutiu, Alfons Kemper, Thomas Neumann: Massively Parallel
 * Sort-Merge Joins in Main Memory Multi-Core Database Systems, VLDB 2012.
 */
class OldMPSMJoinOp : public SortMergeJoinOp
{
	public:
		friend class PrettyPrinterVisitor;

		virtual void accept(Visitor* v) { v->visit(this); }
	
		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual GetNextResultT getNext(unsigned short threadid);
	
	private:
		Comparator buildkeylessthanprobekey;
};

/**
 * This join is an optimization of SortMergeJoinOp for the case when
 * input is prepartitioned and presorted. The main advantage is that
 * there is no need to stage the entire input in a contiguous memory
 * region. Each key can now be processed sequentially, so the buffer
 * should only have enough space to hold the tuples that contain the
 * most frequently occuring join key. 
 */
class PresortedPrepartitionedMergeJoinOp : public JoinOp
{
	public:
		friend class PrettyPrinterVisitor;

		PresortedPrepartitionedMergeJoinOp() { }

		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	private:
		struct PrePreJoinState
		{
			PrePreJoinState()
				: bufidx(0),
				  buildpage(NULL), buildpos(0), builddepleted(false),
				  probepage(NULL), probepos(0), probedepleted(false)
			{ }

			char padding1[64];

			int bufidx;

			Page* buildpage;
			unsigned int buildpos;
			/** 
			 * True if Operator::Finished has been returned.
			 * Checking this is not sufficient to determine whether input has
			 * been depleted: \a buildpage and \a buildpos might still point to
			 * valid tuples in the last page.
			 */
			bool builddepleted;

			Page* probepage;
			unsigned int probepos;
			/** 
			 * True if Operator::Finished has been returned.
			 * Checking this is not sufficient to determine whether input has
			 * been depleted: \a probepage and \a probepos might still point to
			 * valid tuples in the last page.
			 */
			bool probedepleted;

			char padding2[64];
		};

		bool advanceBuild(unsigned short threadid);
		void* readBuildTuple(unsigned short threadid);
		bool advanceProbe(unsigned short threadid);
		void* readProbeTuple(unsigned short threadid);
		bool populateBuffer(unsigned short threadid);
		bool advanceIteratorsAndPopulateBuffer(unsigned short threadid);

		unsigned long long mostfreqbuildkeyoccurances; // MFBKO for short
		vector<Page*> output;
		vector<PrePreJoinState*> preprejoinstate;
		vector<Page*> buildbuf;

		Comparator buildkeylessthanprobekey;
		Comparator buildkeyequalsbuildkey;
		Comparator buildkeyequalsprobekey;
};

/*
 * THIS IS BUGGY; USE OLD IMPLEMENTATION INSTEAD.
 */
class MPSMJoinOp : public SortMergeJoinOp
{
	public:
		friend class PrettyPrinterVisitor;

		virtual void accept(Visitor* v) { v->visit(this); }
	
		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	protected:

		class FakeOp : public Operator 
		{
			public:
				FakeOp()
				{
					for (unsigned int i=0; i<MAX_THREADS; ++i)
						fakeopstate[i] = NULL;
				}

				friend class MPSMJoinOp;
				void accept(Visitor*) { }

				virtual ResultCode scanStart(unsigned short threadid,
					Page* indexdatapage, Schema& indexdataschema)
				{
					return Ready;
				}

				virtual GetNextResultT getNext(unsigned short threadid)
				{
					dbgassert(fakeopstate[threadid] != NULL);
					int idx = fakeopstate[threadid]->idx;
					Page* p = fakeopstate[threadid]->input[idx];
					assert(p != NULL);
					return make_pair(Operator::Finished, p);
				}

				virtual ResultCode scanStop(unsigned short threadid)
				{
					return Ready;
				}

				struct FakeOpState
				{
					FakeOpState()
						: idx(0), maxidx(0)
					{
						for (unsigned int i=0; i<MAX_THREADS; ++i)
						{
							input[i] = NULL;
							start[i] = NULL;
							size[i] = 0;
							counters[i] = 0;
						}
					}

					int idx;
					int maxidx;
					Page* input[MAX_THREADS];
					void* start[MAX_THREADS];
					unsigned int size[MAX_THREADS];
					unsigned long long counters[MAX_THREADS];
				};

				FakeOpState* fakeopstate[MAX_THREADS];
		};

		PresortedPrepartitionedMergeJoinOp mergejoinop;
		FakeOp fakebuildop;
		FakeOp fakeprobeop;

		Page* indexdatapage;
		Schema* indexdataschema;
};

/**
 * Keeps tuples whose that match the specified predicate.
 * Takes a string parameter \a value and \a op, of the form:
 * value := <literal>
 * op := "<" | "<=" | "=" | "==" | "<>" | "!=" | ">=" | ">"
 *
 * For example, if \a op is "<" and \a value is "5" this means that the
 * operator will only return tuples whose specified field is less than 5.
 */
class Filter : public MapWrapper {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
	
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void mapinit(Schema& schema);
		virtual void map(void* tuple, Page* out, Schema& schema);

	private:
		Comparator comparator;
		char value[FILTERMAXWIDTH];

		unsigned int fieldno;	//< for pretty printing only
		string opstr;			//< for pretty printing only
};

/**
 * Operator writes into a memory segment. It takes the following configuration
 * parameters: \a policy, \a numanodes, \a paths and \a size.
 *
 * Size is a scalar specifiying the size of each segment, in bytes.
 *
 * size := <scalar>
 *
 *
 * Policy specifies the numa affinitization policy.
 *
 * policy := "bind" | "round-robin" | "interleave"
 *
 *
 * Numanodes specifies the numa node(s) this policy is applicable to.
 *
 * numanodes := < scalar > | < list of scalars >
 *
 *
 * Paths specifies pathnames that will be used as prefixes for naming the
 * output segments.
 * 
 * paths := < string > | < list of strings >
 *
 *
 * If \a policy is \a bind, this asks the writer to bind all alocated segments
 * to a single numa node and assign the segments the common prefix specified.
 * Exactly one \a numanodes and \a paths element are expected.
 *
 * If \a policy is \a round-robin, this asks the writer to cycle through each
 * numa node and assign the segments the common prefix for that node.
 * There should be as many \a numanodes as \a paths.
 *
 * If \a policy is \a interleave, this asks the writer to interleave each OS
 * page to the specified nodes. This placement is done in a finer granularity
 * than \a size, and only a single path prefix is used. Many \a numanodes and
 * only a single \a paths prefix are expected.
 *
 */
class MemSegmentWriter : public virtual SingleInputOp 
{
	public:
		MemSegmentWriter() 
			: policy(POLICY_UNSET), counter("0000000")
		{ }

		friend class PrettyPrinterVisitor;

		virtual void accept(Visitor* v) { v->visit(this); }

		void init(libconfig::Config& root, libconfig::Setting& node);
		void threadInit(unsigned short threadid);
		GetNextResultT getNext(unsigned short threadid);
		void threadClose(unsigned short threadid);

		enum NumaPolicy
		{
			POLICY_UNSET,
			POLICY_BIND,
			POLICY_RR,
			POLICY_INTERLEAVE
		};

	private:
		NumaPolicy policy;
		vector<unsigned short> numanodes;
		vector<string> paths;
		string counter;
};

/**
 * Operator keeps track of cycles spent in scanStart(), getNext() and
 * scanStop() in its subtree, and prints the values through the
 * PrettyPrintVisitor. It doesn't take any configuration parameters.
 * The operator uses the timestamp counter in modern processors.
 *
 * Prerequisite: Caller must have been affinitized to a single execution
 * context first, otherwise numbers will be unreliable if caller gets scheduled
 * to a different logical processor.
 */
class CycleAccountant : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);

		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	private:
		struct CyclesPerOp
		{
			CyclesPerOp()
				: ScanStartCycles(0), GetNextCycles(0), ScanStopCycles(0)
			{}

			char padding1[64];
			volatile unsigned long long ScanStartCycles, GetNextCycles, ScanStopCycles;
			char padding2[64];
		};

		vector<CyclesPerOp*> cycles;
};

/**
 * Rearranges attributes in a tuple.
 *
 * Takes a parameter \a projection of the form:
 * projection := [ <attribute-proj>, <attribute-proj>, ... ]
 * attribute-proj := "$<scalar>"
 *
 * For example, if the input schema is (int, decimal, varchar) and 
 * \a projection is [ "$1", "$0" ], this means that the operator will output
 * tuples with a schema of (decimal, int).
 */
class Project : public MapWrapper {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
	
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void mapinit(Schema& schema);
		virtual void map(void* tuple, Page* out, Schema& schema);

	private:
		vector<unsigned short> projlist;
};

/**
 * Checks that the call order does not violate state contract.
 */
class CallStateChecker : public virtual SingleInputOp
{
	public:
		CallStateChecker() 
			: objstate(ObjStateUninitialized)
		{ }

		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

		// Assertion to catch that atomics operate on correct byte footprint.
		//
		static_assert(sizeof(unsigned long) == sizeof(void*));

		static const unsigned long ObjStateUninitialized = 0;
		static const unsigned long ObjStateInitialized = 1;

		static const unsigned long ThreadStateUninitialized = 0;
		static const unsigned long ThreadStateInitialized = 1;
		static const unsigned long ThreadStateScanStarted = 2;
		static const unsigned long ThreadStateGetNextReturnedFinished = 3;

	private:
		volatile unsigned long objstate;

		vector<unsigned long> threadstate;

		void atomicallyTransitionTo(const unsigned short threadid, 
				const unsigned long oldstate,
				const unsigned long newstate);
};

/**
 * Pretty-prints schema at point of insertion.
 */
class SchemaPrinter : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		inline virtual void init(libconfig::Config& root, libconfig::Setting& node)
		{
			SingleInputOp::init(root, node);
			schema = nextOp->getOutSchema();
		}
			
		inline virtual GetNextResultT getNext(unsigned short threadid)
		{
			return nextOp->getNext(threadid);
		}
};

/**
 * Pretty-prints schema at point of insertion.
 */
class CallCountPrinter : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		CallCountPrinter()
			: cntStart(0), cntNext(0), cntStop(0)
		{ }

		inline virtual void init(libconfig::Config& root, libconfig::Setting& node)
		{
			SingleInputOp::init(root, node);
			schema = nextOp->getOutSchema();
		}
			
		inline virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema)
		{
			atomic_increment(&cntStart);
			return nextOp->scanStart(threadid, indexdatapage, indexdataschema);
		}

		inline virtual GetNextResultT getNext(unsigned short threadid)
		{
			atomic_increment(&cntNext);
			return nextOp->getNext(threadid);
		}

		inline virtual ResultCode scanStop(unsigned short threadid)
		{
			atomic_increment(&cntStop);
			return nextOp->scanStop(threadid);
		}
	
	private:
		unsigned int cntStart;
		char padding1[128];
		unsigned int cntNext;
		char padding2[128];
		unsigned int cntStop;
		char padding3[128];
};

//sortlimit
//an orderby with a limit. with small limits, no need to worry about disk spill
class SortLimit : public virtual SingleInputOp  {
	public:
		friend class PrettyPrinterVisitor;

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
	
		virtual void accept(Visitor* v) { v->visit(this); }
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual void threadInit(unsigned short threadid) ;
		virtual void threadClose(unsigned short threadid) ;

    protected:
        class sortnode{
            public:
                sortnode():payload(NULL), next(NULL), prev(NULL){}
                ~sortnode(){ delete reinterpret_cast<char*>(payload);}
                void* payload;
                sortnode* next;
                sortnode* prev;
        };
        sortnode* createNode(void* tuple, int size);

	private:

        class State {
            public:
                State(Page* i, Operator::ResultCode r, unsigned int c) 
                    : input(i), prevresult(r), prevoffset(c)
                { }

				char padding1[64];
                Page* input;
                Operator::ResultCode prevresult;
                unsigned int prevoffset;
				char padding2[64];
        };

        vector<Page*> output;
        vector<State> state;

        vector<unsigned short> orderby;

        void sortHelper(void* tuple);

        //AT THE MOMENT THIS IS SINGLE THREADED
        vector<ConjunctionEvaluator> faster;
        vector< vector <Comparator::Comparison> > allpossible;
        sortnode* head;
        sortnode* last;
        int sortnodesize;
        //AT THE MOMENT THIS IS SINGLE THREADED


        bool asc;
        int limit;
};


/**
 * When pretty-printing, operator reports tuples that passed through.
 */
class TupleCountPrinter : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
			
		virtual GetNextResultT getNext(unsigned short threadid);

	private:
		vector<unsigned int> tuples;
};

/**
 * A generator class that outputs a configurable amount of (value, garbage)
 * tuples.
 *
 * Parameters:
 * sizeinmb = <literal>
 * width = <literal>
 *
 * \a tuples specifies the number of tuples each calling thread will receive.
 * \a width specifies the width (in bytes) of each tuple, which must be at least 4.
 */
class IntGeneratorOp : public virtual ZeroInputOp
{
	public:
		IntGeneratorOp()
			: tuplewidth(0), totaltuples(0)
		{ }

		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	private:
		/**
		 * Returns next tuple, or NULL if this thread has produced more than 
		 * \a totaltuples already.
		 */
		void* produceOne(unsigned short threadid);

		vector<char*> scratchspace;

		vector<Page*> output;
		vector<CtLong> producedsofar;
		unsigned short tuplewidth;
		CtLong totaltuples;
};

/**
 * Operator keeps count of performance events in scanStart(), getNext() and
 * scanStop() in its subtree, and prints the values through the
 * PrettyPrintVisitor. It doesn't take any configuration parameters.
 *
 * Prerequisite: Caller must have been affinitized to a single execution
 * context first, otherwise numbers will be unreliable if caller gets scheduled
 * to a different logical processor.
 */
class PerfCountPrinter : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);

		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);

		static const unsigned int MAX_COUNTERS = 8;

	private:
		struct EventsPerOp
		{
			EventsPerOp()
			{
				for (unsigned int i=0; i<MAX_COUNTERS; ++i)
				{
					ScanStartCnt[i] = 0;
					GetNextCnt[i] = 0;
					ScanStopCnt[i] = 0;
				}
			}

			char padding1[64];
			unsigned long long ScanStartCnt[MAX_COUNTERS];
			unsigned long long GetNextCnt[MAX_COUNTERS];
			unsigned long long ScanStopCnt[MAX_COUNTERS];
			char padding2[64];
		};

		vector<EventsPerOp> events;
};

/**
 * Operator prints statistics about bit entropy for every tuple.
 *
 * Unlike other printer operators, because data is too verbose (64*threads
 * tuples), it returns them as the operator output and not via the
 * PrettyPrinter class.
 *
 * Outputs a fixed schema of: 
 *  1. thread id (int)
 *  2. bit (int)
 *  3. times bit zero (long)
 *  4. times bit one (long)
 */
class BitEntropyPrinter : public MapWrapper
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);

		/** 
		 * Override MapWrapper::scanStart with SingleInputOp::scanStart, as 
		 * BitEntropyPrinter doesn't need any state.
		 */
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema)
		{ 
			return SingleInputOp::scanStart(threadid, indexdatapage, indexdataschema); 
		}

		virtual GetNextResultT getNext(unsigned short threadid);

		virtual void mapinit(Schema& schema);
		
		/** Never called from BitEntropyPrinter::getNext(). */
		virtual void map(void* tuple, Page* out, Schema& schema)
		{ }

	private:
		short fieldno;
};

/**
 * Operator consumes input, xor-ing every four bytes of every tuple.
 */
class ConsumeOp : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		void init(libconfig::Config& root, libconfig::Setting& node);

		void threadInit(unsigned short threadid);
		GetNextResultT getNext(unsigned short threadid);
		void threadClose(unsigned short threadid);

	protected:
		vector<Page*> vec;
};

/**
 * Opeartor adds column containing the thread id of the current worker, and
 * prepends it to the output. Useful for tracing through execution when
 * debugging.
 */
class ThreadIdPrependOp : public MapWrapper
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual GetNextResultT getNext(unsigned short threadid);

		virtual void mapinit(Schema& schema);
		
		virtual void map(unsigned short threadid, void* tuple, Page* out, Schema& schema);

		/** Never called from ThreadIdPrependOp::getNext(). */
		virtual void map(void* tuple, Page* out, Schema& schema)
		{ 
			assert(false);
		}

};

/**
 * Operator sorts and range-partitions input. Currently the output is
 * partitions of equal range within the min-max range specified, but the
 * algorithm can easily be adapted to work for arbitrary partition
 * ranges. The number of partitions is equal to the number of threads
 * specified.
 *
 * Takes parameters:
 * \li \c attr The attribute to sort and partition on, starting from 0.
 * \li \c keyrange A range of values, such as [1, 1024] that specify the
 * min-max range of keys in the input, inclusive of the values
 * specified. (That is, in this example, the smallest key is 1 and the
 * largest key is 1024.)
 * \li \c threads The number of threads participating, which is also the 
 * number of output partitions.
 * \li \c maxtuples The number of tuples of the input. This is used to size 
 * the buffer that will store the input for sorting.
 * \li \c presorted If "yes", the input will be buffered, but no sorting 
 * will happen under the assumption that the input was already sorted.
 */
class SortAndRangePartitionOp : public virtual SingleInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	protected:
		struct PartitionState {
			PartitionState();

			char padding1[64];

			/**
			 * This thread's output partition ranges.
			 */
			unsigned int idxstart[MAX_THREADS];

			unsigned long long sortcycles;
			unsigned long long usedbytes;

			char padding2[64];
		};
		vector<PartitionState*> partitionstate;

		vector<Page*> output;
		vector<Page*> input;

		PThreadLockCVBarrier barrier;

		unsigned int attribute;
		unsigned long perthreadtuples;
		unsigned short threads;
		bool presorted;

		vector<CtLong> mininclusive; ///< Minimum (inclusive) of each partition range.
		vector<CtLong> maxexclusive; ///< Maximum (exclusive) of each partition range.
};

/**
 * Operator range-partitions input. Currently the output is partitions of equal
 * range within the min-max range specified, but the algorithm can easily be
 * adapted to work for arbitrary partition ranges, if the range function supports this. 
 * The number of partitions is equal to the number of threads specified.
 *
 * Takes parameters:
 * \li \c attr The attribute to partition on, starting from 0.
 * \li \c range A range of values, such as [1, 1024] that specify the
 * min-max range of keys in the input, inclusive of the values
 * specified. (That is, in this example, the smallest key is 1 and the
 * largest key is 1024.)
 * \li \c buckets The number of of output partitions, which is also the number
 * of threads participating in the partitioning.
 * \li \c maxtuples The number of tuples of the input. This is used to size 
 * the buffer that will store the input for sorting.
 * \li \c sort If "yes", output will be sorted.
 * \li \c sortattr (Optional) Attribute to sort on, if sorting has been
 * requested, starting from 0. By default, the same as \c attr.
 */
class PartitionOp : public virtual SingleInputOp 
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	protected:
		struct PartitionState {
			PartitionState();

			char padding1[64];

			/**
			 * This thread's input histogram for partition i.
			 */
			unsigned int tuplesforpartition[MAX_THREADS];

			/**
			 * This thread's output partition location for partition i.
			 */
			unsigned int idxstart[MAX_THREADS];

			unsigned long long bufferingcycles;
			unsigned long long sortcycles;
			unsigned long long usedtuples;

			/**
			 * First tuple to be returned at next getNext for this thread.
			 */
			unsigned int outputloc;

			/**
			 * Real page returned to next operator. This is a "fake" page, it
			 * is just an overlay over data held in output[threadid] to avoid
			 * memory copying.
			 */
			Page trueoutput;

			char padding2[64];
		};
		vector<PartitionState*> partitionstate;

		vector<Page*> output;
		vector<Page*> input;

		PThreadLockCVBarrier barrier;

		unsigned int attribute;
		unsigned long perthreadtuples;

		TupleHasher hashfn;

		bool sortoutput;
		unsigned int sortattribute;
};

#ifdef ENABLE_HDF5
/**
 * Operator reads multiple 1-dimensional datasets from HDF5 file, and 
 * combines them into rows.
 *
 * Parameters:
 * \li \c file HDF5 file to read.
 * \li \c pick An array of strings that specify the datasets in the HDF5 file
 * that will be read. All datasets must be 1-dimensional and have the same
 * length.
 * \li \c totalpartitions Total partitions reading this file. If omitted,
 * defaults to 1.
 * \li \c thispartition Partition number for this scan, from 0 up to (and
 * including) totalpartitions-1. If omitted, defaults to 0.
 *
 * Currently single-threaded only.
 */
class ScanHdf5Op : public virtual ZeroInputOp 
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

	protected:
		void copySpaceIntoOutput(unsigned int i);
		unsigned int maxOutputColumnWidth();

		hid_t hdf5file;
		vector<hid_t> hdf5sets;
		vector<hid_t> hdf5space;
		hid_t memspace;

		Page* output;
		void* staging;

		unsigned long long totaltuples;
		unsigned long long sizeintup;

		unsigned int thispartition;
		unsigned int totalpartitions;

		string filename;
		vector<string> datasetnames;

	private:
		unsigned long long origoffset;
		unsigned long long nexttuple;
};

#ifdef ENABLE_FASTBIT
/**
 * Operator reads specific indexes of 1-dimensional datasets from HDF5 file,
 * and combines them into rows. It uses a FastBit index to perform the lookups
 * and discover the specific array offsets that need to be accessed.
 *
 * Parameters:
 * \li \c file HDF5 file to read.
 * \li \c pick An array of strings that specify the datasets in the HDF5 file
 * that will be read. All datasets must be 1-dimensional and have the same
 * length.
 * \li \c indexdirectory The directory containing the FastBit index.
 * \li \c indexdataset The name of the dataset to perform lookups against using
 * FastBit.
 *
 * Currently single-threaded only.
 */
class IndexHdf5Op : public ScanHdf5Op
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

	protected:
		vector<hsize_t> rowstoaccess;
		unsigned int currentoffset;

	private:
		string indexdirectory;
		string indexdataset;
};

/**
 * Random lookups in HDF5 file.
 * \li \c file HDF5 file to read.
 * \li \c pick An array of strings that specify the datasets in the HDF5 file
 * that will be read. All datasets must be 1-dimensional and have the same
 * length.
 * \li \c randomlookups Number of random lookups to perform.
 */
class RandomLookupsHdf5Op : public IndexHdf5Op
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);

	private:
		unsigned long long hdf5length;
		unsigned long long randomlookups;
};
#endif // ENABLE_FASTBIT
#endif // ENABLE_HDF5

/**
 * Hash join operator, where the probe side is an index scan.
 *
 * Parameters:
 * Same as HashJoinOp.
 */
class IndexHashJoinOp : public HashJoinOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);

	private:
		Schema idxdataschema;
		vector<Page*> idxdatapage;
};

#ifdef ENABLE_FASTBIT
/**
 * FastBit index scan operator.
 *
 * Parameters:
 * \li \c indexdirectory The directory containing the FastBit index.
 * \li \c indexdataset The name of the dataset to perform lookups against
 * during scanStart(), if operating in a semi-join fashion. May be omitted, if
 * this functionality is not desired.
 * \li \c condition A string with a filter condition to apply. May be omitted, if
 * no filtering is desired.
 * \li \c pick An array of strings that specify the datasets that will be read
 * to form the output tuple.
 * \li \c schema Description of schema.
 */
class FastBitScanOp : public ZeroInputOp
{
	public:
		friend class PrettyPrinterVisitor;
		virtual void accept(Visitor* v) { v->visit(this); }

		virtual void init(libconfig::Config& root, libconfig::Setting& node);
		virtual void threadInit(unsigned short threadid);
		virtual ResultCode scanStart(unsigned short threadid,
			Page* indexdatapage, Schema& indexdataschema);
		virtual GetNextResultT getNext(unsigned short threadid);
		virtual ResultCode scanStop(unsigned short threadid);
		virtual void threadClose(unsigned short threadid);
		virtual void destroy();

	private:
		ibis::part* ibispart;
		vector<Page*> output;
		vector<ibis::query*> ibisquery;
		vector<bool> depleted;

		vector<string> col_names;
		string conditionstr;
		string indexdirectory;
		string indexdataset;

		unsigned long long totaltuples;
		unsigned long long totalkeylookups;
};
#endif // FASTBIT

#endif // __MYOPERATOR__
