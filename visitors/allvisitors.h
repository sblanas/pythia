
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

#ifndef __MYALLVISITORS__
#define __MYALLVISITORS__

#include "../util/affinitizer.h"

/**
 * Visitor which differentiates nodes based on the number of inputs: scan (zero
 * inputs), one input and two input.
 */
class SimpleVisitor : public Visitor {
	public:
		virtual void simplevisit(SingleInputOp* op) = 0;
		virtual void simplevisit(DualInputOp* op) = 0;
		virtual void simplevisit(ZeroInputOp* op) = 0;

		void visit(SingleInputOp* op) { this->simplevisit(op); }
		void visit(Filter* op) { this->simplevisit(op); }
		void visit(SortLimit* op) { this->simplevisit(op); }
		void visit(ConsumeOp* op) { this->simplevisit(op); }
		void visit(PartitionOp* op) { this->simplevisit(op); }
		void visit(GenericAggregate* op) { this->simplevisit(op); }
		void visit(AggregateSum* op) { this->simplevisit(op); }
		void visit(AggregateCount* op) { this->simplevisit(op); }
		void visit(MergeOp* op) { this->simplevisit(op); }
		void visit(MapWrapper* op) { this->simplevisit(op); }
		void visit(MemSegmentWriter* op) { this->simplevisit(op); }
		void visit(CycleAccountant* op) { this->simplevisit(op); }
		void visit(Project* op) { this->simplevisit(op); }
		void visit(CallStateChecker* op) { this->simplevisit(op); }
		void visit(SchemaPrinter* op) { this->simplevisit(op); }
		void visit(TupleCountPrinter* op) { this->simplevisit(op); }
		void visit(PerfCountPrinter* op) { this->simplevisit(op); }
		void visit(CallCountPrinter* op) { this->simplevisit(op); }

		void visit(DualInputOp* op) { this->simplevisit(op); }
		void visit(JoinOp* op) { this->simplevisit(op); }
		void visit(PresortedPrepartitionedMergeJoinOp* op) { this->simplevisit(op); }
		void visit(SortMergeJoinOp* op) { this->simplevisit(op); }
		void visit(MPSMJoinOp* op) { this->simplevisit(op); }
		void visit(OldMPSMJoinOp* op) { this->simplevisit(op); }
		void visit(HashJoinOp* op) { this->simplevisit(op); }
		void visit(IndexHashJoinOp* op) { this->simplevisit(op); }

		void visit(ShuffleOp* op) { this->simplevisit(op); }

		void visit(ZeroInputOp* op) { this->simplevisit(op); }
		void visit(ScanOp* op) { this->simplevisit(op); }
		void visit(ParallelScanOp* op) { this->simplevisit(op); }
		void visit(PartitionedScanOp* op) { this->simplevisit(op); }
		void visit(IntGeneratorOp* op) { this->simplevisit(op); }
#ifdef ENABLE_HDF5
		void visit(ScanHdf5Op* op) { this->simplevisit(op); }
#ifdef ENABLE_FASTBIT
		void visit(IndexHdf5Op* op) { this->simplevisit(op); }
		void visit(RandomLookupsHdf5Op* op) { this->simplevisit(op); }
#endif
#endif
#ifdef ENABLE_FASTBIT
		void visit(FastBitScanOp* op) { this->simplevisit(op); }
#endif
};

class RecursiveDestroyVisitor : public SimpleVisitor {
	public:
		virtual void simplevisit(SingleInputOp* op);
		virtual void simplevisit(DualInputOp* op);
		virtual void simplevisit(ZeroInputOp* op);
};

class ThreadInitVisitor : public SimpleVisitor {
	public:
		ThreadInitVisitor(unsigned short tid) : threadid(tid) { }
		virtual void simplevisit(SingleInputOp* op);
		virtual void simplevisit(DualInputOp* op);
		virtual void simplevisit(ZeroInputOp* op);
		virtual void visit(MergeOp* op);
	private:
		unsigned short threadid;
};

class ThreadCloseVisitor : public SimpleVisitor {
	public:
		ThreadCloseVisitor(unsigned short tid) : threadid(tid) { }
		virtual void simplevisit(SingleInputOp* op);
		virtual void simplevisit(DualInputOp* op);
		virtual void simplevisit(ZeroInputOp* op);
		virtual void visit(MergeOp* op);
	private:
		unsigned short threadid;
};

class RecursiveFreeVisitor : public SimpleVisitor {
	public:
		RecursiveFreeVisitor() { }
		virtual void simplevisit(SingleInputOp* op);
		virtual void simplevisit(DualInputOp* op);
		virtual void simplevisit(ZeroInputOp* op);
};

class PrettyPrinterVisitor : public Visitor {
	public:
		PrettyPrinterVisitor() : identation(0) { }

		void visit(SingleInputOp* op); 
		void visit(Filter* op);
		void visit(SortLimit* op);
		void visit(GenericAggregate* op);
		void visit(AggregateSum* op);
		void visit(AggregateCount* op);
		void visit(MergeOp* op);
		void visit(MapWrapper* op);
		void visit(MemSegmentWriter* op);
		void visit(CycleAccountant* op);
		void visit(Project* op);
		void visit(CallStateChecker* op);
		void visit(SchemaPrinter* op);
		void visit(TupleCountPrinter* op);
		void visit(PerfCountPrinter* op);
		void visit(ConsumeOp* op);
		void visit(CallCountPrinter* op);
		void visit(PartitionOp* op);

		void visit(DualInputOp* op); 
		void visit(JoinOp* op); 
		void visit(PresortedPrepartitionedMergeJoinOp* op); 
		void visit(SortMergeJoinOp* op); 
		void visit(MPSMJoinOp* op); 
		void visit(OldMPSMJoinOp* op); 
		void visit(HashJoinOp* op); 
		void visit(IndexHashJoinOp* op); 

		void visit(ShuffleOp* op);

		void visit(ZeroInputOp* op); 
		void visit(ScanOp* op);
		void visit(ParallelScanOp* op);
		void visit(PartitionedScanOp* op);
#ifdef ENABLE_HDF5
		void visit(ScanHdf5Op* op);
#ifdef ENABLE_FASTBIT
		void visit(IndexHdf5Op* op);
		void visit(RandomLookupsHdf5Op* op);
#endif
#endif
#ifdef ENABLE_FASTBIT
		void visit(FastBitScanOp* op);
#endif

		void visit(IntGeneratorOp* op);

	private:
		void printSortMergeJoin(SortMergeJoinOp* op);
		void printHashJoinOp(HashJoinOp* op);
		void printHashTableStats(HashTable& ht);
		void printAffinitization(Affinitizer* op);

		void printIdent();
		int identation;
};

#endif
