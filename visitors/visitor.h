
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

#ifndef __MYVISITOR__
#define __MYVISITOR__

class SingleInputOp;
class Filter;
class GenericAggregate;
class AggregateSum;
class AggregateCount;
class MergeOp;
class MapWrapper;
class MemSegmentWriter;
class CycleAccountant;
class Project;
class CallStateChecker;
class SchemaPrinter;
class TupleCountPrinter;
class PerfCountPrinter;
class CallCountPrinter;
class ConsumeOp;
class PartitionOp;

class DualInputOp;
class JoinOp;
class PresortedPrepartitionedMergeJoinOp;
class SortMergeJoinOp;
class MPSMJoinOp;
class OldMPSMJoinOp;
class HashJoinOp;
class IndexHashJoinOp;

class ShuffleOp;
class SortLimit;

class ZeroInputOp;
class ScanOp;
class ParallelScanOp;
class PartitionedScanOp;
class IntGeneratorOp;
#ifdef ENABLE_HDF5 
class ScanHdf5Op;
#ifdef ENABLE_FASTBIT
class IndexHdf5Op;
class RandomLookupsHdf5Op;
#endif
#endif
#ifdef ENABLE_FASTBIT
class FastBitScanOp;
#endif

class Visitor {
	public:
		virtual void visit(SingleInputOp* op) = 0;
		virtual void visit(Filter* op) = 0;
		virtual void visit(SortLimit* op) = 0;
		virtual void visit(GenericAggregate* op) = 0;
		virtual void visit(AggregateSum* op) = 0;
		virtual void visit(AggregateCount* op) = 0;
		virtual void visit(MergeOp* op) = 0;
		virtual void visit(MapWrapper* op) = 0;
		virtual void visit(MemSegmentWriter* op) = 0;
		virtual void visit(CycleAccountant* op) = 0;
		virtual void visit(Project* op) = 0;
		virtual void visit(CallStateChecker* op) = 0;
		virtual void visit(SchemaPrinter* op) = 0;
		virtual void visit(TupleCountPrinter* op) = 0;
		virtual void visit(PerfCountPrinter* op) = 0;
		virtual void visit(CallCountPrinter* op) = 0;
		virtual void visit(ConsumeOp* op) = 0;
		virtual void visit(PartitionOp* op) = 0;

		virtual void visit(DualInputOp* op) = 0;
		virtual void visit(JoinOp* op) = 0;
		virtual void visit(PresortedPrepartitionedMergeJoinOp* op) = 0;
		virtual void visit(SortMergeJoinOp* op) = 0;
		virtual void visit(MPSMJoinOp* op) = 0;
		virtual void visit(OldMPSMJoinOp* op) = 0;
		virtual void visit(HashJoinOp* op) = 0;
		virtual void visit(IndexHashJoinOp* op) = 0;

		virtual void visit(ShuffleOp* op) = 0;

		virtual void visit(ZeroInputOp* op) = 0;
		virtual void visit(ScanOp* op) = 0;
		virtual void visit(ParallelScanOp* op) = 0;
		virtual void visit(PartitionedScanOp* op) = 0;
		virtual void visit(IntGeneratorOp* op) = 0;
#ifdef ENABLE_HDF5
		virtual void visit(ScanHdf5Op* op) = 0;
#ifdef ENABLE_FASTBIT
		virtual void visit(IndexHdf5Op* op) = 0;
		virtual void visit(RandomLookupsHdf5Op* op) = 0;
#endif
#endif
#ifdef ENABLE_FASTBIT
		virtual void visit(FastBitScanOp* op) = 0;
#endif

		virtual ~Visitor() { }
};

#endif
