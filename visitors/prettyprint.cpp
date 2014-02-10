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

#include "../operators/operators.h"
#include "../operators/operators_priv.h"
#include "allvisitors.h"
#include "../util/numaasserts.h"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <cmath>
using namespace std;

/**
 * Add commas every three characters, counting from the end. This adds
 * thousands seperators if \a input is a number.
 */
string addcommas_str(const string& input)
{
    string ret;
    ostringstream ss;

    int count = 0;

    for (string::const_reverse_iterator it = input.rbegin(); it != input.rend(); ++it, ++count)
    {
        if (count == 3)
        {
            ss << ",";
            count = 0;
        }

        ss << *it;
    }

    string ss_str = ss.str();

    for (string::const_reverse_iterator it = ss_str.rbegin(); it != ss_str.rend(); ++it)
    {
        ret += *it;
    }
    return ret;
}

template <typename T>
string addcommas(const T& input)
{
	ostringstream ss;
	ss << input;
	return addcommas_str(ss.str());
}


template <typename T> 
T op_addone(const T v) 
{ 
	return v+1; 
}

template <typename T>
string printvec(const vector<T>& str, const int width = -1) 
{
	ostringstream oss;
	oss << "[";
	if (str.size() != 0)
	{
		if (width != -1)
		{
			oss.width(width);
			oss.fill('0');
		}
		oss << str.at(0);
		for (unsigned int idx=1; idx<str.size(); ++idx)
		{
			oss << ", ";
			if (width != -1)
			{
				oss.width(width);
				oss.fill('0');
			}
			oss << str.at(idx);
		}
	}
	oss << "]";
	return oss.str();
}

template <>
string printvec<char>(const vector<char>& str, const int width)
{
	vector<unsigned int> vec(str.begin(), str.end());
	return printvec(vec, width);
}

template <typename T>
string printvecaddone(const vector<T>& str, const int width = -1) 
{
	vector<T> plusone;
	plusone.resize(str.size());
	transform(str.begin(), str.end(), 
			plusone.begin(), op_addone<T>);
	return printvec(plusone, width);
}

void printSchema(Schema& s)
{
	for (unsigned int i=0; i<s.columns(); ++i)
	{
		ColumnSpec spec = s.get(i);
		switch (spec.type)
		{
			case CT_INTEGER: 
				cout << "int";
				break;
			case CT_LONG: 
				cout << "long";
				break;
			case CT_DECIMAL:
				cout << "decimal";
				break;
			case CT_CHAR:
				cout << "char(" << spec.size << ")";
				break;
			case CT_DATE:
				cout << "date(" << spec.formatstr << ")";
				break;
			case CT_POINTER:
				cout << "pointer";
				break;
			default:
				cout << "???";
		}
		cout << ", ";
	}
	if (s.columns() != 0)
		cout << "\b\b";
}

void PrettyPrinterVisitor::printIdent() {
	for (int i=0; i<identation; ++i)
		cout << '\t';
}

void PrettyPrinterVisitor::visit(SingleInputOp* op) {
	printIdent();
	cout << "UNKNOWN SINGLE INPUT" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(Filter* op) {
	printIdent();
	ColumnSpec cs = op->getOutSchema().get(op->fieldno);
	Schema dummyschema;
	dummyschema.add(cs);
	cout << "Filter (" 
		<< "fieldno=" << op->fieldno + 1
		<< ", " << "predicate=\"" << op->opstr << " " 
					<< dummyschema.prettyprint(op->value, ',') << "\"" 
		<< ")" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(GenericAggregate* op) {
	printIdent();
	cout << "UNKNOWN AGGREGATION ("
		<< "agg-fields=" << printvecaddone(op->aggfields) 
		<< ")" << endl; 

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->hashtable.at(i).nbuckets == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		printHashTableStats(op->hashtable[i]);
	}

	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(AggregateSum* op) {
	printIdent();
	cout << "AggregateSum ("
		<< "agg-fields=" << printvecaddone(op->aggfields) << ", "
		<< "sumonfield=" << op->sumfieldno + 1 << ")" << endl;

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->hashtable.at(i).nbuckets == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		printHashTableStats(op->hashtable[i]);
	}

	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(ShuffleOp* op) {
	printIdent();
	cout << "Shuffle (fieldno=" << op->fieldno + 1
		<< ", value=" << op->value << ")" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(AggregateCount* op) {
	printIdent();
	cout << "AggregateCount ("
		<< "agg-fields=" << printvecaddone(op->aggfields) 
		<< ")" << endl; 

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->hashtable.at(i).nbuckets == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		printHashTableStats(op->hashtable[i]);
	}

	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(DualInputOp* op) {
	printIdent();
	cout << "UNKNOWN DUAL INPUT" << endl;
	identation++;
	op->probeOp->accept(this);
	identation--;
	op->buildOp->accept(this);
}

void PrettyPrinterVisitor::printHashTableStats(HashTable& ht)
{
	cout << "HashTable (";
	cout << "buckets=" << addcommas(ht.nbuckets);
	cout << ", ";
	cout << "bucketsize=" << ht.bucksize / ht.tuplesize << " tuples";
	cout << ", ";
	cout << "spills=" << ht.statSpills();
	cout << ")" << endl;

	vector<unsigned int> v = ht.statBuckets();
	for (unsigned int i=0; i<v.size(); ++i)
	{
		if (v[i] == 0)
			continue;
		printIdent();
		cout << ". " << setfill(' ') << setw(12) << addcommas(v[i]);
		cout << " buckets have " << setw(3) << i << " tuples." << endl;
	}
}

void printJoinProjection(const vector<JoinOp::JoinPrjT>& prj)
{
	for (unsigned int i=0; i<prj.size(); ++i)
	{
		switch (prj[i].first)
		{
			case JoinOp::BuildSide:
				cout << "B";
				break;
			case JoinOp::ProbeSide:
				cout << "P";
				break;
			default:
				cout << "?";
		}
		cout << "$" << prj[i].second + 1 << ", ";
	}
	if (prj.size() != 0)
		cout << "\b\b";
}

void PrettyPrinterVisitor::visit(PresortedPrepartitionedMergeJoinOp* op) 
{
	printIdent();
	cout << "PresortedPrepartitionedMergeJoin (";

	cout << "on B$" << op->joinattr1 + 1 << "=P$" << op->joinattr2 + 1;
	cout << ", ";

	cout << "project=[";
	printJoinProjection(op->projection);
	cout << "], ";
	cout << "mostfreqbuildkeyoccurances=" << op->mostfreqbuildkeyoccurances;
	cout << ")" << endl;

	for (unsigned int i=0; i<op->barriers.size(); ++i)
	{
		printIdent();
		cout << ". ThreadGroup " << i << ": ["; 
		bool printedsomething=false;
		for (unsigned int j=0; j<op->threadgroups.size(); ++j)
		{
			if (op->threadgroups[j] != i)
				continue;

			cout << setw(2) << setfill('0') << j << ", ";
			printedsomething=true;
		}
		if (printedsomething)
			cout << "\b\b";
		cout << "]" << endl;
	}

	identation++;
	printIdent();
	cout << "Build" << endl;
	op->buildOp->accept(this);
	identation--;
	op->probeOp->accept(this);
}

void PrettyPrinterVisitor::visit(JoinOp* op) {
	printIdent();
	cout << "Join (";

	cout << "on B$" << op->joinattr1 + 1 << "=P$" << op->joinattr2 + 1;
	cout << ", ";

	cout << "project=[";
	printJoinProjection(op->projection);
	cout << "])" << endl;

	for (unsigned int i=0; i<op->barriers.size(); ++i)
	{
		printIdent();
		cout << ". ThreadGroup " << i << ": ["; 
		bool printedsomething=false;
		for (unsigned int j=0; j<op->threadgroups.size(); ++j)
		{
			if (op->threadgroups[j] != i)
				continue;

			cout << setw(2) << setfill('0') << j << ", ";
			printedsomething=true;
		}
		if (printedsomething)
			cout << "\b\b";
		cout << "]" << endl;
	}

	identation++;
	op->buildOp->accept(this);
	identation--;
	op->probeOp->accept(this);
}

void PrettyPrinterVisitor::visit(SortMergeJoinOp* op) 
{
	printIdent();
	cout << "SortMergeJoin (";
	printSortMergeJoin(op);
}

void PrettyPrinterVisitor::visit(OldMPSMJoinOp* op) 
{
	printIdent();
	cout << "MPSMJoin (";
	printSortMergeJoin(op);
}

void PrettyPrinterVisitor::visit(MPSMJoinOp* op) 
{
	printIdent();
	cout << "BuggyMPSMJoin (" << endl;
	for (unsigned int i=0; i<MAX_THREADS; ++i)
	{
		if (op->fakeprobeop.fakeopstate[i] == NULL)
			continue;
		
		printIdent();
		cout << ". #" << i << endl;

		for (unsigned int j=0; j<MAX_THREADS; ++j)
		{
			if (op->fakeprobeop.fakeopstate[i]->counters[j]==0) 
				continue;

			printIdent();
			cout << ".   . "
				<< setw(12) << fixed << setprecision(2) << setfill(' ') 
					<< (op->fakeprobeop.fakeopstate[i]->counters[j]) / 1000. / 1000.
				<< " mil cycles, start at "
				<< setw(16) << setfill(' ') << hex 
					<< op->fakeprobeop.fakeopstate[i]->start[j]
				<< " at NUMA " << dec 
					<< numafromaddress(op->fakeprobeop.fakeopstate[i]->start[j])
				<< ", process " 
				<< setw(12) << fixed << setfill(' ') << dec
					<< op->fakeprobeop.fakeopstate[i]->size[j] 
				<< " bytes" << endl;
		}
	}
	
	printIdent();
	cout << "MPSMJoin (";
	printSortMergeJoin(op);
}

void PrettyPrinterVisitor::printSortMergeJoin(SortMergeJoinOp* op) 
{
	cout << "on B$" << op->joinattr1 + 1 << "=P$" << op->joinattr2 + 1;
	cout << ", ";

	if (op->prepartfn.buckets() > 1)
	{
		cout << "build prepartitioned, ";
	}
	else
	{
		cout << "build not prepartitioned, ";
	}

	if (op->buildpresorted)
	{
		cout << "build presorted, ";
	}
	else
	{
		cout << "sort build, ";
	}

	if (op->probepresorted)
	{
		cout << "probe presorted, ";
	}
	else
	{
		cout << "sort probe, ";
	}


	cout << "project=[";
	printJoinProjection(op->projection);
	cout << "])" << endl;

	for (unsigned int i=0; i<op->barriers.size(); ++i)
	{
		printIdent();
		cout << ". ThreadGroup " << i << ": ["; 
		bool printedsomething=false;
		for (unsigned int j=0; j<op->threadgroups.size(); ++j)
		{
			if (op->threadgroups[j] != i)
				continue;

			cout << setw(2) << setfill('0') << j << ", ";
			printedsomething=true;
		}
		if (printedsomething)
			cout << "\b\b";
		cout << "]" << endl;
	}

	if (op->prepartfn.buckets() > 1)
	{
		unsigned int threads = op->prepartfn.buckets();
		unsigned int rangemaxchar = 1 +
			lrint(log10(op->prepartfn.minimumforbucket(threads)-1));
		for (unsigned int i=0; i<threads; ++i)
		{
			printIdent();
			cout << ". Thread " << setw(2) << setfill('0') << i << ": " 
				<< "Join key range [" 
				<< setw(rangemaxchar) << setfill(' ') 
					<< op->prepartfn.minimumforbucket(i) << "-" 
				<< setw(rangemaxchar) << setfill(' ') 
					<< op->prepartfn.minimumforbucket(i+1) - 1 
				<< "]";
			if (op->sortmergejoinstate[i] != 0)
			{
				cout << ", setting iterators for "
					<< setw(12) << fixed << setprecision(2) << setfill(' ') 
						<< (op->sortmergejoinstate[i]->setitercycles) / 1000. / 1000.
					<< " cycles";
				cout << ", read "
					<< setw(13) << setfill(' ') 
						<< addcommas(op->sortmergejoinstate[i]->probetuplesread)
					<< " probe tuples";
			}
			cout << endl;
		}
	}

	identation++;
	printIdent();
	cout << "Build" << endl;
	if (op->buildpresorted == false)
	{
		printIdent();
		cout << "Sort (attribute=" << op->joinattr1 + 1 << ")" << endl;
	}

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->sortmergejoinstate[i] == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< (op->sortmergejoinstate[i]->buildsortcycles) / 1000. / 1000.
			<< " mil cycles to sort "
			<< setw(15) << setfill(' ') 
			<< addcommas(op->sortmergejoinstate[i]->buildusedbytes) 
			<< " bytes" << endl;
	}
	op->buildOp->accept(this);

	identation--;
	if (op->probepresorted == false)
	{
		printIdent();
		cout << "Sort (attribute=" << op->joinattr2 + 1 << ")" << endl;
	}

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->sortmergejoinstate[i] == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< (op->sortmergejoinstate[i]->probesortcycles) / 1000. / 1000.
			<< " mil cycles to sort "
			<< setw(15) << setfill(' ') 
			<< addcommas(op->sortmergejoinstate[i]->probeusedbytes) 
			<< " bytes" << endl;
	}
	op->probeOp->accept(this);
}

void PrettyPrinterVisitor::printHashJoinOp(HashJoinOp* op)
{
	cout << "on B$" << op->joinattr1 + 1 << "=P$" << op->joinattr2 + 1;
	cout << ", ";

	cout << "project=[";
	printJoinProjection(op->projection);
	cout << "])" << endl;

	for (unsigned int i=0; i<op->barriers.size(); ++i)
	{
		printIdent();
		cout << ". ThreadGroup " << i << ": ["; 
		bool printedsomething=false;
		for (unsigned int j=0; j<op->threadgroups.size(); ++j)
		{
			if (op->threadgroups[j] != i)
				continue;

			cout << setw(2) << setfill('0') << j << ", ";
			printedsomething=true;
		}
		if (printedsomething)
			cout << "\b\b";
		cout << "]" << endl;
	}

	identation++;
	printIdent();
	cout << "Build (allocon="; 
	if (op->allocpolicy.empty())
		cout << "local";
	else
		cout << printvec(op->allocpolicy);
	cout << ")" << endl;
	for (unsigned int i=0; i<op->groupleader.size(); ++i)
	{
		if (op->hashtable.at(i).nbuckets == 0)
			continue;

		printIdent();
		cout << ". Group " << setw(2) << setfill('0') << i << ": ";
		printHashTableStats(op->hashtable[i]);
	}
	op->buildOp->accept(this);

	identation--;
}

void PrettyPrinterVisitor::visit(HashJoinOp* op) 
{
	printIdent();
	cout << "HashJoin (";

	printHashJoinOp(op);

	printIdent();
	cout << "Probe" << endl;
	op->probeOp->accept(this);
}

void PrettyPrinterVisitor::visit(IndexHashJoinOp* op) 
{
	printIdent();
	cout << "HashJoin (";

	printHashJoinOp(op);

	printIdent();
	cout << "IndexedProbe" << endl;
	op->probeOp->accept(this);
}
void PrettyPrinterVisitor::visit(ScanOp* op) {
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Scan (";
	cout << "filetype=" << (op->parsetext ? "text" : "binary");

	cout << ", ";
	if (op->globparam == Table::SortFiles)
	{
		cout << "sort";
	}
	else
	{
		assert(op->globparam == Table::PermuteFiles);
		cout << "permute";
	}
	cout << " filenames";

	if (op->parsetext)
	{
		cout << ", ";
		cout << "separators=\"" << op->separators << "\"";
	}

	if (op->verbose==Table::VerboseLoad)
	{
		cout << ", ";
		cout << "verbose";
	}

	cout << ")" << endl; 
	for (unsigned int i=0; i<op->vec_filename.size(); ++i)
	{
		printIdent();
		cout << ". #" << i << ": \"" << op->vec_filename.at(i) << "\"" << endl;
	}
}

void PrettyPrinterVisitor::visit(ParallelScanOp* op) {
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Parallel";
	cout << "Scan (";
	cout << "filetype=" << (op->parsetext ? "text" : "binary");

	cout << ", ";
	if (op->globparam == Table::SortFiles)
	{
		cout << "sort";
	}
	else
	{
		assert(op->globparam == Table::PermuteFiles);
		cout << "permute";
	}
	cout << " filenames";

	if (op->parsetext)
	{
		cout << ", ";
		cout << "separators=\"" << op->separators << "\"";
	}

	if (op->verbose==Table::VerboseLoad)
	{
		cout << ", ";
		cout << "verbose";
	}

	cout << ")" << endl; 
	for (unsigned int i=0; i<op->vec_filename.size(); ++i)
	{
		printIdent();
		cout << ". " << printvec(op->vec_grouptothreadlist.at(i), 2) << ": \"" 
			<< op->vec_filename.at(i) << "\"" << endl;
	}
}

void PrettyPrinterVisitor::visit(PartitionedScanOp* op) {
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Partitioned";
	cout << "Scan (";
	cout << "filetype=" << (op->parsetext ? "text" : "binary");

	cout << ", ";
	if (op->globparam == Table::SortFiles)
	{
		cout << "sort";
	}
	else
	{
		assert(op->globparam == Table::PermuteFiles);
		cout << "permute";
	}
	cout << " filenames";

	if (op->parsetext)
	{
		cout << ", ";
		cout << "separators=\"" << op->separators << "\"";
	}

	if (op->verbose==Table::VerboseLoad)
	{
		cout << ", ";
		cout << "verbose";
	}

	cout << ")" << endl; 
	for (unsigned int i=0; i<op->vec_filename.size(); ++i)
	{
		printIdent();
		cout << ". #" << setw(2) << setfill('0') << i << ": \"" << op->vec_filename.at(i) << "\"" << endl;
	}
}

void PrettyPrinterVisitor::visit(MergeOp* op) {
	printIdent();
	cout << "Merge (spawnedthreads=" << op->spawnedthr << ")" << endl;
	printAffinitization(&op->affinitizer);
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(MapWrapper* op) {
	printIdent();
	cout << "MapWrapper (" << op->description << ")" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(MemSegmentWriter* op) 
{
	printIdent();
	cout << "MemSegmentWriter (";
	cout << "size=" << op->buffsize << ", ";
	cout << "policy=";
	switch(op->policy)
	{
		case MemSegmentWriter::POLICY_UNSET:
			cout << "unset, ";
			cout << "name=" << op->paths.at(0);
			break;

		case MemSegmentWriter::POLICY_BIND:
			cout << "bind, ";
			cout << "node=" << op->numanodes.at(0) << ", ";
			cout << "name=" << op->paths.at(0);
			break;

		case MemSegmentWriter::POLICY_RR:
			cout << "round-robin, ";
			cout << "nodes=" << printvec(op->numanodes) << ", ";
			cout << "names=" << printvec(op->paths);
			break;

		case MemSegmentWriter::POLICY_INTERLEAVE:
			cout << "interleave, ";
			cout << "nodes=" << printvec(op->numanodes) << ", ";
			cout << "name=" << op->paths.at(0);
			break;

		default:
			cout << "UNKNOWN";
	}

	cout << ")" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(CycleAccountant* op) 
{
	bool printheader = true;

	printIdent();
	cout << "CycleAccountant" << endl;
	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->cycles.at(i) == NULL)
			continue;

		unsigned long long start = op->cycles.at(i)->ScanStartCycles;
		unsigned long long next  = op->cycles.at(i)->GetNextCycles;
		unsigned long long stop  = op->cycles.at(i)->ScanStopCycles;

		if ( (start == 0) && (next == 0) && (stop == 0) )
			continue;

		if (printheader)
		{
			printIdent();
			cout << ". Thread\t";
			cout << "   ScanStart\t";
			cout << "     GetNext\t";
			cout << "    ScanStop\t";
			cout << "       Total\t";
			cout << endl;
			printheader = false;
		}

		printIdent();
		cout << ".     " << setw(2) << setfill('0') << i << "\t";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< start / 1000. / 1000. << "\t";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< next  / 1000. / 1000. << "\t";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< stop  / 1000. / 1000. << "\t";
		cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
			<< (start + next + stop) / 1000. / 1000. << "\t";
		cout << endl;
	}
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::printAffinitization(Affinitizer* op)
{
	printIdent();
	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->mapping.at(i).numa == Affinitizer::Binding::InvalidBinding)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		cout << "["  << op->mapping.at(i).numa;
		cout << ", " << op->mapping.at(i).socket;
		cout << ", " << op->mapping.at(i).core;
		cout << ", " << op->mapping.at(i).context;
		cout << "] -> LogicalProcessor: ";

		cout << op->topology
			.at(op->mapping.at(i).numa)
			.at(op->mapping.at(i).socket)
			.at(op->mapping.at(i).core)
			.at(op->mapping.at(i).context) << endl;
	}
}

void PrettyPrinterVisitor::visit(Project* op)
{
	printIdent();
	cout << "Projection ("
		<< "attributes=" << printvecaddone(op->projlist)
		<< ")" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(CallStateChecker* op)
{
	printIdent();
	cout << "CallStateChecker" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(SchemaPrinter* op)
{
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(CallCountPrinter* op)
{
	printIdent();
	cout << ". ";
	cout << "scanStart=" << addcommas(op->cntStart) << " ";
	cout << "getNext=" << addcommas(op->cntNext) << " ";
	cout << "scanStop=" << addcommas(op->cntStop) << " ";
	cout << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(SortLimit* op)
{
	printIdent();
	cout << "SortLimit (";
	cout << "orderby=" << printvecaddone(op->orderby);
    if (op->asc == 1){
        cout<<" ascending";
    }else{
        cout<<" descending";
    }
    cout<< ", limit=" << op->limit;
	cout << ")" << endl;
    op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(TupleCountPrinter* op)
{
	printIdent();
	cout << "TupleCountPrinter";

	unsigned long long sum = 0;

	for (int i=0; i<MAX_THREADS; ++i)
	{
		 sum += op->tuples.at(i);
	}

	if (sum != 0)
		cout << " (total=" << addcommas(sum) << " tuples" << ")"; 
	cout << endl;

	for (int i=0; i<MAX_THREADS; ++i)
	{
		if (op->tuples.at(i) == 0)
			continue;

		printIdent();
		cout << ". Thread " << setw(2) << setfill('0') << i << ": ";
		cout << setw(13) << setfill(' ') << addcommas(op->tuples.at(i)) << " tuples" << endl;
	}

    op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(ZeroInputOp* op) 
{
	printIdent();
	cout << "UNKNOWN ZERO INPUT" << endl;
}

void PrettyPrinterVisitor::visit(IntGeneratorOp* op)
{
	printIdent();
	cout << "IntGenerator ("
		<< "tuples=" << addcommas(op->totaltuples) << " (" 
		<< addcommas(op->totaltuples * op->tuplewidth / 1024ull / 1024ull)
		<< " MB) per thread, "
		<< "width=" << op->tuplewidth << " bytes"
		<< ")" << endl;
}

void PrettyPrinterVisitor::visit(PerfCountPrinter* op)
{
	printIdent();
	cout << "PerfCountPrinter" << endl;
	for (unsigned short counter=0; counter<PerfCountPrinter::MAX_COUNTERS; ++counter)
	{
		bool printheader = true;

		for (int i=0; i<MAX_THREADS; ++i)
		{
			unsigned long long start = op->events.at(i).ScanStartCnt[counter];
			unsigned long long next  = op->events.at(i).GetNextCnt[counter];
			unsigned long long stop  = op->events.at(i).ScanStopCnt[counter];
			if ( (start == 0) && (next == 0) && (stop == 0) )
				continue;

			if (printheader)
			{
				printIdent();
				cout << ". PerformanceCounter" << counter << endl;
				printIdent();
				cout << ". . Thread\t";
				cout << "   ScanStart\t";
				cout << "     GetNext\t";
				cout << "    ScanStop\t";
				cout << "       Total\t";
				cout << endl;
				printheader = false;
			}

			printIdent();
			cout << ". .     " << setw(2) << setfill('0') << i << "\t";
			cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
				<< start / 1000. / 1000. << "\t";
			cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
				<< next  / 1000. / 1000. << "\t";
			cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
				<< stop  / 1000. / 1000. << "\t";
			cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
				<< (start + next + stop) / 1000. / 1000. << "\t";
			cout << endl;
		}
	}
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(ConsumeOp* op)
{
	printIdent();
	cout << "Consume" << endl;
	printIdent();
	cout << ". schema=[";
	printSchema(op->nextOp->getOutSchema());
	cout << "] -> " << op->nextOp->getOutSchema().getTupleSize() << " bytes" << endl;
	op->nextOp->accept(this);
}

void PrettyPrinterVisitor::visit(PartitionOp* op)
{
	PartitionOp::PartitionState* state; 
	ExactRangeValueHasher* realfn = 
		dynamic_cast<ExactRangeValueHasher*>(op->hashfn.fn);
	unsigned int threads = realfn->buckets();

	if (op->sortoutput)
	{
		printIdent();
		cout << "Sort ("
			<< "attribute=" << op->sortattribute + 1 
			<< ")" << endl;

		for (unsigned int i=0; i<threads; ++i)
		{
			state = op->partitionstate[i];
			if ((state != NULL) && (state->sortcycles != 0))
			{
				printIdent();
				cout << ". #" << setw(2) << setfill('0') << i << ": ";
				cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
					<< (state->sortcycles) / 1000. / 1000.
					<< " mil cycles to sort output"
					<< endl;
			}
		}
	}

	printIdent();
	cout << "Partition (" 
		<< "attribute=" << op->attribute + 1 
		<< ", "
		<< "range=[" << realfn->minimumforbucket(0) << ","
			<< realfn->minimumforbucket(threads)-1 << "]"
		<< ", "
		<< "partitions=" << threads 
		<< ")" << endl;

	unsigned int rangemaxchar = 1 +
		lrint(log10(realfn->minimumforbucket(threads)-1));
	for (unsigned int i=0; i<threads; ++i)
	{
		printIdent();
		cout << ". #" << setw(2) << setfill('0') << i << ": " 
			<< "[" 
			<< setw(rangemaxchar) << setfill(' ') 
				<< realfn->minimumforbucket(i) << "-" 
			<< setw(rangemaxchar) << setfill(' ') 
				<< realfn->minimumforbucket(i+1) - 1 
			<< "] ";

		state = op->partitionstate[threads-1];
		if (state != NULL && state->idxstart[i] != 0)
		{
			cout << setw(13) << setfill(' ') 
					<< addcommas(state->idxstart[i]) << " tuples out, ";
		}

		state = op->partitionstate[i];
		if ((state != NULL) && (state->bufferingcycles != 0))
		{
			cout << setw(13) << setfill(' ')
					<< addcommas(state->usedtuples) << " tuples in, ";
			cout << setw(12) << fixed << setprecision(2) << setfill(' ') 
				<< (state->bufferingcycles) / 1000. / 1000.
				<< " mil cycles to buffer input";
		}
		cout << endl;
	}
	op->nextOp->accept(this);
}

#ifdef ENABLE_HDF5
void PrettyPrinterVisitor::visit(ScanHdf5Op* op) 
{
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Hdf5Scan (";
	cout << "filename=\"" << op->filename << "\"";
	cout << ", ";
	cout << "partition " << addcommas(op->thispartition+1) 
		<< " of " << addcommas(op->totalpartitions);
	cout << ", ";
	cout << "starts at offset " << addcommas(op->origoffset);
	cout << " and ";
	cout << "reads " << addcommas(op->totaltuples) << " tuples";
	cout << ")" << endl; 

	for (unsigned int i=0; i<op->datasetnames.size(); ++i)
	{
		printIdent();
		cout << ". col #" << i+1 << ": \"" << op->datasetnames.at(i) << "\"" << endl;
	}
}

#ifdef ENABLE_FASTBIT
void PrettyPrinterVisitor::visit(IndexHdf5Op* op) 
{
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Hdf5IndexScan (";
	cout << "filename=\"" << op->filename << "\"";
	cout << ", ";
	cout << "FastBit directory \"" << op->indexdirectory << "\"";
	cout << ", ";
	cout << "index on \"" << op->indexdataset << "\"";
	cout << ")" << endl; 

	for (unsigned int i=0; i<op->datasetnames.size(); ++i)
	{
		printIdent();
		cout << ". col #" << i+1 << ": \"" << op->datasetnames.at(i) << "\"" << endl;
	}
}

void PrettyPrinterVisitor::visit(RandomLookupsHdf5Op* op) 
{
	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "Hdf5RandomLookups (";
	cout << "filename=\"" << op->filename << "\"";
	cout << " with " << addcommas(op->hdf5length) << " tuples";
	cout << ", ";
	cout << addcommas(op->randomlookups) << " random lookups";
	cout << ")" << endl; 

	for (unsigned int i=0; i<op->datasetnames.size(); ++i)
	{
		printIdent();
		cout << ". col #" << i+1 << ": \"" << op->datasetnames.at(i) << "\"" << endl;
	}
}
#endif // ENABLE_FASTBIT
#endif // ENABLE_HDF5

#ifdef ENABLE_FASTBIT
void PrettyPrinterVisitor::visit(FastBitScanOp* op) 
{
	printIdent();
	cout << ". " << addcommas(op->totaltuples) << " tuples total" << endl;

	printIdent();
	cout << ". schema=[";
	printSchema(op->getOutSchema());
	cout << "] -> " << op->getOutSchema().getTupleSize() << " bytes" << endl;

	printIdent();
	cout << "FastBitScan (";
	cout << "directory=\"" << op->indexdirectory << "\"";
	cout << ", ";
	cout << "condition=\"" << op->conditionstr << "\"";
	if ((op->indexdataset.empty() == false) && (op->totalkeylookups != 0))
	{
		cout << ", ";
		cout << addcommas(op->totalkeylookups) << " key lookups on " 
			<< "\"" << op->indexdataset << "\"";
	}
	cout << ")" << endl; 

	for (unsigned int i=0; i<op->col_names.size(); ++i)
	{
		printIdent();
		cout << ". col #" << i+1 << ": \"" << op->col_names.at(i) << "\"" << endl;
	}
}
#endif // ENABLE_FASTBIT
