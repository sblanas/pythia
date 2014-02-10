
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

#include "query.h"

void sanitycheck(libconfig::Config& cfg, libconfig::Setting& cfgnode, 
		const string& branch)
{
	// Check that a branch with the given name exists under cfgnode.
	//
	if (!cfgnode.exists(branch))
		throw MissingParameterException("Cannot find `" + branch + "' attribute in query subtree.");

	// Check that the child has a name.
	//
	if (!cfgnode[branch].exists("name"))
		throw MissingParameterException("Cannot find `name' attribute in query subtree.");

	// Check that the top-level contains a node of this name.
	//
	string name = cfgnode[branch]["name"];
	if (!cfg.getRoot().exists(name))
		throw MissingParameterException("Cannot find description for node `" + name + "'.");

	// Check that the top-level node of this name names a type.
	//
	if (!cfg.getRoot()[name].exists("type"))
		throw MissingParameterException("Cannot find mandatory `type' parameter in description for node `" + name + "'.");
}

void constructsubtree(
		libconfig::Config& cfg,
		libconfig::Setting& cfgnode, 
		Operator** rootaddr, 			//< output
		Query::UserDefinedOpMapT& udops,
		int level,
		Query::OperatorDepthT& depthmap	//< output
		)
{
	// Lookup name and type.
	//
	string name = cfgnode["name"];
	string type = cfg.getRoot()[name]["type"];

	assert(*rootaddr == 0);

	// Based on type: 
	// 1. Allocate and chain in appropriate object
	// 2. Check syntax for child 
	// 3. sanitycheck() branch
	// 4. Recursively call self for children
	// 5. Initialize this node
	//
	if (		type == "scan" 
			||  type == "partitionedscan"
			||  type == "parallelscan"
			||  type == "generator_int"
#ifdef ENABLE_HDF5
			||  type == "hdf5scan"
#ifdef ENABLE_FASTBIT
			||  type == "hdf5index"
			||  type == "hdf5random"
#endif
#endif
#ifdef ENABLE_FASTBIT
			||  type == "fastbitscan"
#endif
	   )
	{
		// Scan operator, no children.
		//
		ZeroInputOp* tmp = NULL;
		if (type == "scan")
			tmp = new ScanOp();
		else if (type == "partitionedscan")
			tmp = new PartitionedScanOp();
		else if (type == "parallelscan")
			tmp = new ParallelScanOp();
		else if (type == "generator_int")
			tmp = new IntGeneratorOp();
#ifdef ENABLE_HDF5
		else if (type == "hdf5scan")
			tmp = new ScanHdf5Op();
#ifdef ENABLE_FASTBIT
		else if (type == "hdf5index")
			tmp = new IndexHdf5Op();
		else if (type == "hdf5random")
			tmp = new RandomLookupsHdf5Op();
#endif
#endif
#ifdef ENABLE_FASTBIT
		else if (type == "fastbitscan")
			tmp = new FastBitScanOp();
#endif

		(*rootaddr) = tmp;
		depthmap[tmp] = level;
	} 
	else if (	type == "hashjoin" 
			||	type == "sortmergejoin"
			||	type == "mpsmjoin"
			||	type == "newmpsmjoin"
			||	type == "preprejoin"
			||	type == "indexhashjoin"
	   )
	{
		// Dual-input operator
		//
		DualInputOp* tmp = NULL;

		if (type == "hashjoin")
			tmp = new HashJoinOp();
		else if (type == "sortmergejoin")
			tmp = new SortMergeJoinOp();
		else if (type == "mpsmjoin")
			tmp = new OldMPSMJoinOp();
		else if (type == "newmpsmjoin")
			tmp = new MPSMJoinOp();
		else if (type == "preprejoin")
			tmp = new PresortedPrepartitionedMergeJoinOp();
		else if (type == "indexhashjoin")
			tmp = new IndexHashJoinOp();

		(*rootaddr) = tmp;
		depthmap[tmp] = level;

		sanitycheck(cfg, cfgnode, "build");
		constructsubtree(cfg, cfgnode["build"], &(tmp->buildOp), udops, level+1, depthmap);

		sanitycheck(cfg, cfgnode, "probe");
		constructsubtree(cfg, cfgnode["probe"], &(tmp->probeOp), udops, level+1, depthmap);
	}
	else
	{
		// Single-input operator
		//
		SingleInputOp* tmp = NULL;

		if (type == "aggregate_sum")
			tmp = new AggregateSum();
		else if (type == "aggregate_count")
			tmp = new AggregateCount();
		else if (type == "merge")
			tmp = new MergeOp();
		else if (type == "shmwriter")
			tmp = new MemSegmentWriter();
		else if (type == "filter")
			tmp = new Filter();
		else if (type == "cycle_accountant")
			tmp = new CycleAccountant();
		else if (type == "projection")
			tmp = new Project();
		else if (type == "checker_callstate")
			tmp = new CallStateChecker();
		else if (type == "printer_schema")
			tmp = new SchemaPrinter();
		else if (type == "printer_tuplecount")
			tmp = new TupleCountPrinter();
		else if (type == "printer_perfcount")
			tmp = new PerfCountPrinter();
		else if (type == "sort")
			tmp = new SortLimit();
		else if (type == "printer_bitentropy")
			tmp = new BitEntropyPrinter();
		else if (type == "consumer")
			tmp = new ConsumeOp();
		else if (type == "printer_callcount")
			tmp = new CallCountPrinter();
		else if (type == "threadidprepend")
			tmp = new ThreadIdPrependOp();
		else if (type == "partition")
			tmp = new PartitionOp();
		else
		{
			// It's a user-defined type?
			//
			Query::UserDefinedOpMapT::iterator it;
			it = udops.find(type);

			// Not a user-defined type, no idea what is it.
			//
			if (it == udops.end())
				throw MissingParameterException("`" + type + "' is neither a built-in nor a user-defined type.");

			// Claim ownership of operator.
			//
			tmp = it->second;
			it->second = 0;
		}

		(*rootaddr) = tmp;
		depthmap[tmp] = level;

		sanitycheck(cfg, cfgnode, "input");
		constructsubtree(cfg, cfgnode["input"], &(tmp->nextOp), udops, level+1, depthmap);
	}

	// Call Operator::init on this node.
	//
	assert(*rootaddr != 0);
	(*rootaddr)->init(cfg, cfg.lookup(name));
}

/**
 * Constructs a query tree from the specified configuration file.
 * The pre-allocated user-defined operators are used if an operator type is
 * unknown. If a pre-allocated operator is used, its entry in the OpMap is
 * emptied, and it's now the responsibility of the Query to destroy the object.
 *
 * @param cfg Configuration file to initialize tree with.
 * @param udops User-defined operator map. Each operator must have been
 * 		allocated with new. If it is used, the entry is set to NULL, and then
 * 		it becomes the Query responsibility to call delete on the operator
 * 		object.
 */
void Query::create(libconfig::Config& cfg, UserDefinedOpMapT& udops)
{
	sanitycheck(cfg, cfg.getRoot(), "treeroot");

	constructsubtree(cfg, cfg.lookup("treeroot"), &tree, udops, 0, operatorDepth);
}

int Query::getOperatorDepth(Operator* op)
{
	OperatorDepthT::iterator it;
	it = operatorDepth.find(op);
	return (it == operatorDepth.end()) ? -1 : it->second;
}
