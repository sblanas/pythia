
Pythia is a parallel, pipelined, open-source query execution engine
optimized for multi-socket, multi-core systems with large main
memories. It is distributed under the 3-clause BSD license by its
authors (refer to the LICENSE and AUTHORS files.)

Commercial and open source database systems typically tightly couple a
query engine and a data storage engine in a single package. This
allows for a number of distinctive features, such as sophisticated
query optimization and transactionally-consistent updates. On the
other hand, users need to go through an onerous data load process
prior to querying any data, regardless of whether they desire such
advanced functionality.

Pythia is a standalone query engine that decouples data analysis from
data storage. The proliferation of new classes of data stores in the
last few years, with vastly different design goals, consistency or
availability guarantees and performance means that important data
today may no longer reside in database systems. Pythia envisions to
bring sophisticated query capabilities over data that are stored in a
number of modern data stores or container formats. In lieu of loading
an entire dataset, users need to specify how their data should be
streamed into Pythia through a *pull-based adaptor interface*. Pythia
currently provides fast adaptors for CSV and HDF5 data, and for data
stored in the FastBit index format. Please refer to the section
"Adding an operator in Pythia" below on how to add and invoke a custom
adaptor for your data.

Pythia is an ongoing project at The Ohio State University where it
serves both as a research vehicle to accelerate innovation in data
management, and it is also used in education as a prototype of a
modern but lean query engine to allow students to experiment with more
sophisticated algorithms and data structures. Pythia has been compiled
and run on power-constrained Atom netbooks, on servers with 1TB of
main memory, and on supercomputers with thousands of nodes, such as 
the Department of Energy National Energy Research Scientific Computing
Center and the Ohio Supercomputing Center. We hope you will also find
it useful.

Sincerely,  
\- The Pythia authors (see AUTHORS file.)


# Pythia query plan specification

Each query plan in Pythia is fully described by a configuration file,
which is a plain text file. Pythia currently uses `libconfig` for
parsing, so the format of the configuration file somewhat resembles
the JSON format. A simple configuration file can be found in the
`drivers/sample_queries/q1/` directory.

The query configuration file has three types of information:

 * Global configuration options: These apply to the entire query tree.
 * Node-specific configuration options: These apply to each specific
   node.
 * The tree structure: This specifies how the nodes introduced earlier
   are linked together to form the query plan. 

## Global configuration options

These affect all nodes of the query plan. For example, the following
snippet specifies that the buffer that operators pass to each other is
1MB big:

	buffsize: 1048576

## Node-specific configuration options

Each node in the query plan is described by its own top-level object.
Each object has the following form:

	nodename: {
		type: "nodetype";
		(... type-specific configuration options ...)
	};

The engine right now understands a number of types, such as:

| scan |
| partitionedscan |
| parallelscan |
| hashjoin |
| aggregate_sum |
| aggregate_count |
| merge |
| shmwriter |
| filter |
| ... |

(Check the Doxygen documentation on the type-specific options for each
operator.)

For example, the following snippet defines two nodes, named `scanL` and
`writeL`, of types `scan` and `shmwriter` respectively:

	scanL:
	{
		type: "scan";

		filetype: "text";
		separators: "|";
		file: "lineitem.tbl";
		schema: [ "int", "int", "int", "int", "decimal" ];
	};

	writeL:
	{
		type: "shmwriter";

		size: 1048576;
		paths: "/dev/shm/lineitem.";
	}

## Tree structure

Each query plan **must** contain a `treeroot` object at the top level.
This object specifies the structure of the tree and contains exactly
one object of type subtree. A subtree object must contain the common
attribute name (a string) that refers to a `nodename` described in
the node-specific options above. The other attributes in the subtree
object are operator-specific. Currently the following additional
information is expected:

| Operator class | Expected subtrees |
| -------------- | ----------------- |
| join | `build`, `probe` |
| scan or generator | (nothing) |
| (all others) | `input` |

An example of a treenode object is the following:

	treeroot :
	{
	    name : "sum";
	    input :
	    {
	        name : "hashjoinPO";
	        build :
	        {
	            name : "scanP";
	        };
	        probe :
	        {
	            name : "hashjoinOL";
	
	            build :
	            {
	                name : "scanO";
	            };
	
				probe:
	            {
	                name : "scanL";
	            };
	        };
	    };
	};

## Real examples

A simple complete example of a configuration file can be found in the
`drivers` directory, in `drivers/sample_queries/q1/query1.conf`.


# Memory allocation in Pythia

We have found that haphazard memory placement can cause data
access patterns that degrade memory throughput by 20X or more.
This is further exacerbated by the inherent non-determinism of
highly-parallel programs, as such degradations may only happen 
only for a few executions of the same program.

Pythia avoids using the standard C++ allocators in
performance-critical code, and implements a custom NUMA-aware memory
allocator that offers a number of highly desired features
automatically:

* in addition to a NUMA-local allocation, the caller can specify a
  particular NUMA node for each allocation.
* each allocation is padded to avoid cache conflicts and false sharing
  between cache lines
* each returned block is word-aligned to avoid the poor performance
  associated with unaligned memory accesses
* large allocations are padded with non-readable/writable pages to
  catch out of bounds errors early

In addition, Pythia's custom allocator employs a *memory broker*
interface that maintains a breakdown of the memory allocated per
component, query and operator. This information can be used to
selectively deny memory allocation requests in case of memory
pressure.

The main allocation function is `numaallocate_onnode`. The signature
of the function is:

	void* numaallocate_onnode(const char tag[4], size_t allocsize, int node, void* source);

The `tag` is a four-character name which describes the purpose of this
allocation. `allocsize` is the size (in bytes) of this allocation.
`node` is the NUMA node that the allocated region should be placed in,
or -1 if the allocation is NUMA-local. `source` is an optional pointer 
to the object performing the allocation. This information can be used
to differentiate between different instances of an object requesting
memory (for example, at different depths of the query tree.) The
memory breakdown per `tag`, `source` and NUMA node can be printed by
calling the `dbgPrintAllocations()` function.

Internally, allocation happens through two code paths. The first path
is a slow allocation, which will use `mmap` to grow the address space
of the process. This path is preferred for large allocations. In some
cases it is necessary to perform a number of small memory requests. To
avoid the time and space overhead of calling `mmap` for each, the
memory allocator statically preallocates one *allocation arena* per
NUMA node on startup, and assigns portions on that space for any small
request. Currently all allocations that are greater than 16MB are
handled through the slow path.

Deallocation requests trigger an actual deallocation only when freeing 
pages that have been allocated with the slow `mmap`-based allocator. 
The fast lookaside-based allocator will only deallocate memory when an
entire region is free; currently there is no support for memory
compaction. Operators that trigger many small memory requests should
allocate memory in batches using the slow allocator interface, and
deallocate once when the operation is completed.


# Adding an operator in Pythia

Pythia is designed to be easily extendible to support user-defined
functionality. Follow the following steps to add a new operator.

1. Append class declaration in `operators/operators.h`, extend any virtual
   methods, and declare `PrettyPrintVisitor` to be a friend class.
1. Document, in Doxygen format, what the new operator does.
1. Add a new .cpp file in folder `operators/`, to provide the
   implementation.
1. Add the new object (.o) file that will be automatically produced by
   this .cpp in the `Makefile`.
1. Add pretty printing for the new operator, under folder `visitors/`.
   Register the new class, at minimum, to the following files:
	a. `visitors/visitors.h` (This is the externally visible file.)
	a. `visitors/allvisitors.h` (This is the internal .h file, only
	   included by the Visitor implementations.)
	a. `visitors/prettyprint.cpp` (This is the class that walks the
	   tree and pretty-prints it. As you have declared this class a
	   friend, it can pretty print private members too.) 
1. (Strongly encouraged.) Write a new test for your code, add it under
   `unit_tests/` and add it in the `Makefile`. Verify it's working
   using `make tests`.
1. Reserve a type name for your new operator. This is what the user
   will use in the "type" parameter in the configuration file to
   instantiate the new operator.
1. Modify method `constructsubtree()` in `query.cpp` to instantiate the
   new object. 


