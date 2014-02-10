
.PHONY: all clean doc distclean unit_tests drivers tags

include system.inc

CPPFLAGS+=-Idist/include/

CXXFLAGS+=-g 
#CXXFLAGS+=-O0 -Wall
CXXFLAGS+=-O3 -Wall

LDFLAGS+=-Ldist/lib/
LDLIBS+=-lconfig++ -lpthread -lrt -lbz2

# Bitonic sort needs SSE4.2 support.
# 
ifneq ($(findstring BITONIC_SORT,$(CPPFLAGS)),)
CXXFLAGS+=-msse4.2
endif

SHELL=/bin/bash		# for HOSTTYPE variable, below
ifeq ($(shell echo $$HOSTTYPE),sparc)
LDLIBS+=-lcpc -lsocket -lnsl
CXXFLAGS+=-mcpu=ultrasparc
endif

# Add libraries
#
ifneq ($(findstring ENABLE_NUMA,$(CPPFLAGS)),)
LDLIBS+=-lnuma
endif
ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
LDLIBS+=-lhdf5
endif
ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
LDLIBS+=-lfastbit
endif

CC=$(CXX)
CFLAGS=$(CXXFLAGS)

FILES = schema.o hash.o \
	ProcessorMap.o Barrier.o \
	perfcounters.o \
	query.o \
	util/hashtable.o \
	util/buffer.o \
	visitors/recursivedestroy.o \
	visitors/recursivefree.o \
	visitors/prettyprint.o \
	visitors/threadinit.o \
	visitors/threadclose.o \
	operators/base.o \
	operators/mapwrapper.o \
	operators/filter.o \
	operators/sortlimit.o \
	operators/genericaggregate.o \
	operators/aggregatecount.o \
	operators/aggregatesum.o \
	operators/scan.o \
	operators/partitionedscan.o \
	operators/parallelscan.o \
	operators/merge.o \
	operators/join.o \
	operators/shuffle.o \
	operators/cycleaccountant.o \
	util/affinitizer.o \
	operators/project.o \
	comparator.o \
	conjunctionevaluator.o \
	rawcompfns.o \
	operators/memsegmentwriter.o \
	operators/loaders/table.o \
	operators/loaders/sfmt/SFMT.o \
	operators/loaders/parser.o \
	operators/loaders/loader.o \
	operators/checker_callstate.o \
	operators/printer_tuplecount.o \
	operators/generator_int.o \
	operators/printer_perfcnt.o \
	operators/bitentropy.cpp \
	operators/threadidprepend.cpp \
	util/numaasserts.o \
	util/numaallocate.o \
	operators/consume.o \
	operators/sortandrangepartition.o \
	operators/partition.o \
	operators/indexjoin.o \

ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
FILES += \
	operators/hdf5scan.o \

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
FILES += \
	operators/hdf5index.o \
	operators/hdf5random.o \

endif
endif

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
FILES += \
	operators/fastbitscan.o \
	operators/hdf5random.o \

endif

#
# UNIT TESTS
#

ifneq ($(findstring ENABLE_HDF5,$(CPPFLAGS)),)
UNIT_TESTS+= \
	unit_tests/testhdf5scan \
	unit_tests/queryhdf5 \

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
UNIT_TESTS+= \
	unit_tests/testindexhdf5 \
	unit_tests/queryindexhashjoin \

endif
endif

ifneq ($(findstring ENABLE_FASTBIT,$(CPPFLAGS)),)
UNIT_TESTS += \
	unit_tests/testfastbit \

endif


UNIT_TESTS += \
	unit_tests/testschema \
	unit_tests/testhash \
	unit_tests/testparser \
	unit_tests/testloader \
	unit_tests/testtable \
	unit_tests/testcomparator \
	unit_tests/testhashtable \
	unit_tests/testmemmaptable \
	unit_tests/testaffinitizer \
	unit_tests/testpagesort \
	unit_tests/getnext \
	unit_tests/conjunctionevaluator \
	unit_tests/querythreadidprepend \
	unit_tests/querymap \
	unit_tests/querymapsequence \
	unit_tests/querydate \
	unit_tests/queryagg \
	unit_tests/queryagg_compositekey \
	unit_tests/queryaggsum \
	unit_tests/queryaggsum_global \
	unit_tests/queryhashjoin \
	unit_tests/querysortmergejoin \
	unit_tests/querysortmergecartesianprod \
	unit_tests/querympsmjoin \
	unit_tests/querympsmpkfkjoin \
	unit_tests/querypartitionedmpsmpkfkjoin \
	unit_tests/querympsmpartitionedjoin \
	unit_tests/querympsmcartesianprod \
	unit_tests/querympsmcartesianprodtwogroups \
	unit_tests/querypreprejoin \
	unit_tests/querypreprejoinpkfk \
	unit_tests/querypreprejoinfkpk \
	unit_tests/querypreprejoincartesianprod \
	unit_tests/queryshuffle \
	unit_tests/querymemsegmentwriter \
	unit_tests/queryproject \
	unit_tests/querysort \
	unit_tests/querypartition \
	unit_tests/testparallelqueue \
	unit_tests/querymerge \
	unit_tests/testpagebitonicsort \


DRIVERS = \
	drivers/executequery \
	drivers/sample_queries/q1/query1 \

# yes, two lines are necessary
define nl


endef

UNIT_TEST_OBJS = $(UNIT_TESTS)

DRIVER_OBJS = $(DRIVERS)

all: dist $(DRIVER_OBJS)


tests: $(UNIT_TEST_OBJS)
	unxz -fk unit_tests/data/ptf_small.h5.xz 
	$(foreach obj,$(UNIT_TEST_OBJS),\
		LD_LIBRARY_PATH=dist/lib/:$$LD_LIBRARY_PATH $(obj)$(nl)\
		)
	rm -f unit_tests/data/ptf_small.h5
	

clean:
	rm -f unit_tests/data/ptf_small.h5
	rm -f *.o
	rm -f visitors/*.o
	rm -f operators/*.o
	rm -f operators/loaders/*.o
	rm -f operators/loaders/sfmt/*.o
	rm -f util/*.o
	rm -f unit_tests/*.o
	rm -f $(UNIT_TEST_OBJS)
	rm -f $(DRIVER_OBJS)

distclean: clean
	rm -rf dist
	rm -rf html
	rm -f tags

doc: Doxyfile
	doxygen

tags: 
	ctags -R --langmap=c++:+.inl --languages=c++ --exclude="dist/* externals/*"

drivers: $(DRIVER_OBJS)

$(UNIT_TEST_OBJS): $(FILES)

$(DRIVER_OBJS): $(FILES)

dist:
	./pre-init.sh
