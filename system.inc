CXX=g++
CXXFLAGS+=-m64

###########################################################
# Controls sanity-checking level. Enable for debug builds. 
# DEBUG2 triggers severe performance penalties.
#
#CPPFLAGS+=-DDEBUG -DDEBUG2

###########################################################
# Controls whether to collect and display memory allocation
# statistics.
#
CPPFLAGS+=-DSTATS_ALLOCATE

###########################################################
# Compile with NUMΑ-awareness extensions?
#
#CPPFLAGS+=-DENABLE_NUMA

###########################################################
# Enables bitonic sort. This is an experimental feature. 
# The algorithm that has been implemented is data-size and 
# data-width specific and not generally usable. Bitonic 
# sort needs SSE 4.2 support from the CPU.
#
#CPPFLAGS+=-DBITONIC_SORT

###########################################################
# Controls compiling of HDF5-specific operators
#
CPPFLAGS+=-DENABLE_HDF5

###########################################################
# FastBit source code is not included
# As of Jan 2014, Pythia v0.1 uses pre-release functionality
#
#CPPFLAGS+=-DENABLE_FASTBIT
