#!/bin/bash
#
# Indexing first column in candidate.first3proj as candidate/id

mkdir -p candidate
cut -f1 candidate.first3proj > id.ascii
g++ ascii2int.cpp -o ascii2int
./ascii2int
rm -f ascii2int
rm -f id.ascii
../../../dist/bin/ibis -d candidate -s id
../../../dist/bin/ibis -d candidate -b "<binning prec=1/>" 
