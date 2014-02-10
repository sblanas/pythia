#!/bin/bash

. system.inc

tar Jxf externals/hdf5-1.8.11.tar.xz
cd hdf5-1.8.11
./configure --prefix $PWD/../dist CFLAGS="$SYSFLAGS -O3" --enable-production
make -j4 install
make distclean
cd ..
rm -rf hdf5-1.8.11

cd externals/libconfig-1.2/
./configure --prefix $PWD/../../dist CFLAGS="$SYSFLAGS -O3" CXXFLAGS="$SYSFLAGS -O3"
make -j4 install
make distclean
cd ../../

cd externals/bzip2-1.0.5/
make -j4 install PREFIX=$PWD/../../dist CFLAGS="$SYSFLAGS -O3"
make clean
cd ../../

#cd externals/fastbit-current/
#./configure --prefix $PWD/../../dist CFLAGS="$SYSFLAGS -O3" CXXFLAGS="$SYSFLAGS -O3"
#make -j4 install
#make distclean
#cd ../../
