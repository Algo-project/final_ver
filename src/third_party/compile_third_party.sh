#!/bin/sh
git submodule init
git submodule update
mkdir build
cd build
cmake ../libcuckoo -DCMAKE_INSTALL_PREFIX=.. -DCMAKE_BUILD_TYPE=Release
make install -j4
cd ..
rm -rf build
mkdir ../include/libcuckoo
cp include/libcuckoo/* ../include/libcuckoo
