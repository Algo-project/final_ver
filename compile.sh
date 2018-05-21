MAKE_JOINER=OFF
MAKE_TEST=ON
BUILD_TYPE=debug

rm -rf build
mkdir build
cd build
cmake -DMAKE_JOINER=$MAKE_JOINER\
   	-DMAKE_TEST=$MAKE_TEST\
   	-DCMAKE_BUILD_TYPE=$BUILD_TYPE\
	../src/
make install
cd ..
rm -rf build
