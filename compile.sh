MAKE_JOINER=ON
MAKE_TEST=ON
BUILD_TYPE=Release

mkdir -p build
cd build
cmake -DMAKE_JOINER=$MAKE_JOINER\
   	-DMAKE_TEST=$MAKE_TEST\
   	-DCMAKE_BUILD_TYPE=$BUILD_TYPE\
	../src/
make install
cd ..
