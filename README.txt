
1. Download source code of Intel's Threading Building Block(tbb) libary -
https://www.threadingbuildingblocks.org/download#code-samples
	Go to source
	And download the package

2. Extract it and cd to the top level dir which has src/ and Makefile and issue "gmake". This will build the tbb

3. Once the tbb is built, issue this -
source <dir>/tbb41_20130116oss/build/linux_ia32_gcc_cc4.5.1_libc2.13_kernel2.6.35.6_release/tbbvars.sh

4. Compile our MR+ framework -

	export CPLUS_INCLUDE_PATH = <tbb dir>/tbb43_20150316oss/include/
	
	g++ map_reduce.cpp -o map_reduce.out -pthread -std=c++0x -ltbb
	
	./map_reduce
	
