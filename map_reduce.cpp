#include "Master.h"

using namespace map_reduce;

int main(const int argc, const char *argv[])
{
	if (argc < 3)
	{
		cerr << "Use: <MR+binary> nReducerTasks nCores" << endl;
		return -1;
	}

	MapReduce<string, int> mr;
	mr.run(atoi(argv[1]), atoi(argv[2]));
}
