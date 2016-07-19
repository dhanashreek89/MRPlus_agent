#include "context.h"

int main()
{
	MRContext<string, int> MRC;
	MRC.emit("Hi there", 1);
	cout << endl << MRC.getTotalNumOfKeyVal();

	MRC.printKeyVal();
	return 0;
}
