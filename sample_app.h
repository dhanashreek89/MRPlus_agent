#include "map_reduce.h"

using namespace map_reduce;

template<typename KeyType, typename ValueType>
class MyMapper : public Mapper<KeyType, ValueType>
{
	int inputSize;
	int iter;
	int stat;
	string word;
   public:
	MyMapper(MRContext<KeyType, ValueType> ctx) : Mapper<KeyType, ValueType>(ctx), inputSize(0), iter(0), stat(0) {}
	int setup() { return 0; }
	int status() { return stat; }
	int cleanup() { return 0; }
	void map(MapInput<KeyType, ValueType> mi)
	{
		inputSize = mi.key.size();
		for(iter = 0; iter < inputSize; iter++)
		{
			if ( iter && inputSize)
				stat = ((100 * iter) / inputSize);
			if(isspace(mi.key[iter]))
			{
				Mapper<KeyType, ValueType>::context.emit(word, 1);
				word = "";
			}
			else
				word += mi.key[iter];
		}
		Mapper<KeyType, ValueType>::context.emit(word, 1);
		
	}
};

template<typename KeyType, typename ValueType>
class MyReducer : public Reducer<KeyType, ValueType>
{
	int inputSize;
        int iter;
        int stat;
	int sum;
  public:
	MyReducer(MRContext<KeyType, ValueType> ctx) : Reducer<KeyType, ValueType>(ctx), inputSize(0), iter(0), stat(0), sum(0) {}
	int setup() { return 0; }
        int status() { return stat; }
        int cleanup() { return 0; }
	void reduce(MRContext<KeyType, ValueType> MapContext)
	{
		inputSize = MapContext.getTotalNumOfKeyVal();
		if (inputSize <= 0)
			return;
		for(iter = 0; iter < inputSize; iter++)
		{
			stat = ((100 * iter) / inputSize);
			sum += MapContext.getValue(iter);
		}

		Reducer<KeyType, ValueType>::context.emit(MapContext.getKey(inputSize-1), sum);
	}	
};
	
