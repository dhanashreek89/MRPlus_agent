#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include "context.h"

namespace map_reduce {

template<typename KeyType1, typename ValueType1> class MapInput;

template<typename KeyType, typename ValueType>
class Mapper
{
  public:
	MRContext<KeyType, ValueType> context;
	Mapper(MRContext<KeyType, ValueType> ctx) : context(ctx) {}	
	virtual int setup() = 0;
	virtual int cleanup() = 0;
	virtual int status() = 0;
	virtual void map(MapInput<KeyType, ValueType> input) = 0;
	int getTotalKeyVals() { return context.getTotalNumOfKeyVal(); }
	void DisplayKeyValues()
        {
                Mapper<KeyType, ValueType>::context.printKeyVal();
        }

};

template<typename KeyType, typename ValueType>
class Reducer
{
  public:
	MRContext<KeyType, ValueType> context;
	Reducer( MRContext<KeyType, ValueType> ctx ) : context(ctx) {}
	virtual int setup() = 0;
        virtual int cleanup() = 0;
	virtual int status() = 0;
	virtual void reduce(MRContext<KeyType, ValueType> mapContext) = 0; 
	int getTotalKeyVals() { return context.getTotalNumOfKeyVal(); }
        void DisplayKeyValues()
        {
                Reducer<KeyType, ValueType>::context.printKeyVal();
        }

};

template<typename KeyType1, typename ValueType1>
class MapInput
{
   public:
	KeyType1 key;
	ValueType1 value;
};

} //namespace map_reduce

#endif
