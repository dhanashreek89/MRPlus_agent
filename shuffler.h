#ifndef SHUFFLER_H
#define SHUFFLER_H

#include "tbb/parallel_sort.h"
#include <vector>
#include <iterator>

using namespace tbb;

template <typename KeyType, typename ValueType>
class Shuffler
{
  public:
	MRContext<KeyType, ValueType> ctx;
	MyMapper<KeyType, ValueType> **allmappers;
	int num_mappers;

	Shuffler<KeyType,ValueType>() {}

	void setShufflerInput(MyMapper<KeyType, ValueType> **allmappers_, int num_mappers_)
	{
		allmappers = allmappers_;
		num_mappers = num_mappers_;
	}

	void setDetails(MyMapper<KeyType, ValueType> **allmappers_, int num_mappers_)
	{
		 allmappers = allmappers_;
		 num_mappers = num_mappers_;
	}

	void Merge()
	{
		//cout << endl << allmappers[1]->getTotalKeyVals();
		for (int i=0; i < num_mappers; i++)
		{
			ctx = allmappers[i]->context;
		}

		cout << endl << "merged key value pairs are : " << endl;
		ctx.printKeyVal();	
	}

	void Sort()
	{
		KeyVal<KeyType, ValueType> s1;

		//typedef typename std::list<KeyVal<KeyType, ValueType>>::iterator type;
		//type list_begin = ctx.m_keyval.begin();
		//type list_end = ctx.m_keyval.end();

		//auto list_begin = ctx.m_keyval.begin();
		//auto list_end = ctx.m_keyval.end();

		parallel_sort(ctx.m_keyval.begin(), ctx.m_keyval.end(), s1);

		//parallel_sort(blocked_range<type>(ctx.m_keyval.begin(), ctx.m_keyval.end()), s1);
		cout << endl << "sorted key value pairs are : " << endl;
		ctx.printKeyVal();
	}

	int h(std::string tmp, int num)
	{
		const char *ch;
	
		ch = tmp.c_str();
		int len = tmp.length();
		int i, sum;
		for (sum=0, i=0; i < len; i++)
     			sum += ch[i];
   		return sum % num;
	}

	bool compareKeys(std::string s1, std::string s2)
	{
		if (s1.compare(s2) == 0)
			return true;
		return false;
	}

	int hashKeys(int num_reducer_tasks, MRContext<KeyType, ValueType> **RedInput)
	{
		int total = 0;

		int n = ctx.getTotalNumOfKeyVal();
		int bucket = 0;

		for(size_t i = 0; i < n; ++i)
		{
			std::stringstream ss;
			ss << ctx.getKey(i);

			bucket = h(ss.str(), num_reducer_tasks);
			if (!RedInput[bucket])
			{
				RedInput[bucket] = new MRContext<KeyType, ValueType>();
				RedInput[bucket]->emit(ctx.getKey(i), ctx.getValue(i));
				total++;
			}
			else
			{
				int size = RedInput[bucket]->getTotalNumOfKeyVal();

				std::stringstream ss1;
				ss1 << RedInput[bucket]->getKey(size-1);

				if ((compareKeys(ss.str(), ss1.str())))
				{
					// add this to this bucket
					RedInput[bucket]->emit(ctx.getKey(i), ctx.getValue(i));
				}
				else
				{
					MRContext<KeyType, ValueType> *temp = RedInput[bucket]->next;
					MRContext<KeyType, ValueType> *prev = NULL;
					bool found = false;

					// traverse the list to find appropriate sub-bucket
					while (temp != NULL)
					{

						prev = temp;
						found = false;

						std::stringstream ss2;
						ss2 << temp->getKey(0);

						if ((compareKeys(ss.str(), ss2.str())))
						{
							temp->emit(ctx.getKey(i), ctx.getValue(i));
							found = true;
							break;
						}

						temp = temp->next;
					}

					if(!found)
					{
						if (prev)
						{
							// create new subcontext
							prev->next = new MRContext<KeyType, ValueType>();
							(prev->next)->emit(ctx.getKey(i), ctx.getValue(i));
							total++;
						}
						else
						{
							// temp was null
							RedInput[bucket]->next = new MRContext<KeyType, ValueType>();
							(RedInput[bucket]->next)->emit(ctx.getKey(i), ctx.getValue(i));
							(RedInput[bucket]->next)->printKeyVal();
							total++;
						}
					}
				}
			}
		}

		return total;
	}

};
#endif

