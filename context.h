#ifndef CONTEXT_H
#define CONTEXT_H

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <sstream>

using namespace std;

template<typename KeyType, typename ValueType>
class KeyVal {
	public:
                KeyType key;
                ValueType value;
		static bool compareWith(const KeyVal<KeyType, ValueType> &a, const KeyVal<KeyType, ValueType> &b)
		{
			return (a.key < b.key);
		}
                bool operator()(const KeyVal<KeyType, ValueType> &a, const KeyVal<KeyType, ValueType> &b) const
                {
                        return compareWith(a,b);
                }
        };


template <typename KeyType, typename ValueType>
class MRContext
{
public:
	KeyVal<KeyType, ValueType> s;

	MRContext<KeyType, ValueType> *next;
	MRContext() : next(NULL) {}

	std::vector<KeyVal<KeyType, ValueType>> m_keyval;
	void emit(KeyType key, ValueType value)
	{
		s.key = key;
		s.value = value;

		m_keyval.push_back(s);
	}
	
	int getTotalNumOfKeyVal() { return m_keyval.size(); }

	void printKeyVal()
	{
		for(size_t i = 0; i < m_keyval.size(); ++i)
		{
			cout << endl << m_keyval[i].key << " " << m_keyval[i].value;
		}
		cout << endl;
	}

	ValueType getValue(int index)
	{
		return m_keyval[index].value;
	}

	KeyType getKey(int index)
	{
		return m_keyval[index].key;
	}

	void operator=(MRContext<KeyType, ValueType> &ctx)
	{
		for(size_t i = 0; i < ctx.getTotalNumOfKeyVal(); i++)
			emit(ctx.getKey(i), ctx.getValue(i));
	}
};

#endif
