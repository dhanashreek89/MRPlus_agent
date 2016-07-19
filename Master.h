#ifndef MASTER_H
#define MASTER_H

#include "map_reduce.h"
#include "sample_app.h" // TODO: should not be needed once you use templatized mapper and reducer class names
#include "shuffler.h"
#include <fstream>
#include <stdio.h>
#include <stdlib.h>


using namespace map_reduce;

template</*typename MapperClass, typename ReducerClass, */typename KeyType, typename ValueType>
class MapReduce
{
  public:
	void runMapper(MapInput<KeyType, ValueType> **mapInputs, MyMapper<KeyType, ValueType> **allmappers)
	{
		std::thread *mapThreads[num_map_tasks];
		for (int i = 0; i < num_map_tasks; ++i)
			mapThreads[i] = new std::thread(&MyMapper<KeyType, ValueType>::map, 
						std::ref(allmappers[i]), *mapInputs[i]);

		// demo for getting progress of a mapper thread
		//sleep(2);
		//cout << endl << "progress is " << allmappers[0]->status();

		std::cout << endl << "waiting for mapper threads to join";
		for (int i = 0; i < num_map_tasks; i++)
                {
                        //cout << "\n mapper thread " << i << " progress : " << allmappers[i]->status();
                        mapThreads[i]->join();
                }

		// uncomment this if you want to see map output of each mapper
		/*cout << "\nMapper threads done. Map output is : \n";
                for (int i = 0; i < num_map_tasks; i++)
                {
                        allmappers[i]->DisplayKeyValues();
                }*/

	}

	void runReducer(int totalInputs, MyReducer<KeyType, ValueType> **allreducers, 
			MRContext<KeyType, ValueType> **RedInputTemp)
	{

		MRContext<KeyType, ValueType> *ctx, *ctx1, *prev;
		int j, z, k;
		bool flag = true;

		MRContext<KeyType, ValueType> *RedContext[NUMBER_CORES];
		MRContext<KeyType, ValueType> *RedInput[NUMBER_CORES];
		std::thread *redThreads[NUMBER_CORES];
		int inputsDone = 0;

		k = 0;
		for (int i = 0; i < totalInputs / NUMBER_CORES; ++i)
		{
			z = 0;
			j = 0;
			while(j < NUMBER_CORES && k < num_reducer_tasks)
			{
				if (flag)
					ctx = RedInputTemp[k];
				while(j < NUMBER_CORES && ctx)
				{

					RedInput[z] = ctx;
					ctx1 = new MRContext<KeyType, ValueType>;
					RedContext[z] = ctx1;

					allreducers[z] = new MyReducer<KeyType, ValueType>(*RedContext[z]);
					prev = ctx;
					ctx = ctx->next;

					redThreads[z] = new thread(&MyReducer<KeyType, ValueType>::reduce, 
								std::ref(allreducers[z]), *RedInput[z]);

					j++; z++;
					inputsDone++;
				}
				if (!ctx) {
					flag = true;
					++k;
				}
				else {
					flag = false;
					prev = ctx;
					ctx = ctx->next;
				}
			}

			for (int p=0; p < z; ++p)
				redThreads[p]->join();

			//std::cout << endl << "Reducer output";
			for (int p=0; p < z; ++p)
				allreducers[p]->DisplayKeyValues();
		}

		int remaining = totalInputs % NUMBER_CORES;
		z = 0;

		if (remaining > 0)
		{
			if(prev)
				ctx = prev->next;

			int i =0;
			do
			{
				while(ctx)
				{
					RedInput[z] = ctx;
					ctx1 = new MRContext<KeyType, ValueType>;
					allreducers[z] = new MyReducer<KeyType, ValueType>(*RedContext[z]);
					ctx = ctx->next;
					redThreads[z] = new thread(&MyReducer<KeyType, ValueType>::reduce,
								std::ref(allreducers[z]), *RedInput[z]);
					z++;
				}
				ctx = RedInputTemp[k];
				i++;
					
			}while(i<=num_reducer_tasks-k);

			cout << z;
			for (int p=0; p < z; ++p)
				redThreads[p]->join();

			//cout << endl << "Reducer output";
			for (int p=0; p < z; ++p)
                                allreducers[p]->DisplayKeyValues();
		}
	}

	void run(int nReducer, int nCores)
	{
		// 1. Partition the Input
		// 2. Calculate number of map tasks based on total number of partitions and GPU cores
		// 3. Form the MapInput Objects equal to number of partitions
		// 5. Create Objects of the User Defined Mapper class equal to number of map tasks
		// 6. Create threads equal to number of map tasks
		// 7. Assign each thread, map() of each Mapper object
		// 8. Wait for all map threads to finish
		// 10. Invoke the shuffler/sorter to prepare input for User Defined Reducer class
		// 11. Create Reducer objects equal to reducer tasks (specified by user)
		// 12. Create Context objects for each Reducer object and populate it with input for that Reducer
		// 13. Create threads equal to reducer tasks
		// 14. Assign each thread, reduce() of each Reducer object
		// 15. Wait for reducer threads to finish
		// 16. Form the final output and return to user

		//TODO: partition the input here. Taking hardcoded for now

		// set the number of mapper and reducer tasks here. The number should not exceed 60
		num_map_tasks = 2; // hard-coded for now
		num_reducer_tasks = nReducer;
		NUMBER_CORES = nCores;

		// Total number of inputs to mapper will be calculated as
		// file_size / chunk_size
		int totalMapperInputs = 1;
		int totalRedInputs  = 0;
			
		// creating input objects equal to number of cores
		MapInput<KeyType, ValueType> *mapInputs[NUMBER_CORES];
		MRContext<KeyType, ValueType> *ctx;
		MRContext<KeyType, ValueType> *AllCtx[NUMBER_CORES];
		MyMapper<KeyType, ValueType> *allmappers[NUMBER_CORES];
                MyReducer<KeyType, ValueType> *allreducers[NUMBER_CORES];
		MRContext<KeyType, ValueType> *RedInputTemp[num_reducer_tasks];

		Shuffler<KeyType, ValueType> s;

		for (int i = 0; i < NUMBER_CORES; ++i)
		{
			mapInputs[i] = NULL;
			allmappers[i] = NULL;
			AllCtx[i] = NULL;
		}

		// read the input from the file and populate the map input objects
		// for now reading it from only one file, a.txt
		// When the system is fully integrated; this will not be needed
		// since the inputs to mapper will be fed directly by Dat provider threads on the Host CPU
		//for (int i = 0; i < totalMapperInputs / NUMBER_CORES; ++i)
		{
			int i =0;
			mapInputs[i] = new MapInput<KeyType, ValueType>;

			char ch[2] = "a";
			string filename = string(ch);
			filename+= ".txt";

			std::stringstream buffer;
			ifstream f(filename);

			buffer << f.rdbuf();

			string s( buffer.str());
			if (s.c_str()) {
				mapInputs[i]->key = s;
				mapInputs[i]->value = 1;
			}

			cout << endl << "contents of : " << filename << endl;
			cout << mapInputs[i]->key;
			ch[0] += ch[0];

			f.close();						
		}



		for (int i = 0; i < totalMapperInputs / NUMBER_CORES; ++i)
		{
			int flag = 0;
			for (int j = 0; j < NUMBER_CORES; ++j)
			{
				ctx = new MRContext<KeyType, ValueType>;
				AllCtx[j] = ctx;
				allmappers[j] = new MyMapper<KeyType, ValueType>(*AllCtx[j]);
			}

			// send the input array to mapper task
			num_map_tasks = NUMBER_CORES;
			runMapper(mapInputs, allmappers);

			s.setShufflerInput(allmappers, num_map_tasks);
			s.Merge();

		}

		int remaining = totalMapperInputs % NUMBER_CORES;
		if (remaining > 0)
		{
			for (int j = 0; j < remaining; ++j)
			{
				ctx = new MRContext<KeyType, ValueType>;
                                AllCtx[j] = ctx;
                                allmappers[j] = new MyMapper<KeyType, ValueType>(*AllCtx[j]);
			}
			num_map_tasks = remaining;
			runMapper(mapInputs, allmappers);

			s.setShufflerInput(allmappers, num_map_tasks);
                        s.Merge();
		}

		// parallel sort the merged output
		s.Sort();

		// REDUCER PHASE
		for (int k = 0; k < num_reducer_tasks; ++k)
			RedInputTemp[k] = NULL;

		totalRedInputs = s.hashKeys(num_reducer_tasks, RedInputTemp);

		cout << endl << "TOAL RED INPUTS" << totalRedInputs; 
		cout << endl << "Reduced output : ";
		runReducer(totalRedInputs, allreducers, RedInputTemp);
	}

  private:
	int num_map_tasks;
	int num_reducer_tasks;
	int chunk_size; //default size is 64MB
	int NUMBER_CORES;
};

#endif
