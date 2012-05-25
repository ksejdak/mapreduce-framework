/*
 *  Filename	: MapReduce.h
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#define DEFAULT_MAP_TASKS		4
#define DEFAULT_REDUCE_TASKS	4

class MapReduce {
public:
	MapReduce();
	virtual ~MapReduce();

	void setLogging(bool logging);

private:
	int mapTasks;
	int reduceTasks;
	bool logging;
};

#endif /* MAPREDUCE_H_ */
