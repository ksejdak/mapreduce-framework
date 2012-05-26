/*
 *  Filename	: MapReduce.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <toolbox.h>
#include <include/Logger.h>
#include <include/AbstractMapWorker.h>
#include <include/AbstractReduceWorker.h>

template <class K, class V, class I>
class MapReduce {
public:
	MapReduce(int mapNum, int reduceNum);
	virtual ~MapReduce();

	void run();
	void setMap(AbstractMapWorker<K, V> mapWorker);
	void setReduce(AbstractReduceWorker<V, I> reduceWorker);
	void setLogging(bool logging);

private:
	/* number of workers */
	int mapTasksNum;
	int reduceTasksNum;

	/* table of workers: mapping index -> pid */
	int *mapWorkersTab;
	int *reduceWorkersTab;

	/* workers instances */
	AbstractMapWorker<K, V> mapWorker;
	AbstractReduceWorker<V, I> reduceWorker;

	/* pipes linked with workers: pipe[workerNum][desc] */
	int **mapPipes;
	int **reducePipes;

	/* private functions */
	void init();
	void mapSchedule();
	void reduceSchedule();
	bool spawnMapWorker(int i);
	bool spawnReduceWorker(int i);
	void terminateWorkers();
};

/* ====================== DEFINITION ====================== */

template <class K, class V, class I>
MapReduce<K, V, I>::MapReduce(int mapNum, int reduceNum) {
	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;

	mapWorkersTab = NULL;
	reduceWorkersTab = NULL;

	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;
}

template <class K, class V, class I>
MapReduce<K, V, I>::~MapReduce() {
	// TODO: check for childs and kill them if necessary

	if(mapWorkersTab)
		delete [] mapWorkersTab;

	if(reduceWorkersTab)
		delete [] reduceWorkersTab;

	// TODO: close and delete pipes
}

template <class K, class V, class I>
void MapReduce<K, V, I>::run() {
	// TODO: implement
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setMap(AbstractMapWorker<K, V> mapWorker) {
	this->mapWorker = mapWorker;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setReduce(AbstractReduceWorker<V, I> reduceWorker) {
	this->reduceWorker = reduceWorker;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setLogging(bool logging) {
	Logger::getInstance()->setActive(logging);
}

template <class K, class V, class I>
void MapReduce<K, V, I>::init() {
	/* pid tables */
	mapWorkersTab = new int[mapTasksNum];
	reduceWorkersTab = new int[reduceTasksNum];

	/* pipes */
	mapPipes = new int *[mapTasksNum];
	for(int i = 0; i < mapTasksNum; ++i)
		mapPipes[i] = new int[2];

	reducePipes = new int *[reduceTasksNum];
	for(int i = 0; i < reduceTasksNum; ++i)
		reducePipes = new int[2];
}

template <class K, class V, class I>
void MapReduce<K, V, I>::mapSchedule() {
	// TODO: implement
}

template <class K, class V, class I>
void MapReduce<K, V, I>::reduceSchedule() {
	// TODO: implement
}

template <class K, class V, class I>
bool MapReduce<K, V, I>::spawnMapWorker(int i) {
	int pid;
	Logger::getInstance()->writeFile("Spawning map worker...");

	/* child */
	if((pid = fork()) == 0)
		mapWorker.map();

	/* error */
	else if(pid < 0) {
		Logger::getInstance()->writeFile("ERROR: creating process");
		SHOWERR("cannot create process");
		terminateWorkers();

		return false;
	}

	/* parent */
	else
		mapWorkersTab[i] = pid;

	Logger::getInstance()->writeFile("Success!");

	return true;
}

template <class K, class V, class I>
bool MapReduce<K, V, I>::spawnReduceWorker(int i) {
	int pid;
	Logger::getInstance()->writeFile("Spawning reduce worker...");

	/* child */
	if((pid = fork()) == 0)
		reduceWorker.reduce();

	/* error */
	else if(pid < 0) {
		Logger::getInstance()->writeFile("ERROR: creating process");
		SHOWERR("cannot create process");
		terminateWorkers();
		return false;
	}

	/* parent */
	else
		reduceWorkersTab[i] = pid;

	Logger::getInstance()->writeFile("Success!");

	return true;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::terminateWorkers() {
	/* terminate map tasks */
	for(int i = 0; i < mapTasksNum; ++i)
		kill(mapWorkersTab[i], 15);

	/* terminate reduce tasks */
	for(int i = 0; i < reduceTasksNum; ++i)
		kill(reduceWorkersTab[i], 15);
}


#endif /* MAPREDUCE_H_ */
