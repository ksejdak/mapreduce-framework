/*
 *  Filename	: MapReduce.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include <map>
#include <vector>
#include <sstream>
#include <cstdlib>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <signal.h>
#include <toolbox.h>
#include <src/Logger.h>
#include <src/ProcessInfo.h>
#include <include/AbstractMapWorker.h>
#include <include/AbstractReduceWorker.h>
using namespace std;

template <class K, class V, class I>
class MapReduce {
public:
	MapReduce(int mapNum, int reduceNum);
	virtual ~MapReduce();

	void run();

	void setDataReader(vector<pair<K, V> > (*dataReaderFunc)());
	void setMap(AbstractMapWorker<K, V> mapWorker);
	void setReduce(AbstractReduceWorker<K, I> reduceWorker);
	void consoleLogging(bool active);
	void fileLogging(bool active);

private:
	/* number of workers */
	int mapTasksNum;
	int reduceTasksNum;

	/* map of workers: mapping pid -> stats */
	map<int, ProcessInfo *> mapStats;
	map<int, ProcessInfo *> reduceStats;
	ProcessInfo current;

	/* prepare data function */
	vector<pair<K, V> > (*dataReaderFunc)();

	/* data function */
	vector<pair<K, V> > data; // hope it's not copied when fork

	/* temporary files for map workers */
	FILE* tmpFiles[];

	/* workers instances */
	AbstractMapWorker<K, V> mapWorker;
	AbstractReduceWorker<V, I> reduceWorker;

	/* private functions */
	bool spawnMapWorker(ProcessInfo *info);
	bool spawnReduceWorker(ProcessInfo *info);
	void runMap();
	void runReduce();
	void terminateWorkers();
	void removePidEntry(int pid);
};

/* ====================== DEFINITION ====================== */

template <class K, class V, class I>
MapReduce<K, V, I>::MapReduce(int mapNum, int reduceNum) {
	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;

	mapStats.clear();
	reduceStats.clear();
}

template <class K, class V, class I>
MapReduce<K, V, I>::~MapReduce() {
	terminateWorkers();
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setDataReader(vector<pair<K, V> > (*dataReaderFunc)()) {
	this->dataReaderFunc = dataReaderFunc;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::run() {

	/* TODO: check if dataReaderFunc has been set */

	ProcessInfo *info;

	/* user-defined partitioning */
	data = dataReaderFunc();
	int partSize = data.size() / mapTasksNum;
	/* TODO: for odd numbers last offset will be set invalid */

	/* TODO: check if offset <> 0 */

	Logger::getInstance()->log("Spawning map workers...\n");
	int partOffset = 0;
	for(int i = 0; i < mapTasksNum; ++i) {
		info = new ProcessInfo;
		info->setWorkerNo(i);
		info->setDataOffsets(partOffset, partSize);
		if(!spawnMapWorker(info)) {
			terminateWorkers();
			exit(1);
		}
		partOffset += partSize;
	}

	/* wait until all map tasks finish */
	int status, pid;
	for(int i = 0; i < mapTasksNum; ++i) {
		pid = wait(&status);
		if(WEXITSTATUS(status) == 0) {
			Logger::getInstance()->log("MapWorker finished\n", pid);
			removePidEntry(pid);
		}
	}

	/* TODO: do fragmentation:
	 * 			1. count maximal and minimal value (calling user's defined function)
	 * 			1. or just sort results (probably easier)
	 * 			2. divide range into R pieces
	 * 			3. create magic table which states which fragment belongs to given reduce process
	 * spawn ReduceWorkers
	 */

	/* remember of closing FILE* descriptors such as array tmpFiles[] */
}

template <class K, class V, class I>
void MapReduce<K, V, I>::runMap() {
	close(current.getOutputDesc());

	/* TODO: implement
	 * tell master process anything
	 */

	vector<pair<K, V> > mapResult;
	vector<pair<K, V> > rowResult;
	for (int i = current.getStartDataOffset(); i < current.getEndDataOffset(); ++i) {
		pair<K, V> row = data[i];
		rowResult = mapWorker.map(row.first, row.second);
		/* merging results */
		mapResult.insert(mapResult.begin(), rowResult.begin(), rowResult.end());
	}


	/* saving results to file */
	FILE* tmpFile = tmpfile();
	if (!tmpFile) {
		; // error temp file cannot be created
	}
	tmpFiles[current.getWorkerNo()] = tmpFile;

	/* probably there's need to replace FILE* with fstream to
	 * serialize all objects i.e. mapResult*/

	exit(0);
}

template <class K, class V, class I>
void MapReduce<K, V, I>::runReduce() {
	close(current.getOutputDesc());

	/* TODO: implement
	 * 1. read magic table of fragmentation
	 * 2. get fragments which belong to process
	 * 3. sort them
	 * 4. group them (ex. (K,V) => (K,list(V)) ~ ("a",5), ("b",3), ("a",2) => ("a", list(5,2)), ("b",3) )
	 * 3. do mapReduce on this fragments
	 * 4. save output to result file
	 */
	sleep(2);

	exit(0);
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setMap(AbstractMapWorker<K, V> mapWorker) {
	this->mapWorker = mapWorker;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::setReduce(AbstractReduceWorker<K, I> reduceWorker) {
	this->reduceWorker = reduceWorker;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::consoleLogging(bool active) {
	Logger::getInstance()->setConsole(active);
}

template <class K, class V, class I>
void MapReduce<K, V, I>::fileLogging(bool active) {
	Logger::getInstance()->setFile(active);
}

template <class K, class V, class I>
bool MapReduce<K, V, I>::spawnMapWorker(ProcessInfo *info) {
	/* create pipe */
	int desc[2];
	pipe(desc);

	/* set stats */
	info->setBufDesc(desc[0], desc[1]);
	info->setType(MAP_WORKER);
	current = *info;

	/* child */
	int pid;
	if((pid = fork()) == 0)
		runMap();

	/* error */
	else if(pid < 0) {
		Logger::getInstance()->log("ERROR: creating process\n");

		return false;
	}

	/* parent */
	else {
		close(info->getInputDesc());
		info->setPid(pid);
		mapStats[pid] = info;
		Logger::getInstance()->log("MapWorker spawned\n", pid);
	}

	return true;
}

template <class K, class V, class I>
bool MapReduce<K, V, I>::spawnReduceWorker(ProcessInfo *info) {
	/* create pipe */
	int desc[2];
	pipe(desc);

	/* set stats */
	info->setBufDesc(desc[0], desc[1]);
	info->setType(REDUCE_WORKER);
	current = *info;

	/* child */
	int pid;
	if((pid = fork()) == 0)
		runReduce();

	/* error */
	else if(pid < 0) {
		Logger::getInstance()->log("ERROR: creating process\n");

		return false;
	}

	/* parent */
	else {
		close(info->getInputDesc());
		info->setPid(pid);
		reduceStats[pid] = info;
		Logger::getInstance()->log("ReduceWorker spawned", pid);
	}

	return true;
}

template <class K, class V, class I>
void MapReduce<K, V, I>::terminateWorkers() {
	map<int, ProcessInfo *>::iterator it;
	int pid;

	if(!mapStats.size())
		goto reduce;

	/* terminate map tasks */
	Logger::getInstance()->log("Terminating MapWorkers...\n");
	for(it = mapStats.begin(); it != mapStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("\n", pid);
	}

reduce:
	if(!reduceStats.size())
		return;

	/* terminate reduce tasks */
	Logger::getInstance()->log("Terminating ReduceWorkers...\n");
	for(it = reduceStats.begin(); it != reduceStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("\n", pid);
	}
}

template <class K, class V, class I>
void MapReduce<K, V, I>::removePidEntry(int pid) {
	map<int, ProcessInfo *>::iterator it;

	if((it = mapStats.find(pid)) != mapStats.end()) {
		close(mapStats[pid]->getOutputDesc());
		delete mapStats[pid];
		mapStats.erase(pid);
	}
	else if((it = reduceStats.find(pid)) != reduceStats.end()) {
		close(reduceStats[pid]->getInputDesc());
		delete reduceStats[pid];
		reduceStats.erase(pid);
	}
}

#endif /* MAPREDUCE_H_ */
