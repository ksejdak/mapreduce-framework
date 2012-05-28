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
#include <cstdio>
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

class MapReduce {
public:
	MapReduce(int mapNum, int reduceNum);
	virtual ~MapReduce();

	void run();

	void setDataReader(vector<pair<string, string> > (*dataReaderFunc)());
	void setMap(AbstractMapWorker* mapWorker);
	void setReduce(AbstractReduceWorker* reduceWorker);
	void consoleLogging(bool active);
	void fileLogging(bool active);

private:
	/* number of workers */
	unsigned int mapTasksNum;
	unsigned int reduceTasksNum;

	/* map of workers: mapping pid -> stats */
	map<int, ProcessInfo *> mapStats;
	map<int, ProcessInfo *> reduceStats;
	ProcessInfo current;

	/* prepare data function */
	vector<pair<string, string> > (*dataReaderFunc)();

	/* data function */
	vector<pair<string, string> > data; // hope it's not copied when fork

	/* temporary filenames */
	list<string> tmpFileNames;

	/* workers instances */
	AbstractMapWorker* mapWorker;
	AbstractReduceWorker* reduceWorker;

	/* private functions */
	bool spawnMapWorker(ProcessInfo *info);
	bool spawnReduceWorker(ProcessInfo *info);
	void runMap();
	void runReduce();
	void terminateWorkers();
	void removePidEntry(int pid);
	void writeTmpFile(FILE* tmpFile, pair<string, string> row);
	bool readTmpFile(FILE* tmpFile, string &k, string &v);
	static bool compareKeysAssigment(pair<string, int> first, pair<string, int> second);
};

/* ====================== DEFINITION ====================== */

MapReduce::MapReduce(int mapNum, int reduceNum) {
	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;
	dataReaderFunc = NULL;
	mapWorker = NULL;
	reduceWorker = NULL;

	mapStats.clear();
	reduceStats.clear();
}

MapReduce::~MapReduce() {
	terminateWorkers();
}

void MapReduce::setDataReader(vector<pair<string, string> > (*dataReaderFunc)()) {
	this->dataReaderFunc = dataReaderFunc;
}

void MapReduce::run() {

	if (!this->dataReaderFunc) {
		Logger::getInstance()->log("No dataReaderFunc function set!");
		exit(1); // no need to terminateWorkers at this moment
	}

	ProcessInfo *info;

	/* user-defined partitioning */
	data = dataReaderFunc();

	/* assigning jobs for MapWorkers */
	unsigned int dataSize = data.size();
	if (dataSize < mapTasksNum) {
		mapTasksNum = dataSize;
		char num[2];
		sprintf(num, "%d", mapTasksNum);
		Logger::getInstance()->log(string("Too many MapWorkers! Limiting to: ") + num);
	}

	int partSize = dataSize / mapTasksNum;

	Logger::getInstance()->log("Spawning map workers...");
	int partOffset = 0;
	for(unsigned int i = 0; i < mapTasksNum; ++i) {
		info = new ProcessInfo;
		info->setWorkerNo(i);
		if ((double)dataSize/(double)(mapTasksNum * partSize) > 1) {
			--dataSize;
			info->setDataOffsets(partOffset, partOffset+partSize+1);
			partOffset += partSize + 1;
		} else {
			info->setDataOffsets(partOffset, partOffset+partSize);
			partOffset += partSize;
		}
		if(!spawnMapWorker(info)) {
			terminateWorkers();
			exit(1);
		}
	}

	/* wait until all map tasks finish */
	int status, pid;
	for(unsigned int i = 0; i < mapTasksNum; ++i) {
		pid = wait(&status);
		if(WEXITSTATUS(status) == 0) {
			Logger::getInstance()->log("MapWorker finished", pid);
		}
	}

	/* receive filenames from mapWorkers */
	char readbuffer[50];
	memset(readbuffer,0,50);
	for (map<int, ProcessInfo *>::iterator it = mapStats.begin(); it != mapStats.end(); ++it) {
		read((it->second)->getOutputDesc(), readbuffer, sizeof(readbuffer));
		string fileName = string(readbuffer);
		if (fileName.compare("!") != 0) {
			tmpFileNames.push_back(string(readbuffer));
		}
		removePidEntry(it->second->getPid());
	}

	/* sorting keys */
	list<pair<string, int> > keysAssigment;
	for (list<string>::iterator it = tmpFileNames.begin(); it != tmpFileNames.end(); ++it) {
		FILE* tmpFile = fopen(it->c_str(), "rb");
		string k, v;
		while (readTmpFile(tmpFile, k,v)) {
			keysAssigment.push_back(make_pair(k, 0));
		}
		fclose(tmpFile);
	}
	keysAssigment.sort(compareKeysAssigment);
	keysAssigment.unique();

	/* fragmentation */
	size_t keysCount = keysAssigment.size();
	if (keysCount < reduceTasksNum) {
		reduceTasksNum = keysCount;
		char num[2];
		sprintf(num, "%d", reduceTasksNum);
		Logger::getInstance()->log(string("Too many ReduceWorkers! Limiting to: ") + num);
	}

	unsigned int keysPerReduceTask = keysCount/reduceTasksNum;
	list<pair<string, int> >::iterator it = keysAssigment.begin();

	for (unsigned int reduceTaskNo = 0; reduceTaskNo < reduceTasksNum; ++reduceTaskNo) {
		for (unsigned int i = 0; i < keysPerReduceTask; ++i, ++it) {
			it->second = reduceTaskNo;
		}
		if ((double)keysCount/(double)(reduceTasksNum * keysPerReduceTask) > 1) {
			--keysCount;
			it++->second = reduceTaskNo;
		}
	}

	for (list<pair<string, int> >::iterator it = keysAssigment.begin(); it != keysAssigment.end(); ++it) {
		cout << it->first << " -> " << it->second << endl;
	}







	/* TODO: do fragmentation:
	 * spawn ReduceWorkers
	 */
}

bool MapReduce::compareKeysAssigment(pair<string, int> first, pair<string, int> second) {
	if (first.first.compare(second.first) > 0) { // first string greater than second string
		return false;
	} else {
		return true;
	}
}

void MapReduce::writeTmpFile(FILE* tmpFile, pair<string, string> row) {
	const char* key = row.first.c_str();
	size_t keyLen = strlen(key) + 1;

	const char* value = row.second.c_str();
	size_t valLen = strlen(value) + 1;

	fwrite((void*)&keyLen, sizeof(size_t), 1, tmpFile);
	fwrite(key, sizeof(char), keyLen, tmpFile);
	fwrite((void*)&valLen, sizeof(size_t), 1, tmpFile);
	fwrite(value, sizeof(char), valLen, tmpFile);
}

bool MapReduce::readTmpFile(FILE* tmpFile, string &k, string &v) {
	size_t result;
	size_t keyLen;
	result = fread(&keyLen, sizeof(keyLen), 1, tmpFile);
	if (result > 0) {
		char key[keyLen];
		result = fread(key, sizeof(key[0]), keyLen, tmpFile);
		if (result != keyLen) {
			Logger::getInstance()->log("Corrupted tmpfile");
			return false;
		}

		size_t valLen;
		result = fread(&valLen, sizeof(valLen), 1, tmpFile);
		if (result != 1) {
			Logger::getInstance()->log("Corrupted tmpfile",0);
			return false;
		}

		char value[valLen];
		result = fread(value, sizeof(value[0]), valLen, tmpFile);
		if (result != valLen) {
			Logger::getInstance()->log("Corrupted tmpfile",0);
			return false;
		}

		k = string(key);
		v = string(value);
		return true;
	} else {
		return false;
	}

}

void MapReduce::runMap() {
	close(current.getOutputDesc());

	vector<pair<string, string> > mapResult;
	vector<pair<string, string> > rowResult;

	for (int i = current.getStartDataOffset(); i < current.getEndDataOffset(); ++i) {
		pair<string, string> row = data[i];
		rowResult = mapWorker->map(row.first, row.second);
		/* merging results */
		mapResult.insert(mapResult.begin(), rowResult.begin(), rowResult.end());
	}

	char fileName[] = "tmp/mapReduce.XXXXXX";
	int fileDesc;
	FILE* fileHnd;

	if ((fileDesc = mkstemp(fileName)) == -1 || (fileHnd = fdopen(fileDesc, "wb+")) == NULL) {
		if (fileDesc != -1) {
			unlink(fileName);
			close(fileDesc);
		}
		Logger::getInstance()->log("Cannot create tempfile in mapWorker, DATA LOST!",current.getPid());
		write(current.getInputDesc(), "!", (strlen("!")+1)); //error
		exit(1);
	}

	write(current.getInputDesc(), fileName, (strlen(fileName)+1));

	for (vector<pair<string, string> >::iterator it = mapResult.begin(); it != mapResult.end(); ++it) {
		writeTmpFile(fileHnd, *it);
	}

	fclose(fileHnd);

	exit(0);
}

void MapReduce::runReduce() {
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

void MapReduce::setMap(AbstractMapWorker* mapWorker) {
	this->mapWorker = mapWorker;
}

void MapReduce::setReduce(AbstractReduceWorker* reduceWorker) {
	this->reduceWorker = reduceWorker;
}

void MapReduce::consoleLogging(bool active) {
	Logger::getInstance()->setConsole(active);
}

void MapReduce::fileLogging(bool active) {
	Logger::getInstance()->setFile(active);
}

bool MapReduce::spawnMapWorker(ProcessInfo *info) {
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
		Logger::getInstance()->log("ERROR: creating process");

		return false;
	}

	/* parent */
	else {
		close(info->getInputDesc());
		info->setPid(pid);
		mapStats[pid] = info;
		Logger::getInstance()->log("MapWorker spawned", pid);
	}

	return true;
}

bool MapReduce::spawnReduceWorker(ProcessInfo *info) {
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
		Logger::getInstance()->log("ERROR: creating process");

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

void MapReduce::terminateWorkers() {
	map<int, ProcessInfo *>::iterator it;
	int pid;

	if(!mapStats.size())
		goto reduce;

	/* terminate map tasks */
	Logger::getInstance()->log("Terminating MapWorkers...");
	for(it = mapStats.begin(); it != mapStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("terminating..", pid);
	}

reduce:
	if(!reduceStats.size())
		return;

	/* terminate reduce tasks */
	Logger::getInstance()->log("Terminating ReduceWorkers...");
	for(it = reduceStats.begin(); it != reduceStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("terminating..", pid);
	}
}

void MapReduce::removePidEntry(int pid) {
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
