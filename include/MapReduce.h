/*
 *  Filename	: MapReduce.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include <map>
#include <set>
#include <vector>
#include <sstream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <signal.h>
#include <toolbox.h>
#include <include/Logger.h>
#include <include/ProcessInfo.h>
#include <include/AbstractMapWorker.h>
#include <include/AbstractReduceWorker.h>
using namespace std;

class MapReduce {
public:
	MapReduce(int mapNum, int reduceNum);
	virtual ~MapReduce();

	void run();

	void setDataReader(pair<string, string> (*dataReaderFunc)());
	void setMap(AbstractMapWorker* mapWorker);
	void setReduce(AbstractReduceWorker* reduceWorker);
	void consoleLogging(bool active);
	void fileLogging(bool active);
	void setRemoveTempFiles(bool active);
	void setDelimiter(string delim);

private:
	string delimiter; // used in pipes to point out end of certain data flow

	/* debug parameters */
	bool removeTempFiles;

	/* number of workers */
	unsigned int mapTasksNum;
	unsigned int reduceTasksNum;

	/* map of workers: mapping pid -> stats */
	map<int, ProcessInfo *> mapStats;
	map<int, ProcessInfo *> reduceStats;
	ProcessInfo current;

	/* prepare data function */
	pair<string, string> (*dataReaderFunc)();

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
	void writeStringToPipe(int pipeDesc, string _str);
	bool readStringFromPipe(int pipeDesc, string &_str);
	bool readTmpFile(FILE* tmpFile, string &k, string &v);
	void spawnWorkers();
	static bool compareKeys(pair<string, string> first, pair<string, string> second);
};

#endif /* MAPREDUCE_H_ */
