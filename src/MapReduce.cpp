/*
 * Filename	: MapReduce.cpp
 * Author	: Kuba Sejdak & Maciej Dobrowolski
 * Created on	: 02-06-2012
 */

#include <include/MapReduce.h>

/*
 * Sets up environment: initializes necessary variables
 * 						creates directories
 * 						check starting conditions
 */
MapReduce::MapReduce(int mapNum, int reduceNum) {
	if (mapNum < 1 || reduceNum < 1) {
		Logger::getInstance()->log("mapNum and reduceNum parameters must be greater than 0!");
		exit(1);
	}

	delimiter = "#";
	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;
	dataReaderFunc = NULL;
	mapWorker = NULL;
	reduceWorker = NULL;
	removeTempFiles = true;

	mapStats.clear();
	reduceStats.clear();

	/* create tmp directory */
	mkdir("tmp", S_IRWXU);
}

/*
 * Destructor kills any working children processes and removes tmp folder
 * if its told so (by setRemoveTempFiles method).
 */
MapReduce::~MapReduce() {
	terminateWorkers();
	if (removeTempFiles) {
		if (rmdir("tmp") != 0) {
			Logger::getInstance()->log("Cannot remove tmp directory! (probably it contains some files)");
		}
	}
}

/*
 * Sets delimiter string which will be send at the end of transmission certain
 * data flow (i.e. temporary files' names). It is also used in transmission with
 * known items number to show that something went wrong.
 */
void MapReduce::setDelimiter(string delim) {
	delimiter = delim;
}

/*
 * Sets user-defined data reader function used in first stage of data processing.
 * Function should read data from user-defined source and convert it to format
 * of <string, string> which will be given as input to map process.
 * i.e. for word counting program it can return pair of data <filename, line>,
 * 		if user foresees only one file and don't want to distinct between them
 * 		map process will omit filename and split lines into words.
 */
void MapReduce::setDataReader(pair<string, string> (*dataReaderFunc)()) {
	this->dataReaderFunc = dataReaderFunc;
}

/*
 * Sets up workers' processes
 */
void MapReduce::spawnWorkers() {
	ProcessInfo *info;

	/* spawning mapWorkers */
	Logger::getInstance()->log("Spawning map workers...");
	for (unsigned int i = 0; i < mapTasksNum; ++i) {
		info = new ProcessInfo;
		info->setWorkerNo(i);
		if (!spawnMapWorker(info)) {
			terminateWorkers();
			exit(1);
		}
	}

	/* spawning reduceWorkers */
	Logger::getInstance()->log("Spawning reduce workers...");
	for (unsigned int i = 0; i < reduceTasksNum; ++i) {
		info = new ProcessInfo;
		info->setWorkerNo(i);
		if (!spawnReduceWorker(info)) {
			terminateWorkers();
			exit(1);
		}
	}
}


/*
 * Master process' function to make magic work, exchanges data
 * with children processes.
 */
void MapReduce::run() {

	if (!this->dataReaderFunc) {
		Logger::getInstance()->log("No dataReaderFunc function set!");
		exit(1); // no need to terminateWorkers at this moment
	}

	spawnWorkers();

	/* assigning pairs of <string, string> to MapWorkers */
	map<int, ProcessInfo *>::iterator mapStatsIt = mapStats.begin();
	pair<string, string> data;
	for (;;) {
		/* user-defined partitioning */
		data = dataReaderFunc();

		/* sending values to mapWorkers via pipe */
		if (!data.first.empty()) {
			writeStringToPipe(mapStatsIt->second->getInputDesc2(), data.first);
			writeStringToPipe(mapStatsIt->second->getInputDesc2(), data.second);

			/* changing mapWorker processes */
			if (++mapStatsIt == mapStats.end())
				mapStatsIt = mapStats.begin();

		} else {
			break;
		}
	}
	/* end of sending data (sending delimiter) */
	for (mapStatsIt = mapStats.begin(); mapStatsIt != mapStats.end(); ++mapStatsIt) {
		writeStringToPipe(mapStatsIt->second->getInputDesc2(), string(delimiter));
	}

	/* wait until all map tasks finish */
	int status, pid;
	for (unsigned int i = 0; i < mapTasksNum; ++i) {
		pid = wait(&status);
		if (WEXITSTATUS(status) == 0) {
			Logger::getInstance()->log("MapWorker finished", pid);
		}
	}

	/* receive filenames from mapWorkers */
	list<string> tmpFileNames;
	string fileName;
	for (map<int, ProcessInfo *>::iterator it = mapStats.begin(); it
			!= mapStats.end(); ++it) {
		readStringFromPipe((it->second)->getOutputDesc(), fileName);
		Logger::getInstance()->log("Stored tempfile: " + fileName,
				it->second->getPid());
		if (fileName.compare(delimiter) != 0) {
			tmpFileNames.push_back(fileName);
		}

		removePidEntry(it->second->getPid());
	}

	/* getting & sorting keys */
	list<string> keysToReduce;
	for (list<string>::iterator it = tmpFileNames.begin(); it
			!= tmpFileNames.end(); ++it) {
		FILE* tmpFile = fopen(it->c_str(), "rb");
		string k, v;
		while (readTmpFile(tmpFile, k, v)) {
			keysToReduce.push_back(k);
		}
		fclose(tmpFile);
	}
	keysToReduce.sort();
	keysToReduce.unique();

	/* fragmentation - assigning keys to reduceWorkers */
	size_t keysCount = keysToReduce.size();
	unsigned int keysPerReduceTask = keysCount / reduceTasksNum;
	list<string>::iterator it = keysToReduce.begin();
	for (map<int, ProcessInfo *>::iterator procIterator = reduceStats.begin(); procIterator
			!= reduceStats.end(); ++procIterator) {
		for (unsigned int i = 0; i < keysPerReduceTask; ++i, ++it) {
			writeStringToPipe(procIterator->second->getInputDesc(), *it);
		}
		if ((double) keysCount / (double) (reduceTasksNum * keysPerReduceTask)
				> 1) {
			--keysCount;
			writeStringToPipe(procIterator->second->getInputDesc(), *it);
			++it;
		}
	}
	/* sending delimiter to reduce workers */
	for (map<int, ProcessInfo *>::iterator procIterator = reduceStats.begin(); procIterator
				!= reduceStats.end(); ++procIterator) {
		writeStringToPipe(procIterator->second->getInputDesc(), delimiter);
	}

	/* sending temporary files names to all ReduceWorkers */
	for (map<int, ProcessInfo *>::iterator procIterator = reduceStats.begin(); procIterator
			!= reduceStats.end(); ++procIterator) {
		for (list<string>::iterator fileNameIterator = tmpFileNames.begin(); fileNameIterator
				!= tmpFileNames.end(); ++fileNameIterator) {
			writeStringToPipe(procIterator->second->getInputDesc(),
					*fileNameIterator);
		}
		writeStringToPipe(procIterator->second->getInputDesc(), delimiter); // delimiter
	}

	/* wait until all reduce tasks finish */
	for (unsigned int i = 0; i < reduceTasksNum; ++i) {
		pid = wait(&status);
		if (WEXITSTATUS(status) == 0) {
			Logger::getInstance()->log("ReduceWorker finished", pid);
		}
		removePidEntry(pid);
	}

	/* removing temp files */
	if (removeTempFiles) {
		list<string>::iterator it;
		for (it = tmpFileNames.begin(); it != tmpFileNames.end(); ++it) {
			if (remove(it->c_str()) != 0)
				Logger::getInstance()->log("Error deleting tempfile: " + *it);
			else
				Logger::getInstance()->log("Deleting tempfile: " + *it);
		}
	}
}

/*
 * Used as an input to list's sort method, compares pairs of <string, string>
 * using only first ingredient (pair.first)
 */
bool MapReduce::compareKeys(pair<string, string> first,
		pair<string, string> second) {
	if (first.first.compare(second.first) > 0) { // first string greater than second string
		return false;
	} else {
		return true;
	}
}

/*
 * Writes binary structure of pair<string, string> to file which hander is
 * tmpFile.
 * Data is written in following format:
 * row:
 * {1 x size_t - size of pair.first}
 * {? x char - pair.first - ? is previous number}
 * {1 x size_t - size of pair.second}
 * {? x char - pair.second - ? is previous number}
 */
void MapReduce::writeTmpFile(FILE* tmpFile, pair<string, string> row) {
	const char* key = row.first.c_str();
	size_t keyLen = strlen(key) + 1;

	const char* value = row.second.c_str();
	size_t valLen = strlen(value) + 1;

	fwrite((void*) &keyLen, sizeof(size_t), 1, tmpFile);
	fwrite(key, sizeof(char), keyLen, tmpFile);
	fwrite((void*) &valLen, sizeof(size_t), 1, tmpFile);
	fwrite(value, sizeof(char), valLen, tmpFile);
}

/*
 * Reads two strings from given file handler where data are laid in
 * following format:
 * {1 x size_t - size of pair.first}
 * {? x char - pair.first - ? is previous number}
 * {1 x size_t - size of pair.second}
 * {? x char - pair.second - ? is previous number}
 * returns:
 * 		pair.first as string k
 * 		pair.second as string v
 */
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
			Logger::getInstance()->log("Corrupted tmpfile", 0);
			return false;
		}

		char value[valLen];
		result = fread(value, sizeof(value[0]), valLen, tmpFile);
		if (result != valLen) {
			Logger::getInstance()->log("Corrupted tmpfile", 0);
			return false;
		}

		k = string(key);
		v = string(value);
		return true;
	} else {
		return false;
	}

}

/*
 * Writes string to given pipe in following format:
 * {1 x size_t - length of preceding string}
 * {? x char - string of length as specified above}
 */
void MapReduce::writeStringToPipe(int pipeDesc, string _str) {
	const char* str = _str.c_str();
	size_t strLen = strlen(str) + 1;

	write(pipeDesc, &strLen, sizeof(strLen));
	write(pipeDesc, str, strLen * sizeof(char));
}

/*
 * Reads string from given pipe in following format:
 * {1 x size_t - length of preceding string}
 * {? x char - string of length as specified above}
 * returns it as string _str
 *
 * method returns true if string was successfully read, otherwise false
 */
bool MapReduce::readStringFromPipe(int pipeDesc, string &_str) {
	size_t result;
	size_t strLen;
	result = read(pipeDesc, &strLen, sizeof(strLen));
	if (result > 0) {
		char str[strLen];
		result = read(pipeDesc, str, sizeof(str[0]) * strLen);
		if (result != sizeof(str[0]) * strLen) {
			Logger::getInstance()->log("Corrupted pipe data");
			return false;
		}

		_str = string(str);
		return true;
	} else {
		return false;
	}
}

/*
 * Method run by map children processes, receives data from pipe,
 * executes mapWorker->map() on each received pair, finally saves
 * results to file and tells parent process what is name of this file.
 */
void MapReduce::runMap() {

	vector<pair<string, string> > mapResult;
	vector<pair<string, string> > rowResult;

	string k, v;
	while (readStringFromPipe(current.getOutputDesc2(), k) && k.compare("#")
			!= 0 && readStringFromPipe(current.getOutputDesc2(), v)) {
		rowResult = mapWorker->map(k, v);

		/* merging results */
		mapResult.insert(mapResult.begin(), rowResult.begin(), rowResult.end());
	}

	/* creating temporary file and generating its name */
	char fileName[] = "tmp/mapReduce.XXXXXX";
	int fileDesc;
	FILE* fileHnd;

	if ((fileDesc = mkstemp(fileName)) == -1 || (fileHnd = fdopen(fileDesc,
			"wb+")) == NULL) {
		if (fileDesc != -1) {
			unlink(fileName);
			close(fileDesc);
		}
		Logger::getInstance()->log("Cannot create tempfile in mapWorker"
			", DATA LOST!", current.getPid());
		writeStringToPipe(current.getInputDesc(), delimiter);
		exit(1);
	}

	/* writing generated filename to parent via pipe */
	writeStringToPipe(current.getInputDesc(), string(fileName));

	if (mapResult.size() > 0) {
		/* saving map results to temporary file */
		vector<pair<string, string> >::iterator it;
		for (it = mapResult.begin(); it != mapResult.end(); ++it)
			writeTmpFile(fileHnd, *it);
	}

	fclose(fileHnd);

	exit(0);
}

/*
 * Method run by reduce children processes, receives keys, file names
 * from parent process via pipe, gathers these keys data from files,
 * sorts it, groups it in lists (common key method) and executes
 * reduceWorker->reduce() on all pairs of <key, list(data)>.
 * Finally saves results as text file and sends master process its
 * name.
 */
void MapReduce::runReduce() {
	/* getting assigned keys from pipe */
	set<string> myWorkScope;
	string key;
	while (readStringFromPipe(current.getOutputDesc(), key) && key.compare("#")
			!= 0) {
		myWorkScope.insert(key);
	}

	/* getting temporary files names from pipe */
	vector<string> fileNames;
	string fileName;
	while (readStringFromPipe(current.getOutputDesc(), fileName)
			&& fileName.compare(delimiter) != 0) {
		fileNames.push_back(fileName);
	}

	/* setting output file name */
	ostringstream ss;
	ss << current.getWorkerNo();
	string outputFileName = "ReduceOutput." + ss.str();

	/* searching within temp files for associated keys */
	list<pair<string, string> > myKeysAndValues;
	vector<string>::iterator fileNameIterator;
	for (fileNameIterator = fileNames.begin(); fileNameIterator
			!= fileNames.end(); ++fileNameIterator) {
		FILE* tmpFile = fopen(fileNameIterator->c_str(), "rb");
		string k, v;
		while (readTmpFile(tmpFile, k, v)) {
			if (myWorkScope.find(k) != myWorkScope.end())
				myKeysAndValues.push_back(make_pair(k, v));
		}
		fclose(tmpFile);
	}

	/* sorting */
	myKeysAndValues.sort(compareKeys);

	/* grouping and reducing */
	list<pair<string, vector<string> > > allResults;
	list<pair<string, string> >::iterator pairIterator;
	for (pairIterator = myKeysAndValues.begin(); pairIterator
			!= myKeysAndValues.end();) {
		list<string> allKeyValues;
		string key = pairIterator->first;

		allKeyValues.push_back(pairIterator->second);

		/* grouping */
		while (++pairIterator != myKeysAndValues.end()
				&& pairIterator->first.compare(key) == 0)
			allKeyValues.push_back(pairIterator->second);

		/* reducing */
		allResults.push_back(make_pair(key, reduceWorker->reduce(key,
				allKeyValues)));

	}

	/* writing reduce output to file */
	FILE* output = fopen(outputFileName.c_str(), "w+");
	if (output == NULL) {
		Logger::getInstance()->log("Cannot create file to write output",
				current.getPid());
	} else {
		list<pair<string, vector<string> > >::iterator resultsIterator;
		for (resultsIterator = allResults.begin(); resultsIterator
				!= allResults.end(); ++resultsIterator) {
			string outputValues = (resultsIterator->second)[0];
			for (unsigned int i = 1; i < resultsIterator->second.size(); ++i) {
				outputValues.append((resultsIterator->second)[i]);
				outputValues.append(", ");
			}

			string tmp = resultsIterator->first + " " + outputValues;
			fprintf(output, "%s\n", tmp.c_str());
		}
		fclose(output);
	}

	exit(0);
}

/*
 * Sets user-defined map worker class object
 */
void MapReduce::setMap(AbstractMapWorker* mapWorker) {
	this->mapWorker = mapWorker;
}

/*
 * Sets user-defined reduce worker class object
 */
void MapReduce::setReduce(AbstractReduceWorker* reduceWorker) {
	this->reduceWorker = reduceWorker;
}

/*
 * Sets if logs should be visible in console
 */
void MapReduce::consoleLogging(bool active) {
	Logger::getInstance()->setConsole(active);
}

/*
 * Sets if logs should be saved to log file
 */
void MapReduce::fileLogging(bool active) {
	Logger::getInstance()->setFile(active);
}

/*
 * Spawns single map worker, sets its ProcessInfo entry
 * and closes pipes descriptors. Returns true if everything
 * went right and process is ready to work, otherwise false.
 */
bool MapReduce::spawnMapWorker(ProcessInfo *info) {
	/* create pipe to receive data to process */
	int desc2[2];
	pipe(desc2);

	/* create pipe to send temporary file name */
	int desc[2];
	pipe(desc);

	/* set stats */
	info->setBufDesc(desc[0], desc[1]);
	info->setBufDesc2(desc2[0], desc2[1]);
	info->setType(MAP_WORKER);
	current = *info;

	/* child */
	int pid;
	if ((pid = fork()) == 0) {
		close(info->getOutputDesc());
		close(info->getInputDesc2());
		runMap();
	}

	/* error */
	else if (pid < 0) {
		Logger::getInstance()->log("ERROR: creating process");

		return false;
	}

	/* parent */
	else {
		close(info->getInputDesc());
		close(info->getOutputDesc2());
		info->setPid(pid);
		mapStats[pid] = info;
		Logger::getInstance()->log("MapWorker spawned", pid);
	}

	return true;
}

/*
 * Spawns single reduce worker, sets its ProcessInfo entry
 * and closes pipes descriptors. Returns true if everything
 * went right and process is ready to work, otherwise false.
 */
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
	if ((pid = fork()) == 0) {
		close(info->getInputDesc());
		runReduce();
	}

	/* error */
	else if (pid < 0) {
		Logger::getInstance()->log("ERROR: creating process");

		return false;
	}

	/* parent */
	else {
		close(info->getOutputDesc());
		info->setPid(pid);
		reduceStats[pid] = info;
		Logger::getInstance()->log("ReduceWorker spawned", pid);
	}

	return true;
}

/*
 * Kills all active workers
 */
void MapReduce::terminateWorkers() {
	map<int, ProcessInfo *>::iterator it;
	int pid;

	if (!mapStats.size())
		goto reduce;

	/* terminate map tasks */
	Logger::getInstance()->log("Terminating MapWorkers...");
	for (it = mapStats.begin(); it != mapStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("terminating...", pid);
	}

	reduce: if (!reduceStats.size())
		return;

	/* terminate reduce tasks */
	Logger::getInstance()->log("Terminating ReduceWorkers...");
	for (it = reduceStats.begin(); it != reduceStats.end(); ++it) {
		pid = (*it).second->getPid();
		kill(pid, 15);
		removePidEntry(pid);
		Logger::getInstance()->log("terminating...", pid);
	}
}

/*
 * Removes PID entry from mapStats and reduceStats maps.
 */
void MapReduce::removePidEntry(int pid) {
	map<int, ProcessInfo *>::iterator it;

	if ((it = mapStats.find(pid)) != mapStats.end()) {
		close(mapStats[pid]->getOutputDesc());
		delete mapStats[pid];
		mapStats.erase(pid);
	} else if ((it = reduceStats.find(pid)) != reduceStats.end()) {
		close(reduceStats[pid]->getInputDesc());
		delete reduceStats[pid];
		reduceStats.erase(pid);
	}
}

/*
 * Sets if temporary files (product of mapWorkers) should be
 * removed.
 */
void MapReduce::setRemoveTempFiles(bool active) {
	this->removeTempFiles = active;
}
