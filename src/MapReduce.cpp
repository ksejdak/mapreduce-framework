/*
 * Filename	: MapReduce.cpp
 * Author	: Kuba Sejdak
 * Created on	: 02-06-2012
 */

#include <include/MapReduce.h>

MapReduce::MapReduce(int mapNum, int reduceNum) {
	mapTasksNum = mapNum;
	reduceTasksNum = reduceNum;
	dataReaderFunc = NULL;
	mapWorker = NULL;
	reduceWorker = NULL;

	mapStats.clear();
	reduceStats.clear();

	/* create tmp directory */
	mkdir("tmp", S_IRWXU);
}

MapReduce::~MapReduce() {
	terminateWorkers();
	rmdir("tmp");
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
		Logger::getInstance()->log("Stored tempfile: " + fileName, it->second->getPid());
		if (fileName.compare("!") != 0) {
			tmpFileNames.push_back(string(readbuffer));
		}
		removePidEntry(it->second->getPid());
	}

	/* getting & sorting keys */
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

	Logger::getInstance()->log("Spawning reduce workers...");
	for(unsigned int i = 0; i < reduceTasksNum; ++i) {
		info = new ProcessInfo;
		info->setWorkerNo(i);
		if(!spawnReduceWorker(info)) {
			terminateWorkers();
			exit(1);
		}
	}

	/* sending data to ReduceWorkers */
	string fileNamesToSend = "";
	for (list<string>::iterator it = tmpFileNames.begin(); it != tmpFileNames.end(); ++it) {
		fileNamesToSend.append(*it);
		fileNamesToSend.append("#");
	}
	for (map<int, ProcessInfo *>::iterator it = reduceStats.begin(); it != reduceStats.end(); ++it) {
		// OUTPUT_FILENAME#INPUT_FILENAME0#INPUT_FILENAME1#.. and so on..
		char num[10];
		sprintf(num, "%d", it->second->getPid());
		string dataToSend = string("ReduceOutput") + num + string(".txt#") + fileNamesToSend;
		const char* data = dataToSend.c_str();
		Logger::getInstance()->log("Sending message: '" + dataToSend + string("' to PID: ") + num);
		write(it->second->getInputDesc(), data, (dataToSend.size()));
	}

	/* wait until all reduce tasks finish */
	for(unsigned int i = 0; i < reduceTasksNum; ++i) {
		pid = wait(&status);
		if(WEXITSTATUS(status) == 0) {
			Logger::getInstance()->log("ReduceWorker finished", pid);
		}
		removePidEntry(pid);
	}

	/* removing temp files */
	if(Logger::getInstance()->ifRemoveTempFiles()) {
		list<string>::iterator it;
		for(it = tmpFileNames.begin(); it != tmpFileNames.end(); ++it) {
			if(remove( it->c_str() ) != 0 )
				Logger::getInstance()->log("Error deleting tempfile: " + *it );
			else
				Logger::getInstance()->log("Deleting tempfile: " + *it );
		}
	}
}

bool MapReduce::compareKeysAssigment(pair<string, int> first, pair<string, int> second) {
	if (first.first.compare(second.first) > 0) { // first string greater than second string
		return false;
	} else {
		return true;
	}
}

bool MapReduce::compareKeys(pair<string, string> first, pair<string, string> second) {
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

	for(int i = current.getStartDataOffset(); i < current.getEndDataOffset(); ++i) {
		pair<string, string> row = data[i];
		rowResult = mapWorker->map(row.first, row.second);

		/* merging results */
		mapResult.insert(mapResult.begin(), rowResult.begin(), rowResult.end());
	}

	char fileName[] = "tmp/mapReduce.XXXXXX";
	int fileDesc;
	FILE* fileHnd;

	if((fileDesc = mkstemp(fileName)) == -1 || (fileHnd = fdopen(fileDesc,
			       	       	       	       	     "wb+")) == NULL) {
		if (fileDesc != -1) {
			unlink(fileName);
			close(fileDesc);
		}
		Logger::getInstance()->log("Cannot create tempfile in mapWorker"
					   ", DATA LOST!",current.getPid());
		write(current.getInputDesc(), "!", (strlen("!")+1)); //error
		exit(1);
	}

	write(current.getInputDesc(), fileName, (strlen(fileName)+1));

	vector<pair<string, string> >::iterator it;
	for(it = mapResult.begin(); it != mapResult.end(); ++it)
		writeTmpFile(fileHnd, *it);

	fclose(fileHnd);

	exit(0);
}

void MapReduce::runReduce() {
	close(current.getInputDesc());

	/* getting parameters from pipe */
	char readbuffer[500];
	memset(readbuffer,0,500);
	read(current.getOutputDesc(), readbuffer, sizeof(readbuffer));

	/* decoding parameters */
	vector<string> parameters;
	char *pch;
	pch = strtok(readbuffer,"#");
	while (pch != NULL) {
		parameters.push_back(pch);
		pch = strtok(NULL, "#");
	}

	/* setting parameters */
	string outputFileName = parameters[0];

	/* removing output filename from parameters */
	parameters.erase(parameters.begin());

	/* building scope of work (set of keys) */
	set<string> myWorkScope;
	list<pair<string, int> >::iterator it;
	for(it = keysAssigment.begin(); it != keysAssigment.end(); ++it) {
		if(it->second == current.getWorkerNo())
			myWorkScope.insert(it->first);
	}

	/* searching within temp files for associated keys */
	list<pair<string, string> > myKeysAndValues;
	vector<string>::iterator it2;
	for(it2 = parameters.begin(); it2 != parameters.end(); ++it2) {
		FILE* tmpFile = fopen(it2->c_str(), "rb");
		string k, v;
		while(readTmpFile(tmpFile, k,v)) {
			if(myWorkScope.find(k) != myWorkScope.end())
				myKeysAndValues.push_back(make_pair(k, v));
		}
		fclose(tmpFile);
	}

	/* sorting and grouping and reducing*/
	list<pair<string, vector<string> > > allResults;
	myKeysAndValues.sort(compareKeys);
	list<pair<string, string> >::iterator it3;
	for(it3 = myKeysAndValues.begin(); it3 != myKeysAndValues.end(); ) {
		list<string> allKeyValues;
		string key = it3->first;

		allKeyValues.push_back(it3->second);

		/* grouping */
		while(++it3 != myKeysAndValues.end() &&
						it3->first.compare(key) == 0)
			allKeyValues.push_back(it3->second);

		/* reducing */
		allResults.push_back(make_pair(key, reduceWorker->reduce(key,
				     	     	     	     allKeyValues)));

	}

	/* writing reduce output to file */
	FILE* output = fopen(outputFileName.c_str(), "w+");
	if(output == NULL) {
		Logger::getInstance()->log("Cannot create file to write output",
					   current.getPid());
	}
	else {
		list<pair<string, vector<string> > >::iterator it4;
		for(it4 = allResults.begin(); it4 != allResults.end(); ++it4) {
			string outputValues = (it4->second)[0];
			for(unsigned int i = 1; i < it4->second.size(); ++i) {
				outputValues.append((it4->second)[i]);
				outputValues.append(", ");
			}

			string tmp = it4->first + " " + outputValues;
			fprintf(output, "%s\n", tmp.c_str());
		}
		fclose(output);
	}

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
		close(info->getOutputDesc());
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
		Logger::getInstance()->log("terminating...", pid);
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
		Logger::getInstance()->log("terminating...", pid);
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
