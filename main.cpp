/*
 *  Filename	: main.cpp
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <vector>
#include <utility>
#include <version.h>

#include <include/MapReduce.h>

using namespace std;

vector<string> inputFileNames;

class myMapWorker : public AbstractMapWorker {
	vector<pair<string,string> > map(string key, string value) {
		vector<pair<string,string> > result;

		istringstream iss(value);
		vector<string> tokens;
		copy(istream_iterator<string>(iss),
		         istream_iterator<string>(),
		         back_inserter<vector<string> >(tokens));

		for(vector<string>::iterator it=tokens.begin(); it!=tokens.end(); ++it)
			result.push_back(make_pair(*it, "1"));

		return result;
	}
};

class myReduceWorker : public AbstractReduceWorker {
	vector<string> reduce(string value, list<string> values) {
		vector<string> result;
		ostringstream ss;
		ss << values.size();
		result.push_back(ss.str());
		return result;
	}
};


pair<string, string> dataReaderFunc() {
	static vector<string>::iterator it = inputFileNames.begin();
	static fstream file;

	/* in this moment it works only if file is not opened */
	if (!file.good()) {
		file.open(it->c_str(), ios::in);
	}

	/* if EOF - change file for another */
	if (file.eof()) {
		file.close();
		++it;
		if (it == inputFileNames.end()) {
			return make_pair("","");
		} else {
			file.open(it->c_str(), ios::in);
		}
	}

	/* if file cannot be opened */
	if (!file.good()) {
		cerr << "Cannot open file: " << *it << endl;
		exit(1);
	}

	string line;
	getline(file, line);

	return make_pair(*it, line);
}

void showIntro() {
	cout << "Welcome to MapReduce test program!" << endl;
	cout << "Version: " << VERSION << endl;
	cout << "Authors: " << endl
	     << "\t Kuba Sejdak" << endl
	     << "\t Maciej Dobrowolski" << endl << endl;
}

void showUsage() {
	cout << "Usage:" << endl;
	cout << "./map-reduce-framework MAP_WORKERS_NUM REDUCE_WORKERS_NUM INPUT_FILE0 [INPUT_FILE1] ..." << endl;
}

int main(int argc, char *argv[]) {
	showIntro();

	if (argc < 4) {
		showUsage();
		exit(1);
	}

	istringstream iss(argv[1]);
	int mapWorkers;
	iss >> mapWorkers;
	iss.clear();

	int reduceWorkers;
	iss.str(argv[2]);
	iss >> reduceWorkers;

	for (int i = 3; i < argc; ++i) {
		inputFileNames.push_back(argv[i]);
	}

	/* init of worker classes */
	myMapWorker mapWorker;
	myReduceWorker reduceWorker;

	MapReduce framework(mapWorkers, reduceWorkers);
	framework.consoleLogging(true);
	framework.fileLogging(true);
	framework.setDataReader(dataReaderFunc);
	framework.setRemoveTempFiles(true);
	framework.setMap(&mapWorker);
	framework.setReduce(&reduceWorker);
	framework.run();
	return 0;
}

