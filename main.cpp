/*
 *  Filename	: main.cpp
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <iostream>
#include <string>

#include <vector>
#include <utility>

#include <version.h>
#include <include/MapReduce.h>
using namespace std;

class myMapWorker : public AbstractMapWorker {
	vector<pair<string,string> > map(string key, string value) {
		vector<pair<string,string> > result;
		result.push_back(make_pair(key,value));
		std::cout << "myMapWorker called!\n";
		return result;
	}
};


vector<pair<string, string> > dataReaderFunc() {
	vector<pair<string, string> > exList;
	exList.push_back( make_pair("wiersz.txt", "Litwo ojczyzno moja") );
	exList.push_back( make_pair("wiersz.txt", "Ty jestes moja ojczyzna") );
	exList.push_back( make_pair("wiersz.txt", "I o Tobie nie zapomne!") );
	exList.push_back( make_pair("wiersz.txt", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txta", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txts", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txtd", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txtf", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txtg", "Nigdy!") );
	exList.push_back( make_pair("wiersz.txtga", "Nigdy!") );
	return exList;
}

void showIntro() {
	cout << "Welcome to MapReduce test program!" << endl;
	cout << "Version: " << VERSION << endl;
	cout << "Authors: " << endl
	     << "\t Kuba Sejdak" << endl
	     << "\t Maciek Dobrowolski" << endl << endl;
}

int main(int argc, char *argv[]) {
	showIntro();

	MapReduce framework(4, 4);
	framework.consoleLogging(true);
	framework.fileLogging(true);
	framework.setDataReader(dataReaderFunc);

	myMapWorker mapWorker;
	framework.setMap(&mapWorker);
	// TODO: implement MapWorker
	// framework.setMap();
	// TODO: implement ReduceWorker
	// framework.setReduce();
	framework.run();
	return 0;
}

