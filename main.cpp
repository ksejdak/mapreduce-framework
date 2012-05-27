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


vector<pair<string, string> > dataReaderFunc() {
	vector<pair<string, string> > exList;
	exList.push_back( make_pair("wiersz.txt", "Litwo") );
	exList.push_back( make_pair("wiersz.txt", "ojczyzno") );
	exList.push_back( make_pair("wiersz.txt", "moja") );
	exList.push_back( make_pair("wiersz.txt", "Litwo") );
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

	MapReduce<string, string, string::iterator> framework(4, 4);
	framework.consoleLogging(true);
	framework.fileLogging(true);
	framework.setDataReader(dataReaderFunc);
	// TODO: implement MapWorker
	// framework.setMap();
	// TODO: implement ReduceWorker
	// framework.setReduce();
	framework.run();
	return 0;
}

