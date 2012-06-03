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
	static int a = -1;
	++a;
	if (a==0) return make_pair("wiersz.txt", "Litwo ojczyzno moja");
	if (a==1) return make_pair("wiersz.txt", "Ty jestes moja ojczyzna");
	if (a==2) return make_pair("wiersz.txt", "I o Tobie nie zapomne!");
	if (a==3) return make_pair("wiersz.txt", "Nigdy!");
	if (a==4) return make_pair("wiersz.txta", "Nigdy!");
	if (a==5) return make_pair("wiersz.txts", "Nigdy!");
	if (a==6) return make_pair("wiersz.txtd", "Nigdy!");
	if (a==7) return make_pair("wiersz.txtf", "Nigdy!");
	if (a==8) return make_pair("wiersz.txtg", "Nigdy!");
	if (a==9) return make_pair("wiersz.txtga", "Nigdy!");
	return make_pair("","");
}

void showIntro() {
	cout << "Welcome to MapReduce test program!" << endl;
	cout << "Version: " << VERSION << endl;
	cout << "Authors: " << endl
	     << "\t Kuba Sejdak" << endl
	     << "\t Maciej Dobrowolski" << endl << endl;
}

int main(int argc, char *argv[]) {
	showIntro();

	MapReduce framework(2, 4);
	framework.consoleLogging(true);
	framework.fileLogging(true);
	framework.setDataReader(dataReaderFunc);
	framework.setRemoveTempFiles(true);

	myMapWorker mapWorker;
	myReduceWorker reduceWorker;
	framework.setMap(&mapWorker);
	framework.setReduce(&reduceWorker);
	framework.run();
	return 0;
}

