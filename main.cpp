/*
 *  Filename	: main.cpp
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <iostream>
#include <string>
#include <version.h>
#include <include/MapReduce.h>
using namespace std;

void showIntro();

int main(int argc, char *argv[]) {
	showIntro();

	MapReduce<string, string, string::iterator> framework(4, 4);
	framework.consoleLogging(true);
	framework.fileLogging(true);
	// TODO: implement MapWorker
	// framework.setMap();
	// TODO: implement ReduceWorker
	// framework.setReduce();
	framework.run("dupa.txt");

	return 0;
}

void showIntro() {
	cout << "Welcome to MapReduce test program!" << endl;
	cout << "Version: " << VERSION << endl;
	cout << "Authors: " << endl
	     << "\t Kuba Sejdak" << endl
	     << "\t Maciek Dobrowolski" << endl << endl;
}
