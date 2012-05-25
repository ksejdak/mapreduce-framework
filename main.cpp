/*
 *  Filename	: main.cpp
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <iostream>
#include <version.h>
using namespace std;

void showIntro();

int main(int argc, char *argv[]) {
	showIntro();

	return 0;
}

void showIntro() {
	cout << "Welcome to MapReduce test program!" << endl;
	cout << "Version: " << VERSION << endl;
	cout << "Authors: " << endl
		 << "\t Kuba Sejdak" << endl
		 << "\t Maciek Dobrowolski" << endl;
}
