/*
 *  Filename	: Logger.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef LOGGER_H_
#define LOGGER_H_

#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
using namespace std;

class Logger {
public:
	static Logger *getInstance() {
		static Logger instance;

		return &instance;
	}
	virtual ~Logger();

	void setConsole(bool active);
	void setFile(bool active);
	void setOutputFile(string filename);
	void log(string message, int pid = 0);

private:
	Logger();
	void writeConsole(string message);
	void writeFile(string message);

	string filename;
	bool console;
	bool file;
};

#endif /* LOGGER_H_ */
