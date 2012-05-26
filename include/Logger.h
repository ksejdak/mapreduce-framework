/*
 *  Filename	: Logger.h
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef LOGGER_H_
#define LOGGER_H_

#include <string>
#include <fstream>
#include <iostream>
using namespace std;

class Logger {
public:
	static Logger *getInstance() {
		static Logger instance;

		return &instance;
	}
	virtual ~Logger();

	void setActive(bool active);
	void setOutputFile(string filename);
	void writeConsole(string message);
	void writeFile(string message);

private:
	Logger();

	string filename;
	bool active;
};

#endif /* LOGGER_H_ */
