/*
 *  Filename	: Logger.cpp
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <include/Logger.h>

Logger::Logger() {
	filename = "log.txt";
	active = false;
}

Logger::~Logger() {
}

void Logger::setActive(bool active) {
	this->active = active;
}

void Logger::setOutputFile(string filename) {
	this->filename = filename;
}

void Logger::writeConsole(string message) {
	if(!active)
		return;

	clog << message << endl;
}

void Logger::writeFile(string message) {
	if(!active)
		return;

	fstream file;
	file.open(filename.c_str(), ios::out | ios::app);

	message += "\n";
	file.write(message.c_str(), message.size());

	file.close();
}
