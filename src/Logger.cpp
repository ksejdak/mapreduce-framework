/*
 *  Filename	: Logger.cpp
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include <include/Logger.h>

Logger::Logger() {
	filename = "log.txt";
}

Logger::~Logger() {
}

void Logger::setOutputFile(string filename) {
	this->filename = filename;
}

void Logger::writeConsole(string message) {
	clog << message << endl;
}

void Logger::writeFile(string message) {
	fstream file;
	file.open(filename.c_str(), ios::out | ios::app);

	message += "\n";
	file.write(message.c_str(), message.size());

	file.close();
}
