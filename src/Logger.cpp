/*
 *  Filename	: Logger.cpp
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#include "Logger.h"

Logger::Logger() {
	filename = "mapreduce_log.txt";
	console = false;
	file = false;
}

Logger::~Logger() {
}

void Logger::setConsole(bool active) {
	this->console = active;
}

void Logger::setFile(bool active) {
	this->file = active;
}

void Logger::setOutputFile(string filename) {
	this->filename = filename;
}

void Logger::log(string message, int pid) {
	message.append("\n");
	if(pid) {
		stringstream ss;
		ss << pid;
		message = "[PID " + ss.str() + "] " + message;
	}

	if(console)
		writeConsole(message);

	if(file)
		writeFile(message);
}

void Logger::writeConsole(string message) {
	clog << message;
}

void Logger::writeFile(string message) {
	fstream file;
	file.open(filename.c_str(), ios::out | ios::app);

	file.write(message.c_str(), message.size());

	file.close();
}
