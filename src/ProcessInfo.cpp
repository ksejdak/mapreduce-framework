/*
 *  Filename	: ProcessInfo.cpp
 *  Author	: Kuba Sejdak
 *  Created on	: 26-05-2012
 */

#include "../include/ProcessInfo.h"

ProcessInfo::ProcessInfo() {
	pid = 0;
	memset(bufDesc, 0, 2);
	type = MAP_WORKER;
}

ProcessInfo::~ProcessInfo() {
}

void ProcessInfo::setPid(int pid) {
	this->pid = pid;
}

void ProcessInfo::setWorkerNo(int number) {
	this->number = number;
}

void ProcessInfo::setBufDesc(int d1, int d2) {
	bufDesc[0] = d1;
	bufDesc[1] = d2;
}

void ProcessInfo::setBufDesc2(int d1, int d2) {
	bufDesc2[0] = d1;
	bufDesc2[1] = d2;
}

void ProcessInfo::setType(ProcessType type) {
	this->type = type;
}

int ProcessInfo::getPid() {
	return pid;
}

int ProcessInfo::getInputDesc() {
	return bufDesc[1];
}

int ProcessInfo::getOutputDesc() {
	return bufDesc[0];
}

int ProcessInfo::getInputDesc2() {
	return bufDesc2[1];
}

int ProcessInfo::getOutputDesc2() {
	return bufDesc2[0];
}

int ProcessInfo::getType() {
	return type;
}

int ProcessInfo::getWorkerNo() {
	return number;
}
