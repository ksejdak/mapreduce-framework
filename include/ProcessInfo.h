/*
 *  Filename	: ProcessInfo.h
 *  Author	: Kuba Sejdak
 *  Created on	: 26-05-2012
 */

#ifndef PROCESSINFO_H_
#define PROCESSINFO_H_

#include <cstring>

enum ProcessType {
	MAP_WORKER = 0,
	REDUCE_WORKER
};

class ProcessInfo {
public:
	ProcessInfo();
	virtual ~ProcessInfo();

	void setPid(int pid);
	void setBufDesc(int d1, int d2);
	void setBufDesc2(int d1, int d2);
	void setType(ProcessType type);
	void setWorkerNo(int number);

	int getPid();
	int getInputDesc();
	int getOutputDesc();
	int getInputDesc2();
	int getOutputDesc2();
	int getType();
	int getWorkerNo();

private:
	int pid;
	int bufDesc[2];
	int bufDesc2[2];
	ProcessType type;

	int number;


};

#endif /* PROCESSINFO_H_ */
