/*
 *  Filename	: AbstractReduceWorker.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTREDUCEWORKER_H_
#define ABSTRACTREDUCEWORKER_H_

#include <list>

class AbstractReduceWorker {
public:
	AbstractReduceWorker() {}
	virtual ~AbstractReduceWorker() {}

	virtual void reduce(string value, list<string>::iterator it) = 0;
};

#endif /* ABSTRACTREDUCEWORKER_H_ */
