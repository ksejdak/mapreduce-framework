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

	virtual vector<string> reduce(string value, list<string> values) = 0;
};

#endif /* ABSTRACTREDUCEWORKER_H_ */
