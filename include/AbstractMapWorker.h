/*
 *  Filename	: AbstractMapWorker.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTMAPWORKER_H_
#define ABSTRACTMAPWORKER_H_

#include <unistd.h>
#include <vector>

class AbstractMapWorker {
public:
	AbstractMapWorker() {}
	virtual ~AbstractMapWorker() {}

	virtual vector<pair<string,string> > map(string key, string value) = 0;

};

#endif /* ABSTRACTMAPWORKER_H_ */
