/*
 *  Filename	: AbstractMapWorker.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTMAPWORKER_H_
#define ABSTRACTMAPWORKER_H_

#include <unistd.h>

template <class K, class V>
class AbstractMapWorker {
public:
	AbstractMapWorker() {}
	virtual ~AbstractMapWorker() {}

	virtual void map(K key, V value) {}
	virtual int32_t parition(int mapNum, string filename) { return 5; }
};

#endif /* ABSTRACTMAPWORKER_H_ */
