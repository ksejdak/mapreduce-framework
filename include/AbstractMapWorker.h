/*
 *  Filename	: AbstractMapWorker.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTMAPWORKER_H_
#define ABSTRACTMAPWORKER_H_

#include <unistd.h>
#include <vector>

template <class K, class V>
class AbstractMapWorker {
public:
	AbstractMapWorker() {}
	virtual ~AbstractMapWorker() {}

	virtual vector<pair<K,V> > map(K key, V value) {}

};

#endif /* ABSTRACTMAPWORKER_H_ */
