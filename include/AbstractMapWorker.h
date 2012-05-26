/*
 *  Filename	: AbstractMapWorker.h
 *  Author	: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTMAPWORKER_H_
#define ABSTRACTMAPWORKER_H_

template <class K, class V>
class AbstractMapWorker {
public:
	AbstractMapWorker() {}
	virtual ~AbstractMapWorker() {}

	virtual void map(K key, V value) {}
};

#endif /* ABSTRACTMAPWORKER_H_ */
