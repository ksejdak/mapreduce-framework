/*
 *  Filename	: MapWorker.h
 *  Author		: Kuba Sejdak
 *  Created on	: 26-05-2012
 */

#ifndef MAPWORKER_H_
#define MAPWORKER_H_

#include <include/AbstractMapWorker.h>

template <class K, class V>
class MapWorker : public AbstractMapWorker<K, V> {
public:
	MapWorker();
	virtual ~MapWorker();

	virtual void map(K key, V value);
};

/* ====================== DEFINITION ====================== */

template<class K, class V>
MapWorker<K, V>::MapWorker() {
}

template<class K, class V>
MapWorker<K, V>::~MapWorker() {
}

template<class K, class V>
void MapWorker<K, V>::map(K key, V value) {
}

#endif /* MAPWORKER_H_ */
