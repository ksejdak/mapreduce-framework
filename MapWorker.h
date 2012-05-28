/*
 *  Filename	: MapWorker.h
 *  Author		: Kuba Sejdak
 *  Created on	: 26-05-2012
 */

#ifndef MAPWORKER_H_
#define MAPWORKER_H_

#include <include/AbstractMapWorker.h>

class MapWorker : public AbstractMapWorker {
public:
	MapWorker();
	virtual ~MapWorker();

	virtual void map(string key, string value);
};

/* ====================== DEFINITION ====================== */

MapWorker::MapWorker() {
}

MapWorker::~MapWorker() {
}

void MapWorker::map(K key, V value) {
}

#endif /* MAPWORKER_H_ */
