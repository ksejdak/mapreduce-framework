/*
 *  Filename	: AbstractReduceWorker.h
 *  Author		: Kuba Sejdak
 *  Created on	: 25-05-2012
 */

#ifndef ABSTRACTREDUCEWORKER_H_
#define ABSTRACTREDUCEWORKER_H_

template <class V, class I>
class AbstractReduceWorker {
public:
	AbstractReduceWorker() {}
	virtual ~AbstractReduceWorker() {}

	virtual void reduce(V value, I it) {}
};

#endif /* ABSTRACTREDUCEWORKER_H_ */
