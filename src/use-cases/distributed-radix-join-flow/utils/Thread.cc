/*
 * Thread.cpp
 *
 *  Created on: 06.01.2015
 *      Author: cbinnig
 */

#include "Thread.h"
#include "ThreadScheduler.h"

void utils::Thread::setFinished(bool finished) {
	std::unique_lock<std::mutex> lck(thread_mutex);
	this->finished = finished;
	thread_cv.notify_all();
	lck.unlock();
}

void utils::Thread::start() {
	this->setFinished(0);
	ThreadScheduler::push(this);
}

void utils::Thread::join() {
	std::unique_lock<std::mutex> lck(thread_mutex);

	if (this->finished){
		lck.unlock();
		return;
	}

	thread_cv.wait(lck);
	lck.unlock();
}