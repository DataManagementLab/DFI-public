/*
 * ThreadScheduler.hpp
 *
 *  Created on: 03.01.2015
 *      Author: cbinnig
 */

#ifndef THREADSCHEDULER_HPP_
#define THREADSCHEDULER_HPP_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <thread>
#include <mutex>
#include <iostream>
#include <queue>
#include <condition_variable>

// #include "../Config.h"
#include "Thread.h"
namespace utils
{
class ThreadScheduler {
private:
	static bool running;
	static std::thread* threads;
	static size_t numThreads;

	static std::queue<Thread*> pending;
	static std::mutex pending_mutex;
	static std::condition_variable pending_cv;

	static void doWork(int tid);

public:
	static void start(size_t numThreads);

	static void push(Thread* t);

	static void stop();

	static bool isRunning();
};
}
#endif /* THREADSCHEDULER_HPP_ */
