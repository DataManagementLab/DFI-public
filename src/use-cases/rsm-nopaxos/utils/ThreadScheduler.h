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
    const static int cpus[]; // = {10,11,12,13,14,15,16,17,18,19,30,31,32,33,34,35,36,37,38,39,0,1,2,3,4,5,6,7,8,9,20,21,22,23,24,25,26,27,28,29};
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
