/*
 * ThreadScheduler.cpp
 *
 *  Created on: 03.01.2015
 *      Author: cbinnig
 */

#include <math.h>
#include <sched.h>
#include "../Settings.h"
#include "ThreadScheduler.h"

bool utils::ThreadScheduler::running = 0;
size_t utils::ThreadScheduler::numThreads;
std::thread* utils::ThreadScheduler::threads;

std::queue<utils::Thread*> utils::ThreadScheduler::pending;
std::mutex utils::ThreadScheduler::pending_mutex;
std::condition_variable utils::ThreadScheduler::pending_cv;

void utils::ThreadScheduler::doWork(int tid) {
	int cpu = rdma::Config::NUMA_THREAD_CPUS[rdma::Config::RDMA_NUMAREGION][tid];
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	sched_setaffinity(0, sizeof(cpuset), &cpuset);
	// std::cout << "Created worker thread, pinned to cpu: " + std::to_string(cpu) + '\n';

	// execute functions in pending queue
	while (ThreadScheduler::running) {
		std::unique_lock<std::mutex> lck(pending_mutex);
		while (pending.size() == 0 && ThreadScheduler::running != 0) {
			pending_cv.wait(lck);
		}

		if (!ThreadScheduler::running) {
			lck.unlock();
			return;
		}

		//cout << "ThreadScheduler: Thread " <<tid << " is running!" << endl;
		utils::Thread* t = pending.front();
		pending.pop();
		//cout << "Size of q " << pending.size() << " tid " << tid << endl << flush;
		lck.unlock();

		t->tid = tid;

		//execute thread
		t->run();
		t->setFinished(1);
	}
}

void utils::ThreadScheduler::start(size_t numThreads) {
	utils::ThreadScheduler::running = 1;

	//size_t numThreads = (size_t) ceil(thread::hardware_concurrency() / CLIENTS_SIZE);
	threads = new std::thread[numThreads];

	std::cout << "Starting " << numThreads << " threads in utils::ThreadScheduler!" << "\n";
	for (size_t i = 0; i < numThreads; ++i) {
		threads[i] = std::thread(utils::ThreadScheduler::doWork, i);
	}
}

void utils::ThreadScheduler::push(utils::Thread* t) {
	if (utils::ThreadScheduler::running) {
		std::unique_lock<std::mutex> lck(pending_mutex);
		pending.push(t);
		pending_cv.notify_one();
		lck.unlock();
	}
}

void utils::ThreadScheduler::stop() {
	utils::ThreadScheduler::running = 0;
	pending_cv.notify_all();
	for (size_t i = 0; i < numThreads; ++i) {
		threads[i].join();
	}
}

bool utils::ThreadScheduler::isRunning() {
	return running;
}