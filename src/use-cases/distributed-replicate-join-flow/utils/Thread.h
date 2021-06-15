/*
 * Thread.hpp
 *
 *  Created on: 03.01.2015
 *      Author: cbinnig
 */

#ifndef THREAD_RDMA_HPP_
#define THREAD_RDMA_HPP_

#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>

// #include "../Config.hpp"
namespace utils
{
	
class Thread {
private:
	bool finished = 0;
	std::mutex thread_mutex;
	std::condition_variable thread_cv;

public:
	virtual ~Thread(){}

	void start();

	virtual void run(){};

	void join();

	void setFinished(bool finished);

	int tid;
};
	
}

#endif /* THREAD_RDMA_HPP_ */
