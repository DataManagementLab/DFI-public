#pragma once
#include <sched.h>
#include <chrono>

static void set_thread_affinity(int threadid)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(threadid, &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);
}


inline static void busySleep(size_t sleep_us) {
    if (sleep_us == 0)
        return;
    auto start = std::chrono::system_clock::now();
    auto now = start;
    while(std::chrono::duration_cast<std::chrono::microseconds>(now - start).count() < (int64_t)sleep_us)
    {
        now = std::chrono::system_clock::now();
    }
}