#pragma once

#include <chrono>


template<typename Function, typename Unit=std::chrono::microseconds>
inline auto time_it(Function function) {
    auto start = chrono::high_resolution_clock::now();
   
    function();

    auto end = chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<Unit>(end - start).count();
}