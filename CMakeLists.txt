cmake_minimum_required(VERSION 3.9)

project(dfi)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_INSTALL_PREFIX "/usr/local/" CACHE PATH
"Install path prefix, prepended onto install directories." FORCE)


# for GDB debugging
# set(CMAKE_BUILD_TYPE RelWithDebInfo)

# Compiler Options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLINUX -DHUGEPAGE -std=c++17 -pthread -Wall -Wextra -mavx -mavx2 -mavx512f -mavx512dq -mavx512cd -mavx512bw -fmax-errors=1")#  -Werror")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG") #-fsanitize=address
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -flto")
endif()

if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g")
endif()


# the compiler flags for compiling C++ sources
MESSAGE( STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS} )

add_subdirectory(src)

file(COPY
    ${CMAKE_CURRENT_SOURCE_DIR}/src/conf/DFI.conf
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/bin/conf/
)
