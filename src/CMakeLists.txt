include(GNUInstallDirs)

add_subdirectory(utils)
add_subdirectory(flow-api)
add_subdirectory(dfi)

# RDMA manager
add_subdirectory(rdma-manager)

add_subdirectory(use-cases/distributed-radix-join-flow)
add_subdirectory(use-cases/distributed-replicate-join-flow)

add_subdirectory(use-cases/rsm-paxos)
add_subdirectory(use-cases/rsm-nopaxos)

add_subdirectory(gtest)


include_directories(${ProtobufIncludePath}) #RDMA proto headers
include_directories(${DFIProtobufIncludePath})

# message("rdma proto msg path: " ${ProtobufIncludePath})
# message("DFI proto msg path: " ${DFIProtobufIncludePath})



# Building DFI as a shared library

# the already existing static libraries cannot be reused as compilation flags are
# not the same, e.g. -fPIC (position-independent code)
FILE(GLOB_RECURSE LIB_SOURCES
    "dfi/*.cc"
    "net/*.cc"
    "utils/*.cc"
    "rdma-manager/src/*/*.cc"
)
MESSAGE(LIB_SOURCES)
# By setting the "GENERATED" property, existence and other validity checks at
# configuration time are bypassed. Needed as protobuf sources do not exist yet at this time.
set_source_files_properties(${PROTO_SRC} PROPERTIES GENERATED 1)
list(APPEND LIB_SOURCES ${PROTO_SRC})
set_source_files_properties(${DFI_PROTO_SRC} PROPERTIES GENERATED 1)
list(APPEND LIB_SOURCES ${DFI_PROTO_SRC})
list(REMOVE_DUPLICATES LIB_SOURCES)

add_library(dfi_lib SHARED  ${LIB_SOURCES})

# printing included sources as sanity check
string (REPLACE ";" "\n\t" LIB_SRC_PRINT "${LIB_SOURCES}")
set(LIB_SRC_PRINT "\n\t${LIB_SRC_PRINT}")
# MESSAGE(STATUS "Source files included in shared library: " "${LIB_SRC_PRINT}" )

add_dependencies(dfi_lib dfi_message) # protobuf message files must have been generated
add_dependencies(dfi_lib rdma_lib) # protobuf message files must have been generated

set_target_properties(dfi_lib PROPERTIES
    VERSION 0.1
    SOVERSION 1
    OUTPUT_NAME "dfi"
)

# Include libraries we depend on
find_package(IBVerbs REQUIRED)
find_package(RdmaCm REQUIRED)
find_package(Numa REQUIRED)
find_package(Protobuf REQUIRED)
find_package(ZMQ REQUIRED)
find_package(Threads REQUIRED)

target_link_libraries(dfi_lib
    ${IBVERBS_LIBRARY}
    ${RDMACM_LIBRARY}
    ${NUMA_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${ZMQ_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
)

# Install shared library
install(TARGETS dfi_lib
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Install header files while preserving hierarchy
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/dfi
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/flow-api
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/memory-api
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/utils
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dfi
    FILES_MATCHING PATTERN "*.h*"
    PATTERN "CMakeFiles" EXCLUDE
)
# Generated protobuf headers get a special treatment as they are found at
# the build path
install(
    DIRECTORY ${DFIProtobufIncludePath}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dfi/dfi
    FILES_MATCHING PATTERN "*.h*"
    PATTERN "CMakeFiles" EXCLUDE
)

# RDMA Manager Headers
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/rdma-manager/src
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dfi/rdma-manager
    FILES_MATCHING PATTERN "*.h*"
    PATTERN "CMakeFiles" EXCLUDE
)

# RDMA Manager Protobuf Headers
install(
    DIRECTORY ${ProtobufIncludePath}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/dfi/rdma-manager/src
    FILES_MATCHING PATTERN "*.h*"
    PATTERN "CMakeFiles" EXCLUDE
)