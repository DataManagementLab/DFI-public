set(RSMNOPAXOS_CPP
  utils/HashTable.h
  utils/Thread.h
  utils/Thread.cc
  utils/ThreadScheduler.h
  utils/ThreadScheduler.cc
  NoPaxosAcceptor.h 
  NoPaxosClient.h 
  KV_SM.h
  Settings.h
  Settings.cc
) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(rsmnopaxos_lib ${RSMNOPAXOS_CPP})
target_include_directories(rsmnopaxos_lib PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
set_target_properties(rsmnopaxos_lib PROPERTIES LINKER_LANGUAGE CXX)

add_executable(rsm_nopaxos Main.cc)

target_link_libraries(rsm_nopaxos
  flow_api
  rsmnopaxos_lib
)