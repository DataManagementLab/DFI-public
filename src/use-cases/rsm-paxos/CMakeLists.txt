set(RSMPAXOS_CPP
  PaxosLeader.h
  PaxosAcceptor.h
  utils/Thread.h
  utils/Thread.cc
  utils/ThreadScheduler.h
  utils/ThreadScheduler.cc
  Settings.h
  Settings.cc
) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(rsmpaxos_lib ${RSMPAXOS_CPP})
target_include_directories(rsmpaxos_lib PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
set_target_properties(rsmpaxos_lib PROPERTIES LINKER_LANGUAGE CXX)

add_executable(rsm_paxos Main.cc)

target_link_libraries(rsm_paxos
  flow_api
  rsmpaxos_lib
)