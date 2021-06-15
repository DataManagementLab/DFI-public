
set(FLOW_RADIX_JOIN_CPP
  Settings.h
  Settings.cc
  utils/Thread.h
  utils/Thread.cc
  utils/ThreadScheduler.h
  utils/ThreadScheduler.cc
) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(flow_radix_join_use_case ${FLOW_RADIX_JOIN_CPP})
target_include_directories(flow_radix_join_use_case PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

add_executable(distributed_radix_join_flow Main.cc)

target_link_libraries(distributed_radix_join_flow
  flow_api
  flow_radix_join_use_case
)
