set(UTILS_SRC
  StringHelper.h
  Filehelper.h
  Config.h
  Logging.h
  Config.cc
  Timer.h
  Network.h
  MPMCQueue.h
  PerfEvent.h
  ) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(utils ${UTILS_SRC})
target_include_directories(utils PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
