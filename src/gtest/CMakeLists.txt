find_package(GTest REQUIRED)
enable_testing()

MACRO(SUBDIRLIST result curdir)
FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
SET(dirlist "")
FOREACH(child ${children})
    IF(IS_DIRECTORY ${curdir}/${child})
        LIST(APPEND dirlist ${child})
    ENDIF()
ENDFOREACH()
SET(${result} ${dirlist})
ENDMACRO()

SUBDIRLIST(SUBDIRS ".")

include_directories(${GTEST_INCLUDE_DIRS})
foreach(subdir ${SUBDIRS})
include_directories(${subdir})
endforeach()

file(GLOB test_SRC ./*/*.cc)
message(${test_SRC})

add_executable(gtests ${test_SRC})
# Standard linking to gtest stuff.
target_link_libraries(gtests GTest::GTest  pthread
GTest::Main
flow_api)