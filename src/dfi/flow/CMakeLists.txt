set(FLOW_SRC
  FlowHandle.h
  FlowSourceInterface.h
  FlowTarget.h
  FlowTarget.cc
) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(flow_dfi ${FLOW_SRC})
target_include_directories(flow_dfi PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flow_dfi
  memory_dfi 
  registry_dfi
  type_dfi
  dfi_message
)