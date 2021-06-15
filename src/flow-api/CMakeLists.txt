set(FLOW_API_SRC
  dfi.h
  schema.h
  shuffle_flow_init.h
  shuffle_flow_source.h
  shuffle_flow_target.h
  combiner_flow_init.h
  combiner_flow_source.h
  combiner_flow_target.h
  replicate_flow_init.h
  replicate_flow_source.h
  replicate_flow_target.h
  flow_barrier_init.h
  flow_barrier.h
  dfi_node.h
  registry_server.h
  ) # Adding headers required for portability reasons http://voices.canonical.com/jussi.pakkanen/2013/03/26/a-list-of-common-cmake-antipatterns/
add_library(flow_api ${FLOW_API_SRC})
target_include_directories(flow_api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flow_api
  memory_dfi 
  flow_dfi
)

set_target_properties(flow_api PROPERTIES CUDA_SEPARABLE_COMPILATION ON)
set_target_properties(flow_api PROPERTIES CUDA_RESOLVE_DEVICE_SYMBOLS ON)